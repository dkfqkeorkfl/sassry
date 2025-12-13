use std::collections::HashMap;

use std::sync::Arc;

use super::super::exchange::*;
use super::super::webserver::websocket::*;
use cassry::{secrecy::ExposeSecret, tokio::sync::RwLock, *};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::future;
use rust_decimal::Decimal;
use serde_json::*;

#[derive(Default, Debug)]
pub struct RestAPI;

impl RestAPI {
    pub fn orderstate_tostring(s: &OrderState) -> &'static str {
        match s {
            OrderState::Ordering(_) => "Created",
            OrderState::Opened => "New",
            OrderState::Canceling(_) => "PendingCancel",
            OrderState::PartiallyFilled => "PartiallyFilled",
            OrderState::Filled => "Filled",
            OrderState::Rejected => "Rejected",
            OrderState::Cancelled => "Cancelled",
        }
    }
    pub fn parse_order(
        ptime: PacketTime,
        json: &mut serde_json::Value,
    ) -> anyhow::Result<OrderPtr> {
        let symbol = json["symbol"].as_str().ok_or(anyhowln!("invalid symbol"))?;
        let price = to_decimal_with_json(&json["price"])?;
        let amount = to_decimal_with_json(&json["qty"])?;
        let created = json["createdTime"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap();
        let updated = json["updatedTime"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap();

        let aproc = to_decimal_with_json(&json["cumExecQty"]).unwrap_or(Decimal::ZERO);
        let (avg, proceed) = if aproc != Decimal::ZERO {
            let vproc = to_decimal_with_json(&json["cumExecValue"])?;
            if symbol.ends_with("USD") {
                (aproc / vproc, CurrencyPair::new_quote(aproc))
            } else {
                (vproc / aproc, CurrencyPair::new_base(aproc))
            }
        } else {
            (Decimal::ZERO, CurrencyPair::default())
        };

        let state = match json["orderStatus"].as_str().unwrap() {
            "Created" => OrderState::Ordering(Utc::now()),
            "New" => OrderState::Opened,
            "PendingCancel" => OrderState::Canceling(Utc::now()),
            "PartiallyFilled" => OrderState::PartiallyFilled,
            "Filled" => OrderState::Filled,
            "Cancelled" => OrderState::Cancelled,
            _ => OrderState::Rejected,
        };

        let oid = json["orderId"]
            .as_str()
            .ok_or(anyhowln!("oid is invalid"))?;
        let cid = json["orderLinkId"]
            .as_str()
            .ok_or(anyhowln!("cid is invalid"))?;
        let side = json["side"]
            .as_str()
            .and_then(|s| match s {
                "Buy" => Some(OrderSide::Buy),
                "Sell" => Some(OrderSide::Sell),
                _ => None,
            })
            .ok_or(anyhowln!("side is invalid"))?;

        let order = Order {
            ptime: ptime,
            oid: oid.to_string(),
            cid: cid.to_string(),

            kind: OrderKind::Limit,
            price: price,
            amount: amount,
            side: side,

            fee: CurrencyPair::default(),
            state: state,
            avg: avg,
            proceed,
            is_postonly: json["timeInForce"].as_bool().unwrap_or(false),
            is_reduce: json["reduceOnly"].as_bool().unwrap_or(false),
            created: Utc.timestamp_millis_opt(created).unwrap(),
            updated: Utc.timestamp_millis_opt(updated).unwrap(),

            detail: json.take(),
        };

        Ok(Arc::new(order))
    }
}

#[async_trait]
impl exchange::RestApiTrait for RestAPI {
    fn support_amend(&self, _context: &ExchangeContextPtr) -> bool {
        true
    }

    async fn request_order_amend(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        params: &[(OrderPtr, AmendParam)],
    ) -> anyhow::Result<OrderResult> {
        let category = match &market.market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let bodies = future::join_all(params.iter().map(|(order, param)| {
            let mut body = json!({
                "category" : category,
                "symbol" : market.market_id.symbol(),
                "orderId" : order.oid.clone(),
            });

            match *param {
                AmendParam::P(num) => {
                    body["price"] = num.to_string().into();
                }
                AmendParam::A(num) => {
                    body["qty"] = num.to_string().into();
                }
                AmendParam::PA((price, amount)) => {
                    body["price"] = price.to_string().into();
                    body["qty"] = amount.to_string().into();
                }
            }
            let readable = &(*self);
            async move {
                readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::POST,
                            "/v5/order/amend",
                            body,
                        ),
                    )
                    .await
            }
        }))
        .await;

        let mut ret = OrderResult::new(
            util::get_epoch_first().into(),
            MarketVal::Pointer(market.clone()),
        );
        bodies.into_iter().enumerate().for_each(|(i, result)| {
            let (order, _param) = &params[i];
            match result {
                Ok((mut value, ptime)) => {
                    let mut newer = order.as_ref().clone();
                    newer.ptime = ptime;
                    newer.detail = value["result"].take();
                    ret.success.insert_raw(newer);
                }
                Err(e) => {
                    ret.errors.insert(order.oid.clone(), e);
                }
            }
        });
        Ok(ret)
    }

    async fn request_order_cancel(
        &self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> anyhow::Result<OrderResult> {
        let market_id = params
            .get_market_id()
            .ok_or(anyhowln!("occur error in bybit::request_order_query"))?;
        let category = match market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let bodies = future::join_all(params.get_datas().keys().map(|oid| {
            let body = json!({
                "category" : category,
                "symbol": params.get_market().symbol(),
                "order_id": oid,
            });

            let cloend_oid = oid.clone();
            let readable = &(*self);
            async move {
                let result = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::POST,
                            "/v5/order/cancel",
                            body,
                        ),
                    )
                    .await;
                (cloend_oid, result)
            }
        }))
        .await;

        let mut ret = OrderResult::new(util::get_epoch_first().into(), params.get_market().clone());
        for (oid, result) in bodies {
            let matched = result.and_then(|(v, t)| {
                let order = params
                    .get_datas()
                    .get(&oid)
                    .ok_or(anyhowln!("Data for the canceled order cannot be found."))?;
                Ok((order, v, t))
            });

            match matched {
                Ok((origin, value, ptime)) => {
                    let mut order = origin.as_ref().clone();
                    order.ptime = ptime;
                    order.state = OrderState::Canceling(Utc::now());
                    order.detail = value;

                    ret.success.insert_raw(order);
                }
                Err(e) => {
                    ret.errors.insert(oid, e);
                }
            }
        }
        Ok(ret)
    }

    async fn request_order_submit(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> anyhow::Result<OrderResult> {
        let category = match market.market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let bodies = future::join_all(params.iter().map(|param| {
            let mut body = json!({
                "category" : category,
                "orderType":"Limit",
                "symbol":market.market_id.symbol(),
                "price":param.price.to_string(),
                "qty":param.amount.to_string(),
                "timeInForce": if param.is_postonly {
                    "PostOnly"
                } else {
                    "GTC"
                },
            });

            if !param.cid.is_empty() {
                body["clientId"] = serde_json::Value::from(param.cid.clone());
            }

            body["side"] =
                serde_json::Value::from(if param.side.is_buy() { "Buy" } else { "Sell" });
            body["positionIdx"] = serde_json::Value::from(0);

            let readable = &(*self);
            async move {
                readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::POST,
                            "/v5/order/create",
                            body,
                        ),
                    )
                    .await
            }
        }))
        .await;

        let mut ret = OrderResult::new(
            util::get_epoch_first().into(),
            MarketVal::Pointer(market.clone()),
        );
        for (i, result) in bodies.into_iter().enumerate() {
            let param = &params[i];
            match result {
                Ok((value, ptime)) => {
                    let oid = value["result"]["orderId"].as_str().unwrap().to_string();
                    let mut order = Order::from_order_param(param);
                    order.oid = oid;
                    order.state = OrderState::Ordering(Utc::now());
                    order.detail = value;
                    order.ptime = ptime;

                    ret.success.insert_raw(order);
                }
                Err(e) => {
                    ret.errors.insert(i.to_string(), e);
                }
            }
        }
        Ok(ret)
    }

    async fn request_order_query(
        &self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> anyhow::Result<OrderResult> {
        let market_id = params
            .get_market_id()
            .ok_or(anyhowln!("occur error in bybit::request_order_query"))?;
        let category = match market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let bodies = future::join_all(params.get_datas().keys().map(|oid| {
            let body = json!({
                "category" : category,
                "orderId" : oid,
                "limit" : 50,
            });

            let cloned_oid = oid.clone();
            let readable = &(*self);
            async move {
                let res = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::GET,
                            "/v5/order/realtime",
                            body,
                        ),
                    )
                    .await;
                (cloned_oid, res)
            }
        }))
        .await;

        let mut ret = OrderResult::new(util::get_epoch_first().into(), params.get_market().clone());
        for (oid, result) in bodies {
            let matched = result.and_then(|(v, t)| {
                let order = params
                    .get_datas()
                    .get(&oid)
                    .ok_or(anyhowln!("Data for the canceled order cannot be found."))?;
                Ok((order, v, t))
            });

            match matched {
                Ok((_origin, mut value, ptime)) => {
                    for item in value["result"]["list"].as_array_mut().unwrap() {
                        match RestAPI::parse_order(ptime.clone(), item) {
                            Ok(order) => {
                                ret.success.insert(order);
                            }
                            Err(e) => {
                                ret.errors.insert(oid.clone(), e);
                            }
                        };
                    }
                }
                Err(e) => {
                    ret.errors.insert(oid, e);
                }
            }
        }
        Ok(ret)
    }

    async fn request_order_opened(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> anyhow::Result<OrderSet> {
        let category = match &market.market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let body = json!({
            "category" : category,
            "symbol" : market.market_id.symbol(),
            "limit" : 50,
            "openOnly" : 0
        });

        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::GET, "/v5/order/realtime", body),
            )
            .await?;

        let result = pr.0["result"].as_object_mut().unwrap();
        let nxt = result["nextPageCursor"].as_str().unwrap();
        let mut ret = OrderSet::new_with_cursor(
            pr.1,
            MarketVal::Pointer(market.clone()),
            CursorType::PrevNxt(String::default(), nxt.to_string()),
        );

        for item in result["list"].as_array_mut().unwrap() {
            let order = RestAPI::parse_order(ret.get_packet_time().clone(), item)?;
            ret.insert(order);
        }

        Ok(ret)
    }

    async fn request_order_search(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> anyhow::Result<OrderSet> {
        let category = match market.market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let mut body = json!({
            "category" : category,
            "symbol" : market.market_id.symbol(),
            "limit" : 50
        });

        if let Some(s) = &param.state {
            body["orderStatus"] = serde_json::Value::from(RestAPI::orderstate_tostring(s));
        }

        if let Some((nxt, _)) = &param.page {
            body["cursor"] = serde_json::Value::from(nxt.as_str());
        }

        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::GET, "/v5/order/history", body),
            )
            .await?;

        let result = pr.0["result"].as_object_mut().unwrap();
        let nxt = result["nextPageCursor"].as_str().unwrap();
        let mut ret = OrderSet::new_with_cursor(
            pr.1,
            MarketVal::Pointer(market.clone()),
            CursorType::PrevNxt(String::default(), nxt.to_string()),
        );
        for item in result["list"].as_array_mut().unwrap() {
            let order = RestAPI::parse_order(ret.get_packet_time().clone(), item)?;
            ret.insert(order);
        }
        Ok(ret)
    }

    async fn request_orderbook(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        _quantity: SubscribeQuantity,
    ) -> anyhow::Result<OrderBook> {
        let category = match market.market_id {
            MarketID::InverseFuture { .. } | MarketID::InversePerpetual { .. } => Ok("inverse"),
            MarketID::LinearPerpetual { .. } | MarketID::LinearFuture { .. } => Ok("linear"),
            MarketID::Spot(_) => Ok("spot"),
            _ => Err(anyhowln!("occur error in bybit::request_orderbook")),
        }?;

        let body = json!({
            "symbol": market.market_id.symbol(),
            "category": category,
            "limit": 15
        });

        let (mut root, packettime) = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::GET, "/v5/market/orderbook", body),
            )
            .await?;

        let result = root["result"].as_object().unwrap();
        let updated = Utc
            .timestamp_millis_opt(result["ts"].as_i64().unwrap())
            .unwrap();
        let mut orderbook = OrderBook::new(packettime, MarketVal::Pointer(market.clone()), updated);
        let proc = |item: &serde_json::Value| -> anyhow::Result<OrderBookQuotePtr> {
            serde_json::from_value::<Arc<(LazyDecimal, LazyDecimal)>>(item.clone())
                .map_err(anyhow::Error::from)
        };

        if let Some(items) = result["a"].as_array() {
            orderbook.ask = items.iter().map(proc).collect::<anyhow::Result<Vec<_>>>()?;
        }

        if let Some(items) = result["b"].as_array() {
            orderbook.bid = items.iter().map(proc).collect::<anyhow::Result<Vec<_>>>()?;
        }

        orderbook.detail = root.take();
        Ok(orderbook)
    }

    async fn request_position(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> anyhow::Result<HashMap<MarketID, PositionSet>> {
        let category = match market.market_id {
            MarketID::Spot(_) | MarketID::Margin(_) => Ok("spot"),
            MarketID::LinearPerpetual(_) | MarketID::LinearFuture(_) => Ok("linear"),
            MarketID::InversePerpetual(_) | MarketID::InverseFuture(_) => Ok("inverse"),
            _ => Err(anyhowln!("occur error in bybit::request_order_submit")),
        }?;

        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/v5/position/list",
                    json!({
                        "category" : category,
                        "symbol": market.market_id.symbol()
                    }),
                ),
            )
            .await?;

        let mut positions = HashMap::<MarketID, PositionSet>::new();
        let list = pr.0["result"]
            .as_object_mut()
            .and_then(|v| v["list"].as_array_mut())
            .unwrap();
        for obj in list {
            let size = to_decimal_with_json(&obj["size"]).unwrap_or(Decimal::ZERO);
            if size == Decimal::ZERO {
                continue;
            }

            let side = if obj["side"].as_str().unwrap().to_string() == "Buy" {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            let avg = to_decimal_with_json(&obj["avgPrice"])?;
            let unrealised_pnl = to_decimal_with_json(&obj["unrealisedPnl"])?;
            let liquidation = to_decimal_with_json(&obj["liqPrice"]).unwrap_or(Decimal::ZERO);
            let leverage = to_decimal_with_json(&obj["leverage"])?;
            let created = obj["createdTime"].as_str().unwrap().parse::<i64>()?;
            let updated = obj["updatedTime"].as_str().unwrap().parse::<i64>()?;

            let position = Position {
                ptime: pr.1.clone(),
                side: side.clone(),
                avg,
                size: size.abs(),
                unrealised_pnl,
                leverage,
                liquidation,
                updated: Utc.timestamp_millis_opt(updated).unwrap(),
                opened: Utc.timestamp_millis_opt(created).unwrap(),

                detail: obj.take(),
            };

            if let Some(set) = positions.get_mut(&market.market_id) {
                set.insert_raw(position);
            } else {
                let mut set =
                    PositionSet::new(pr.1.clone(), MarketVal::Symbol(market.market_id.clone()));
                set.insert_raw(position);
                positions.insert(market.market_id.clone(), set);
            }
        }

        Ok(positions)
    }

    async fn request_wallet(
        &self,
        context: &ExchangeContextPtr,
        _: &str,
    ) -> anyhow::Result<DataSet<Asset>> {
        let body = json!({
            "accountType" : "UNIFIED",
        });
        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/v5/account/wallet-balance",
                    body,
                ),
            )
            .await?;

        let mut assets = DataSet::<Asset>::new(pr.1, UpdateType::Snapshot);
        let coins = pr.0["result"]["list"]
            .as_array_mut()
            .and_then(|list| {
                list.iter_mut().find(|v| {
                    v["accountType"]
                        .as_str()
                        .filter(|str| *str == "UNIFIED")
                        .is_some()
                })
            })
            .and_then(|v| v["coin"].as_array_mut())
            .ok_or(anyhowln!("occur error in request_wallet(bybit)"))?;
        for obj in coins {
            let total = to_decimal_with_json(&obj["walletBalance"])?;
            if total == Decimal::ZERO {
                continue;
            }

            let currency = obj["coin"].as_str().unwrap().to_string();
            let locked = to_decimal_with_json(&obj["locked"])?;
            let asset = Asset {
                ptime: assets.get_packet_time().clone(),
                updated: assets.get_packet_time().recvtime.clone(),
                currency: currency.clone(),
                free: total - locked,
                lock: locked,
                detail: obj.take(),
            };
            assets.insert_raw(currency, asset);
        }

        Ok(assets)
    }

    async fn sign(
        &self,
        ctx: &ExchangeContextPtr,
        mut param: exchange::RequestParam,
    ) -> anyhow::Result<exchange::RequestParam> {
        let milli = Utc::now().timestamp_millis();

        param.body["api_key"] =
            serde_json::Value::from(ctx.param.key.key.expose_secret().to_string());
        param.body["timestamp"] = serde_json::Value::from(milli);

        let urlcode = json::url_encode(&param.body)?;
        let key = ring::hmac::Key::new(
            ring::hmac::HMAC_SHA256,
            ctx.param.key.secret.expose_secret().as_bytes(),
        );
        let s = hex::encode(ring::hmac::sign(&key, urlcode.as_bytes()));

        param.body["sign"] = serde_json::Value::from(s);
        return Ok(param);
    }

    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: exchange::RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)> {
        let (res, time) = self.req_async(context, param).await?;
        let json = res
            .json::<serde_json::Value>()
            .await
            .map_err(|e| e.into())
            .and_then(|j| {
                let code = j["retCode"].as_i64().unwrap_or(1);
                if code != 0 {
                    Err(anyhowln!("invalid response : {}", j.to_string()))
                } else {
                    Ok(j)
                }
            })?;
        return Ok((json, time));
    }

    async fn request_market(
        &self,
        context: &ExchangeContextPtr,
    ) -> anyhow::Result<DataSet<Market, MarketID>> {
        let bodies = future::join_all(["spot", "linear", "inverse"].iter().map(|category| {
            let body = json!({
                "category" : category
            });

            let readable = self.clone();
            async move {
                readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::GET,
                            "/v5/market/instruments-info",
                            body,
                        ),
                    )
                    .await
            }
        }))
        .await;

        let mut markets = DataSet::<Market, MarketID>::default();
        for result in bodies {
            let (mut res, packet) = result?;
            let list = res["result"]
                .as_object_mut()
                .and_then(|v| v["list"].as_array_mut())
                .ok_or(anyhowln! {
                    "occur error in Bybit::request_market"
                })?;
            for item in list {
                let symbol = item["symbol"].as_str().unwrap().to_string();
                let k = match item["contractType"].as_str().unwrap_or("Spot") {
                    "Spot" => MarketID::Spot(symbol.clone()),
                    "LinearPerpetual" => MarketID::LinearPerpetual(symbol.clone()),
                    "InversePerpetual" => MarketID::InversePerpetual(symbol.clone()),

                    "InverseFutures" => MarketID::InverseFuture(symbol.clone()),
                    _ => MarketID::Derivatives(item["symbol"].as_str().unwrap().to_string()),
                };

                struct ExtDatas {
                    pp: Decimal,
                    plimit: [Decimal; 2],

                    ap: Decimal,
                    alimit: [CurrencyPair; 2],
                }

                let ext = if let MarketID::Spot(_) = &k {
                    let pinfo = item
                        .get_mut("priceFilter")
                        .and_then(|v| v.as_object_mut())
                        .unwrap();
                    let pp = to_decimal_with_json(&pinfo["tickSize"])?;
                    let ainfo = item
                        .get_mut("lotSizeFilter")
                        .and_then(|v| v.as_object_mut())
                        .unwrap();
                    let ap = to_decimal_with_json(&ainfo["basePrecision"])?;
                    let amin = to_decimal_with_json(&ainfo["minOrderQty"])?;
                    let amax = to_decimal_with_json(&ainfo["maxOrderQty"])?;
                    ExtDatas {
                        pp,
                        plimit: [Decimal::MIN, Decimal::MAX],
                        ap,
                        alimit: [CurrencyPair::new_base(amin), CurrencyPair::new_base(amax)],
                    }
                } else {
                    let pinfo = item
                        .get_mut("priceFilter")
                        .and_then(|v| v.as_object_mut())
                        .unwrap();
                    let pp = to_decimal_with_json(&pinfo["tickSize"])?;
                    let pmin = to_decimal_with_json(&pinfo["minPrice"])?;
                    let pmax = to_decimal_with_json(&pinfo["maxPrice"])?;

                    let ainfo = item
                        .get_mut("lotSizeFilter")
                        .and_then(|v| v.as_object_mut())
                        .unwrap();
                    let ap = to_decimal_with_json(&ainfo["qtyStep"])?;
                    let amin = to_decimal_with_json(&ainfo["maxOrderQty"])?;
                    let amax = to_decimal_with_json(&ainfo["minOrderQty"])?;
                    ExtDatas {
                        pp,
                        plimit: [pmin, pmax],
                        ap,
                        alimit: [CurrencyPair::new_base(amin), CurrencyPair::new_base(amax)],
                    }
                };

                let market = Market {
                    ptime: packet.clone(),
                    market_id: k.clone(),
                    state: if item["status"].as_str().unwrap() != "Trading" {
                        MarketState::Disable
                    } else {
                        MarketState::Work
                    },

                    quote_currency: item["quoteCoin"]
                        .as_str()
                        .ok_or(anyhowln!("invalid quoteCoin"))?
                        .to_uppercase(),
                    base_currency: item["baseCoin"]
                        .as_str()
                        .ok_or(anyhowln!("invalid baseCoin"))?
                        .to_uppercase(),
                    contract_size: Decimal::ONE,
                    fee: FeeInfos::default(),
                    amount_limit: ext.alimit,
                    price_limit: ext.plimit,
                    pp_kind: PrecisionKind::Tick(ext.pp),
                    ap_kind: PrecisionKind::Tick(ext.ap),

                    updated: packet.recvtime.clone(),
                    detail: item.take(),
                };

                let ptr = Arc::new(market);
                match &k {
                    MarketID::Spot(_)
                    | MarketID::Margin(_)
                    | MarketID::LinearPerpetual(_)
                    | MarketID::InversePerpetual(_) => {
                        let symbol = format!("{}/{}", ptr.base_currency, ptr.quote_currency);
                        markets.insert(k.from_symbol(symbol), ptr.clone());
                    }
                    _ => {}
                }

                if let MarketID::Spot(_) = &k {
                } else {
                    markets.insert(MarketID::Derivatives(symbol), ptr.clone());
                }
                markets.insert(k, ptr);
            }
        }

        Ok(markets)
    }
}

#[derive(Default)]
pub struct WebsocketItf {
    orderbooks: RwLock<HashMap<MarketID, OrderBook>>,
}

impl WebsocketItf {
    async fn parse_private(
        &self,
        path: &str,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match tp {
            "position" => {
                let mut positions = HashMap::<MarketID, PositionSet>::new();
                for data in root["data"].as_array_mut().unwrap() {
                    let symbol = data["symbol"].as_str().unwrap().to_string();
                    let market_id = MarketID::Derivatives(symbol.clone());
                    let avg = float::to_decimal_with_json(&data["entryPrice"])?;
                    let leverage = float::to_decimal_with_json(&data["leverage"])?;
                    let size = float::to_decimal_with_json(&data["size"])?;
                    let unrealised_pnl = float::to_decimal_with_json(&data["unrealisedPnl"])?;
                    let liquidation =
                        float::to_decimal_with_json(&data["liqPrice"]).unwrap_or(Decimal::ZERO);
                    let opened = data["createdTime"].as_str().unwrap().parse::<i64>()?;
                    let updated = data["updatedTime"].as_str().unwrap().parse::<i64>()?;
                    let updated_datetime = Utc.timestamp_millis_opt(updated).unwrap();

                    let sides = match data["positionIdx"].as_i64().unwrap() {
                        1 => vec![OrderSide::Buy],
                        2 => vec![OrderSide::Sell],
                        _ => match data["side"].as_str().unwrap() {
                            "Buy" => vec![OrderSide::Buy],
                            "Sell" => vec![OrderSide::Sell],
                            _ => vec![OrderSide::Buy, OrderSide::Sell],
                        },
                    };

                    let proto = Position {
                        ptime: PacketTime::new(&updated_datetime),
                        side: OrderSide::Buy,
                        avg: avg,
                        size: size,
                        unrealised_pnl: unrealised_pnl,
                        leverage: leverage,
                        liquidation: liquidation,
                        opened: Utc.timestamp_millis_opt(opened).unwrap(),
                        updated: updated_datetime,
                        detail: std::mem::replace(data, Default::default()),
                    };

                    for side in sides {
                        let mut position = proto.clone();
                        position.side = side;

                        if let Some(set) = positions.get_mut(&market_id) {
                            set.insert_raw(position);
                        } else {
                            let mut set = PositionSet::new(
                                position.get_packet_time().clone(),
                                MarketVal::Symbol(market_id.clone()),
                            );
                            set.insert_raw(position);
                            positions.insert(market_id.clone(), set);
                        }
                    }
                }

                SubscribeResult::Position(positions)
            }
            "wallet" => {
                let time = Utc::now();
                let mut assets = DataSet::<Asset>::new(PacketTime::new(&time), UpdateType::Partial);
                let coins = root["data"]
                    .as_array_mut()
                    .and_then(|list| {
                        list.iter_mut().find(|v| {
                            v["accountType"]
                                .as_str()
                                .filter(|str| *str == "UNIFIED")
                                .is_some()
                        })
                    })
                    .and_then(|v| v["coin"].as_array_mut())
                    .unwrap();
                for data in coins {
                    let coin = data["coin"].as_str().unwrap().to_string();
                    let total = float::to_decimal_with_json(&data["walletBalance"])?;
                    let lock = float::to_decimal_with_json(&data["locked"])?;
                    let asset = Asset {
                        ptime: time.into(),
                        updated: time.clone(),
                        currency: coin.clone(),
                        free: total - lock,
                        lock: lock,
                        detail: std::mem::replace(data, Default::default()),
                    };
                    assets.insert_raw(coin, asset);
                }
                SubscribeResult::Balance(assets)
            }
            "order" => {
                let time = Utc::now();
                let mut ret = HashMap::<MarketID, OrderSet>::default();
                for data in root["data"].as_array_mut().unwrap() {
                    let symbol = data["symbol"].as_str().unwrap().to_string();
                    let market_id = MarketID::Derivatives(symbol);
                    let os = if let Some(os) = ret.get_mut(&market_id) {
                        os
                    } else {
                        let os =
                            OrderSet::new(PacketTime::new(&time), MarketVal::Symbol(market_id.clone()));
                        ret.insert(market_id.clone(), os);
                        ret.get_mut(&market_id).unwrap()
                    };

                    let order = RestAPI::parse_order(time.into(), data)?;
                    os.insert(order);
                }

                SubscribeResult::Order(ret)
            }

            _ => SubscribeResult::None,
        };

        match result {
            SubscribeResult::None => self.parse_public(path, tp, root).await,
            _ => Ok(result),
        }
    }

    async fn parse_public(
        &self,
        path: &str,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        let mut topic = tp.split(".").peekable();
        let result = match *topic.peek().unwrap() {
            "orderbook" => {
                let time = Utc
                    .timestamp_millis_opt(root["ts"].as_i64().unwrap())
                    .unwrap();
                let ptime = PacketTime {
                    sendtime: time,
                    recvtime: Utc::now(),
                };

                let data = &root["data"];
                let symbol = data["s"]
                    .as_str()
                    .map(|str| match path {
                        "spot" => MarketID::Spot(str.to_string()),
                        _ => MarketID::Derivatives(str.to_string()),
                    })
                    .unwrap();

                let mut orderbooks = self.orderbooks.write().await;
                let orderbook = if let Some(orderbook) = orderbooks.get_mut(&symbol) {
                    if root["type"].as_str().unwrap() == "snapshot" {
                        *orderbook = OrderBook::new(ptime, MarketVal::Symbol(symbol), time.clone());
                        orderbook
                    } else {
                        orderbook.ptime = ptime;
                        orderbook.updated = time;
                        orderbook
                    }
                } else {
                    let orderbook =
                        OrderBook::new(ptime, MarketVal::Symbol(symbol.clone()), time.clone());

                    orderbooks.insert(symbol.clone(), orderbook);
                    orderbooks.get_mut(&symbol).unwrap()
                };

                if let Some(items) = data["b"].as_array() {
                    for item in items {
                        orderbook
                            .update_with_jarray(OrderBookSide::Bid, item.clone())
                            .await?;
                    }
                }

                if let Some(items) = data["a"].as_array() {
                    for item in items {
                        orderbook
                            .update_with_jarray(OrderBookSide::Ask, item.clone())
                            .await?;
                    }
                }

                orderbook.detail = root;
                SubscribeResult::Orderbook(orderbook.clone())
            }
            "publicTrade" => {
                let time = Utc
                    .timestamp_millis_opt(root["ts"].as_i64().unwrap())
                    .unwrap();

                let mut ret = HashMap::<MarketID, PublicTradeSet>::default();
                let datas = root["data"]
                    .as_array_mut()
                    .ok_or(anyhowln!("invalid data member"))?;
                for data in datas {
                    let side = data["S"].as_str().unwrap();
                    let price = data["p"].as_str().unwrap();
                    let amount = data["v"].as_str().unwrap();
                    let updated = data["T"].as_i64().unwrap();

                    let trade = PublicTrade {
                        ptime: PacketTime::new(&time),
                        updated: Utc.timestamp_millis_opt(updated).unwrap(),
                        quote: (price.into(), amount.into()),
                        side: if side == "Buy" {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        },
                        detail: std::mem::replace(data, Default::default()),
                    };

                    let symbol = data["s"].as_str().unwrap().to_string();
                    let market_id = MarketID::Derivatives(symbol);
                    if let Some(set) = ret.get_mut(&market_id) {
                        set.insert_raw(trade);
                    } else {
                        let mut set = PublicTradeSet::new(
                            PacketTime::new(&time),
                            MarketVal::Symbol(market_id.clone()),
                            None,
                        );
                        set.insert_raw(trade);
                        ret.insert(market_id, set);
                    }
                }
                SubscribeResult::PublicTrades(ret)
            }
            _ => SubscribeResult::None,
        };
        Ok(result)
    }
}
#[async_trait]
impl websocket::ExchangeSocketTrait for WebsocketItf {
    async fn subscribe(
        &self,
        _ctx: &ExchangeContextPtr,
        client: Websocket,
        request: &Option<(SubscribeType, serde_json::Value)>,
        subscribed: &HashMap<SubscribeType, Vec<serde_json::Value>>,
    ) -> anyhow::Result<()> {
        let s = if let Some((ty, value)) = request {
            HashMap::from([(ty.clone(), vec![value])])
        } else {
            subscribed
                .iter()
                .map(|(ty, v)| (ty.clone(), v.iter().collect::<Vec<_>>()))
                .collect::<HashMap<_, _>>()
        };

        let body = s
            .iter()
            .map(|(ty, v)| {
                v.iter()
                    .map(|value| match ty {
                        SubscribeType::Order => "order".to_string(),
                        SubscribeType::Position => "position".to_string(),
                        SubscribeType::Balance => "wallet".to_string(),
                        SubscribeType::Orderbook => {
                            let quantity: SubscribeQuantity =
                                serde_json::from_str(value["quantity"].as_str().unwrap()).unwrap();
                            let size = match quantity {
                                SubscribeQuantity::Much => 500,
                                SubscribeQuantity::Least(str) | SubscribeQuantity::Fixed(str) => {
                                    let quantity = str.parse::<usize>().unwrap();
                                    let levels = vec![1, 50, 200, 500];
                                    let idx = levels.binary_search(&quantity).unwrap_or_else(|e| {
                                        if e >= levels.len() {
                                            levels.len() - 1
                                        } else {
                                            e
                                        }
                                    });
                                    levels[idx]
                                }
                                _ => 1,
                            };

                            format!("orderbook.{}.{}", size, value["symbol"].as_str().unwrap())
                        }
                        SubscribeType::PublicTrades => {
                            format!("publicTrade.{}", value["symbol"].as_str().unwrap())
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .map(|s| json!(s))
            .collect::<Vec<serde_json::Value>>();

        let client_param = json!({
            "op" : "subscribe",
            "args" : json!(body),
        });

        client.send_text(client_param.to_string()).await?;
        Ok(())
    }

    async fn parse_msg(
        &self,
        ctx: &ExchangeContextPtr,
        socket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match signal {
            Signal::Opened => {
                let cloned_socket = socket.clone();
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                        if cloned_socket.is_connected().await {
                            let str = json!({
                                "op":"ping"
                            })
                            .to_string();
                            if let Err(e) = cloned_socket.send(Message::Text(str)).await {
                                cassry::error!(
                                    "occur error for send ping to bybit : {}",
                                    e.to_string()
                                );
                            }
                        } else {
                            break;
                        }
                    }
                });

                if socket
                    .get_param_as_connect()
                    .map(|param| param.url.path().contains("/private"))
                    .unwrap_or(false)
                {
                    let expires = Utc::now().timestamp_millis() + 5000;
                    let payload = format!("GET/realtime{}", expires);

                    let key = ring::hmac::Key::new(
                        ring::hmac::HMAC_SHA256,
                        ctx.param.key.secret.expose_secret().as_bytes(),
                    );
                    let sign = hex::encode(ring::hmac::sign(&key, payload.as_bytes()));
                    let json = json!({
                        "op":"auth",
                        "args":[ctx.param.key.key.expose_secret().to_string(), expires, sign]
                    });
                    socket.send(Message::Text(json.to_string())).await?;
                    SubscribeResult::None
                } else {
                    SubscribeResult::Authorized(true)
                }
            }

            Signal::Received(data) => match data {
                Message::Text(text) => {
                    let json: serde_json::Value =
                        serde_json::from_str(text.as_str()).unwrap_or_default();
                    if json["op"]
                        .as_str()
                        .map(|str| str == "auth")
                        .unwrap_or(false)
                    {
                        Ok(SubscribeResult::Authorized(
                            json["success"].as_bool().unwrap(),
                        ))
                    } else if let Some(str) = json["topic"].as_str().map(String::from) {
                        let path = socket
                            .get_param_as_connect()
                            .and_then(|param| {
                                let last = param.url.path().rsplit("/").next().and_then(|str| {
                                    if str.is_empty() {
                                        None
                                    } else {
                                        Some(str.to_string())
                                    }
                                });

                                last
                            })
                            .ok_or(anyhowln!("occur error for parsed url in bybit"))?;
                        self.parse_private(&path, &str, json).await
                    } else {
                        Ok(SubscribeResult::None)
                    }?
                }
                _ => SubscribeResult::None,
            },
            _ => SubscribeResult::None,
        };
        Ok(result)
    }

    async fn make_websocket_param(
        &self,
        ctx: &ExchangeContextPtr,
        group: &String,
        _request: &Option<(SubscribeType, serde_json::Value)>,
    ) -> anyhow::Result<ConnectParams> {
        let mut param = ctx.param.websocket.clone();
        param.add_path(group.trim_start_matches("/").split("/").collect::<Vec<_>>())?;
        Ok(param)
    }

    async fn make_group_and_key(&self, param: &SubscribeParam) -> Option<(String, String)> {
        match param.ty {
            SubscribeType::Balance => Some(("/v5/private".to_string(), "asset".to_string())),
            SubscribeType::Order | SubscribeType::Position => {
                let symbol = param.value["market"].as_str()?;
                let key = format!("{}{}", param.ty.clone() as u32, symbol);
                Some(("/v5/private".to_string(), key))
            }
            SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                let symbol = param.value["market"].as_str()?;
                let market_id = serde_json::from_str::<MarketID>(symbol).ok()?;
                let group = match market_id {
                    MarketID::LinearFuture(_) | MarketID::LinearPerpetual(_) => {
                        "/v5/public/linear"
                    }
                    MarketID::InverseFuture(_) | MarketID::InversePerpetual(_) => {
                        "/v5/public/inverse"
                    }
                    MarketID::Spot(_) => "/v5/public/spot",
                    _ => return None,
                };
                let key = format!("{}{}", param.ty.clone() as u32, symbol);
                Some((group.to_string(), key))
            }
        }
    }
}
