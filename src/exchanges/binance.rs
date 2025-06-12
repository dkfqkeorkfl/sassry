use std::collections::HashMap;
use std::sync::Arc;
use std::str::FromStr;
use std::hash::{Hash, Hasher};

use super::super::exchange::*;
use super::super::webserver::websocket::*;
use cassry::{tokio::sync::RwLock, *};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::future;
use hmac::{Hmac, Mac};
use rust_decimal::Decimal;
use serde_json::*;
use sha2::Sha256;

#[derive(Default, Debug)]
pub struct RestAPI;

impl RestAPI {
    pub fn orderstate_tostring(s: &OrderState) -> &'static str {
        match s {
            OrderState::Ordering(_) => "NEW",
            OrderState::Opened => "NEW",
            OrderState::Canceling(_) => "PENDING_CANCEL",
            OrderState::PartiallyFilled => "PARTIALLY_FILLED",
            OrderState::Filled => "FILLED",
            OrderState::Rejected => "REJECTED",
            OrderState::Cancelled => "CANCELED",
        }
    }

    pub fn parse_order(
        ptime: PacketTime,
        json: &mut serde_json::Value,
    ) -> anyhow::Result<OrderPtr> {
        let symbol = json["symbol"].as_str().ok_or(anyhowln!("invalid symbol"))?;
        let price = to_decimal_with_json(&json["price"])?;
        let amount = to_decimal_with_json(&json["origQty"])?;
        let created = json["time"]
            .as_i64()
            .ok_or(anyhowln!("invalid time"))?;

        let side = match json["side"].as_str().ok_or(anyhowln!("invalid side"))? {
            "BUY" => OrderSide::Buy,
            "SELL" => OrderSide::Sell,
            _ => return Err(anyhowln!("invalid side")),
        };

        let kind = match json["type"].as_str().ok_or(anyhowln!("invalid type"))? {
            "LIMIT" => OrderKind::Limit,
            "MARKET" => OrderKind::Market,
            _ => return Err(anyhowln!("invalid type")),
        };

        let state = match json["status"].as_str().ok_or(anyhowln!("invalid status"))? {
            "NEW" => OrderState::Opened,
            "PARTIALLY_FILLED" => OrderState::PartiallyFilled,
            "FILLED" => OrderState::Filled,
            "CANCELED" => OrderState::Cancelled,
            "REJECTED" => OrderState::Rejected,
            "EXPIRED" => OrderState::Cancelled,
            _ => return Err(anyhowln!("invalid status")),
        };

        let order = Order {
            ptime,
            updated: Utc.timestamp_millis_opt(created).unwrap(),
            oid: json["orderId"].as_i64().unwrap_or(0).to_string(),
            cid: json["clientOrderId"].as_str().unwrap_or("").to_string(),
            kind,
            price,
            amount,
            side,
            is_postonly: json["timeInForce"].as_str().unwrap_or("") == "PO",
            is_reduce: false,
            state,
            avg: to_decimal_with_json(&json["avgPrice"])?,
            proceed: CurrencyPair::new(
                CurrencySide::Quote,
                to_decimal_with_json(&json["cummulativeQuoteQty"])?,
            ),
            fee: CurrencyPair::new(
                CurrencySide::Quote,
                to_decimal_with_json(&json["commission"])?,
            ),
            created: Utc.timestamp_millis_opt(created).unwrap(),
            detail: json.take(),
        };

        Ok(Arc::new(order))
    }

    fn to_decimal_with_json(json: &serde_json::Value) -> anyhow::Result<Decimal> {
        match json {
            Value::Number(n) => Ok(Decimal::from_str(&n.to_string())?),
            Value::String(s) => Ok(Decimal::from_str(s)?),
            _ => Ok(Decimal::ZERO),
        }
    }
}

#[async_trait]
impl exchange::RestApiTrait for RestAPI {
    fn support_amend(&self, _context: &ExchangeContextPtr) -> bool {
        false
    }

    async fn request_order_amend(
        &self,
        _context: &ExchangeContextPtr,
        _market: &MarketPtr,
        _params: &[(OrderPtr, AmendParam)],
    ) -> anyhow::Result<OrderResult> {
        Err(anyhowln!("Binance does not support order amendment"))
    }

    async fn request_order_cancel(
        &self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> anyhow::Result<OrderResult> {
        let mut ret = OrderResult::default();
        let mut futures = Vec::new();

        for (_, order) in params.get_datas() {
            let context = context.clone();
            let order = order.clone();
            let future = async move {
                let mut param = exchange::RequestParam::default();
                param.method = "DELETE".into();
                param.path = format!("/api/v3/order");
                param.query = vec![
                    ("symbol".into(), order.get_symbol().into()),
                    ("orderId".into(), order.oid.clone()),
                ];

                let (res, time) = self.request(&context, param).await?;
                let mut order = RestAPI::parse_order(time, &mut res.clone())?;
                order.state = OrderState::Canceling(order.created);
                Ok::<_, anyhow::Error>(order)
            };
            futures.push(future);
        }

        let results = future::join_all(futures).await;
        for result in results {
            match result {
                Ok(order) => ret.success.insert(order),
                Err(e) => {
                    cassry::error!("Failed to cancel order: {}", e);
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
        let mut ret = OrderResult::default();
        let mut futures = Vec::new();

        for param in params {
            let context = context.clone();
            let market = market.clone();
            let param = param.clone();
            let future = async move {
                let mut req_param = exchange::RequestParam::default();
                req_param.method = "POST".into();
                req_param.path = "/api/v3/order".into();
                req_param.query = vec![
                    ("symbol".into(), market.kind.symbol().into()),
                    ("side".into(), param.side.to_string().into()),
                    ("type".into(), param.kind.to_string().into()),
                    ("quantity".into(), param.amount.to_string().into()),
                ];

                if param.kind == OrderKind::Limit {
                    req_param.query.push(("price".into(), param.price.to_string().into()));
                    if param.is_postonly {
                        req_param.query.push(("timeInForce".into(), "GTX".into()));
                    }
                }

                if !param.cid.is_empty() {
                    req_param.query.push(("newClientOrderId".into(), param.cid.into()));
                }

                let (res, time) = self.request(&context, req_param).await?;
                let order = RestAPI::parse_order(time, &mut res.clone())?;
                Ok::<_, anyhow::Error>(order)
            };
            futures.push(future);
        }

        let results = future::join_all(futures).await;
        for result in results {
            match result {
                Ok(order) => ret.success.insert(order),
                Err(e) => {
                    cassry::error!("Failed to submit order: {}", e);
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
        let mut ret = OrderResult::default();
        let mut futures = Vec::new();

        for (_, order) in params.get_datas() {
            let context = context.clone();
            let order = order.clone();
            let future = async move {
                let mut param = exchange::RequestParam::default();
                param.method = "GET".into();
                param.path = "/api/v3/order".into();
                param.query = vec![
                    ("symbol".into(), order.get_symbol().into()),
                    ("orderId".into(), order.oid.clone()),
                ];

                let (res, time) = self.request(&context, param).await?;
                let order = RestAPI::parse_order(time, &mut res.clone())?;
                Ok::<_, anyhow::Error>(order)
            };
            futures.push(future);
        }

        let results = future::join_all(futures).await;
        for result in results {
            match result {
                Ok(order) => ret.success.insert(order),
                Err(e) => {
                    cassry::error!("Failed to query order: {}", e);
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
        let mut param = exchange::RequestParam::default();
        param.method = "GET".into();
        param.path = "/api/v3/openOrders".into();
        param.query = vec![("symbol".into(), market.kind.symbol().into())];

        let (res, time) = self.request(context, param).await?;
        let mut orders = OrderSet::new(time, MarketVal::Pointer(market.clone()));

        for order_json in res.as_array().ok_or(anyhowln!("invalid response"))? {
            let mut order_json = order_json.clone();
            let order = RestAPI::parse_order(time.clone(), &mut order_json)?;
            orders.insert(order);
        }

        Ok(orders)
    }

    async fn request_order_search(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> anyhow::Result<OrderSet> {
        let mut req_param = exchange::RequestParam::default();
        req_param.method = "GET".into();
        req_param.path = "/api/v3/allOrders".into();
        req_param.query = vec![("symbol".into(), market.kind.symbol().into())];

        if let Some(start_time) = param.start_time {
            req_param.query.push(("startTime".into(), start_time.timestamp_millis().to_string()));
        }
        if let Some(end_time) = param.end_time {
            req_param.query.push(("endTime".into(), end_time.timestamp_millis().to_string()));
        }
        if let Some(limit) = param.limit {
            req_param.query.push(("limit".into(), limit.to_string()));
        }

        let (res, time) = self.request(context, req_param).await?;
        let mut orders = OrderSet::new(time, MarketVal::Pointer(market.clone()));

        for order_json in res.as_array().ok_or(anyhowln!("invalid response"))? {
            let mut order_json = order_json.clone();
            let order = RestAPI::parse_order(time.clone(), &mut order_json)?;
            orders.insert(order);
        }

        Ok(orders)
    }

    async fn request_orderbook(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        _quantity: SubscribeQuantity,
    ) -> anyhow::Result<OrderBook> {
        let mut param = exchange::RequestParam::default();
        param.method = "GET".into();
        param.path = "/api/v3/depth".into();
        param.query = vec![
            ("symbol".into(), market.kind.symbol().into()),
            ("limit".into(), "100".into()),
        ];

        let (res, time) = self.request(context, param).await?;
        let mut orderbook = OrderBook {
            ptime: time,
            updated: Utc::now(),
            market: MarketVal::Pointer(market.clone()),
            ask: Vec::new(),
            bid: Vec::new(),
            detail: Value::Null,
        };

        let bids = res["bids"].as_array().ok_or(anyhowln!("invalid bids"))?;
        let asks = res["asks"].as_array().ok_or(anyhowln!("invalid asks"))?;

        for bid in bids {
            let price = RestAPI::to_decimal_with_json(&bid[0])?;
            let amount = RestAPI::to_decimal_with_json(&bid[1])?;
            orderbook.bid.push(Arc::new((price.into(), amount.into())));
        }

        for ask in asks {
            let price = RestAPI::to_decimal_with_json(&ask[0])?;
            let amount = RestAPI::to_decimal_with_json(&ask[1])?;
            orderbook.ask.push(Arc::new((price.into(), amount.into())));
        }

        Ok(orderbook)
    }

    async fn request_position(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> anyhow::Result<HashMap<MarketKind, PositionSet>> {
        let mut param = exchange::RequestParam::default();
        param.method = "GET".into();
        param.path = "/api/v3/account".into();

        let (res, time) = self.request(context, param).await?;
        let mut positions = HashMap::new();
        let mut position_set = PositionSet::new(time, MarketVal::Pointer(market.clone()));

        for balance in res["balances"].as_array().ok_or(anyhowln!("invalid balances"))? {
            let asset = balance["asset"].as_str().ok_or(anyhowln!("invalid asset"))?;
            let free = RestAPI::to_decimal_with_json(&balance["free"])?;
            let locked = RestAPI::to_decimal_with_json(&balance["locked"])?;

            if free > Decimal::ZERO || locked > Decimal::ZERO {
                let position = Position {
                    ptime: time.clone(),
                    updated: Utc::now(),
                    side: OrderSide::Buy,
                    avg: Decimal::ONE,
                    size: free + locked,
                    unrealised_pnl: Decimal::ZERO,
                    leverage: Decimal::ONE,
                    liquidation: Decimal::ZERO,
                    opened: Utc::now(),
                    detail: balance.clone(),
                };
                position_set.insert(Arc::new(position));
            }
        }

        positions.insert(market.kind.clone(), position_set);
        Ok(positions)
    }

    async fn request_wallet(
        &self,
        context: &ExchangeContextPtr,
        _: &str,
    ) -> anyhow::Result<DataSet<Asset, String>> {
        let mut param = exchange::RequestParam::default();
        param.method = "GET".into();
        param.path = "/api/v3/account".into();

        let (res, time) = self.request(context, param).await?;
        let mut assets = DataSet::<Asset, String>::default();

        for balance in res["balances"].as_array().ok_or(anyhowln!("invalid balances"))? {
            let asset = balance["asset"].as_str().ok_or(anyhowln!("invalid asset"))?;
            let free = RestAPI::to_decimal_with_json(&balance["free"])?;
            let locked = RestAPI::to_decimal_with_json(&balance["locked"])?;

            if free > Decimal::ZERO || locked > Decimal::ZERO {
                let asset = Asset {
                    ptime: time.clone(),
                    updated: Utc::now(),
                    currency: asset.to_string(),
                    lock: locked,
                    free,
                    detail: balance.clone(),
                };
                assets.insert(asset.currency.clone(), Arc::new(asset));
            }
        }

        Ok(assets)
    }

    async fn sign(
        &self,
        ctx: &ExchangeContextPtr,
        mut param: exchange::RequestParam,
    ) -> anyhow::Result<exchange::RequestParam> {
        let timestamp = Utc::now().timestamp_millis().to_string();
        param.query.push(("timestamp".into(), timestamp));

        let query_string = param
            .query
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<_>>()
            .join("&");

        let mut mac = Hmac::<Sha256>::new_from_slice(ctx.param.key.secret.as_bytes())
            .map_err(|_| anyhowln!("invalid key length"))?;
        mac.update(query_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        param.query.push(("signature".into(), signature));
        Ok(param)
    }

    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: exchange::RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)> {
        let mut time = PacketTime::default();
        time.sendtime = Utc::now();

        let mut param = if !context.param.key.key.is_empty() {
            self.sign(context, param).await?
        } else {
            param
        };

        let url = format!("{}{}", context.param.restapi.url, param.path);
        let mut builder = context.requester.request(
            reqwest::Method::from_bytes(param.method.as_bytes())?,
            &url,
        );

        for (key, value) in &param.query {
            builder = builder.query(&[(key, value)]);
        }

        if !context.param.key.key.is_empty() {
            builder = builder.header("X-MBX-APIKEY", &context.param.key.key);
        }

        let res = builder.send().await?;
        time.recvtime = Utc::now();

        if context.param.config.eject < time.laytency() {
            return Err(anyhowln!(
                "occur eject {}({})",
                url,
                time.laytency().to_string()
            ));
        }

        let json = res.json::<serde_json::Value>().await?;
        Ok((json, time))
    }

    async fn request_market(
        &self,
        context: &ExchangeContextPtr,
    ) -> anyhow::Result<DataSet<Market, MarketKind>> {
        let mut param = exchange::RequestParam::default();
        param.method = "GET".into();
        param.path = "/api/v3/exchangeInfo".into();

        let (res, time) = self.request(context, param).await?;
        let mut markets = DataSet::<Market, MarketKind>::default();

        for symbol in res["symbols"].as_array().ok_or(anyhowln!("invalid symbols"))? {
            let status = symbol["status"].as_str().ok_or(anyhowln!("invalid status"))?;
            if status != "TRADING" {
                continue;
            }

            let symbol_str = symbol["symbol"].as_str().ok_or(anyhowln!("invalid symbol"))?;
            let base_asset = symbol["baseAsset"].as_str().ok_or(anyhowln!("invalid baseAsset"))?;
            let quote_asset = symbol["quoteAsset"].as_str().ok_or(anyhowln!("invalid quoteAsset"))?;

            let market_kind = MarketKind::Spot(symbol_str.to_string());
            let market = Market {
                ptime: time.clone(),
                updated: Utc::now(),
                kind: market_kind.clone(),
                state: MarketState::Work,
                quote_currency: quote_asset.to_string(),
                base_currency: base_asset.to_string(),
                contract_size: Decimal::ONE,
                fee: FeeInfos::default(),
                amount_limit: [
                    CurrencyPair::new_base(Decimal::ZERO),
                    CurrencyPair::new_base(Decimal::MAX),
                ],
                price_limit: [Decimal::ZERO, Decimal::MAX],
                pp_kind: PrecisionKind::Tick(Decimal::from_str("0.00000001")?),
                ap_kind: PrecisionKind::Tick(Decimal::from_str("0.00000001")?),
                detail: symbol.clone(),
            };

            markets.insert(market_kind, Arc::new(market));
        }

        Ok(markets)
    }
}

#[derive(Default, Debug)]
pub struct WebsocketItf {
    orderbooks: RwLock<HashMap<MarketKind, OrderBook>>,
}

impl WebsocketItf {
    async fn parse_private(
        &self,
        _path: &str,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        match tp {
            "outboundAccountPosition" => {
                let mut assets = DataSet::<Asset, String>::default();
                let time = PacketTime::default();

                for balance in root["B"].as_array().ok_or(anyhowln!("invalid balances"))? {
                    let currency = balance["a"].as_str().ok_or(anyhowln!("invalid asset"))?.to_string();
                    let asset = Asset {
                        ptime: time.clone(),
                        updated: Utc::now(),
                        currency: currency.clone(),
                        free: RestAPI::to_decimal_with_json(&balance["f"])?,
                        lock: RestAPI::to_decimal_with_json(&balance["l"])?,
                        detail: balance.clone(),
                    };
                    assets.insert(currency, Arc::new(asset));
                }

                Ok(SubscribeResult::Balance(assets))
            }
            "executionReport" => {
                let time = PacketTime::default();
                let order = RestAPI::parse_order(time, &mut root)?;
                let symbol = root["s"].as_str().ok_or(anyhowln!("invalid symbol"))?;
                let market_kind = MarketKind::Spot(symbol.to_string());
                let mut orders = OrderSet::new(time, MarketVal::Symbol(market_kind));
                orders.insert(order);
                Ok(SubscribeResult::Order(HashMap::from([(
                    market_kind,
                    orders,
                )])))
            }
            _ => Err(anyhowln!("unknown type: {}", tp)),
        }
    }

    async fn parse_public(
        &self,
        _path: &str,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        match tp {
            "depthUpdate" => {
                let symbol = root["s"].as_str().ok_or(anyhowln!("invalid symbol"))?;
                let market_kind = MarketKind::Spot(symbol.to_string());
                let time = PacketTime::default();

                let mut orderbook = OrderBook {
                    ptime: time.clone(),
                    updated: Utc::now(),
                    market: MarketVal::Symbol(market_kind),
                    ask: Vec::new(),
                    bid: Vec::new(),
                    detail: root.clone(),
                };

                for bid in root["b"].as_array().ok_or(anyhowln!("invalid bids"))? {
                    let price = RestAPI::to_decimal_with_json(&bid[0])?;
                    let amount = RestAPI::to_decimal_with_json(&bid[1])?;
                    if amount > Decimal::ZERO {
                        orderbook.bid.push(Arc::new((
                            LazyDecimal::from(price.to_string()),
                            LazyDecimal::from(amount.to_string()),
                        )));
                    }
                }

                for ask in root["a"].as_array().ok_or(anyhowln!("invalid asks"))? {
                    let price = RestAPI::to_decimal_with_json(&ask[0])?;
                    let amount = RestAPI::to_decimal_with_json(&ask[1])?;
                    if amount > Decimal::ZERO {
                        orderbook.ask.push(Arc::new((
                            LazyDecimal::from(price.to_string()),
                            LazyDecimal::from(amount.to_string()),
                        )));
                    }
                }

                Ok(SubscribeResult::Orderbook(orderbook))
            }
            _ => Err(anyhowln!("unknown type: {}", tp)),
        }
    }
}

#[async_trait]
impl websocket::ExchangeSocketTrait for WebsocketItf {
    async fn subscribe(
        &self,
        _ctx: &ExchangeContextPtr,
        client: Websocket,
        s: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<()> {
        let mut streams = Vec::new();

        for (stype, params) in s {
            for param in params {
                let stream = match stype {
                    SubscribeType::Orderbook => {
                        format!("{}@depth@100ms", param.0["market"].as_str().unwrap())
                    }
                    SubscribeType::Balance => "!userData".to_string(),
                    SubscribeType::Order => "!userData".to_string(),
                    _ => continue,
                };
                streams.push(stream);
            }
        }

        if !streams.is_empty() {
            let subscribe_msg = json!({
                "method": "SUBSCRIBE",
                "params": streams,
                "id": 1
            });

            client.send_text(serde_json::to_string(&subscribe_msg)?).await?;
        }

        Ok(())
    }

    async fn parse_msg(
        &self,
        _ctx: &ExchangeContextPtr,
        _socket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        match signal {
            Signal::Recived(msg) => {
                if let Message::Text(txt) = msg {
                    let mut root: serde_json::Value = serde_json::from_str(txt)?;
                    
                    if root["e"].is_string() {
                        let tp = root["e"].as_str().ok_or(anyhowln!("invalid type"))?;
                        if root["!userData"].is_object() {
                            self.parse_private("", tp, root).await
                        } else {
                            self.parse_public("", tp, root).await
                        }
                    } else {
                        Err(anyhowln!("invalid message format"))
                    }
                } else {
                    Err(anyhowln!("invalid message type"))
                }
            }
            _ => Err(anyhowln!("invalid signal")),
        }
    }

    async fn make_websocket_param(
        &self,
        ctx: &ExchangeContextPtr,
        _group: &String,
        _subscribes: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<WebsocketParam> {
        let mut param = WebsocketParam::default();
        param.url = if ctx.param.key.is_testnet {
            "wss://testnet.binance.vision/ws"
        } else {
            "wss://stream.binance.vision/ws"
        }
        .to_string();
        Ok(param)
    }

    async fn make_group_and_key(
        &self,
        s: &SubscribeType,
        param: &SubscribeParam,
    ) -> Option<(String, String)> {
        match s {
            SubscribeType::Orderbook => {
                let symbol = param.0["market"].as_str()?;
                Some(("/ws".to_string(), format!("{}@depth@100ms", symbol)))
            }
            SubscribeType::Balance | SubscribeType::Order => {
                Some(("/ws".to_string(), "!userData".to_string()))
            }
            _ => None,
        }
    }
} 