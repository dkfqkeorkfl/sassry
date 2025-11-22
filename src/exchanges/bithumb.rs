use std::clone;
use std::collections::HashMap;
use std::collections::HashSet;

use std::sync::Arc;

use super::super::exchange::*;
use super::super::webserver::websocket::*;
use cassry::tokio::sync::RwLock;
use cassry::{float, secrecy::ExposeSecret, *};

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::future;
use rust_decimal::Decimal;
use serde_json::*;

#[derive(Debug)]
pub struct RestAPI {
    pp_kind_krw: PrecisionKind,
    specific_pp_precision: HashMap<String, PrecisionKind>,
    specific_ap_precision: HashMap<String, PrecisionKind>,
}

impl Default for RestAPI {
    fn default() -> Self {
        let ranges = vec![
            TickRange(
                float::to_decimal("0").unwrap(),
                float::to_decimal("0.0001").unwrap(),
            ),
            TickRange(
                float::to_decimal("1").unwrap(),
                float::to_decimal("0.001").unwrap(),
            ),
            TickRange(
                float::to_decimal("10").unwrap(),
                float::to_decimal("0.01").unwrap(),
            ),
            TickRange(
                float::to_decimal("100").unwrap(),
                float::to_decimal("1").unwrap(),
            ),
            TickRange(
                float::to_decimal("5000").unwrap(),
                float::to_decimal("5").unwrap(),
            ),
            TickRange(
                float::to_decimal("10000").unwrap(),
                float::to_decimal("10").unwrap(),
            ),
            TickRange(
                float::to_decimal("50000").unwrap(),
                float::to_decimal("100").unwrap(),
            ),
            TickRange(
                float::to_decimal("100000").unwrap(),
                float::to_decimal("500").unwrap(),
            ),
            TickRange(
                float::to_decimal("500000").unwrap(),
                float::to_decimal("1000").unwrap(),
            ),
        ];

        let mut specific_pp_precision = HashMap::new();
        specific_pp_precision.insert("USDT-BTC".to_string(), float::to_decimal("0.01").unwrap());
        specific_pp_precision.insert("USDT-ETH".to_string(), float::to_decimal("0.01").unwrap());
        specific_pp_precision.insert("USDT-SOL".to_string(), float::to_decimal("0.01").unwrap());
        specific_pp_precision.insert("USDT-XRP".to_string(), float::to_decimal("0.0001").unwrap());
        specific_pp_precision.insert(
            "USDT-USDC".to_string(),
            float::to_decimal("0.0001").unwrap(),
        );
        specific_pp_precision.insert("USDT-TRX".to_string(), float::to_decimal("0.0001").unwrap());
        specific_pp_precision.insert(
            "USDT-DOGE".to_string(),
            float::to_decimal("0.00001").unwrap(),
        );
        specific_pp_precision.insert("USDT-WLD".to_string(), float::to_decimal("0.001").unwrap());
        let specific_pp_precision = specific_pp_precision
            .iter()
            .map(|(symbol, precision)| (symbol.to_string(), PrecisionKind::Tick(precision.clone())))
            .collect::<HashMap<_, _>>();

        let mut specific_ap_precision = HashMap::new();
        specific_ap_precision.insert(
            "USDT-BTC".to_string(),
            float::to_decimal("0.000001").unwrap(),
        );
        specific_ap_precision.insert("USDT-ETH".to_string(), float::to_decimal("0.0001").unwrap());
        specific_ap_precision.insert("USDT-SOL".to_string(), float::to_decimal("0.001").unwrap());
        specific_ap_precision.insert("USDT-XRP".to_string(), float::to_decimal("0.1").unwrap());
        specific_ap_precision.insert("USDT-USDC".to_string(), float::to_decimal("0.001").unwrap());
        specific_ap_precision.insert("USDT-TRX".to_string(), float::to_decimal("1").unwrap());
        specific_ap_precision.insert("USDT-DOGE".to_string(), float::to_decimal("1").unwrap());
        specific_ap_precision.insert("USDT-WLD".to_string(), float::to_decimal("0.1").unwrap());
        specific_ap_precision.insert(
            "BTC-XRP".to_string(),
            float::to_decimal("0.0000001").unwrap(),
        );
        let specific_ap_precision = specific_ap_precision
            .iter()
            .map(|(symbol, precision)| (symbol.to_string(), PrecisionKind::Tick(precision.clone())))
            .collect::<HashMap<_, _>>();

        RestAPI {
            pp_kind_krw: PrecisionKind::RangeTick(ranges.into()),
            specific_ap_precision: specific_ap_precision,
            specific_pp_precision: specific_pp_precision,
        }
    }
}
impl RestAPI {
    pub fn orderstate_tostring(s: &OrderState) -> &'static str {
        match s {
            OrderState::Opened => "wait",
            OrderState::Filled => "done",
            OrderState::Cancelled => "cancel",
            _ => "wait",
        }
    }

    pub fn parse_order(ptime: PacketTime, json: serde_json::Value) -> anyhow::Result<OrderPtr> {
        // 공식 문서: uuid - 주문의 고유 아이디
        let order_id = json["uuid"].as_str().ok_or(anyhowln!("invalid uuid"))?;
        let side = if "bid" == json["side"].as_str().ok_or(anyhowln!("invalid side"))? {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };

        // 공식 문서: ord_type - 주문 방식 ("limit" 등)
        let okind = match json["ord_type"]
            .as_str()
            .ok_or(anyhowln!("invalid ord_type"))?
        {
            "limit" => OrderKind::Limit,
            _ => OrderKind::Limit, // 기본값
        };

        // 공식 문서: price - 주문 당시 화폐 가격 (NumberString)
        let price = float::to_decimal_with_json(&json["price"])?;
        // 공식 문서: state - 주문 상태 ("wait", "done", "cancel" 등)
        let mut state = match json["state"].as_str().ok_or(anyhowln!("invalid state"))? {
            "wait" => OrderState::Opened,
            "done" => OrderState::Filled,
            "cancel" => OrderState::Cancelled,
            _ => return Err(anyhowln!("unknown state")),
        };

        // 공식 문서: created_at - 주문 생성 시간 (DateString)
        let created_at = json["created_at"]
            .as_str()
            .ok_or(anyhowln!("invalid created_at"))
            .and_then(|s| {
                chrono::DateTime::parse_from_rfc3339(s)
                    .map_err(|e| anyhowln!("invalid created_at: {}", e))
            })?;

        // 공식 문서: volume - 사용자가 입력한 주문 양 (NumberString)
        let amount = float::to_decimal_with_json(&json["volume"])?;

        // 공식 문서: executed_volume - 체결된 양 (NumberString)
        let executed_volume =
            float::to_decimal_with_json(&json["executed_volume"]).unwrap_or(Decimal::ZERO);
        let (avg, proceed) = if executed_volume != Decimal::ZERO {
            if state == OrderState::Opened {
                state = OrderState::PartiallyFilled;
            }

            let weight = json["trades"]
                .as_array()
                .ok_or(anyhowln!("invalid trades"))?
                .iter()
                .fold(Decimal::ZERO, |acc, item| {
                    let price =
                        float::to_decimal_with_json(&item["price"]).unwrap_or(Decimal::ZERO);
                    let quantity =
                        float::to_decimal_with_json(&item["volume"]).unwrap_or(Decimal::ZERO);
                    acc + price * quantity
                });
            (
                weight / executed_volume,
                CurrencyPair::new_base(executed_volume),
            )
        } else {
            (Decimal::ZERO, CurrencyPair::default())
        };

        // fee 정보 계산 (paid_fee 사용)
        let paid_fee = float::to_decimal_with_json(&json["paid_fee"]).unwrap_or(Decimal::ZERO);
        let fee = if paid_fee != Decimal::ZERO {
            if side == OrderSide::Buy {
                CurrencyPair::new_quote(paid_fee)
            } else {
                CurrencyPair::new_base(paid_fee)
            }
        } else {
            CurrencyPair::default()
        };

        let order = Order {
            ptime: ptime,
            oid: order_id.to_string(),
            cid: Default::default(),

            kind: okind,
            price: price,
            amount: amount,
            side: side,

            fee: fee,
            state: state,
            avg: avg,
            proceed,
            is_postonly: false,
            is_reduce: false,
            created: created_at.with_timezone(&Utc),
            updated: Utc::now(),

            detail: json,
        };

        Ok(Arc::new(order))
    }

    pub fn make_jwt(
        key: &Arc<ExchangeKey>,
        body: &serde_json::Value,
    ) -> anyhow::Result<(String, String)> {
        let nonce = uuid::Uuid::new_v4().to_string();
        let timestamp = Utc::now().timestamp_millis();

        let (querystring, payload) = if body.is_null() {
            let payload = json!({
                "nonce" : nonce,
                "timestamp": timestamp,
                "access_key" : key.key.expose_secret().to_string(),
            });
            (Default::default(), payload)
        } else {
            let querystring = json::url_encode(body)?;
            let query_hash = ring::digest::digest(&ring::digest::SHA512, querystring.as_bytes());
            let payload = json!({
                "nonce" : nonce,
                "timestamp": timestamp,
                "access_key" : key.key.expose_secret().to_string(),
                "query_hash" : hex::encode(query_hash.as_ref()),
                "query_hash_alg" : "SHA512",
            });

            (querystring, payload)
        };

        let token = jsonwebtoken::encode(
            &Default::default(),
            &payload,
            &jsonwebtoken::EncodingKey::from_secret(key.secret.expose_secret().as_bytes()),
        )?;

        Ok((querystring, token))
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
        Err(anyhowln!("Bithumb does not support order amendment"))
    }

    async fn request_order_cancel(
        &self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> anyhow::Result<OrderResult> {
        let bodies = future::join_all(params.get_datas().keys().map(|oid| {
            let body = json!({
                "order_id": oid,
            });

            let readable = &(*self);
            async move {
                let result = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(reqwest::Method::DELETE, "/v2/order", body),
                    )
                    .await;
                (oid.clone(), result)
            }
        }))
        .await;

        let mut ret = OrderResult::new(util::get_epoch_first().into(), params.get_market().clone());
        for (oid, result) in bodies {
            match result {
                Ok((_root, ptime)) => {
                    let order = params
                        .get_datas()
                        .get(&oid)
                        .ok_or(anyhowln!("Data for the canceled order cannot be found."))?;

                    let mut new_order = order.as_ref().clone();
                    new_order.state = OrderState::Canceling(ptime.recvtime.clone());
                    new_order.ptime = ptime;
                    ret.success.insert_raw(new_order);
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
        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let bodies = future::join_all(params.iter().map(|param| {
            let side = if param.side.is_buy() { "bid" } else { "ask" };
            let body = json!({
                "market": market.kind.symbol(),
                "order_type": "limit",
                "side": side,
                "price": param.price.to_string(),
                "volume": param.amount.to_string(),
            });

            let readable = &(*self);
            async move {
                readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(reqwest::Method::POST, "/v2/orders", body),
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
                Ok((root, ptime)) => {
                    let oid = root["order_id"]
                        .as_str()
                        .ok_or(anyhowln!("invalid uuid in response"))?
                        .to_string();

                    let created_at = root["created_at"]
                        .as_str()
                        .ok_or(anyhowln!("invalid created_at"))
                        .and_then(|s| {
                            chrono::DateTime::parse_from_rfc3339(s)
                                .map_err(|e| anyhowln!("invalid created_at: {}", e))
                        })?;
                    let created_at = created_at.with_timezone(&Utc);

                    let mut order = Order::from_order_param(param);
                    order.oid = oid;
                    order.state = OrderState::Ordering(created_at.clone());
                    order.detail = root;
                    order.ptime = ptime;
                    order.created = created_at;
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
        let bodies = future::join_all(params.get_datas().keys().map(|oid| {
            let body = json!({
                "uuid": oid,
            });
            let readable = &(*self);
            async move {
                let result = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(reqwest::Method::GET, "/v1/order", body),
                    )
                    .await;
                (oid, result)
            }
        }))
        .await;
        // 빗썸은 개별 주문 조회를 지원하지 않으므로 주문 리스트를 조회

        let mut ret = OrderResult::new(util::get_epoch_first().into(), params.get_market().clone());
        for (oid, result) in bodies {
            match result {
                Ok((value, ptime)) => {
                    let order = RestAPI::parse_order(ptime.clone(), value)?;
                    ret.success.insert(order);
                }
                Err(e) => {
                    ret.errors.insert(oid.clone(), e);
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
        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let symbol = market.kind.symbol();
        let (order_currency, payment_currency) = if let Some(pos) = symbol.find('_') {
            (&symbol[..pos], &symbol[pos + 1..])
        } else {
            return Err(anyhowln!("invalid symbol format for bithumb: {}", symbol));
        };

        let body = json!({
            "endpoint": "/info/orders",
            "order_currency": order_currency,
            "payment_currency": payment_currency,
        });

        let (_, ptime) = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::POST, "/info/orders", body),
            )
            .await?;

        let ret = OrderSet::new(ptime, MarketVal::Pointer(market.clone()));
        Ok(ret)
    }

    async fn request_order_search(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        _param: &OrdSerachParam,
    ) -> anyhow::Result<OrderSet> {
        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let symbol = market.kind.symbol();
        let (order_currency, payment_currency) = if let Some(pos) = symbol.find('_') {
            (&symbol[..pos], &symbol[pos + 1..])
        } else {
            return Err(anyhowln!("invalid symbol format for bithumb: {}", symbol));
        };

        let body = json!({
            "endpoint": "/info/orders",
            "order_currency": order_currency,
            "payment_currency": payment_currency,
        });

        // 빗썸은 페이징을 지원하지 않으므로 page 파라미터는 무시
        // state 필터링은 클라이언트 측에서 처리

        let (_, ptime) = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::POST, "/info/orders", body),
            )
            .await?;

        let ret = OrderSet::new(ptime, MarketVal::Pointer(market.clone()));
        Ok(ret)
    }

    async fn request_orderbook(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        _quantity: SubscribeQuantity,
    ) -> anyhow::Result<OrderBook> {
        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let (mut root, packettime) = self
            .request(
                context,
                exchange::RequestParam::new_mpb(
                    reqwest::Method::GET,
                    "/v1/orderbook",
                    json!({ "markets": market.kind.symbol() }),
                ),
            )
            .await?;

        let root = root
            .as_array_mut()
            .and_then(|v| v.get_mut(0))
            .ok_or(anyhowln!("invalid response format"))?;

        let time = root["timestamp"]
            .as_i64()
            .and_then(|s| Utc.timestamp_millis_opt(s).single())
            .ok_or(anyhowln!("invalid timestamp"))?;
        let mut orderbook = OrderBook::new(packettime, MarketVal::Pointer(market.clone()), time);
        for obj in root["orderbook_units"]
            .as_array()
            .ok_or(anyhowln!("invalid orderbook_units"))?
        {
            let ask_price = &obj["ask_price"];
            let bid_price = &obj["bid_price"];

            if !ask_price.is_null() {
                orderbook.ask.push(Arc::new((
                    LazyDecimal::from(ask_price.to_string()),
                    LazyDecimal::from(obj["ask_size"].to_string()),
                )));
            }

            if !bid_price.is_null() {
                orderbook.bid.push(Arc::new((
                    LazyDecimal::from(bid_price.to_string()),
                    LazyDecimal::from(obj["bid_size"].to_string()),
                )));
            }
        }

        orderbook.detail = root.clone();
        Ok(orderbook)
    }

    async fn request_position(
        &self,
        _context: &ExchangeContextPtr,
        _market: &MarketPtr,
    ) -> anyhow::Result<HashMap<MarketKind, PositionSet>> {
        // 빗썸은 현물 거래만 지원하므로 포지션 없음
        Ok(HashMap::new())
    }

    async fn request_wallet(
        &self,
        context: &ExchangeContextPtr,
        _: &str,
    ) -> anyhow::Result<DataSet<Asset>> {
        let (packet, ptime) = self
            .request(
                context,
                exchange::RequestParam::new_mp(reqwest::Method::GET, "/v1/accounts"),
            )
            .await?;

        let mut assets = DataSet::<Asset>::new(ptime, UpdateType::Snapshot);
        for item in packet
            .as_array()
            .ok_or(anyhowln!("invalid response format"))?
        {
            let currency = item["currency"]
                .as_str()
                .ok_or(anyhowln!("invalid currency"))?;
            let total = float::to_decimal_with_json(&item["balance"])?;
            let locked = float::to_decimal_with_json(&item["locked"])?;

            let asset = Asset {
                ptime: assets.get_packet_time().clone(),
                updated: assets.get_packet_time().recvtime.clone(),
                currency: currency.to_string(),
                free: total - locked,
                lock: locked,
                detail: item.clone(),
            };
            assets.insert_raw(currency.to_string(), asset);
        }

        Ok(assets)
    }

    async fn sign(
        &self,
        ctx: &ExchangeContextPtr,
        mut param: exchange::RequestParam,
    ) -> anyhow::Result<exchange::RequestParam> {
        let (querystring, jwt) = RestAPI::make_jwt(&ctx.param.key, &param.body)?;
        // 헤더 설정
        param.headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", jwt))?,
        );

        if !querystring.is_empty()
            && (param.method == reqwest::Method::GET || param.method == reqwest::Method::DELETE)
        {
            param.path = format!("{}?{}", param.path, querystring);
            param.body = Default::default();
        }

        Ok(param)
    }

    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: exchange::RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)> {
        let (packet, ptime) = self.req_async(context, param).await?;
        let status = packet.status();
        let json = match packet.json::<serde_json::Value>().await {
            Ok(json) => json,
            Err(e) => {
                return Err(anyhowln!("failed to parse response: {}", e));
            }
        };

        let str = json.to_string();
        println!(
            "bithumb response({:?}: {}): {}",
            status,
            ptime.laytency().as_seconds_f64(),
            str
        );
        if !status.is_success() {
            return Err(anyhowln!("bithumb API error: {}", str));
        }

        Ok((json, ptime))
    }

    async fn request_market(
        &self,
        context: &ExchangeContextPtr,
    ) -> anyhow::Result<DataSet<Market, MarketKind>> {
        let ordered = {
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()?;
            let response = client.get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=20&page=1")
            .header("User-Agent", "Mozilla/5.0 (Rust reqwest)").send().await?;

            let body = response.json::<Vec<serde_json::Value>>().await?;
            // serde_json::from_str::<Vec<serde_json::Value>>(&body)?;
            let ordered = body
                .iter()
                .filter_map(|item| item["symbol"].as_str())
                .map(|s| s.to_string().to_uppercase())
                .collect::<HashSet<_>>();
            ordered
        };

        let symbols = {
            let (root, _packet) = self
                .request(
                    context,
                    exchange::RequestParam::new_mp(reqwest::Method::GET, "/v1/market/all"),
                )
                .await?;

            let mut symbols = root
                .as_array()
                .map(|v| {
                    v.iter()
                        .filter_map(|item| item["market"].as_str())
                        .filter(|s| {
                            s.split_once('-')
                                .filter(|(_, base)| ordered.contains(*base))
                                .is_some()
                        })
                        .map(|s| s.to_string())
                        .collect::<Vec<_>>()
                })
                .ok_or(anyhowln!("invalid market response format"))?;

            symbols.push("KRW-WLD".to_string());
            symbols.push("USDT-WLD".to_string());
            symbols
        };

        let bodies = future::join_all(symbols.iter().map(|item| {
            let readable = &(*self);
            async move {
                let ret = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::GET,
                            "/v1/orders/chance",
                            json!({
                                "market": item,
                            }),
                        ),
                    )
                    .await;

                ret
            }
        }))
        .await;

        let mut markets = DataSet::<Market, MarketKind>::default();
        for result in bodies {
            let (root, packet) = result?;

            let market = &root["market"];
            let symbol = market["id"].as_str().ok_or(anyhowln!("invalid symbol"))?;
            let (quote, base) = match symbol.split_once('-') {
                Some((quote, base)) => (quote, base),
                None => return Err(anyhowln!("invalid symbol: {}", symbol)),
            };

            let state = if root["state"].as_str().filter(|s| *s == "active").is_none() {
                MarketState::Work
            } else {
                MarketState::Disable
            };

            let fee = FeeInfos {
                bm: CurrencyPair::new_quote(float::to_decimal_with_json(&root["maker_bid_fee"])?),
                bt: CurrencyPair::new_quote(float::to_decimal_with_json(&root["bid_fee"])?),
                sm: CurrencyPair::new_base(float::to_decimal_with_json(&root["maker_ask_fee"])?),
                st: CurrencyPair::new_base(float::to_decimal_with_json(&root["ask_fee"])?),
            };

            let least = float::to_decimal_with_json(&market["bid"]["min_total"])?;
            let kind = MarketKind::Spot(symbol.to_string());
            let ap_kind =
                self.specific_ap_precision
                    .get(symbol)
                    .cloned()
                    .unwrap_or(PrecisionKind::Tick(
                        float::to_decimal("0.00000001").unwrap(),
                    ));
            let pp_kind = self
                .specific_pp_precision
                .get(symbol)
                .cloned()
                .unwrap_or_else(|| {
                    if quote == "BTC" {
                        PrecisionKind::Tick(float::to_decimal("0.00000001").unwrap())
                    } else {
                        self.pp_kind_krw.clone()
                    }
                });

            let market = Market {
                ptime: packet.clone(),
                updated: Utc::now(),
                kind: kind.clone(),
                state: state,

                quote_currency: quote.to_string(),
                base_currency: base.to_string(),
                contract_size: Decimal::ONE,
                fee: fee,
                amount_limit: [
                    CurrencyPair::new_quote(least),
                    CurrencyPair::new_base(Decimal::MAX),
                ],
                price_limit: [Decimal::ZERO, Decimal::MAX],
                pp_kind: pp_kind,
                ap_kind: ap_kind,
                detail: root,
            };

            markets.insert_raw(kind, market);
        }

        Ok(markets)
    }
}

struct OrderProceed {
    pub order: Option<Order>,
    pub excute_fund: Decimal,
    pub excute_volume: Decimal,
    pub fee: Decimal,
}

impl OrderProceed {
    pub fn new(order: Option<Order>) -> Self {
        Self {
            order: order,
            excute_fund: Decimal::ZERO,
            excute_volume: Decimal::ZERO,
            fee: Decimal::ZERO,
        }
    }
}

#[derive(Default)]
pub struct WebsocketItf {
    proceeds: RwLock<HashMap<String, OrderProceed>>,
}

impl WebsocketItf {
    async fn parse_private(
        &self,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match tp {
            "myAsset" => {
                let ptime = root["tms"].as_i64().ok_or(anyhowln!("invalid timestamp"))?;
                let updated = root["asttms"]
                    .as_i64()
                    .ok_or(anyhowln!("invalid timestamp"))?;
                let time = Utc.timestamp_millis_opt(ptime).unwrap();
                let updated = Utc.timestamp_millis_opt(updated).unwrap();

                let mut assets = DataSet::<Asset>::new(PacketTime::new(&time), UpdateType::Partial);
                for item in root["ast"].as_array_mut().ok_or(anyhowln!("invalid ast"))? {
                    let currency = item["cu"]
                        .as_str()
                        .ok_or(anyhowln!("invalid currency"))?
                        .to_string();
                    let total = float::to_decimal_with_json(&item["b"])?;
                    let locked = float::to_decimal_with_json(&item["l"])?;

                    let asset = Asset {
                        ptime: time.into(),
                        updated: updated.clone(),
                        currency: currency.clone(),
                        free: total - locked,
                        lock: locked,
                        detail: item.take(),
                    };
                    assets.insert_raw(currency, asset);
                }

                SubscribeResult::Balance(assets)
            }
            "myOrder" => {
                let mut cached_orders = self.proceeds.write().await;
                let oid = root["uid"].as_str().ok_or(anyhowln!("invalid uuid"))?;
                let tiemstamp = root["tms"].as_i64().ok_or(anyhowln!("invalid timestamp"))?;
                let state = match root["s"].as_str().ok_or(anyhowln!("invalid state"))? {
                    "wait" => OrderState::Opened,
                    "done" => OrderState::Filled,
                    "trade" => OrderState::PartiallyFilled,
                    "cancel" => OrderState::Cancelled,
                    _ => return Err(anyhowln!("unknown state")),
                };

                let proceed = cached_orders
                    .entry(oid.to_string())
                    .or_insert(OrderProceed::new(None));
                let excuted_fund = float::to_decimal_with_json(&root["ef"])?;
                let avg = if excuted_fund != Decimal::ZERO {
                    proceed.excute_fund += excuted_fund;
                    proceed.excute_volume += float::to_decimal_with_json(&root["ev"])?;
                    proceed.fee += float::to_decimal_with_json(&root["pf"])?;
                    proceed.excute_fund / proceed.excute_volume
                } else if proceed.excute_volume != Decimal::ZERO {
                    proceed.excute_fund / proceed.excute_volume
                } else {
                    Decimal::ZERO
                };

                let symbol = root["cd"].as_str().ok_or(anyhowln!("invalid code"))?;
                let kind = MarketKind::Spot(symbol.to_string());
                let time = Utc.timestamp_millis_opt(tiemstamp).unwrap();
                let mut order = if let Some(order) = proceed.order.as_mut() {
                    order.ptime = PacketTime::from_sendtime(&time);
                    order.state = state;
                    order.avg = avg;
                    order.proceed = CurrencyPair::new_base(proceed.excute_volume);
                    order.fee = CurrencyPair::new_quote(proceed.fee);
                    order.updated = time;
                    order.detail = root.take();
                    order.clone()
                } else if state == OrderState::PartiallyFilled {
                    // trade일 때, wait를 먼저 받지 않으면 데이터를 신뢰하지 못함.
                    // 주문 즉시, 체결될 때 wait없이 trade먼저 받을 수 있음.
                    return Ok(SubscribeResult::None);
                } else {
                    // cancel, done, wait
                    let created = root["otms"]
                        .as_i64()
                        .ok_or(anyhowln!("invalid timestamp"))?;
                    let side =
                        if "BID" == root["ab"].as_str().ok_or(anyhowln!("invalid ask_bid"))? {
                            OrderSide::Buy
                        } else {
                            OrderSide::Sell
                        };

                    let order = Order {
                        ptime: PacketTime::from_sendtime(&time),
                        updated: time,
                        oid: oid.to_string(),
                        cid: Default::default(),
                        kind: OrderKind::Limit,
                        price: float::to_decimal_with_json(&root["p"])?,
                        amount: float::to_decimal_with_json(&root["v"])?,
                        side: side,
                        is_postonly: false,
                        is_reduce: false,
                        state: state.clone(),
                        avg: avg,
                        proceed: CurrencyPair::new_base(proceed.excute_volume),
                        fee: CurrencyPair::new_quote(proceed.fee),
                        created: Utc.timestamp_millis_opt(created).unwrap(),
                        detail: root.take(),
                    };
                    order
                };

                let remained_volume = float::to_decimal_with_json(&order.detail["rv"])?;
                match &order.state {
                    OrderState::Opened => {
                        proceed.order = Some(order.clone());
                    }
                    OrderState::Filled => {
                        // done일 때, 특정 상황에서 remained_volume가 이상하게 나오는 문제로 인하여 별도 예외 처리리
                        if order.proceed_real() != order.amount {
                            if order.avg == Decimal::ZERO {
                                cassry::error!(
                                    "excute volume and avg are invaild: {} -> avg({}), proceed({})",
                                    serde_json::to_string(&order)?,
                                    order.price,
                                    order.amount
                                );
                                order.avg = order.price;
                            } else {
                                cassry::error!(
                                    "excute volume is invaild: {} -> proceed({})",
                                    serde_json::to_string(&order)?,
                                    order.amount
                                );
                            }
                            order.proceed = CurrencyPair::new_base(order.amount);
                        }
                        cached_orders.remove(&order.oid.to_string());
                    }
                    OrderState::Cancelled => {
                        // cancelled일 때, remained_volume는 매우 잘 나옴.
                        if order.proceed_real() + remained_volume != order.amount {
                            if order.avg == Decimal::ZERO {
                                cassry::error!(
                                    "excute volume and avg are invaild: {} -> avg({}), proceed({})",
                                    serde_json::to_string(&order)?,
                                    order.price,
                                    order.amount - remained_volume
                                );
                                order.avg = order.price;
                            } else {
                                cassry::error!(
                                    "excute volume is invaild: {} -> proceed({})",
                                    serde_json::to_string(&order)?,
                                    order.amount - remained_volume
                                );
                            }
                            order.proceed = CurrencyPair::new_base(order.amount - remained_volume);
                        }
                        cached_orders.remove(&order.oid.to_string());
                    }

                    _ => {}
                };

                let mut os = OrderSet::new(PacketTime::new(&time), MarketVal::Symbol(kind.clone()));
                os.insert_raw(order);
                let mut ret = HashMap::<MarketKind, OrderSet>::default();
                ret.insert(kind, os);
                SubscribeResult::Order(ret)
            }
            _ => SubscribeResult::None,
        };

        Ok(result)
    }

    async fn parse_public(
        &self,
        tp: &str,
        mut root: serde_json::Value,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match tp {
            "orderbook" => {
                let symbol = root["cd"].as_str().ok_or(anyhowln!("invalid code"))?;
                let tiemstamp = root["tms"].as_i64().ok_or(anyhowln!("invalid timestamp"))?;
                let time = Utc.timestamp_micros(tiemstamp).unwrap();
                let mut orderbook = OrderBook::new(
                    PacketTime::from_sendtime(&time),
                    MarketVal::Symbol(MarketKind::Spot(symbol.to_string())),
                    time.clone(),
                );

                for item in root["obu"]
                    .as_array_mut()
                    .ok_or(anyhowln!("invalid orderbook_units"))?
                {
                    let ask_price = &item["ap"];
                    let bid_price = &item["bp"];

                    if !ask_price.is_null() {
                        orderbook.ask.push(Arc::new((
                            LazyDecimal::from(ask_price.to_string()),
                            LazyDecimal::from(item["as"].to_string()),
                        )));
                    }

                    if !bid_price.is_null() {
                        orderbook.bid.push(Arc::new((
                            LazyDecimal::from(bid_price.to_string()),
                            LazyDecimal::from(item["bs"].to_string()),
                        )));
                    }
                }
                orderbook.detail = root.take();
                SubscribeResult::Orderbook(orderbook)
            }
            "trade" => {
                let time = Utc::now();
                let mut ret = HashMap::<MarketKind, PublicTradeSet>::default();

                let content = root["content"]
                    .as_object()
                    .or_else(|| root["data"].as_object())
                    .ok_or(anyhowln!("invalid trade response format"))?;

                // 빗썸 체결 정보 형식 확인 필요
                let symbol = content["symbol"]
                    .as_str()
                    .ok_or(anyhowln!("invalid symbol in trade"))?;
                let kind = MarketKind::Spot(symbol.to_string());

                let price = content["price"]
                    .as_str()
                    .ok_or(anyhowln!("invalid price in trade"))?;
                let volume = content["volume"]
                    .as_str()
                    .ok_or(anyhowln!("invalid volume in trade"))?;
                let side_str = content["side"].as_str().unwrap_or("buy");
                let datetime = content["datetime"]
                    .as_str()
                    .and_then(|s| {
                        // ISO 8601 또는 타임스탬프 형식 파싱
                        if let Ok(timestamp) = s.parse::<i64>() {
                            Utc.timestamp_millis_opt(timestamp).single()
                        } else {
                            chrono::DateTime::parse_from_rfc3339(s)
                                .ok()
                                .map(|dt| Utc.from_utc_datetime(&dt.naive_utc()))
                        }
                    })
                    .unwrap_or(Utc::now());

                let trade = PublicTrade {
                    ptime: PacketTime::new(&time),
                    updated: datetime,
                    quote: (price.into(), volume.into()),
                    side: if side_str == "bid" || side_str == "buy" {
                        OrderSide::Buy
                    } else {
                        OrderSide::Sell
                    },
                    detail: json!(content),
                };

                if let Some(set) = ret.get_mut(&kind) {
                    set.insert_raw(trade);
                } else {
                    let mut set = PublicTradeSet::new(
                        PacketTime::new(&time),
                        MarketVal::Symbol(kind.clone()),
                        None,
                    );
                    set.insert_raw(trade);
                    ret.insert(kind, set);
                }

                SubscribeResult::PublicTrades(ret)
            }
            "ticker" => {
                // ticker는 PublicTradeSet 대신 다른 형식일 수 있지만,
                // 현재 구조에서는 None 반환
                SubscribeResult::None
            }
            _ => SubscribeResult::None,
        };
        Ok(result)
    }

    pub fn specify_quantity(quantity: SubscribeQuantity) -> anyhow::Result<u64> {
        let num = match quantity {
            SubscribeQuantity::Much => 30,
            SubscribeQuantity::Least(str) | SubscribeQuantity::Fixed(str) => {
                let parsed = str.parse::<usize>()?;
                parsed
            }
            _ => 1,
        };

        Ok(num as u64)
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
            let mut copied = subscribed
                .iter()
                .map(|(ty, v)| (ty.clone(), v.iter().collect::<Vec<_>>()))
                .collect::<HashMap<_, _>>();
            copied.entry(ty.clone()).or_insert(vec![]).push(value);
            copied
        } else {
            subscribed
                .iter()
                .map(|(ty, v)| (ty.clone(), v.iter().collect::<Vec<_>>()))
                .collect::<HashMap<_, _>>()
        };

        // 빗썸 웹소켓 요청 형식: [{"ticket": "UUID"}, {"type": "...", "codes": [...]}, {"format": "DEFAULT"}]
        let mut items = vec![json!({
            "ticket": uuid::Uuid::new_v4().to_string(),
        })];
        let body = s
            .iter()
            .map(|(ty, vs)| {
                let param = match ty {
                    SubscribeType::Balance => json!({
                        "type": "myAsset",
                    }),
                    SubscribeType::Position => {
                        // 빗썸은 현물 거래만 지원하므로 포지션 없음
                        return Err(anyhowln!("invalid position in bithumb"));
                    }
                    SubscribeType::Orderbook => {
                        let symbols = vs
                            .iter()
                            .filter_map(|v| v["symbol"].as_str())
                            .collect::<Vec<_>>();

                        json!({
                            "type": "orderbook",
                            "codes": symbols,
                        })
                    }
                    SubscribeType::Order => {
                        let items = vs
                            .iter()
                            .map(|v| v["symbol"].as_str())
                            .collect::<Option<Vec<_>>>()
                            .ok_or(anyhowln!("invalid symbol in order"))?;

                        json!({
                            "type": "myOrder",
                            "codes": items,
                        })
                    }
                    SubscribeType::PublicTrades => {
                        let symbols = vs
                            .iter()
                            .filter_map(|v| v["symbol"].as_str())
                            .collect::<Vec<_>>();

                        json!({
                            "type": "trade",
                            "codes": symbols,
                        })
                    }
                };
                Ok(param)
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        items.extend(body);
        items.push(json!({
            "format": "SIMPLE",
        }));

        let request = serde_json::Value::Array(items);
        client.send_text(request.to_string()).await?;

        Ok(())
    }

    async fn parse_msg(
        &self,
        _ctx: &ExchangeContextPtr,
        _socket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        let result = match signal {
            Signal::Opened => SubscribeResult::Authorized(true),
            Signal::Received(data) => match data {
                Message::Binary(data) => {
                    let text = String::from_utf8_lossy(data.as_slice()).to_string();
                    let json: serde_json::Value = serde_json::from_str(text.as_str())?;
                    let ty = json["ty"]
                        .as_str()
                        .ok_or(anyhowln!("invalid type"))?
                        .to_string();
                    let result = match ty.as_str() {
                        "myAsset" | "myOrder" => self.parse_private(ty.as_str(), json).await,
                        "orderbook" => self.parse_public(ty.as_str(), json).await,
                        _ => Err(anyhowln!("unknown type: {}", ty)),
                    }?;
                    result
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
    ) -> anyhow::Result<WebsocketParam> {
        if group == "/v1/private" {
            let (_, jwt) = RestAPI::make_jwt(&ctx.param.key, &Default::default())?;
            let mut param = WebsocketParam::default();
            param.url = format!("{}{}", ctx.param.websocket.url, group);
            param.header.insert(
                reqwest::header::AUTHORIZATION.to_string(),
                format!("Bearer {}", jwt),
            );
            Ok(param)
        } else if let Some((path, _)) = group.split_once("?"){
            let mut param = WebsocketParam::default();
            param.url = format!("{}{}", ctx.param.websocket.url, path);
            Ok(param)
        }
        else {
            let mut param = WebsocketParam::default();
            param.url = format!("{}{}", ctx.param.websocket.url, group);
            Ok(param)
        }
    }

    async fn make_group_and_key(&self, param: &SubscribeParam) -> Option<(String, String)> {
        match param.ty {
            SubscribeType::Position => {
                // 빗썸은 현물 거래만 지원하므로 포지션 없음
                None
            }
            SubscribeType::Balance => Some(("/v1/private".to_string(), "asset".to_string())),
            SubscribeType::Order => {
                let symbol = param.value["market"].as_str()?;
                let key = format!("{}{}", param.ty.clone() as u32, symbol);
                Some(("/v1/private".to_string(), key))
            }

            SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                let market = param.value["market"].as_str()?;
                let key = format!("{}{}", param.ty.clone() as u32, market);
                let symbol = param.value["symbol"].as_str()?;
                if let Some((quote, _base)) = symbol.split_once("-") {
                    let group = format!("/v1?{}", quote);
                    Some((group, key))
                } else {
                    Some(("/v1".to_string(), key)) // Public 웹소켓
                }
            }
        }
    }
}
