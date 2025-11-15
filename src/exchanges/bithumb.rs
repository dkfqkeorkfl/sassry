use std::collections::HashMap;

use std::sync::Arc;

use super::super::exchange::*;
use cassry::{float, secrecy::ExposeSecret, *};

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
            OrderState::Opened => "wait",
            OrderState::Filled => "done",
            OrderState::Cancelled => "cancel",
            _ => "wait",
        }
    }

    pub fn parse_order(
        ptime: PacketTime,
        json: &mut serde_json::Value,
    ) -> anyhow::Result<OrderPtr> {
        // 공식 문서: uuid - 주문의 고유 아이디
        let order_id = json["uuid"].as_str().ok_or(anyhowln!("invalid uuid"))?;
        let side = match json["side"].as_str().ok_or(anyhowln!("invalid side"))? {
            "bid" => OrderSide::Buy,
            "ask" => OrderSide::Sell,
            _ => return Err(anyhowln!("unknown side")),
        };

        // 공식 문서: ord_type - 주문 방식 ("limit" 등)
        let kind = match json["ord_type"]
            .as_str()
            .ok_or(anyhowln!("invalid ord_type"))?
        {
            "limit" => OrderKind::Limit,
            _ => OrderKind::Limit, // 기본값
        };

        // 공식 문서: price - 주문 당시 화폐 가격 (NumberString)
        let price = float::to_decimal_with_json(&json["price"])?;
        // 공식 문서: state - 주문 상태 ("wait", "done", "cancel" 등)
        let state = match json["state"].as_str().ok_or(anyhowln!("invalid state"))? {
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
            // 수수료는 quote currency로 간주
            CurrencyPair::new_quote(paid_fee)
        } else {
            CurrencyPair::default()
        };

        let order = Order {
            ptime: ptime,
            oid: order_id.to_string(),
            cid: Default::default(),

            kind: kind,
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

            detail: json.take(),
        };

        Ok(Arc::new(order))
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
        let market = params
            .market_ptr()
            .ok_or(anyhowln!("occur error in bithumb::request_order_cancel"))?;

        // 빗썸 심볼 형식: BASE_QUOTE (예: BTC_KRW)
        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let symbol = market.kind.symbol();
        let (order_currency, payment_currency) = if let Some(pos) = symbol.find('_') {
            (&symbol[..pos], &symbol[pos + 1..])
        } else {
            return Err(anyhowln!("invalid symbol format for bithumb: {}", symbol));
        };

        let bodies = future::join_all(params.get_datas().values().map(|order| {
            let order_id = order.oid.clone();

            let body = json!({
                "endpoint": "/trade/cancel",
                "uuid": order_id.clone(),
            });

            let readable = &(*self);
            async move {
                let result = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::DELETE,
                            "/trade/cancel",
                            body,
                        ),
                    )
                    .await;
                (order_id, result)
            }
        }))
        .await;

        let mut ret = OrderResult::new(util::get_epoch_first().into(), params.get_market().clone());

        for (oid, result) in bodies {
            match result {
                Ok((_value, ptime)) => {
                    let order = params
                        .get_datas()
                        .get(&oid)
                        .ok_or(anyhowln!("Data for the canceled order cannot be found."))?;

                    let mut new_order = order.as_ref().clone();
                    new_order.ptime = ptime;
                    new_order.state = OrderState::Canceling(Utc::now());
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
        let symbol = market.kind.symbol();
        let (order_currency, payment_currency) = if let Some(pos) = symbol.find('_') {
            (&symbol[..pos], &symbol[pos + 1..])
        } else {
            return Err(anyhowln!("invalid symbol format for bithumb: {}", symbol));
        };

        let bodies = future::join_all(params.iter().map(|param| {
            let order_type = if param.side.is_buy() { "bid" } else { "ask" };
            let body = json!({
                "endpoint": "/trade/place",
                "order_currency": order_currency,
                "payment_currency": payment_currency,
                "units": param.amount.to_string(),
                "price": param.price.to_string(),
                "type": order_type,
            });

            let readable = &(*self);
            async move {
                readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::POST,
                            "/trade/place",
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
                    let oid = value["uuid"]
                        .as_str()
                        .ok_or(anyhowln!("invalid uuid in response"))?
                        .to_string();

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
        let market = params
            .market_ptr()
            .ok_or(anyhowln!("occur error in bithumb::request_order_query"))?;

        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let symbol = market.kind.symbol();
        let (order_currency, payment_currency) = if let Some(pos) = symbol.find('_') {
            (&symbol[..pos], &symbol[pos + 1..])
        } else {
            return Err(anyhowln!("invalid symbol format for bithumb: {}", symbol));
        };

        // 빗썸은 개별 주문 조회를 지원하지 않으므로 주문 리스트를 조회
        let body = json!({
            "endpoint": "/info/orders",
            "order_currency": order_currency,
            "payment_currency": payment_currency,
        });

        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::POST, "/info/orders", body),
            )
            .await?;

        let mut ret = OrderResult::new(util::get_epoch_first().into(), params.get_market().clone());

        // 빗썸 응답 형식 확인 필요 - data 배열 또는 단일 객체일 수 있음
        let orders = if let Some(array) = pr.0["data"].as_array_mut() {
            array
        } else {
            return Ok(ret);
        };

        let requested_oids: std::collections::HashSet<String> =
            params.get_datas().keys().cloned().collect();

        for item in orders {
            // uuid를 먼저 추출하여 borrowing 문제 해결
            let order_id_opt = item
                .get("uuid")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(order_id) = order_id_opt {
                if requested_oids.contains(&order_id) {
                    match RestAPI::parse_order(pr.1.clone(), item) {
                        Ok(order) => {
                            ret.success.insert(order);
                        }
                        Err(e) => {
                            ret.errors.insert(order_id, e);
                        }
                    }
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

        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::POST, "/info/orders", body),
            )
            .await?;

        let mut ret = OrderSet::new(pr.1, MarketVal::Pointer(market.clone()));

        let orders = if let Some(array) = pr.0["data"].as_array_mut() {
            array
        } else {
            return Ok(ret);
        };

        for item in orders {
            let status = item["state"].as_str().unwrap_or("");
            // "wait" 상태만 오픈된 주문으로 간주
            if status == "wait" {
                match RestAPI::parse_order(ret.get_packet_time().clone(), item) {
                    Ok(order) => {
                        ret.insert(order);
                    }
                    Err(_) => {
                        // 파싱 오류는 무시
                    }
                }
            }
        }

        Ok(ret)
    }

    async fn request_order_search(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        param: &OrdSerachParam,
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

        let mut pr = self
            .request(
                context,
                exchange::RequestParam::new_mpb(reqwest::Method::POST, "/info/orders", body),
            )
            .await?;

        let mut ret = OrderSet::new(pr.1, MarketVal::Pointer(market.clone()));

        let orders = if let Some(array) = pr.0["data"].as_array_mut() {
            array
        } else {
            return Ok(ret);
        };

        for item in orders {
            // state 필터링
            if let Some(state) = &param.state {
                let status = item["state"].as_str().unwrap_or("");
                let item_state = match status {
                    "wait" => OrderState::Opened,
                    "done" => OrderState::Filled,
                    "cancel" => OrderState::Cancelled,
                    _ => OrderState::Rejected,
                };

                if !matches!(
                    (state, &item_state),
                    (OrderState::Opened, OrderState::Opened)
                        | (OrderState::Filled, OrderState::Filled)
                        | (OrderState::Cancelled, OrderState::Cancelled)
                        | (OrderState::Rejected, OrderState::Rejected)
                ) {
                    continue;
                }
            }

            match RestAPI::parse_order(ret.get_packet_time().clone(), item) {
                Ok(order) => {
                    ret.insert(order);
                }
                Err(_) => {
                    // 파싱 오류는 무시
                }
            }
        }

        Ok(ret)
    }

    async fn request_orderbook(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        _quantity: SubscribeQuantity,
    ) -> anyhow::Result<OrderBook> {
        // 고빈도 트레이딩 최적화: split 결과 캐싱 또는 직접 파싱
        let symbol = market.kind.symbol();
        let (order_currency, payment_currency) = if let Some(pos) = symbol.find('_') {
            (&symbol[..pos], &symbol[pos + 1..])
        } else {
            return Err(anyhowln!("invalid symbol format for bithumb: {}", symbol));
        };

        let path = format!("/public/orderbook/{}_{}", order_currency, payment_currency);

        let (mut root, packettime) = self
            .request(
                context,
                exchange::RequestParam::new_mp(reqwest::Method::GET, &path),
            )
            .await?;

        let status = root["status"].as_str().unwrap_or("");
        if status != "0000" {
            return Err(anyhowln!("bithumb API error: {}", root.to_string()));
        }

        let data = root["data"]
            .as_object()
            .ok_or(anyhowln!("invalid response format"))?;

        let timestamp = data["timestamp"]
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(Utc::now().timestamp_millis());

        let updated = Utc.timestamp_millis_opt(timestamp).unwrap();

        let mut orderbook = OrderBook::new(packettime, MarketVal::Pointer(market.clone()), updated);

        let proc = |item: &serde_json::Value| -> anyhow::Result<OrderBookQuotePtr> {
            let price = item["price"].as_str().ok_or(anyhowln!("invalid price"))?;
            let quantity = item["quantity"]
                .as_str()
                .ok_or(anyhowln!("invalid quantity"))?;
            let quote: OrderBookQuote = (price.into(), quantity.into());
            Ok(Arc::new(quote))
        };

        if let Some(bids) = data["bids"].as_array() {
            orderbook.bid = bids.iter().map(proc).collect::<anyhow::Result<Vec<_>>>()?;
        }

        if let Some(asks) = data["asks"].as_array() {
            orderbook.ask = asks.iter().map(proc).collect::<anyhow::Result<Vec<_>>>()?;
        }

        orderbook.detail = root.take();
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
                exchange::RequestParam::new_mp(reqwest::Method::POST, "/v1/accounts"),
            )
            .await?;

        let mut assets = DataSet::<Asset>::new(ptime, UpdateType::Snapshot);
        for item in packet.as_array().ok_or(anyhowln!("invalid response format"))? {
            let currency = item["currency"].as_str().ok_or(anyhowln!("invalid currency"))?;
            let total = float::to_decimal_with_json(&item["balance"])?;
            let locked = float::to_decimal_with_json(&item["locked"])?;
            
            let asset = Asset {
                ptime: ptime.clone(),
                updated: Utc::now(),
                currency: currency.to_string(),
                free: total - locked,
                lock: locked,
                detail: item.clone(),
            };
            assets.insert_raw(currency.to_string(), Arc::new(asset));
        }
        
        Ok(assets)
    }

    async fn sign(
        &self,
        ctx: &ExchangeContextPtr,
        mut param: exchange::RequestParam,
    ) -> anyhow::Result<exchange::RequestParam> {
        // Public API는 서명 불필요
        if param.method == reqwest::Method::GET {
            return Ok(param);
        }

        // Private API 서명 생성
        let nonce = uuid::Uuid::new_v4().to_string();
        let timestamp = Utc::now().timestamp_millis().to_string();
        let payload = if param.body.is_null() {
            json!({
                "nonce" : nonce,
                "timestamp": timestamp,
                "access_key" : ctx.param.key.key.expose_secret(),
            })
        } else {
            let query_hash = ring::digest::digest(
                &ring::digest::SHA512,
                json::url_encode(&param.body)?.as_bytes(),
            );
            json!({
                "nonce" : nonce,
                "timestamp": timestamp,
                "access_key" : ctx.param.key.key.expose_secret(),
                "query_hash" : hex::encode(query_hash.as_ref()),
                "query_hash_alg" : "sha512",
            })
        };

        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
            &payload,
            &jsonwebtoken::EncodingKey::from_rsa_pem(
                ctx.param.key.secret.expose_secret().as_bytes(),
            )
            .unwrap(),
        )?;

        // 헤더 설정
        param.headers.insert(
            reqwest::header::AUTHORIZATION,
            reqwest::header::HeaderValue::from_str(&format!("Bearer {}", token))?,
        );

        Ok(param)
    }

    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: exchange::RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)> {
        let pr = self.req_async(context, param).await?;

        let json =
            pr.0.json::<serde_json::Value>()
                .await
                .map_err(|e| e.into())
                .and_then(|j| {
                    // 빗썸 응답 형식: {"status": "0000", "data": {...}}
                    let status = j["status"].as_str().unwrap_or("");
                    if status != "0000" {
                        Err(anyhowln!("bithumb API error: {}", j.to_string()))
                    } else {
                        Ok(j)
                    }
                })?;

        Ok((json, pr.1))
    }

    async fn request_market(
        &self,
        context: &ExchangeContextPtr,
    ) -> anyhow::Result<DataSet<Market, MarketKind>> {
        let (root, packet) = self
            .request(
                context,
                exchange::RequestParam::new_mp(reqwest::Method::GET, "/v1/market/all"),
            )
            .await?;

        let symbols = root
            .as_array()
            .map(|v| {
                v.iter()
                    .filter_map(|item| item["market"].as_str())
                    .collect::<Vec<_>>()
            })
            .ok_or(anyhowln!("invalid market response format"))?;

        let bodies = future::join_all(symbols.iter().map(|item| {
            let readable = &(*self);
            async move {
                let ret = readable
                    .request(
                        context,
                        exchange::RequestParam::new_mpb(
                            reqwest::Method::GET,
                            "/v1/market/all",
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
            let (base, quote) = match symbol.split_once('-') {
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
                sm: CurrencyPair::new_quote(float::to_decimal_with_json(&root["maker_ask_fee"])?),
                st: CurrencyPair::new_quote(float::to_decimal_with_json(&root["ask_fee"])?)
            };

            let limits = &market["bid"];
            limits["price_unit"]
                .as_str()
                .ok_or(anyhowln!("invalid price unit"))?;
            limits["min_total"]
                .as_str()
                .ok_or(anyhowln!("invalid min_total"))?;

            let kind = MarketKind::Spot(symbol.to_string());

            let market = Market {
                ptime: packet.clone(),
                updated: Utc::now(),
                kind: kind.clone(),
                state: state,

                quote_currency: quote.to_string(),
                base_currency: base.to_string(),
                contract_size: Decimal::ONE,
                fee: fee,
                amount_limit: [CurrencyPair::new_base(Decimal::ZERO), CurrencyPair::new_base(Decimal::MAX)],
                price_limit: [Decimal::ZERO, Decimal::MAX],
                pp_kind: Default::default(),
                ap_kind: Default::default(),
                detail: root,
            };

            markets.insert(kind, Arc::new(market));
        }
  
        Ok(markets)
    }
}
