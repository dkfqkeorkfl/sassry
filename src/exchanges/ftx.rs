// use std::borrow::BorrowMut;
// use std::collections::HashMap;

// use std::sync::Arc;

// use crate::opk;
// use anyhow::{Ok, Result};
// use axum::async_trait;
// use chrono::TimeZone;
// use chrono::Utc;
// use futures::future;
// use num::ToPrimitive;
// use sassry::exchange::*;
// use sassry::protocols::*;
// use reqwest::header::HeaderValue;
// use rust_decimal::Decimal;
// use serde_json::json;

// pub struct FTX;




// impl FTX {
//     pub fn parse_order(json: &mut serde_json::Value, time : PacketTime) -> Result<OrderPtr> {
//         let amount = sassry::to_decimal_with_json(&json["price"])?;
//         let price = sassry::to_decimal_with_json(&json["size"])?;
//         let created = chrono::DateTime::parse_from_rfc2822(json["createdAt"].as_str().unwrap())?;

//         let procceed = if !json["avgFillPrice"].is_null() {
//             let avg = sassry::to_decimal_with_json(&json["avgFillPrice"])?;
//             let filled = sassry::to_decimal_with_json(&json["filledSize"])?;
//             OrderProcceed::new_with_quote(avg, filled)
//         } else {
//             OrderProcceed::default()
//         };

//         let state = match json["status"].as_str().unwrap() {
//             "filled" => OrderState::Done,
//             "closed" => OrderState::Canceled,
//             "cancelled" => OrderState::Canceled,
//             _ => OrderState::Opened,
//         };

//         let order = Order {
//             id: json["id"].to_string(),
//             kind: OrderKind::Limit,
//             symbol: json["market"].to_string(),
//             market: None,
//             price: price,
//             amount: amount,
//             side: if json["side"].to_string() == "buy" {
//                 OrderSide::Buy
//             } else {
//                 OrderSide::Sell
//             },

//             fee: CurrencyPair::default(),
//             state: state,
//             procceed: procceed,
//             is_postonly: json["postOnly"].as_bool().unwrap(),
//             created: Utc.from_utc_datetime(&created.naive_utc()),
//             updated: Utc::now(),
//             client_id: "".to_string(),
//             detail: json.take(),
//             time : time
//         };

//         Ok(OrderPtr::new(order))
//     }
// }

// #[async_trait]
// impl RestapiAbs for FTX {
//     async fn request_order_cancel(
//         &self,
//         optional: &Optional,
//         params: &HashMap<String, OrderPtr>,
//     ) -> Result<HashMap<String, OrderPtr>> {
//         let bodies = future::join_all(params.into_iter().map(|pr| {
//             let path = format!("/orders/{}", pr.0);
//             async move {
//                 let res = self
//                     .request(
//                         optional,
//                         reqwest::Method::DELETE,
//                         path.as_str(),
//                         reqwest::header::HeaderMap::new(),
//                         serde_json::Value::default(),
//                     )
//                     .await?;
//                 Ok(res)
//             }
//         }))
//         .await;

//         let is_ok = bodies.iter().all(|b| b.is_ok());
//         if !is_ok {
//             let err = bodies.iter().find(|b| b.is_err());
//             if let Err(e) = err.unwrap() {
//                 return Err(anyhowln!(
//                     "request fail - cancel order {}",
//                     e.to_string()
//                 ));
//             }
//         }

//         let mut ret = HashMap::<String, OrderPtr>::new();
//         for p in params {
//             let mut order = Order::default();
//             order.clone_from(p.1.as_ref());
//             order.state = OrderState::Canceling(Utc::now());
//             let id = order.id.clone();
//             ret.insert(id, Arc::new(order));
//         }

//         Ok(ret)
//     }

//     async fn request_order_submit(
//         &self,
//         optional: &Optional,
//         params: &[OrderParam],
//     ) -> Result<HashMap<String, OrderPtr>> {
//         let bodies = future::join_all(params.into_iter().map(|param| {
//             let price = param.price.to_f64();
//             let amount = param.amount.to_f64();

//             let mut body = json!({
//                 "type":"limit",
//                 "market":param.market.symbol,
//                 "side":if param.side == OrderSide::Buy {
//                     "buy"
//                 } else {
//                     "sell"
//                 },
//                 "price":price.unwrap(),
//                 "amount":amount.unwrap(),
//                 "postOnly":param.is_postonly,
//             });

//             if !param.client_id.is_empty() {
//                 body["clientId"] = serde_json::Value::String(param.client_id.clone());
//             }

//             async move {
//                 let res = self
//                     .request(
//                         optional,
//                         reqwest::Method::POST,
//                         "/orders",
//                         reqwest::header::HeaderMap::new(),
//                         body,
//                     )
//                     .await?;
//                 Ok(res)
//             }
//         }))
//         .await;

//         let mut ret = HashMap::<String, OrderPtr>::new();
        
//         for mut b in bodies {
//             match b.as_mut() {
//                 std::result::Result::Ok(pr) => {
//                     let root = pr.0["result"].borrow_mut();
//                     let order = FTX::parse_order(root, pr.1.to_owned())?;
//                     let id = order.id.clone();
//                     ret.insert(id, order);
//                 }
//                 Err(e) => {}
//             }
//         }

//         Ok(ret)
//     }

//     async fn request_order_query(
//         &self,
//         optional: &Optional,
//         params : &HashMap<String, OrderPtr>
//     ) -> Result<HashMap<String, OrderPtr>> {
//         let bodies = future::join_all(params.into_iter().map(|param| {
//             let starttime = param.1.created.timestamp_millis();
//             let body = json!({
//                 "start_time":starttime,
//                 "end_time":starttime+1
//             });

//             async move {
//                 let res = self
//                     .request(
//                         optional,
//                         reqwest::Method::POST,
//                         format!("/order/history/{}",body.to_string()).as_str(),
//                         reqwest::header::HeaderMap::new(),
//                         body,
//                     )
//                     .await?;
//                 Ok(res)
//             }
//         }))
//         .await;

//         let mut ret = HashMap::<String, OrderPtr>::new();
        
//         for mut b in bodies {
//             match b.as_mut() {
//                 std::result::Result::Ok(pr) => {
//                     let root = pr.0["result"].borrow_mut();
//                     let order = FTX::parse_order(root, pr.1.to_owned())?;
//                     let id = order.id.clone();
//                     ret.insert(id, order);
//                 }
//                 Err(e) => {}
//             }
//         }

//         Ok(ret)
//     }
//     async fn request_order_opened(
//         &self,
//         optional: &Optional,
//         market: &MarketPtr,
//     ) -> Result<HashMap<String, OrderPtr>> {
//         let path = format!("/orders?market={}", market.symbol);
//         let mut pr = self
//             .request(
//                 optional,
//                 reqwest::Method::GET,
//                 path.as_str(),
//                 reqwest::header::HeaderMap::new(),
//                 serde_json::Value::default(),
//             )
//             .await?;

//         let mut ret = HashMap::<String, OrderPtr>::new();
//         for item in pr.0["result"].as_array_mut().unwrap() {
//             let order = FTX::parse_order(item, pr.1.clone())?;
//             let id = order.id.clone();
//             ret.insert(id, order);
//         }
//         Ok(ret)
//     }

//     async fn request_orderbook(
//         &self,
//         optional: &Optional,
//         market: &MarketPtr,
//     ) -> Result<OrderBook> {
//         let path = format!("/markets/{}/orderbook", market.symbol);

//         let mut pr = self
//             .request(
//                 optional,
//                 reqwest::Method::GET,
//                 path.as_str(),
//                 reqwest::header::HeaderMap::new(),
//                 serde_json::Value::default(),
//             )
//             .await?;

//         let result = pr.0["result"].as_object().unwrap();
//         let mut orb = OrderBook::default();
//         orb.time = pr.1;
//         for item in result["asks"].as_array().unwrap() {
//             let items = item.as_array().unwrap();
//             let price = sassry::to_decimal(&items[0].to_string())?;
//             let amount = sassry::to_decimal(&items[1].to_string())?;
//             orb.asks.push_back((price, amount));
//         }

//         for item in result["bids"].as_array().unwrap() {
//             let items = item.as_array().unwrap();
//             let price = sassry::to_decimal(&items[0].to_string())?;
//             let amount = sassry::to_decimal(&items[1].to_string())?;
//             orb.bids.push_back((price, amount));
//         }

//         orb.detail = pr.0.take();
//         Ok(orb)
//     }

//     async fn request_position(&self, optional: &Optional) -> Result<DataSet<Position>> {
//         let mut pr = self
//             .request(
//                 optional,
//                 reqwest::Method::GET,
//                 "/positions?showAvgPrice=true",
//                 reqwest::header::HeaderMap::new(),
//                 serde_json::Value::default(),
//             )
//             .await?;

//         let mut positions = HashMap::<String, Position>::new();
//         for obj in pr.0["result"].as_array_mut().unwrap() {
//             let size = sassry::to_decimal_with_json(&obj["netSize"])?;
//             if size == Decimal::ZERO {
//                 continue;
//             }

//             let symbol = obj["future"].as_str().unwrap().to_string();
//             let avg = sassry::to_decimal_with_json(&obj["recentAverageOpenPrice"])?;
//             let pnl = sassry::to_decimal_with_json(&obj["unrealizedPnl"])?;
//             let liquidation = sassry::to_decimal_with_json(&obj["estimatedLiquidationPrice"])?;

//             let position = Position {
//                 symbol: symbol.clone(),
//                 side: if size > Decimal::ZERO {
//                     OrderSide::Buy
//                 } else {
//                     OrderSide::Sell
//                 },
//                 avg: avg,
//                 size: size.abs(),
//                 pnl: pnl,
//                 leverage: Decimal::ONE,
//                 liquidation: liquidation,
//                 opened: Utc::now(),

//                 detail: obj.take(),
//             };
//             positions.insert(symbol, position);
//         }
//         Ok(DataSet::<Position>::new(pr.1, positions))
//     }

//     async fn request_wallet(&self, optional: &Optional) -> Result<DataSet<Asset>> {
//         let mut pr = self
//             .request(
//                 optional,
//                 reqwest::Method::GET,
//                 "/wallet/balances",
//                 reqwest::header::HeaderMap::new(),
//                 serde_json::Value::default(),
//             )
//             .await?;

//         let mut assets = HashMap::<String, Asset>::new();
//         for obj in pr.0["result"].as_array_mut().unwrap() {
//             let currency = obj["coin"].as_str().unwrap().to_string();
//             let total = sassry::to_decimal_with_json(&obj["total"])?;
//             let free = sassry::to_decimal_with_json(&obj["free"])?;
//             let asset = Asset {
//                 currency: currency.clone(),
//                 lock: total - free,
//                 free: free.clone(),

//                 detail: obj.take(),
//             };
//             assets.insert(currency, asset);
//         }

//         Ok(DataSet::<Asset>::new(pr.1, assets))
//     }

//     async fn request_market(&self, optional: &Optional) -> Result<Arc<Context>> {
//         let mut pr = self
//             .request(
//                 optional,
//                 reqwest::Method::GET,
//                 "/markets",
//                 reqwest::header::HeaderMap::new(),
//                 serde_json::Value::default(),
//             )
//             .await?;

//         let ctx = Arc::new(Context::new(pr.1));
//         let mut markets = ctx.markets.lock().await;
//         for m in pr.0["result"].as_array_mut().unwrap() {
//             let k = if let Some(f) = m["futureType"].as_str() {
//                 match f {
//                     "perpetual" => MarketKind::Perpetual(
//                         m["underlying"].as_str().unwrap().to_string(),
//                         "USD".to_string(),
//                         Decimal::ONE,
//                     ),
//                     "future" => MarketKind::Future(
//                         m["underlying"].as_str().unwrap().to_string(),
//                         "USD".to_string(),
//                         Decimal::ONE,
//                     ),
//                     _ => MarketKind::Derivatives(
//                         m["underlying"].as_str().unwrap().to_string(),
//                         "USD".to_string(),
//                         Decimal::ONE,
//                     ),
//                 }
//             } else {
//                 MarketKind::Spot(
//                     m["baseCurrency"].as_str().unwrap().to_string(),
//                     m["quoteCurrency"].as_str().unwrap().to_string(),
//                 )
//             };

//             let pp = sassry::to_decimal(&m["priceIncrement"].to_string())?;
//             let ap = sassry::to_decimal(&m["sizeIncrement"].to_string())?;

//             let market = sassry::protocols::Market {
//                 symbol: m["name"].as_str().unwrap().to_string(),
//                 state: if m["restricted"].as_bool().unwrap() {
//                     MarketState::Disable
//                 } else {
//                     MarketState::Work
//                 },

//                 kind: k.clone(),
//                 fee: FeeInfos::default(),
//                 amount_limit: [CurrencyPair::base_zero(), CurrencyPair::base_max()],
//                 price_limit: [Decimal::ZERO, Decimal::MAX],
//                 pp_kind: PrecisionKind::Tick(pp.clone()),
//                 ap_kind: PrecisionKind::Tick(ap.clone()),
//                 pp_calc: Box::new(move |price, side, lvl| -> anyhow::Result<Decimal> {
//                     return sassry::protocols::Market::cp_tick(&price, &pp, side, lvl);
//                 }),
//                 ap_calc: Box::new(move |amount, _price, side, lvl| -> anyhow::Result<Decimal> {
//                     return sassry::protocols::Market::cp_tick(&amount, &ap, side, lvl);
//                 }),
//                 detail: m.take(),
//             };

//             markets.insert(k, Arc::new(market));
//         }

//         // println!("{:?}", ret);
//         // for (key, val) in ret.iter() {
//         //     println!("{:?}", key);
//         // }
//         Ok(ctx.clone())
//     }

//     async fn request(
//         &self,
//         optional: &Optional,
//         method: reqwest::Method,
//         path: &str,
//         headers: reqwest::header::HeaderMap,
//         body: serde_json::Value,
//     ) -> anyhow::Result<(serde_json::Value, PacketTime)> {
//         let pr = self
//             .req_async(optional, method, path, headers, body)
//             .await?;

//         let json: serde_json::Value = pr.0.json().await?;
//         if !json["success"].as_bool().unwrap_or(false) {
//             return Err(anyhowln!("fail response {}", json.to_string()));
//         }

//         println!("{}", json.to_string());
//         return Ok((json, pr.1));
//     }

//     fn sign<'a>(&self, mut auth: Authority<'a>) -> Result<Authority<'a>> {
//         let milli = Utc::now().timestamp_millis();
//         auth.payload = auth.body.to_string();
        
//         let payload = format!(
//             "{}{}/api{}{}",
//             milli,
//             auth.method.as_str(),
//             auth.path,
//             auth.body
//         );

//         let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, auth.option.secret.as_bytes());
//         let s = ring::hmac::sign(&key, payload.as_bytes());

//         auth.headers
//             .append("FTX-KEY", HeaderValue::from_str(&auth.option.key).unwrap());
//         auth.headers.append(
//             "FTX-SIGN",
//             HeaderValue::from_str(&hex::encode(s).to_lowercase()).unwrap(),
//         );
//         auth.headers
//             .append("FTX-TS", HeaderValue::from_str(&milli.to_string()).unwrap());
//         return Ok(auth);
//     }
// }
