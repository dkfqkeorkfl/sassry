use std::collections::HashSet;
use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bitflags::bitflags;
use chrono::Utc;
use reqwest::ClientBuilder;
use tokio::sync::Mutex;

use super::super::webserver::websocket::*;
use super::protocols::*;
use cassry::*;

use super::websocket::*;
pub struct RequestParam {
    pub method: reqwest::Method,
    pub path: String,
    pub headers: reqwest::header::HeaderMap,
    pub body: serde_json::Value,
}

impl RequestParam {
    pub fn new_mp(method: reqwest::Method, path: &str) -> Self {
        RequestParam {
            method: method,
            path: path.to_string(),
            headers: Default::default(),
            body: Default::default(),
        }
    }

    pub fn new_mpb(method: reqwest::Method, path: &str, body: serde_json::Value) -> Self {
        RequestParam {
            method: method,
            path: path.to_string(),
            headers: Default::default(),
            body: body,
        }
    }

    pub fn new_mphb(
        method: reqwest::Method,
        path: &str,
        headers: reqwest::header::HeaderMap,
        body: serde_json::Value,
    ) -> Self {
        RequestParam {
            method: method,
            path: path.to_string(),
            headers: headers,
            body: body,
        }
    }
}

#[async_trait]
pub trait RestApiTrait: Send + Sync + 'static {
    async fn sign(
        &self,
        context: &ExchangeContextPtr,
        param: RequestParam,
    ) -> anyhow::Result<RequestParam>;
    async fn request(
        &self,
        context: &ExchangeContextPtr,
        param: RequestParam,
    ) -> anyhow::Result<(serde_json::Value, PacketTime)>;

    async fn request_market(
        &self,
        context: &ExchangeContextPtr,
    ) -> anyhow::Result<DataSet<Market, MarketKind>>;
    async fn request_wallet(
        &self,
        context: &ExchangeContextPtr,
        optional: &str,
    ) -> anyhow::Result<DataSet<Asset>>;
    async fn request_position(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> anyhow::Result<HashMap<MarketKind, PositionSet>>;

    async fn request_orderbook(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        quantity: SubscribeQuantity,
    ) -> anyhow::Result<OrderBook>;

    async fn request_order_submit(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> anyhow::Result<OrderResult>;
    async fn request_order_search(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> anyhow::Result<OrderSet>;
    async fn request_order_opened(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
    ) -> anyhow::Result<OrderSet>;
    async fn request_order_cancel(
        &self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> anyhow::Result<OrderResult>;
    async fn request_order_query(
        &self,
        context: &ExchangeContextPtr,
        params: &OrderSet,
    ) -> anyhow::Result<OrderResult>;

    fn support_amend(&self, _context: &ExchangeContextPtr) -> bool {
        false
    }

    async fn request_order_amend(
        &self,
        context: &ExchangeContextPtr,
        market: &MarketPtr,
        params: &[(OrderPtr, AmendParam)],
    ) -> anyhow::Result<OrderResult>;

    async fn req_async(
        &self,
        context: &ExchangeContextPtr,
        param: RequestParam,
    ) -> anyhow::Result<(reqwest::Response, PacketTime)>
    where
        Self: Sized,
    {
        let signed_result = self.sign(&context, param).await?;
        let fullpath = if signed_result.method == reqwest::Method::GET
            || signed_result.method == reqwest::Method::DELETE
        {
            if signed_result.body.is_null() {
                format!("{}{}", context.param.restapi.url, signed_result.path)
            } else {
                let urlcode = json::url_encode(&signed_result.body)?;
                format!(
                    "{}{}?{}",
                    context.param.restapi.url, signed_result.path, urlcode
                )
            }
        } else {
            format!("{}{}", context.param.restapi.url, signed_result.path)
        };

        cassry::debug!(
            "requesting method({:?}) url({}), body({})",
            signed_result.method,
            &fullpath,
            &signed_result.body.to_string()
        );
        let builder = match signed_result.method {
            reqwest::Method::GET => {
                let b = context.requester.get(&fullpath);
                Ok(b)
            }
            reqwest::Method::PUT => {
                let b = context.requester.put(&fullpath).json(&signed_result.body);
                Ok(b)
            }
            reqwest::Method::DELETE => {
                let b = context
                    .requester
                    .delete(&fullpath)
                    .json(&signed_result.body);
                Ok(b)
            }
            reqwest::Method::POST => {
                let b = context.requester.post(&fullpath).json(&signed_result.body);
                Ok(b)
            }
            _ => Err(anyhowln!(
                "invalid method: {}",
                signed_result.method.to_string()
            )),
        }?;

        let mut time = PacketTime::default();
        let res = builder.headers(signed_result.headers).send().await?;
        time.recvtime = Utc::now();

        // let date = res.headers().get("date").unwrap().to_str()?;
        // let datetime = chrono::DateTime::parse_from_rfc2822(date)?;
        // time.proctime = Utc.from_utc_datetime(&datetime.naive_utc());

        if context.param.config.eject < time.laytency() {
            return Err(anyhowln!(
                "occur eject {}({})",
                fullpath.to_string(),
                time.laytency().as_seconds_f64()
            ));
        }

        return Ok((res, time));
    }
}

pub struct Exchange {
    context: ExchangeContextPtr,

    restapi: Arc<dyn RestApiTrait>,
    websocket: ExchangeSocket,
}

#[derive(Debug, Clone)]
struct CreateFlag {
    flags: Arc<Mutex<u64>>,
}

bitflags! {
    struct ExchangeCreateOpt: u64 {
        const CheckWallet = 0b00000001;
        const CheckAuth = 0b00000010;
        const IsDirty = 0b00000100;

    }
}

impl CreateFlag {
    fn new() -> Self {
        Self {
            flags: Arc::new(Mutex::new(0)),
        }
    }

    pub async fn set_flag(&mut self, flag: ExchangeCreateOpt, value: bool) {
        let mut locked = self.flags.lock().await;
        if value {
            *locked |= flag.bits();
        } else {
            *locked &= !flag.bits();
        }
    }

    pub async fn has_flag(&self, flag: u64) -> bool {
        let locked = self.flags.lock().await;
        *locked & flag == flag
    }
}

impl Exchange {
    pub fn get_websocket(&self) -> ExchangeSocket {
        self.websocket.clone()
    }

    pub async fn new<ResAPI, Websocket>(
        option: ExchangeParam,
        recorder: localdb::LocalDB,
        builder: Option<ClientBuilder>,
    ) -> anyhow::Result<Arc<Self>>
    where
        Websocket: ExchangeSocketTrait + Default + 'static,
        ResAPI: RestApiTrait + Default,
    {
        let client = builder
            .unwrap_or_else(|| {
                reqwest::Client::builder()
                    .pool_max_idle_per_host(10)
                    .pool_idle_timeout(Some(std::time::Duration::from_secs(30)))
                    .http2_prior_knowledge()
                    .gzip(true)
                    .use_rustls_tls()
            })
            .build()?;
        let context = Arc::new(ExchangeContext::new(option, recorder, client));

        let restapi = ResAPI::default();
        if let Err(e) = restapi.request_wallet(&context, "").await {
            cassry::error!(
                "cannot request wallet to check apikey during initializing : {}",
                e.to_string()
            );
        }
        *context.storage.markets.write().await = restapi.request_market(&context).await?;

        let restapi = Arc::new(restapi);
        let is_connected = CreateFlag::new();
        let is_connected_2 = is_connected.clone();
        let restapi_wpt = Arc::downgrade(&restapi);
        let context_wpt = Arc::downgrade(&context);
        let callback = move |signal, subsicrebe| {
            let mut is_connected = is_connected_2.clone();
            let restapi_wpt = restapi_wpt.clone();
            let context_wpt = context_wpt.clone();
            async move {
                if let Some(context_ptr) = context_wpt.upgrade() {
                    match &signal {
                        Signal::Received(m) => {
                            if let Message::Pong(_) = m {
                                cassry::trace!(
                                    "signal({:?}) subsribe({})",
                                    &signal,
                                    serde_json::to_string(&subsicrebe).unwrap_or("".to_string())
                                );
                            }
                        }
                        Signal::Opened => {
                            if let Some(restapi) = restapi_wpt.upgrade() {
                                // 간혹 restapi로 snapshot을 받고 weboscket으로 업데이트 하는 경우가 있어서 request wallet을 해준다.
                                let ret = match restapi.request_wallet(&context_ptr, "").await {
                                    anyhow::Result::Ok(wallet) => {
                                        context_ptr.update(SubscribeResult::Balance(wallet)).await
                                    }
                                    anyhow::Result::Err(e) => Err(e),
                                };

                                if let Err(e) = ret {
                                    cassry::error!("cannot initilize because occured error for connecting : {}", e.to_string());
                                    is_connected
                                        .set_flag(ExchangeCreateOpt::IsDirty, true)
                                        .await;
                                } else {
                                    is_connected
                                        .set_flag(ExchangeCreateOpt::CheckWallet, true)
                                        .await;
                                }
                            }
                        }
                        Signal::Closed => {
                            let mut locked = context_ptr.storage.positions.write().await;
                            *locked = HashMap::<MarketKind, PositionSet>::default();
                            drop(locked);

                            let mut locked = context_ptr.storage.assets.write().await;
                            *locked =
                                DataSet::<Asset>::new(Default::default(), UpdateType::Snapshot);
                            drop(locked);
                        }
                        _ => {}
                    }

                    if let SubscribeResult::Err(e) = &subsicrebe {
                        cassry::error!("SubscribeResult::Err : {}", e.to_string());
                    }
                    else if let SubscribeResult::Authorized(success) = &subsicrebe {
                        if *success {
                            is_connected
                                .set_flag(ExchangeCreateOpt::CheckAuth, true)
                                .await;
                        } else {
                            cassry::error!(
                                "cannot initilize because failed authorize for connecting."
                            );
                            is_connected
                                .set_flag(ExchangeCreateOpt::IsDirty, true)
                                .await;
                        }
                    }

                    if let Err(e) = context_ptr.update(subsicrebe).await {
                        cassry::error!("occur error for updating : {}", e.to_string());
                    };
                }
            }
        };

        let sp = SubscribeParam::balance().build();
        let ws = ExchangeSocket::new::<Websocket, _, _>(context.clone(), callback).await?;
        if ws.is_subscribed(&sp).await.is_some() {
            ws.subscribe(sp).await?;
            loop {
                if is_connected
                    .has_flag(ExchangeCreateOpt::IsDirty.bits())
                    .await
                {
                    return Err(anyhowln!(
                        "invalid auth or wallet for connecting websockets"
                    ));
                } else if is_connected
                    .has_flag(
                        ExchangeCreateOpt::CheckWallet.bits() | ExchangeCreateOpt::CheckAuth.bits(),
                    )
                    .await
                {
                    cassry::info!("success that connect websocket : url({}), check-wallet(true)  check-auth(true)", context.param.websocket.url);
                    break;
                }

                cassry::info!(
                    "waiting for creating websocket : url({}), , check-wallet({})  check-auth({})",
                    context.param.websocket.url,
                    is_connected
                        .has_flag(ExchangeCreateOpt::CheckWallet.bits())
                        .await,
                    is_connected
                        .has_flag(ExchangeCreateOpt::CheckAuth.bits())
                        .await
                );
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }

        let ret = Arc::new(Exchange {
            context: context,
            restapi,
            websocket: ws,
        });

        Ok(ret)
    }

    pub async fn support_amend(&self) -> bool {
        self.restapi.support_amend(&self.context)
    }

    pub async fn get_markets(&self) -> Vec<MarketKind> {
        let lock = self.get_storage().markets.read().await;
        lock.get_datas()
            .values()
            .map(|v| &v.kind)
            .collect::<HashSet<_>>()
            .iter()
            .map(|k| (**k).clone())
            .collect::<Vec<_>>()
    }

    pub async fn find_market(&self, kind: &MarketKind) -> Option<MarketPtr> {
        let lock = self.get_storage().markets.read().await;
        lock.get_datas().get(kind).cloned()
    }

    pub fn get_option(&self) -> &ExchangeParam {
        &self.context.param
    }

    fn get_storage(&self) -> &ExchangeStorage {
        &self.context.storage
    }

    pub fn get_context(&self) -> ExchangeContextPtr {
        self.context.clone()
    }

    pub async fn request_wallet(&self, optional: &str) -> anyhow::Result<DataSet<Asset>> {
        let sparam = SubscribeParam::balance().build();

        let (is_subscribed, need_subscribe) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&sparam).await {
                (
                    self.websocket.is_connected_all().await && is_subscribed,
                    !is_subscribed,
                )
            } else {
                (false, false)
            };
        let ret = if is_subscribed {
            let locked = self.get_storage().assets.read().await;
            cassry::debug!(
                "imported asset from storage : date({}), laytency({})",
                locked.get_packet_time().recvtime.to_string(),
                locked.get_packet_time().laytency().as_seconds_f64()
            );
            locked.clone()
        } else {
            let ret = self.restapi.request_wallet(&self.context, optional).await?;
            ret
        };

        if need_subscribe {
            self.websocket.subscribe(sparam).await?;
        }

        Ok(ret)
    }

    pub async fn request_position(
        &self,
        market: &MarketPtr,
    ) -> anyhow::Result<HashMap<MarketKind, PositionSet>> {
        let sparam = SubscribeParam::position().market(market.clone()).build();
        let (is_subscribed, need_subscribe, is_support) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&sparam).await {
                (
                    self.websocket.is_connected_all().await && is_subscribed,
                    !is_subscribed,
                    true,
                )
            } else {
                (false, false, false)
            };

        let imported_ret = if is_subscribed {
            let locked = self.get_storage().positions.read().await;
            if let Some(value) = locked.get(&market.kind) {
                cassry::debug!(
                    "imported position from storage : date({}), laytency({})",
                    value.get_packet_time().recvtime.to_string(),
                    value.get_packet_time().laytency().as_seconds_f64()
                );
                anyhow::Result::Ok(locked.clone())
            } else {
                cassry::debug!(
                    "imported position from storage but cannot find market({}).",
                    serde_json::to_string(&market.kind).unwrap_or(String::default())
                );

                anyhow::Result::Err(locked.clone())
            }
        } else {
            anyhow::Result::Err(HashMap::<MarketKind, PositionSet>::new())
        };

        let ret = match imported_ret {
            anyhow::Result::Ok(imported) => imported,
            anyhow::Result::Err(mut imported) => {
                let mut ret = self.restapi.request_position(&self.context, market).await?;

                if is_support {
                    if !ret.contains_key(&market.kind) {
                        ret.insert(
                            market.kind.clone(),
                            PositionSet::new(
                                Default::default(),
                                MarketVal::Pointer(market.clone()),
                            ),
                        );
                    }

                    self.context
                        .update(SubscribeResult::Position(ret.clone()))
                        .await?;
                    cassry::debug!(
                        "synchronized position from requested data to storage : market({})",
                        serde_json::to_string(&market.kind).unwrap_or(String::default())
                    );
                }

                imported.extend(ret);
                imported
            }
        };

        if need_subscribe {
            self.websocket.subscribe(sparam).await?;
        }

        Ok(ret)
    }

    pub async fn request_orderbook(
        &self,
        market: &MarketPtr,
        quantity: SubscribeQuantity,
    ) -> anyhow::Result<OrderBookPtr> {
        let sparam = SubscribeParam::orderbook()
            .market(market.clone())
            .quantity(quantity.clone())
            .speed(SubscribeSpeed::Fastest)
            .build();

        let (is_subscribed, need_subscribe, _is_support) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&sparam).await {
                (
                    self.websocket.is_connected_all().await && is_subscribed,
                    !is_subscribed,
                    true,
                )
            } else {
                (false, false, false)
            };

        let ret = if is_subscribed {
            let locked = self.context.storage.orderbook.read().await;
            if let Some(orderbook) = locked.get(&market.kind).cloned() {
                Some(orderbook)
            } else {
                let keys = locked.values().collect::<Vec<_>>();
                let str = serde_json::to_string(&keys)?;
                println!("{}", str);
                None
            }
        } else {
            None
        };

        let ptr = if let Some(ptr) = ret {
            cassry::debug!(
                "imported orderbook from storage : symbol({}), date({}), laytency({}) unsynced({})",
                ptr.market.symbol(),
                ptr.updated.to_string(),
                ptr.get_packet_time().laytency().as_seconds_f64(),
                (Utc::now() - ptr.updated).as_seconds_f64()
            );
            ptr
        } else {
            let data = self
                .restapi
                .request_orderbook(&self.context, market, quantity)
                .await?;
            Arc::new(data)
        };

        if need_subscribe {
            self.websocket.subscribe(sparam).await?;
        }
        Ok(ptr)
    }
    pub async fn request_order_submit(
        &self,
        market: &MarketPtr,
        params: &[OrderParam],
    ) -> anyhow::Result<OrderResult> {
        if params.is_empty() {
            return Err(anyhowln!("param is empty"));
        }

        let sparam = SubscribeParam::order().market(market.clone()).build();
        if !self.websocket.is_subscribed(&sparam).await.unwrap_or(true) {
            self.websocket.subscribe(sparam).await?;
        }

        let ret = self
            .restapi
            .request_order_submit(&self.context, market, params)
            .await?;
        Ok(ret)
    }

    pub async fn request_order_amend(
        &self,
        market: &MarketPtr,
        params: &[(OrderPtr, AmendParam)],
    ) -> anyhow::Result<OrderResult> {
        if params.is_empty() {
            return Err(anyhowln!("param is empty"));
        }

        let sparam = SubscribeParam::order().market(market.clone()).build();
        if !self.websocket.is_subscribed(&sparam).await.unwrap_or(true) {
            self.websocket.subscribe(sparam).await?;
        }

        let ret = self
            .restapi
            .request_order_amend(&self.context, market, params)
            .await?;
        Ok(ret)
    }

    pub async fn request_order_search(
        &self,
        market: &MarketPtr,
        param: &OrdSerachParam,
    ) -> anyhow::Result<OrderSet> {
        let sparam = SubscribeParam::order().market(market.clone()).build();
        if !self.websocket.is_subscribed(&sparam).await.unwrap_or(true) {
            self.websocket.subscribe(sparam).await?;
        }

        let ret = self
            .restapi
            .request_order_search(&self.context, market, param)
            .await?;
        Ok(ret)
    }

    pub async fn request_order_opened(&self, market: &MarketPtr) -> anyhow::Result<OrderSet> {
        let sparam = SubscribeParam::order().market(market.clone()).build();
        if !self.websocket.is_subscribed(&sparam).await.unwrap_or(true) {
            self.websocket.subscribe(sparam).await?;
        }

        let ret = self
            .restapi
            .request_order_opened(&self.context, market)
            .await?;
        Ok(ret)
    }

    pub async fn request_order_cancel(&self, params: &OrderSet) -> anyhow::Result<OrderResult> {
        let market = params
            .market_ptr()
            .ok_or(anyhowln!("the market of order must be point type"))
            .cloned()?;
        let sparam = SubscribeParam::order().market(market.clone()).build();
        if !self.websocket.is_subscribed(&sparam).await.unwrap_or(true) {
            self.websocket.subscribe(sparam).await?;
        }

        let (mut restapi_param, already_synced) =
            params.filter_new(|(_key, value)| value.state.cancelable());
        if restapi_param.get_datas().is_empty() {
            for (key, value) in already_synced {
                restapi_param.insert_with_key(key, value);
            }
            return Ok(OrderResult::new_with_orders(restapi_param));
        }

        let mut ret = self
            .restapi
            .request_order_cancel(&self.context, &restapi_param)
            .await?;

        for (key, value) in already_synced {
            ret.success.insert_with_key(key, value);
        }

        Ok(ret)
    }

    pub async fn request_order_query(&self, params: &OrderSet) -> anyhow::Result<OrderResult> {
        // order subscribe 호출
        let market = params
            .market_ptr()
            .cloned()
            .ok_or(anyhowln!("the market of order must be point type"))?;
        let sparam = SubscribeParam::order().market(market.clone()).build();

        let (is_subscribed, need_subscribe, is_support) =
            if let Some(is_subscribed) = self.websocket.is_subscribed(&sparam).await {
                (
                    self.websocket.is_connected_all().await && is_subscribed,
                    !is_subscribed,
                    true,
                )
            } else {
                (false, false, false)
            };

        let (mut restapi_param, mut already_synced) =
            params.filter_new(|(_key, value)| value.state.synchronizable());

        if is_subscribed {
            let mut remover = Vec::<String>::default();
            for oid in restapi_param.get_datas().keys() {
                let result = self.context.find_order(&oid, &market.kind).await?;
                if let Some(order) = result {
                    remover.push(oid.clone());
                    already_synced.insert(oid.clone(), order.clone());
                }
            }

            for key in remover {
                restapi_param.remove(&key);
            }
        }

        // 동기화 할 것이 없다면 반환
        let mut ret = if restapi_param.get_datas().is_empty() {
            OrderResult::new_with_orders(restapi_param)
        } else {
            let ret = self
                .restapi
                .request_order_query(&self.context, &restapi_param)
                .await?;

            if is_support {
                let sb = HashMap::<MarketKind, OrderSet>::from([(
                    market.kind.clone(),
                    ret.success.clone(),
                )]);
                self.context.update(SubscribeResult::Order(sb)).await?;
            }

            ret
        };

        for (key, value) in already_synced {
            ret.success.insert_with_key(key, value);
        }

        // Ordering은 아직 query가 되지 않는다면 Ordering으로 변환
        // Canceling은 아직 Canceled가 되지 않았다면 Canceling으로 변환
        // 이후는 restapi 결과 값을 그대로 받는다.
        let proccessing = params.get_datas().values().filter(|value| {
            value.state.is_ordering_or_canceling()
                && !value
                    .state
                    .is_expired(&self.get_option().config.state_expired_duration)
        });
        for value in proccessing {
            match &value.state {
                OrderState::Canceling(_created) => {
                    if let Some(ptr) = ret.success.get_datas_mut().get_mut(&value.oid) {
                        if !ptr.state.is_cancelled() {
                            let mut newer = ptr.as_ref().clone();
                            newer.state = value.state.clone();
                            *ptr = Arc::new(newer);
                        }
                    }
                }
                OrderState::Ordering(..) => {
                    if ret.errors.contains_key(&value.oid) {
                        ret.success.insert(value.clone());
                        ret.errors.remove(&value.oid);
                    }
                }
                _ => {}
            }
        }

        if need_subscribe {
            self.websocket.subscribe(sparam).await?;
        }
        Ok(ret)
    }
}

pub type ExchangeArc = Arc<Exchange>;
