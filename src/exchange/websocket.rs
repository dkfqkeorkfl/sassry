use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, SubAssign},
    sync::Arc,
};

use anyhow::Ok;
use async_trait::async_trait;
use chrono::Utc;
use futures::future::BoxFuture;
use tokio::sync::RwLock;

use super::super::webserver::websocket::*;
use super::protocols::*;
use cassry::*;

#[derive(Clone)]
pub struct SubscribeCallbackHelper {
    pub callback:
        Arc<dyn Fn(Signal, SubscribeResult) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
}

impl SubscribeCallbackHelper {
    pub fn new(
        f: impl Fn(Signal, SubscribeResult) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        SubscribeCallbackHelper {
            callback: Arc::new(f),
        }
    }
}

#[async_trait]
pub trait ExchangeSocketTrait: Send + Sync {
    async fn parse_msg(
        &self,
        context: &ExchangeContextPtr,
        socket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult>;

    async fn subscribe(
        &self,
        context: &ExchangeContextPtr,
        client: Websocket,
        s: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<()>;

    async fn make_websocket_param(
        &self,
        context: &ExchangeContextPtr,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<WebsocketParam>;

    async fn make_group_and_key(
        &self,
        s: &SubscribeType,
        param: &SubscribeParam,
    ) -> Option<(String, String)> {
        let default = serde_json::Value::default();
        let json = match s {
            SubscribeType::Balance | SubscribeType::Position => Some(&default),
            SubscribeType::Order | SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                Some(&param.0)
            }
        };

        if let Some(j) = json {
            let str = format!("{}:{}", s.clone() as u32, j.to_string());
            return Some(("".to_string(), str));
        }

        None
    }
}

struct ConnectionInfo {
    pub checkedtime: chrono::DateTime<Utc>,
    pub retryed: u32,
    pub websocket: Websocket,
    pub subscribes: HashMap<SubscribeType, Vec<SubscribeParam>>,
    pub is_authorized: bool,
}
type ConnectionInfoRwArc = RwArc<ConnectionInfo>;

impl ConnectionInfo {
    pub fn new(websocket: Websocket) -> Self {
        Self {
            checkedtime: Utc::now(),
            retryed: 0,
            websocket: websocket,
            subscribes: Default::default(),
            is_authorized: false,
        }
    }
    pub fn exchange_websocket(&mut self, websocket: Websocket) -> Websocket {
        let previous = self.websocket.clone();
        self.websocket = websocket;
        self.checkedtime = Utc::now();
        self.retryed = 0;
        self.is_authorized = false;
        previous
    }
}

struct Shared {
    pub context: ExchangeContextPtr,
    pub websockets: RwLock<HashMap<String, ConnectionInfoRwArc>>,
    pub websockets_by_id: RwLock<HashMap<String, ConnectionInfoRwArc>>,
    pub interface: Arc<dyn ExchangeSocketTrait>,
    pub connected_cnt: Arc<RwLock<usize>>,
    pub callback_helper: SubscribeCallbackHelper,
}

struct Inner {
    shared: Arc<Shared>,
    subscribes: RwLock<HashSet<String>>,
}

macro_rules! make_receive_callback {
    ($ptr:expr) => {{
        let wpt = std::sync::Arc::downgrade(&$ptr);
        move |websocket, signal| {
            let cloned_wpt = wpt.clone();
            async move {
                if let Some(spt) = cloned_wpt.upgrade() {
                    let result = spt
                        .on_msg(websocket, &signal)
                        .await
                        .unwrap_or_else(SubscribeResult::from);
                    (spt.callback_helper.callback)(signal, result).await;
                }
            }
        }
    }};
}

impl Shared {
    async fn on_msg(
        &self,
        websocket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        match signal {
            Signal::Opened => {
                self.connected_cnt.write().await.add_assign(1);

                cassry::info!(
                    "opened websocket(total:{}, id:{}) : {}",
                    self.connected_cnt.read().await,
                    websocket.get_uuid(),
                    websocket.get_param().unwrap_or_default().url
                );
            }
            Signal::Closed => {
                self.connected_cnt.write().await.sub_assign(1);
                cassry::info!(
                    "closed websocket(total:{}: id:{}) : {}",
                    self.connected_cnt.read().await,
                    websocket.get_uuid(),
                    websocket.get_param().unwrap_or_default().url
                );
            }
            _ => {}
        }

        let result = self
            .interface
            .parse_msg(&self.context, websocket.clone(), signal)
            .await?;

        match &result {
            SubscribeResult::Authorized(success) => {
                cassry::info!(
                    "authorized websocket : {}",
                    websocket.get_param().unwrap_or_default().url
                );

                let id = websocket.get_uuid();
                if let Some(info) = self.find_websocket_by_id(&id).await {
                    let mut locked = info.write().await;
                    if *success {
                        locked.is_authorized = *success;
                        self.interface
                            .subscribe(&self.context, websocket, &locked.subscribes)
                            .await?;
                    } else {
                        locked.websocket.close(None).await?;
                    }
                }
            }
            _ => {}
        }

        Ok(result)
    }

    pub fn new<Interface>(
        context: ExchangeContextPtr,
        interface: Interface,
        callback: SubscribeCallbackHelper,
    ) -> Arc<Self>
    where
        Interface: ExchangeSocketTrait + Default + 'static,
    {
        Arc::new(Shared {
            context: context,
            websockets: Default::default(),
            websockets_by_id: Default::default(),
            interface: Arc::new(interface),
            connected_cnt: Arc::new(RwLock::new(0)),
            callback_helper: callback,
        })
    }

    pub async fn insert_websocket(
        &self,
        group: String,
        info: ConnectionInfo,
    ) -> Option<ConnectionInfoRwArc> {
        let id = info.websocket.get_uuid().to_string();
        let ptr = Arc::new(RwLock::new(info));
        self.websockets_by_id.write().await.insert(id, ptr.clone());
        self.websockets.write().await.insert(group, ptr)
    }

    pub async fn find_websocket(&self, group: &str) -> Option<ConnectionInfoRwArc> {
        self.websockets.read().await.get(group).cloned()
    }

    pub async fn find_websocket_by_id(&self, id: &str) -> Option<ConnectionInfoRwArc> {
        self.websockets_by_id.read().await.get(id).cloned()
    }

    pub async fn get_connected_cnt(&self) -> usize {
        self.connected_cnt.read().await.clone()
    }

    pub async fn get_websocket_cnt(&self) -> usize {
        self.websockets.read().await.len()
    }
}

impl Inner {
    pub async fn check_eject(ctx: Arc<Shared>) -> anyhow::Result<()> {
        let checktime = &ctx.context.param.config.ping_interval;
        let websockets = ctx.websockets.read().await.clone();

        for (group, value) in &websockets {
            let now = Utc::now();
            let mut info = value.write().await;

            let penalty = if let Some(v) = 2i32.checked_pow(info.retryed) {
                v
            } else {
                1
            };

            let interval = *checktime * penalty;
            // if eject time is 5, result is 5 10 20 40 80 160 320;
            let dur = now - info.checkedtime;
            if interval > dur {
                continue;
            }

            info.checkedtime = Utc::now();
            if info.websocket.is_connected().await {
                continue;
            }

            cassry::info!(
                "it is disconnected websocket : url({}), is_connected({})",
                info.websocket.get_param().unwrap_or_default().url,
                info.websocket.is_connected().await
            );

            let ws_param = ctx
                .interface
                .make_websocket_param(&ctx.context, &group, &info.subscribes)
                .await;

            let ws_param = match ws_param {
                std::result::Result::Ok(param) => param,
                Err(e) => {
                    info.retryed += 1;
                    cassry::error!("occur an error for reconnecting : {}", e);
                    continue;
                }
            };

            match Websocket::connect(ws_param, make_receive_callback!(&ctx)).await {
                std::result::Result::Ok(websocket) => {
                    info.exchange_websocket(websocket);
                }
                Err(e) => {
                    info.retryed += 1;
                    cassry::error!("occur an error for reconnecting : {}", e);
                    continue;
                }
            };
        }

        Ok(())
    }

    pub fn get_exchange_context(&self) -> &ExchangeContextPtr {
        &self.shared.context
    }

    pub async fn is_connected(&self) -> bool {
        self.shared.get_connected_cnt().await == self.shared.get_websocket_cnt().await
    }

    pub async fn insert_websocket(
        &self,
        group: String,
        info: ConnectionInfo,
    ) -> Option<ConnectionInfoRwArc> {
        self.shared.insert_websocket(group, info).await
    }

    pub async fn find_websocket(&self, group: &str) -> Option<ConnectionInfoRwArc> {
        self.shared.find_websocket(group).await
    }

    pub async fn new<Interface>(
        context: ExchangeContextPtr,
        callback: SubscribeCallbackHelper,
    ) -> anyhow::Result<Arc<RwLock<Inner>>>
    where
        Interface: ExchangeSocketTrait + Default + 'static,
    {
        let socketcheck = context.param.config.ping_interval.clone();
        let shared = Shared::new::<Interface>(context, Default::default(), callback);
        let cloned_context = Arc::downgrade(&shared);
        tokio::spawn(async move {
            let interval = socketcheck.to_std().unwrap();
            loop {
                tokio::time::sleep(interval).await;

                if let Some(spt) = cloned_context.upgrade() {
                    if let Err(e) = Inner::check_eject(spt).await {
                        cassry::error!("occur error for check ping : {}", e.to_string());
                    }
                } else {
                    break;
                }
            }
        });

        let exchange = Inner {
            shared,
            subscribes: HashSet::<String>::default().into(),
        };

        Ok(Arc::new(RwLock::new(exchange)))
    }

    async fn make_websocket(
        &self,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<SubscribeParam>>,
    ) -> anyhow::Result<ConnectionInfo> {
        let client_param = self
            .shared
            .interface
            .make_websocket_param(&self.get_exchange_context(), group, &subscribes)
            .await?;
        let websocket =
            Websocket::connect(client_param, make_receive_callback!(&self.shared)).await?;
        let info = ConnectionInfo::new(websocket);
        Ok(info)
    }

    async fn is_subscribed(&self, s: &SubscribeType, param: &SubscribeParam) -> Option<bool> {
        let (_, key) = self.shared.interface.make_group_and_key(s, param).await?;
        Some(self.subscribes.read().await.contains(&key))
    }

    async fn subscribe(&self, s: SubscribeType, param: SubscribeParam) -> anyhow::Result<()> {
        let (group, key) = self
            .shared
            .interface
            .make_group_and_key(&s, &param)
            .await
            .ok_or(anyhowln!("This is an unsupported feature."))?;

        let mut subscribes = self.subscribes.write().await;
        if subscribes.contains(&key) {
            return Err(anyhowln!("already subscribed"));
        }

        cassry::info!(
            "proccessing subscribe :({}){}",
            serde_json::to_string(&s).unwrap(),
            serde_json::to_string(&param.0).unwrap()
        );
        let mut params = HashMap::from([(s.clone(), vec![param])]);

        let info = if let Some(websocket) = self.find_websocket(&group).await {
            websocket
        } else {
            cassry::info!("Open websocket to subscribe : {}", &group);
            let websocket = self.make_websocket(&group, &params).await?;
            self.insert_websocket(group.clone(), websocket).await;
            let result = self
                .find_websocket(&group)
                .await
                .ok_or(anyhowln!("occur error that insert client{}", group))?;
            result
        };

        {
            let mut locked = info.write().await;
            if locked.is_authorized {
                self.shared
                    .interface
                    .subscribe(&self.get_exchange_context(), locked.websocket.clone(), &params)
                    .await?;
            }

            cassry::info!("success subscribe : {:?}", &s);
            if let Some(v) = locked.subscribes.get_mut(&s) {
                if let Some(value) = params.values_mut().next() {
                    v.extend(value.drain(..));
                }
            } else {
                locked.subscribes.extend(params);
            }
        }

        subscribes.insert(key);
        Ok(())
    }
}

pub struct ExchangeSocket {
    ptr: Arc<RwLock<Inner>>,
}

impl ExchangeSocket {
    pub async fn new<Interface>(
        context: ExchangeContextPtr,
        callback: SubscribeCallbackHelper,
    ) -> anyhow::Result<Self>
    where
        Interface: ExchangeSocketTrait + Default + 'static,
    {
        let ptr = Inner::new::<Interface>(context, callback).await?;
        Ok(ExchangeSocket { ptr })
    }

    pub async fn is_connected(&self) -> bool {
        self.ptr.read().await.is_connected().await
    }

    pub async fn is_subscribed(&self, s: &SubscribeType, param: &SubscribeParam) -> Option<bool> {
        self.ptr.read().await.is_subscribed(s, param).await
    }

    pub async fn subscribe(&self, s: SubscribeType, param: SubscribeParam) -> anyhow::Result<()> {
        self.ptr.read().await.subscribe(s, param).await?;
        Ok(())
    }
}
