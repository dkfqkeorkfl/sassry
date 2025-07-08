use std::{
    collections::{HashMap, HashSet},
    ops::{AddAssign, SubAssign},
    sync::Arc,
};

use anyhow::Ok;
use async_trait::async_trait;
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt};
use tokio::sync::RwLock;

use super::super::webserver::websocket::*;
use super::protocols::*;
use cassry::*;

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
        s: &HashMap<SubscribeType, Vec<serde_json::Value>>,
    ) -> anyhow::Result<()>;

    async fn make_websocket_param(
        &self,
        context: &ExchangeContextPtr,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<serde_json::Value>>,
    ) -> anyhow::Result<WebsocketParam>;

    async fn make_group_and_key(
        &self,
        param: &SubscribeParam,
    ) -> Option<(String, String)> {
        let default = serde_json::Value::default();
        let json = match param.stype {
            SubscribeType::Balance | SubscribeType::Position => Some(&default),
            SubscribeType::Order | SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                Some(&param.value)
            }
        };

        if let Some(j) = json {
            let str = format!("{}:{}", param.stype.clone() as u32, j.to_string());
            return Some(("".to_string(), str));
        }

        None
    }
}

struct Connection {
    pub checkedtime: chrono::DateTime<Utc>,
    pub retryed: u32,
    pub websocket: Websocket,
    pub subscribes: HashMap<SubscribeType, Vec<serde_json::Value>>,
    pub is_authorized: bool,
}
type ConnectionRwArc = RwArc<Connection>;

impl Connection {
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

struct Inner {
    pub context: ExchangeContextPtr,
    pub interface: Arc<dyn ExchangeSocketTrait>,
    pub callback:
        Arc<dyn Fn(Signal, SubscribeResult) -> BoxFuture<'static, ()> + Send + Sync + 'static>,

    pub subscribes: RwLock<HashSet<String>>,
    pub alived_cnt: Arc<RwLock<usize>>,
    pub connections: RwLock<HashMap<String, ConnectionRwArc>>,
    pub connections_by_id: RwLock<HashMap<String, ConnectionRwArc>>,
}

impl Inner {
    async fn on_msg(
        &self,
        websocket: Websocket,
        signal: &Signal,
    ) -> anyhow::Result<SubscribeResult> {
        match signal {
            Signal::Opened => {
                self.alived_cnt.write().await.add_assign(1);

                cassry::info!(
                    "opened websocket(total:{}, id:{}) : {}",
                    self.alived_cnt.read().await,
                    websocket.get_uuid(),
                    websocket.get_param().unwrap_or_default().url
                );
            }
            Signal::Closed => {
                self.alived_cnt.write().await.sub_assign(1);
                cassry::info!(
                    "closed websocket(total:{}: id:{}) : {}",
                    self.alived_cnt.read().await,
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
        callback: Arc<
            dyn Fn(Signal, SubscribeResult) -> BoxFuture<'static, ()> + Send + Sync + 'static,
        >,
    ) -> Arc<Self>
    where
        Interface: ExchangeSocketTrait + Default + 'static,
    {
        Arc::new(Inner {
            context: context,
            interface: Arc::new(Interface::default()),
            callback: callback,

            subscribes: Default::default(),
            alived_cnt: Arc::new(RwLock::new(0)),
            connections: Default::default(),
            connections_by_id: Default::default(),
        })
    }

    pub async fn insert_connection(
        &self,
        group: String,
        info: Connection,
    ) -> Option<ConnectionRwArc> {
        let id = info.websocket.get_uuid().to_string();
        let ptr = Arc::new(RwLock::new(info));
        self.connections_by_id.write().await.insert(id, ptr.clone());
        self.connections.write().await.insert(group, ptr)
    }

    pub async fn find_connection(&self, group: &str) -> Option<ConnectionRwArc> {
        self.connections.read().await.get(group).cloned()
    }

    pub async fn find_websocket_by_id(&self, id: &str) -> Option<ConnectionRwArc> {
        self.connections_by_id.read().await.get(id).cloned()
    }

    pub async fn count_alived(&self) -> usize {
        self.alived_cnt.read().await.clone()
    }

    pub async fn get_connection_cnt(&self) -> usize {
        self.connections.read().await.len()
    }
}

#[derive(Clone)]
pub struct ExchangeSocket {
    inner: Arc<Inner>,
}

impl ExchangeSocket {
    async fn check_eject(inner: Arc<Inner>) -> anyhow::Result<()> {
        let checktime = &inner.context.param.config.ping_interval;
        let websockets = inner.connections.read().await.clone();

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

            let ws_param = inner
                .interface
                .make_websocket_param(&inner.context, &group, &info.subscribes)
                .await;

            let ws_param = match ws_param {
                std::result::Result::Ok(param) => param,
                Err(e) => {
                    info.retryed += 1;
                    cassry::error!("occur an error for reconnecting : {}", e);
                    continue;
                }
            };

            match ExchangeSocket::make_websocket(&inner, ws_param).await {
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

    pub async fn is_connected_all(&self) -> bool {
        self.inner.count_alived().await == self.inner.get_connection_cnt().await
    }

    pub async fn new<Interface, F, Fut>(
        context: ExchangeContextPtr,
        callback: F,
    ) -> anyhow::Result<Self>
    where
        Interface: ExchangeSocketTrait + Default + 'static,
        F: Fn(Signal, SubscribeResult) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let callback = move |signal, result| callback(signal, result).boxed();
        let interval = context.param.config.ping_interval.to_std()?;
        let inner = Inner::new::<Interface>(context, Arc::new(callback));
        let inner_wpt = Arc::downgrade(&inner);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                if let Some(inner) = inner_wpt.upgrade() {
                    if let Err(e) = ExchangeSocket::check_eject(inner).await {
                        cassry::error!("occur error for check ping : {}", e.to_string());
                    }
                } else {
                    break;
                }
            }
        });

        Ok(ExchangeSocket { inner }.into())
    }

    pub async fn is_subscribed(&self, param: &SubscribeParam) -> Option<bool> {
        let (_, key) = self.inner.interface.make_group_and_key(param).await?;
        Some(self.inner.subscribes.read().await.contains(&key))
    }

    pub async fn subscribe(&self, param: SubscribeParam) -> anyhow::Result<()> {
        let (group, key) = self
            .inner
            .interface
            .make_group_and_key(&param)
            .await
            .ok_or(anyhowln!("This is an unsupported feature."))?;

        let mut subscribes = self.inner.subscribes.write().await;
        if subscribes.contains(&key) {
            return Err(anyhowln!("already subscribed"));
        }

        cassry::info!(
            "proccessing subscribe :({}){}",
            serde_json::to_string(&param.stype).unwrap(),
            serde_json::to_string(&param.value).unwrap()
        );

        let mut params = HashMap::from([(param.stype.clone(), vec![param.value])]);

        let info = if let Some(websocket) = self.find_connection(&group).await {
            websocket
        } else {
            cassry::info!("Open websocket to subscribe : {}", &group);
            let websocket = self.make_connection(&group, &params).await?;
            self.insert_connection(group.clone(), websocket).await;
            let result = self
                .find_connection(&group)
                .await
                .ok_or(anyhowln!("occur error that insert client{}", group))?;
            result
        };

        {
            let mut locked = info.write().await;
            if locked.is_authorized {
                self.inner
                    .interface
                    .subscribe(
                        self.get_exchange_context(),
                        locked.websocket.clone(),
                        &params,
                    )
                    .await?;
            }

            cassry::info!("success subscribe : {:?}", &param.stype);
            if let Some(v) = locked.subscribes.get_mut(&param.stype) {
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

    async fn make_websocket(
        inner: &Arc<Inner>,
        param: WebsocketParam,
    ) -> anyhow::Result<Websocket> {
        let inner = std::sync::Arc::downgrade(inner);
        Websocket::connect(param, move |websocket, signal| {
            let inner = inner.clone();
            async move {
                if let Some(inner) = inner.upgrade() {
                    let result = inner
                        .on_msg(websocket, &signal)
                        .await
                        .unwrap_or_else(SubscribeResult::from);
                    (inner.callback)(signal, result).await;
                }
            }
        })
        .await
    }

    async fn make_connection(
        &self,
        group: &String,
        subscribes: &HashMap<SubscribeType, Vec<serde_json::Value>>,
    ) -> anyhow::Result<Connection> {
        let client_param = self
            .inner
            .interface
            .make_websocket_param(self.get_exchange_context(), group, &subscribes)
            .await?;
        let websocket = ExchangeSocket::make_websocket(&self.inner, client_param).await?;
        Ok(Connection::new(websocket))
    }

    async fn insert_connection(&self, group: String, info: Connection) -> Option<ConnectionRwArc> {
        self.inner.insert_connection(group, info).await
    }

    async fn find_connection(&self, group: &str) -> Option<ConnectionRwArc> {
        self.inner.find_connection(group).await
    }

    fn get_exchange_context(&self) -> &ExchangeContextPtr {
        &self.inner.context
    }
}
