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
        request: &Option<(SubscribeType, serde_json::Value)>,
        subscribed: &HashMap<SubscribeType, Vec<serde_json::Value>>,
    ) -> anyhow::Result<()>;

    async fn make_websocket_param(
        &self,
        context: &ExchangeContextPtr,
        group: &String,
        request: &Option<(SubscribeType, serde_json::Value)>,
    ) -> anyhow::Result<ConnectParams>;

    async fn make_group_and_key(&self, param: &SubscribeParam) -> Option<(String, String)> {
        let default = serde_json::Value::default();
        let json = match param.ty {
            SubscribeType::Balance | SubscribeType::Position => Some(&default),
            SubscribeType::Order | SubscribeType::Orderbook | SubscribeType::PublicTrades => {
                Some(&param.value)
            }
        };

        if let Some(j) = json {
            let str = format!("{}:{}", param.ty.clone() as u32, j.to_string());
            return Some(("".to_string(), str));
        }

        None
    }
}

struct Connection {
    pub group: String,
    pub is_authorized: bool,
    pub subscribes: HashMap<SubscribeType, Vec<serde_json::Value>>,

    pub websocket: Websocket,

    pub lastcheck: chrono::DateTime<Utc>,
    pub retryed: u32,
}
type ConnectionRwArc = RwArc<Connection>;

impl Connection {
    pub fn new(group: String, websocket: Websocket) -> Self {
        Self {
            group: group,
            is_authorized: false,
            subscribes: Default::default(),

            websocket: websocket,

            lastcheck: Utc::now(),
            retryed: 0,
        }
    }
    pub fn update_websocket(&mut self, websocket: Websocket) -> Websocket {
        let previous = self.websocket.clone();
        self.websocket = websocket;
        self.lastcheck = Utc::now();
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
                    websocket.get_connected_url_str().unwrap_or_default()
                );
            }
            Signal::Closed => {
                self.alived_cnt.write().await.sub_assign(1);
                cassry::info!(
                    "closed websocket(total:{}: id:{}) : {}",
                    self.alived_cnt.read().await,
                    websocket.get_uuid(),
                    websocket.get_connected_url_str().unwrap_or_default()
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
                let uuid = websocket.get_uuid();

                cassry::info!(
                    "authorized websocket(uuid:{}, url:{})",
                    uuid,
                    websocket.get_connected_url_str().unwrap_or_default()
                );

                if let Some(conn) = self.find_websocket_by_id(&uuid).await {
                    if *success == false {
                        cassry::error!(
                            "rejected authorize websocket(uuid:{}, url:{})",
                            uuid,
                            websocket.get_connected_url_str().unwrap_or_default()
                        );
                        websocket.close(None).await?;
                    }

                    let mut conn = conn.write().await;
                    if conn.is_authorized == true {
                        cassry::warn!(
                            "websocket is already authorized(uuid:{}, url:{})",
                            uuid,
                            websocket.get_connected_url_str().unwrap_or_default()
                        );
                    }
                    conn.is_authorized = *success;
                    if !conn.subscribes.is_empty() {
                        let subs = conn
                            .subscribes
                            .iter()
                            .map(|(key, value)| {
                                format!(
                                    "({:?}:{})",
                                    key,
                                    value
                                        .iter()
                                        .map(|v| v.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",")
                                )
                            })
                            .collect::<Vec<_>>()
                            .join(",");
                        cassry::info!(
                            "trying to subscribes by stored list(uuid:{}, url:{}): {}",
                            uuid,
                            websocket.get_connected_url_str().unwrap_or_default(),
                            subs
                        );
                        self.interface
                            .subscribe(&self.context, websocket, &None, &conn.subscribes)
                            .await?;
                    }
                } else {
                    cassry::error!(
                        "cannot find subscribing list by id(uuid:{}, url:{})",
                        uuid,
                        websocket.get_connected_url_str().unwrap_or_default()
                    );
                    websocket.close(None).await?;
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

    pub async fn insert_connection(&self, conn: Connection) -> RwArc<Connection> {
        let id = conn.websocket.get_uuid().to_string();
        let group = conn.group.clone();
        let ptr = Arc::new(RwLock::new(conn));
        self.connections_by_id.write().await.insert(id, ptr.clone());
        self.connections.write().await.insert(group, ptr.clone());
        ptr
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
        let mut connections = inner.connections_by_id.write().await;
        let keys = connections.keys().cloned().collect::<Vec<_>>();
        for key in keys {
            let value = if let Some(value) = connections.get(&key).cloned() {
                value
            } else {
                cassry::error!("cannot find connection by id to check eject: {}", key);
                continue;
            };

            let now = Utc::now();
            let mut conn = value.write().await;

            let penalty = if let Some(v) = 2i32.checked_pow(conn.retryed) {
                v
            } else {
                1
            };

            let interval = *checktime * penalty;
            // if eject time is 5, result is 5 10 20 40 80 160 320;
            let dur = now - conn.lastcheck;
            if interval > dur {
                continue;
            }

            conn.lastcheck = Utc::now();
            if conn.websocket.is_connected().await {
                continue;
            }

            cassry::info!(
                "websocket is disconnected : url({}), group({}), uuid({})",
                conn.websocket.get_connected_url_str().unwrap_or_default(),
                conn.group,
                conn.websocket.get_uuid()
            );

            let ws_param = inner
                .interface
                .make_websocket_param(&inner.context, &conn.group, &None)
                .await;

            let ws_param = match ws_param {
                std::result::Result::Ok(param) => param,
                Err(e) => {
                    conn.retryed += 1;
                    cassry::error!("occurred an error by making websocket param : reason({}), retryed({}), url({}), group({}), uuid({})", 
                    e, conn.retryed, conn.websocket.get_connected_url_str().unwrap_or_default(), conn.group, conn.websocket.get_uuid());
                    continue;
                }
            };

            cassry::info!(
                "Recovering the disconnected websocket : url({}) group({}) uuid({})",
                conn.websocket.get_connected_url_str().unwrap_or_default(),
                conn.group,
                conn.websocket.get_uuid()
            );

            match ExchangeSocket::make_websocket(&inner, ws_param).await {
                std::result::Result::Ok(websocket) => {
                    let uuid = websocket.get_uuid().to_string();
                    let prev = conn.update_websocket(websocket);
                    cassry::info!(
                        "updated websocket : url({}), group({}), uuid({}->{})",
                        conn.websocket.get_connected_url_str().unwrap_or_default(),
                        conn.group,
                        prev.get_uuid(),
                        uuid
                    );

                    connections.remove(prev.get_uuid());
                    connections.insert(uuid, value.clone());
                }
                Err(e) => {
                    conn.retryed += 1;
                    cassry::error!("occurred an error by making websocket param : reason({}), retryed({}), url({}), group({}), uuid({})", 
                    e, conn.retryed, conn.websocket.get_connected_url_str().unwrap_or_default(), conn.group, conn.websocket.get_uuid());
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
            .ok_or(anyhowln!(
                "This is an unsupported feature(ty:{:?}) : {}",
                param.ty,
                serde_json::to_string(&param.value)?
            ))?;

        let mut subscribes = self.inner.subscribes.write().await;
        if subscribes.contains(&key) {
            return Err(anyhowln!(
                "already subscribed(ty:{:?}) : {}",
                param.ty,
                serde_json::to_string(&param.value)?
            ));
        }

        cassry::info!(
            "proccessing subscribe({:?}) : {}",
            param.ty,
            serde_json::to_string(&param.value).unwrap()
        );

        let request = Some((param.ty.clone(), param.value));

        let info = if let Some(websocket) = self.find_connection(&group).await {
            websocket
        } else {
            cassry::info!("Open websocket to subscribe : {}", &group);
            let conn = self.make_connection(group, &request).await?;
            conn
        };

        {
            let mut locked = info.write().await;

            if locked.is_authorized {
                self.inner
                    .interface
                    .subscribe(
                        self.get_exchange_context(),
                        locked.websocket.clone(),
                        &request,
                        &locked.subscribes,
                    )
                    .await?;
            }

            cassry::info!("success subscribe : {:?}", &param.ty);
            let (ty, value) = request.unwrap();
            locked.subscribes.entry(ty).or_insert(vec![]).push(value);
        }

        subscribes.insert(key);
        Ok(())
    }

    async fn make_websocket(
        inner: &Arc<Inner>,
        param: ConnectParams,
    ) -> anyhow::Result<Websocket> {
        let inner = Arc::downgrade(inner);
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
        group: String,
        request: &Option<(SubscribeType, serde_json::Value)>,
    ) -> anyhow::Result<RwArc<Connection>> {
        let client_param = self
            .inner
            .interface
            .make_websocket_param(self.get_exchange_context(), &group, request)
            .await?;
        let websocket = ExchangeSocket::make_websocket(&self.inner, client_param).await?;
        let conn = Connection::new(group, websocket);
        Ok(self.inner.insert_connection(conn).await)
    }

    async fn find_connection(&self, group: &str) -> Option<ConnectionRwArc> {
        self.inner.find_connection(group).await
    }

    fn get_exchange_context(&self) -> &ExchangeContextPtr {
        &self.inner.context
    }
}
