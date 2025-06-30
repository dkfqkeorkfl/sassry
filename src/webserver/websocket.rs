use async_trait::async_trait;
use chrono::Utc;
use futures::{future::BoxFuture, FutureExt, SinkExt, StreamExt};
use std::{collections::HashMap, sync::Arc};

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    RwLock,
};

use axum::extract::ws::{Message as AxumMessage, WebSocket as AxumWebsocket};
use tokio_tungstenite::tungstenite::Message as TungsteniteMessage;

use cassry::{
    chrono::DateTime,
    futures::Sink,
    util::{deserialize_chrono_duration, serialize_chrono_duration},
    *,
};

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Message {
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
    Pong(Vec<u8>),
    Close(Option<(u16, String)>),
}

impl Message {
    pub fn from_axum(msg: AxumMessage) -> Self {
        match msg {
            AxumMessage::Text(text) => Message::Text(text.to_string()),
            AxumMessage::Binary(data) => Message::Binary(data.into()),
            AxumMessage::Ping(data) => Message::Ping(data.into()),
            AxumMessage::Pong(data) => Message::Pong(data.into()),
            AxumMessage::Close(frame) => {
                let nf = frame.map(|f| (f.code, f.reason.to_string()));
                Message::Close(nf)
            }
        }
    }

    pub fn from_tungstenite(msg: TungsteniteMessage) -> Self {
        match msg {
            TungsteniteMessage::Text(text) => Message::Text(text.to_string()),
            TungsteniteMessage::Binary(data) => Message::Binary(data.into()),
            TungsteniteMessage::Ping(data) => Message::Ping(data.into()),
            TungsteniteMessage::Pong(data) => Message::Pong(data.into()),
            TungsteniteMessage::Frame(_frame) => Message::Close(None),
            TungsteniteMessage::Close(frame) => {
                let nf = frame.map(|f| (u16::from(f.code), f.reason.to_string()));
                Message::Close(nf)
            }
        }
    }

    pub fn is_close(&self) -> bool {
        match self {
            Message::Close(_) => true,
            _ => false,
        }
    }
}

// AxumMessage에 대한 From 트레이트 구현
impl From<AxumMessage> for Message {
    fn from(msg: AxumMessage) -> Self {
        Message::from_axum(msg)
    }
}

// TungsteniteMessage에 대한 From 트레이트 구현
impl From<TungsteniteMessage> for Message {
    fn from(msg: TungsteniteMessage) -> Self {
        Message::from_tungstenite(msg)
    }
}

impl Into<AxumMessage> for Message {
    fn into(self) -> AxumMessage {
        match self {
            Message::Text(text) => AxumMessage::Text(text.into()),
            Message::Binary(data) => AxumMessage::Binary(data.into()),
            Message::Ping(data) => AxumMessage::Ping(data.into()),
            Message::Pong(data) => AxumMessage::Pong(data.into()),
            Message::Close(frame) => {
                let frame = frame.map(|(code, reason)| axum::extract::ws::CloseFrame {
                    code: code.into(),
                    reason: reason.into(),
                });
                AxumMessage::Close(frame)
            }
        }
    }
}

// // MyWebSocketMessage -> TungsteniteMessage
impl Into<TungsteniteMessage> for Message {
    fn into(self) -> TungsteniteMessage {
        match self {
            Message::Text(text) => TungsteniteMessage::Text(text.into()),
            Message::Binary(data) => TungsteniteMessage::Binary(data.into()),
            Message::Ping(data) => TungsteniteMessage::Ping(data.into()),
            Message::Pong(data) => TungsteniteMessage::Pong(data.into()),
            Message::Close(frame) => TungsteniteMessage::Close(frame.map(|(code, reason)| {
                tokio_tungstenite::tungstenite::protocol::CloseFrame {
                    code: code.into(),
                    reason: reason.into(),
                }
            })),
        }
    }
}

#[derive(Debug)]
pub enum Signal {
    Opened,
    Closed,
    Received(Message),
    Error(anyhow::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketParam {
    pub url: String,
    pub protocol: String,
    pub header: HashMap<String, String>,

    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub eject: chrono::Duration,
    #[serde(
        serialize_with = "serialize_chrono_duration",
        deserialize_with = "deserialize_chrono_duration"
    )]
    pub ping_interval: chrono::Duration,
}

impl Default for WebsocketParam {
    fn default() -> Self {
        Self {
            url: Default::default(),
            protocol: Default::default(),
            header: Default::default(),
            eject: chrono::Duration::seconds(5),
            ping_interval: chrono::Duration::minutes(1),
        }
    }
}

#[async_trait]
pub trait ConnectionItf: Send + Sync + 'static {
    async fn send(&mut self, message: Message) -> anyhow::Result<()>;
    async fn close(&mut self, param: Option<(u16, String)>) -> anyhow::Result<()>;
    async fn is_connected(&self) -> bool;
}
type ConnectionRwArc = Arc<RwLock<dyn ConnectionItf>>;

//Behavior
struct ConnectionReal {
    param: Arc<WebsocketParam>,
    sender: UnboundedSender<Message>,
    is_connect: RwArc<bool>,

    sendping: chrono::DateTime<Utc>,
    recvping: chrono::DateTime<Utc>,
}

#[async_trait]
impl ConnectionItf for ConnectionReal {
    async fn send(&mut self, message: Message) -> anyhow::Result<()> {
        match &message {
            Message::Ping(_) => {}
            _ => {
                cassry::trace!(
                    "sending message using websocket({}) : {:?}",
                    &self.param.url,
                    message
                );
            }
        }

        self.sender.send(message)?;
        Ok(())
    }

    async fn close(&mut self, param: Option<(u16, String)>) -> anyhow::Result<()> {
        self.send(Message::Close(param)).await
    }

    async fn is_connected(&self) -> bool {
        self.is_connect.read().await.clone()
    }
}

impl ConnectionReal {
    pub fn latency(&self) -> chrono::Duration {
        if self.sendping > self.recvping {
            Utc::now() - self.sendping
        } else {
            self.recvping - self.sendping
        }
    }

    pub async fn ping(&mut self) -> anyhow::Result<()> {
        let now = Utc::now();
        let ping = json!(now.timestamp_millis());
        let payload = serde_json::to_vec(&ping)?;
        self.sendping = now;
        self.send(Message::Ping(payload)).await
    }

    pub fn update_pong(&mut self, mili: i64) -> bool {
        if mili == self.sendping.timestamp_millis() {
            self.recvping = Utc::now();
            true
        } else {
            false
        }
    }

    fn init<S, M, E>(
        param: Arc<WebsocketParam>,
        stream: S,
        f: impl Fn(ConnectionRwArc, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<RwLock<Self>>>
    where
        S: futures::Stream<Item = Result<M, E>> + Sink<M, Error = E> + Unpin + Send + 'static,
        M: Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
        Message: From<M> + Into<M>,
    {
        let callback = Arc::new(f);
        let (mut write_half, mut read_half) = stream.split();
        let (sender, mut receiver) = unbounded_channel::<Message>();

        let now = Utc::now();
        let ping_interval = param.ping_interval.to_std()?;
        let is_connected = Arc::new(RwLock::new(true));
        let ws = Arc::from(RwLock::new(Self {
            param: param,
            sender: sender.clone(),
            is_connect: is_connected.clone(),

            sendping: now.clone(),
            recvping: now.clone(),
        }));

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&ws);
        tokio::spawn(async move {
            tokio::time::sleep(ping_interval.clone()).await;

            while *cloned_is_connected.read().await {
                let result = if let Some(ptr) = wpt_ws.upgrade() {
                    let mut locked = ptr.write().await;
                    if locked.latency() > locked.param.eject {
                        Err(anyhowln!("occur eject for ping test"))
                    } else {
                        locked.ping().await
                    }
                } else {
                    Err(anyhowln!("websocket point is NULL"))
                };

                if let Err(e) = result {
                    cassry::info!("{}", e.to_string());

                    let is_connected = cloned_is_connected.read().await;
                    let result = if *is_connected {
                        sender.send(Message::Close(None))
                    } else {
                        std::result::Result::Ok(())
                    };

                    if let Err(e) = result {
                        cassry::error!("{:?}", e.0);
                    }
                }
                tokio::time::sleep(ping_interval.clone()).await;
            }
        });

        let cloned_is_connected = is_connected.clone();
        let wpt_ws = Arc::downgrade(&ws);
        let cloned_callback = callback.clone();
        tokio::spawn(async move {
            while let Some(message) = receiver
                .recv()
                .await
                .filter(|msg| msg.is_close() || wpt_ws.upgrade().is_some())
            {
                if *cloned_is_connected.read().await == false {
                    break;
                } else if let Err(e) = write_half.send(message.into()).await {
                    if let Some(spt) = wpt_ws.upgrade() {
                        cloned_callback(spt, Signal::Error(e.into())).await;
                    }
                }
            }
        });

        let wpt_ws = Arc::downgrade(&ws);
        tokio::spawn(async move {
            if let Some(spt) = wpt_ws.upgrade() {
                callback(spt, Signal::Opened).await;
            }

            while let Some((message, spt)) = read_half.next().await.zip(wpt_ws.upgrade()) {
                let signal = match message.map(Message::from) {
                    std::result::Result::Ok(msg) => {
                        if let Message::Pong(payload) = msg {
                            let result = serde_json::from_slice::<serde_json::Value>(&payload[..])
                                .map_err(anyhow::Error::from)
                                .and_then(|json| {
                                    let sendtime = json.as_i64().ok_or(anyhowln!(
                                        "invalid data from json to i64 for pong"
                                    ))?;
                                    Ok(sendtime)
                                });

                            match result {
                                anyhow::Result::Ok(mili) => {
                                    spt.write().await.update_pong(mili);
                                    Signal::Received(Message::Pong(payload))
                                }
                                Err(e) => Signal::Error(e),
                            }
                        } else {
                            cassry::trace!("recved : {:?}", msg);
                            Signal::Received(msg)
                        }
                    }
                    Err(e) => Signal::Error(e.into()),
                };

                callback(spt, signal).await;
            }

            *is_connected.write().await = false;
            if let Some(spt) = wpt_ws.upgrade() {
                callback(spt, Signal::Closed).await;
            }
        });

        Ok(ws)
    }

    fn accept(
        param: Arc<WebsocketParam>,
        stream: AxumWebsocket,
        f: impl Fn(ConnectionRwArc, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<RwLock<Self>>> {
        ConnectionReal::init(
            param,
            stream,
            f,
        )
    }

    pub async fn connect(
        param: Arc<WebsocketParam>,
        f: impl Fn(ConnectionRwArc, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<RwLock<Self>>> {
        // let callback = Arc::new(Mutex::new(f));
        cassry::debug!("connecting websocket : {}", &param.url);
        let (stream, _) = tokio_tungstenite::connect_async(&param.url).await?;
        cassry::debug!("success for websocket : {}", &param.url);
        ConnectionReal::init(
            param,
            stream,
            f,
        )
    }
}

#[derive(Default)]
pub struct ConnectionNull;

impl ConnectionNull {
    pub fn new() -> Arc<RwLock<Self>> {
        RwLock::new(ConnectionNull::default()).into()
    }
}

#[async_trait]
impl ConnectionItf for ConnectionNull {
    async fn send(&mut self, _message: Message) -> anyhow::Result<()> {
        Ok(())
    }
    async fn close(&mut self, _params: Option<(u16, String)>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn is_connected(&self) -> bool {
        true
    }
}

#[derive(Default, Clone)]
pub struct Websocket {
    conn: Option<(Arc<WebsocketParam>, ConnectionRwArc)>,
    created: DateTime<Utc>,
    uuid : String,
}
pub type ReciveCallback =
    Arc<dyn Fn(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static>;
#[derive(Clone)]
pub struct ReciveCallbackHelper {
    pub callback: Arc<dyn Fn(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static>,
}

impl ReciveCallbackHelper {
    pub fn new(
        f: impl Fn(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> Self {
        ReciveCallbackHelper {
            callback: Arc::new(f),
        }
    }
}

impl Websocket {
    pub fn new(param: Arc<WebsocketParam>, conn: ConnectionRwArc, created: DateTime<Utc>) -> Self {
        Self {
            conn: Some((param, conn)),
            created: created,
            uuid: uuid::Uuid::new_v4().to_string(),
        }
    }

    pub fn accept_with_callback_ptr(
        param: WebsocketParam,
        stream: AxumWebsocket,
        callback: ReciveCallback,
    ) -> anyhow::Result<Self> {
        let param = Arc::new(param);
        let created = Utc::now();
        let cloned = (param.clone(), created.clone());
        let conn = ConnectionReal::accept(param.clone(), stream, move |conn, signal| {
            let cloned_callback = callback.clone();
            let (param, created) = cloned.clone();
            async move {
                let s = Self::new(param, conn, created);
                cloned_callback(s, signal).await;
            }
            .boxed()
        })?;
        Ok(Websocket::new(param, conn, created))
    }

    pub fn accept(
        param: WebsocketParam,
        stream: AxumWebsocket,
        f: impl Fn(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> anyhow::Result<Self> {
        let callback = Arc::new(f);
        Websocket::accept_with_callback_ptr(param, stream, callback)
    }

    pub async fn connect_with_callback_ptr(
        &mut self,
        param: WebsocketParam,
        callback: ReciveCallback,
    ) -> anyhow::Result<()> {
        if let Some((_, c)) = &self.conn {
            let locked = c.read().await;

            if locked.is_connected().await {
                return Err(anyhowln!("socket is already connected"));
            }
        }

        let param = Arc::new(param);
        self.created = Utc::now();
        self.conn = if param.url.is_empty() {
            Some((param, ConnectionNull::new()))
        } else {
            let cloned = (param.clone(), self.created.clone());
            let conn = ConnectionReal::connect(param.clone(), move |conn, signal| {
                let cloned_callback = callback.clone();
                let (param, created) = cloned.clone();
                async move {
                    let s = Self::new(param, conn, created);
                    cloned_callback(s, signal).await;
                }
                .boxed()
            })
            .await?;
            Some((param, conn))
        };
        Ok(())
    }

    pub async fn connect(
        &mut self,
        param: WebsocketParam,
        f: impl Fn(Websocket, Signal) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    ) -> anyhow::Result<()> {
        let callback = Arc::new(f);
        self.connect_with_callback_ptr(param, callback).await
    }

    pub async fn send(&self, message: Message) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhowln!("websocket is disconnected"));
        }

        if let Some((_, c)) = &self.conn {
            c.write().await.send(message).await
        } else {
            Err(anyhowln!("websocket empty is none"))
        }
    }

    pub async fn send_text(&self, text: String) -> anyhow::Result<()> {
        if let Some((_, c)) = &self.conn {
            c.write().await.send(Message::Text(text)).await
        } else {
            Err(anyhowln!("websocket empty is none"))
        }
    }

    pub async fn close(&self, param: Option<(u16, String)>) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhowln!("websocket is already disconnected"));
        }

        if let Some((_, c)) = &self.conn {
            c.write().await.close(param).await
        } else {
            Err(anyhowln!("websocket is empty."))
        }
    }

    pub async fn is_connected(&self) -> bool {
        if let Some((_, c)) = &self.conn {
            return c.read().await.is_connected().await;
        }
        false
    }

    pub fn get_param(&self) -> Option<Arc<WebsocketParam>> {
        if let Some((param, _)) = &self.conn {
            return Some(param.clone());
        }
        None
    }

    pub fn get_created(&self) -> DateTime<Utc> {
        self.created.clone()
    }

    pub fn get_uuid(&self) -> &str {
        &self.uuid
    }
}
