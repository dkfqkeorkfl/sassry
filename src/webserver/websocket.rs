use async_trait::async_trait;
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use serde_with::{serde_as, DisplayFromStr, DurationSecondsWithFrac};
use std::{collections::HashMap, str::FromStr, sync::Arc};

use axum::extract::ws::{Message as AxumMessage, WebSocket as AxumWebsocket};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedSender},
    RwLock,
};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message as TungsteniteMessage};
use url::Url;

use cassry::{chrono::DateTime, futures::Sink, *};

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

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectParams {
    #[serde_as(as = "DisplayFromStr")]
    pub url: Url,
    pub protocol: String,
    pub header: HashMap<String, String>,

    #[serde_as(as = "DurationSecondsWithFrac<String>")]
    pub eject: chrono::Duration,
    #[serde_as(as = "DurationSecondsWithFrac<String>")]
    pub ping_interval: std::time::Duration,
}

impl ConnectParams {
    pub fn add_path(&mut self, paths: std::vec::Vec<&str>) -> anyhow::Result<&mut Self> {
        if let Ok(mut path_segments) = self.url.path_segments_mut() {
            for segment in paths {
                println!("segment: {}", segment);
                path_segments.push(segment);
            }
        } else {
            return Err(anyhowln!("invalid url"));
        }
        Ok(self)
    }

    pub fn from_str(url: &str) -> anyhow::Result<Self> {
        Ok(Self {
            url: Url::from_str(url)?,
            protocol: Default::default(),
            header: Default::default(),
            eject: chrono::Duration::seconds(5),
            ping_interval: std::time::Duration::from_secs(60),
        })
    }
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcceptParams {
    pub addr: std::net::SocketAddr,
    // sassry는 기본적으로 chrono를 채택하여 사용
    #[serde_as(as = "DurationSecondsWithFrac<String>")]
    pub eject: chrono::Duration,

    // tokio는 std::time::Duration을 채택하여 사용
    #[serde_as(as = "DurationSecondsWithFrac<String>")]
    pub ping_interval: std::time::Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WebsocketParams {
    Connect(ConnectParams),
    Accept(AcceptParams),
}

impl WebsocketParams {
    pub fn get_eject(&self) -> &chrono::Duration {
        match self {
            WebsocketParams::Connect(params) => &params.eject,
            WebsocketParams::Accept(params) => &params.eject,
        }
    }

    pub fn get_ping_interval(&self) -> &std::time::Duration {
        match self {
            WebsocketParams::Connect(params) => &params.ping_interval,
            WebsocketParams::Accept(params) => &params.ping_interval,
        }
    }

    pub fn get_connected_url(&self) -> Option<&Url> {
        match self {
            WebsocketParams::Connect(params) => Some(&params.url),
            _ => None,
        }
    }
}

#[async_trait]
pub trait ConnectionItf: Send + Sync + 'static {
    async fn send(&self, message: Message) -> anyhow::Result<()>;
    async fn close(&self, param: Option<(u16, String)>) -> anyhow::Result<()>;
    async fn is_connected(&self) -> bool;
    fn get_uuid(&self) -> &str;
    fn get_created(&self) -> &DateTime<Utc>;
}

//Behavior
struct ConnectionReal {
    param: Arc<WebsocketParams>,
    sender: UnboundedSender<Message>,
    is_connect: RwArc<bool>,

    uuid: String,
    created: DateTime<Utc>,
    sendping: RwLock<chrono::DateTime<Utc>>,
    recvping: RwLock<chrono::DateTime<Utc>>,
}

#[async_trait]
impl ConnectionItf for ConnectionReal {
    async fn send(&self, message: Message) -> anyhow::Result<()> {
        match &message {
            Message::Ping(_) => {}
            _ => {
                cassry::trace!(
                    "sending message using websocket({}) : {:?}",
                    &self.get_uuid(),
                    message
                );
            }
        }

        self.sender.send(message)?;
        Ok(())
    }

    async fn close(&self, param: Option<(u16, String)>) -> anyhow::Result<()> {
        self.send(Message::Close(param)).await
    }

    async fn is_connected(&self) -> bool {
        self.is_connect.read().await.clone()
    }

    fn get_uuid(&self) -> &str {
        &self.uuid
    }

    fn get_created(&self) -> &DateTime<Utc> {
        &self.created
    }
}

impl ConnectionReal {
    pub fn get_param(&self) -> &WebsocketParams {
        &self.param
    }

    pub async fn latency(&self) -> chrono::Duration {
        let send = *self.sendping.read().await;
        let recv = *self.recvping.read().await;
        if send > recv {
            Utc::now() - send
        } else {
            recv - send
        }
    }

    pub async fn ping(&self) -> anyhow::Result<()> {
        let now = Utc::now();
        let ping = json!(now.timestamp_millis());
        let payload = serde_json::to_vec(&ping)?;
        *self.sendping.write().await = now;
        self.send(Message::Ping(payload)).await
    }

    pub async fn update_pong(&self, mili: i64) -> bool {
        let send = *self.sendping.read().await;
        if mili == send.timestamp_millis() {
            *self.recvping.write().await = Utc::now();
            true
        } else {
            false
        }
    }

    fn init<S, M, E, F, Fut>(param: Arc<WebsocketParams>, stream: S, callback: F) -> Arc<Self>
    where
        S: futures::Stream<Item = Result<M, E>> + Sink<M, Error = E> + Unpin + Send + 'static,
        M: Send + 'static,
        E: std::error::Error + Send + Sync + 'static,
        Message: From<M> + Into<M>,

        F: Fn(Arc<dyn ConnectionItf>, Signal) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let callback = Arc::new(callback);
        let (mut write_half, mut read_half) = stream.split();
        let (sender, mut receiver) = unbounded_channel::<Message>();

        let now = Utc::now();
        let is_connected = Arc::new(RwLock::new(true));
        let ws = Arc::from(Self {
            uuid: uuid::Uuid::new_v4().to_string(),
            created: now,

            param: param,
            sender: sender.clone(),
            is_connect: is_connected.clone(),

            sendping: RwLock::new(now),
            recvping: RwLock::new(now),
        });

        let ctx = (
            Arc::downgrade(&ws),
            is_connected.clone(),
            ws.get_param().get_ping_interval().clone(),
        );
        tokio::spawn(async move {
            let (wpt_ws, is_connected, ping_interval) = ctx;
            tokio::time::sleep(ping_interval.clone()).await;

            while *is_connected.read().await {
                let result = if let Some(ptr) = wpt_ws.upgrade() {
                    if ptr.latency().await > *ptr.get_param().get_eject() {
                        Err(anyhowln!("occur eject for ping test"))
                    } else {
                        ptr.ping().await
                    }
                } else {
                    Err(anyhowln!("websocket point is NULL"))
                };

                if let Err(e) = result {
                    cassry::info!("{}", e.to_string());

                    let is_connected = is_connected.read().await;
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

        let ctx = (Arc::downgrade(&ws), is_connected.clone(), callback.clone());
        tokio::spawn(async move {
            let (wpt_ws, is_connected, callback) = ctx;
            while let Some(message) = receiver
                .recv()
                .await
                .filter(|msg| msg.is_close() || wpt_ws.upgrade().is_some())
            {
                if *is_connected.read().await == false {
                    break;
                } else if let Err(e) = write_half.send(message.into()).await {
                    if let Some(spt) = wpt_ws.upgrade() {
                        callback(spt, Signal::Error(e.into())).await;
                    }
                }
            }
        });

        let ctx = (Arc::downgrade(&ws), callback);
        tokio::spawn(async move {
            let (wpt_ws, callback) = ctx;
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
                                    spt.update_pong(mili).await;
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

        ws
    }

    fn accept<F, Fut>(param: Arc<WebsocketParams>, stream: AxumWebsocket, f: F) -> Arc<Self>
    where
        F: Fn(Arc<dyn ConnectionItf>, Signal) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        ConnectionReal::init(param, stream, f)
    }

    pub async fn connect<F, Fut>(param: Arc<WebsocketParams>, f: F) -> anyhow::Result<Arc<Self>>
    where
        F: Fn(Arc<dyn ConnectionItf>, Signal) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        if let WebsocketParams::Connect(params) = param.as_ref() {
            cassry::debug!("connecting websocket : {}", &params.url);
            let mut request = params.url.to_string().into_client_request()?;
            for (key, value) in params.header.iter() {
                let name = axum::http::HeaderName::from_str(key.clone().as_str())?;
                let value = axum::http::HeaderValue::from_str(value.clone().as_str())?;
                request.headers_mut().insert(name, value);
            }

            let (stream, _) = tokio_tungstenite::connect_async(request).await?;
            cassry::debug!("success for websocket : {}", &params.url);
            Ok(ConnectionReal::init(param, stream, f))
        } else {
            Err(anyhowln!("invalid websocket params"))
        }
    }
}

#[derive(Default)]
pub struct ConnectionNull {
    created: DateTime<Utc>,
}

impl ConnectionNull {
    pub fn new() -> Arc<Self> {
        let now = Utc::now();
        Arc::new(ConnectionNull { created: now })
    }
}

#[async_trait]
impl ConnectionItf for ConnectionNull {
    async fn send(&self, _message: Message) -> anyhow::Result<()> {
        Ok(())
    }
    async fn close(&self, _params: Option<(u16, String)>) -> anyhow::Result<()> {
        Ok(())
    }
    async fn is_connected(&self) -> bool {
        true
    }
    fn get_uuid(&self) -> &str {
        ""
    }
    fn get_created(&self) -> &DateTime<Utc> {
        &self.created
    }
}

#[derive(Clone)]
pub struct Websocket {
    conn: Arc<dyn ConnectionItf>,
    param: Arc<WebsocketParams>,
}

impl Websocket {
    fn new(param: Arc<WebsocketParams>, conn: Arc<dyn ConnectionItf>) -> Self {
        Self {
            conn: conn,
            param: param,
        }
    }

    pub fn accept<F, Fut>(param: AcceptParams, stream: AxumWebsocket, f: F) -> anyhow::Result<Self>
    where
        F: Fn(Websocket, Signal) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let param = Arc::new(WebsocketParams::Accept(param));
        let ctx = (param.clone(), Arc::new(f));
        let conn = ConnectionReal::accept(param.clone(), stream, move |conn, signal| {
            let (param, callback) = ctx.clone();
            let ws = Self::new(param, conn);
            async move {
                callback(ws, signal).await;
            }
        });
        Ok(Websocket::new(param, conn))
    }

    pub async fn connect<F, Fut>(param: ConnectParams, f: F) -> anyhow::Result<Self>
    where
        F: Fn(Websocket, Signal) -> Fut + Send + Sync + 'static + Clone,
        Fut: std::future::Future<Output = ()> + Send + 'static,
    {
        let param = Arc::new(WebsocketParams::Connect(param));
        let ctx = (param.clone(), Arc::new(f));
        let conn = ConnectionReal::connect(param.clone(), move |conn, signal| {
            let (param, callback) = ctx.clone();
            let ws = Self::new(param, conn);
            async move {
                callback(ws, signal).await;
            }
        })
        .await?;
        Ok(Websocket::new(param, conn))
    }

    pub async fn send(&self, message: Message) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhowln!("websocket is disconnected"));
        }

        self.conn.send(message).await
    }

    pub async fn send_text(&self, text: String) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhowln!("websocket is disconnected"));
        }

        self.conn.send(Message::Text(text)).await
    }

    pub async fn close(&self, param: Option<(u16, String)>) -> anyhow::Result<()> {
        if self.is_connected().await == false {
            return Err(anyhowln!("websocket is already disconnected"));
        }

        self.conn.close(param).await
    }

    pub async fn is_connected(&self) -> bool {
        self.conn.is_connected().await
    }

    pub fn get_param(&self) -> Arc<WebsocketParams> {
        self.param.clone()
    }

    pub fn get_param_as_connect(&self) -> Option<&ConnectParams> {
        if let WebsocketParams::Connect(params) = self.param.as_ref() {
            Some(params)
        } else {
            None
        }
    }

    pub fn get_connected_url(&self) -> Option<&Url> {
        self.param.get_connected_url()
    }

    pub fn get_connected_url_str(&self) -> Option<String> {
        self.get_connected_url().map(|url| url.to_string())
    }

    pub fn get_created(&self) -> DateTime<Utc> {
        self.conn.get_created().clone()
    }

    pub fn get_uuid(&self) -> &str {
        self.conn.get_uuid()
    }
}
