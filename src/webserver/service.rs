use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use cassry::*;
use super::websocket::*;
use super::super::exchanges::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub debug: bool,
    pub http_port: u16,
    pub https_port: u16,
    pub cert: String,
    pub key: String,

    pub log_yaml: String,
    pub notify_token: String,
    pub notify_log_info: String,
    pub notify_log_warn: String,
    pub notify_log_crit: String,
    pub notify_threshold : f64, 

    pub apikey_sheet: String,
    pub apikey_tab: String,
    pub account_sheet: String,
    pub account_tab: String,

    pub server_eject: f64,
    pub server_ping: f64,

    pub cache_name: String,
    pub salt: String,
}

impl Config {
    pub fn duration_eject(&self) -> chrono::Duration {
        util::f64_to_duration(self.server_eject)
    }
    pub fn duration_ping(&self) -> chrono::Duration {
        util::f64_to_duration(self.server_ping)
    }
}

pub struct Context {
    pub config: Arc<Config>,
    pub exchanges: manager::Manager,
    pub db: Leveldb,
}

#[async_trait]
pub trait ServiceTrait: Send + Sync + 'static {
    async fn open(&mut self, _id: &str, _ws: Websocket) -> anyhow::Result<()> {
        Ok(())
    }

    async fn release(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    async fn proccess(
        &mut self,
        _context: &Arc<Context>,
        _json: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        Ok(Default::default())
    }
}

#[derive(Default)]
pub struct ServiceNone;

impl ServiceTrait for ServiceNone {}

pub struct App {
    inner: Arc<Mutex<dyn ServiceTrait>>,
    id: String,
}

impl Default for App {
    fn default() -> Self {
        let inner = Arc::new(Mutex::new(ServiceNone::default()));
        Self {
            inner: inner,
            id: Default::default(),
        }
    }
}

impl App {
    pub async fn new<A>(id: String) -> Self
    where
        A: ServiceTrait + Default + Sized,
    {
        let inner = Arc::new(Mutex::new(A::default()));
        Self {
            inner: inner,
            id: id,
        }
    }

    pub async fn open(&self, ws: Websocket) -> anyhow::Result<()> {
        let mut locked = self.inner.lock().await;
        locked.open(&self.id, ws).await
    }

    pub async fn release(&self) -> anyhow::Result<()> {
        let mut locked = self.inner.lock().await;
        locked.release().await
    }

    pub async fn proccess(
        &self,
        context: &Arc<Context>,
        json: serde_json::Value,
    ) -> anyhow::Result<serde_json::Value> {
        let mut locked = self.inner.lock().await;
        locked.proccess(context, json).await
    }
}
