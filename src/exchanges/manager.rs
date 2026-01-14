use std::sync::Weak;
use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use cassry::*;
use super::super::exchange::*;
use super::super::webserver::websocket::*;

pub struct Manager {
    tags: Vec<ExchangeTag>,
    keys: HashMap<ExchangeTag, Arc<ExchangeKey>>,
    instatnts: RwLock<HashMap<ExchangeTag, Weak<Exchange>>>,
    db: Arc<LocalDB>,
}

impl Manager {
    pub fn new(keys: HashMap<ExchangeTag, Arc<ExchangeKey>>, db: Arc<LocalDB>) -> Self {
        Self {
            tags: keys.keys().cloned().collect::<Vec<_>>(),
            keys: keys,
            instatnts: Default::default(),
            db: db,
        }
    }

    pub fn get_tags(&self) -> Vec<ExchangeTag> {
        self.tags.clone()
    }

    pub async fn instant(
        &self,
        tag: &ExchangeTag,
        config: ExchangeConfig,
    ) -> anyhow::Result<ExchangeArc> {
        // let config = ExchangeConfig {
        //     ping_interval: chrono::Duration::minutes(1),
        //     eject: chrono::Duration::seconds(5),
        //     sync_expired_duration: chrono::Duration::minutes(5),
        //     state_expired_duration: chrono::Duration::minutes(1),

        //     opt_max_order_chche: 2000,
        //     opt_max_trades_chche: 2000,
        // };

        if let Some(exchange) = self.instatnts.read().await.get(tag).and_then(|weak| weak.upgrade()) {
            cassry::info!("Gets an exchange({}) that is already loaded.", tag);
            return Ok(exchange);
        }

        let key = self
            .keys
            .get(tag)
            .ok_or(anyhowln!("cannot find tag : {}", tag))?;
        let (ws, restapi) = match key.exchange.as_str() {
            "bybit" => {
                let ws = ConnectParams::from_str(if key.is_testnet {
                    "wss://stream-testnet.bybit.com"
                } else {
                    "wss://stream.bybit.com"
                })?;
                let mut restapi = RestAPIParam::default();
                restapi.url = if key.is_testnet {
                    "https://api-testnet.bybit.com"
                } else {
                    "https://api.bybit.com"
                }
                .to_string();
                Some((ws, restapi))
            }
            "bithumb" => {
                let ws = ConnectParams::from_str("wss://ws-api.bithumb.com/websocket")?;
                let mut restapi = RestAPIParam::default();
                restapi.url = "https://api.bithumb.com".to_string();
                Some((ws, restapi))
            }
            _ => None,
        }
        .ok_or(anyhowln!("invalid exchange name : {}", key.exchange))?;

        let param = ExchangeParam {
            websocket: ws,
            restapi: restapi,
            key: key.clone(),
            config: config,
            kind: MarketOpt::All,
        };

        let exchange = match key.exchange.as_str() {
            "bybit" => {
                exchange::Exchange::new::<super::bybit::RestAPI, super::bybit::WebsocketItf>(
                    param,
                    self.db.clone(),
                    None,
                )
                .await
            }
            "bithumb" => {
                exchange::Exchange::new::<super::bithumb::RestAPI, super::bithumb::WebsocketItf>(
                    param,
                    self.db.clone(),
                    None,
                )
                .await
            }
            _ => Err(anyhowln!("invalid exchange name : {}", key.exchange)),
        }?;

        cassry::info!("Exchange({}) loaded successfully", tag);
        self.instatnts.write().await.insert(key.tag.clone(), Arc::downgrade(&exchange));
        Ok(exchange)
    }
}