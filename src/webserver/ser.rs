use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::net::IpAddr;
use axum_client_ip::ClientIp;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayerIP(#[serde_as(as = "DisplayFromStr")] IpAddr);

impl Into<ClientIp> for DisplayerIP {
    fn into(self) -> ClientIp {
        ClientIp(self.0)
    }
}

impl From<ClientIp> for DisplayerIP {
    fn from(value: ClientIp) -> Self {
        DisplayerIP(value.0)
    }
}