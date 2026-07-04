use axum_client_ip::ClientIp;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};
use std::net::IpAddr;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayerIP(#[serde_as(as = "DisplayFromStr")] IpAddr);

impl From<DisplayerIP> for ClientIp {
    fn from(val: DisplayerIP) -> Self {
        ClientIp(val.0)
    }
}

impl From<ClientIp> for DisplayerIP {
    fn from(value: ClientIp) -> Self {
        DisplayerIP(value.0)
    }
}
