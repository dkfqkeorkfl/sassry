pub mod exchange;
pub mod exchanges;
pub mod oauths;
pub mod tcpserver;
pub mod webserver;

pub use cassry;
pub use cassry::*;

pub use async_trait;
pub use derive_more;

pub use axum;
pub use axum_client_ip;
pub use axum_extra;

pub use tower;
pub use tower_cookies;
pub use tower_sessions;

pub use bson;
pub use mongodb;

pub use bitflags;
pub use cassry_derive::ErrCode;
pub use meval;
pub use oauth2;
pub use rand;

pub use validator;

#[cfg(debug_assertions)]
pub const CONFIG_POST_FIX: &str = ".dev";
#[cfg(not(debug_assertions))]
pub const CONFIG_POST_FIX: &str = ".prod";
