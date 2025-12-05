pub mod exchange;
pub mod exchanges;
pub mod tcpserver;
pub mod webserver;
pub mod oauths;


pub use cassry;
pub use cassry::*;

pub use derive_more;
pub use async_trait;
pub use axum;
pub use axum_extra;
pub use tower;
pub use tower_sessions;
pub use tower_cookies;
pub use bitflags;
pub use meval;
pub use rand;
pub use oauth2;
pub use uuid;
pub use cassry_derive::ErrCode;

#[cfg(debug_assertions)]
pub const DEBUG_POST_FIX: &str = ".dev";
#[cfg(not(debug_assertions))]
pub const RELEASE_POST_FIX: &str = ".prod";