use axum::{http::StatusCode, response::{IntoResponse, Response}};
use cassry::*;

#[derive(thiserror::Error, Debug)]
pub enum RespError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<tower_sessions::session::Error> for RespError {
    fn from(err: tower_sessions::session::Error) -> Self {
        RespError::Internal(anyhow::Error::from(err))
    }
}

impl From<serde_json::Error> for RespError {
    fn from(err: serde_json::Error) -> Self {
        RespError::Internal(anyhow::Error::from(err))
    }
}

impl IntoResponse for RespError {
    fn into_response(self) -> Response {
        match self {
            RespError::Unauthorized(msg) => (StatusCode::UNAUTHORIZED, msg).into_response(),
            RespError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            RespError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
            RespError::Internal(err) => {
                cassry::error!("INTERNAL ERROR: {:?}", err);
                (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response()
            }
        }
    }
}