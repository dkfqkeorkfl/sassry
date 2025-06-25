use axum::{http::StatusCode, response::{IntoResponse, Response}};
use cassry::*;

#[derive(thiserror::Error, Debug)]
pub enum RespError {
    #[error("unauthorized: {0}")]
    Unauthorized(String),         // 401: 인증되지 않음 (로그인 필요)

    #[error("forbidden: {0}")]
    Forbidden(String),            // 403: 금지됨 (권한 없음)

    #[error("bad request: {0}")]
    BadRequest(String),           // 400: 잘못된 요청 (요청 형식 오류 등)

    #[error("not found: {0}")]
    NotFound(String),             // 404: 리소스를 찾을 수 없음

    #[error("conflict: {0}")]
    Conflict(String),             // 409: 충돌 (중복 데이터 등)

    #[error("too many requests: {0}")]
    TooManyRequests(String),      // 429: 너무 많은 요청 (속도 제한 등)

    #[error("unprocessable entity: {0}")]
    UnprocessableEntity(String),  // 422: 처리할 수 없는 엔터티 (유효성 검사 실패 등)

    #[error("service unavailable: {0}")]
    ServiceUnavailable(String),   // 503: 서비스 이용 불가 (서버 과부하, 유지보수 등)

    #[error("timeout: {0}")]
    Timeout(String),              // 504: 게이트웨이 타임아웃 (응답 지연)

    #[error(transparent)]
    Internal(#[from] anyhow::Error), // 500: 서버 내부 오류
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
            RespError::Forbidden(msg) => (StatusCode::FORBIDDEN, msg).into_response(),
            RespError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg).into_response(),
            RespError::NotFound(msg) => (StatusCode::NOT_FOUND, msg).into_response(),
            RespError::Conflict(msg) => (StatusCode::CONFLICT, msg).into_response(),
            RespError::TooManyRequests(msg) => (StatusCode::TOO_MANY_REQUESTS, msg).into_response(),
            RespError::UnprocessableEntity(msg) => (StatusCode::UNPROCESSABLE_ENTITY, msg).into_response(),
            RespError::ServiceUnavailable(msg) => (StatusCode::SERVICE_UNAVAILABLE, msg).into_response(),
            RespError::Timeout(msg) => (StatusCode::GATEWAY_TIMEOUT, msg).into_response(),
            RespError::Internal(err) => {
                cassry::error!("INTERNAL ERROR: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
                    .into_response()
            }
        }
    }
}