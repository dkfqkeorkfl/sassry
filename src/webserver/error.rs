use axum::http::StatusCode;
use cassry::*;
use thiserror::Error;
/// HTTP status code별 대표적인 에러 타입들
#[derive(Debug, Error, cassry_derive::ErrCode)]
pub enum HttpError {
    /// 400 Bad Request - 잘못된 요청
    #[status(400)]
    #[error("{0}")]
    BadRequest(String),

    /// 401 Unauthorized - 인증되지 않음
    #[status(401)]
    #[error("{0}")]
    Unauthorized(String),

    /// 403 Forbidden - 금지됨 (권한 없음)
    #[status(403)]
    #[error("{0}")]
    Forbidden(String),

    /// 404 Not Found - 리소스를 찾을 수 없음
    #[status(404)]
    #[error("{0}")]
    NotFound(String),

    /// 405 Method Not Allowed - 허용되지 않은 메서드
    #[status(405)]
    #[error("{0}")]
    MethodNotAllowed(String),

    /// 409 Conflict - 충돌 (중복 데이터 등)
    #[status(409)]
    #[error("{0}")]
    Conflict(String),

    /// 422 Unprocessable Entity - 처리할 수 없는 엔터티 (유효성 검사 실패 등)
    #[status(422)]
    #[error("{0}")]
    UnprocessableEntity(String),

    /// 429 Too Many Requests - 너무 많은 요청 (속도 제한 등)
    #[status(429)]
    #[error("{0}")]
    TooManyRequests(String),

    /// 500 Internal Server Error - 서버 내부 오류
    #[status(500)]
    #[error(transparent)]
    Internal(#[from] anyhow::Error), // 500: 서버 내부 오류

    /// 502 Bad Gateway - 잘못된 게이트웨이
    #[status(502)]
    #[error("{0}")]
    BadGateway(String),

    /// 503 Service Unavailable - 서비스 이용 불가 (서버 과부하, 유지보수 등)
    #[status(503)]
    #[error("{0}")]
    ServiceUnavailable(String),

    /// 504 Gateway Timeout - 게이트웨이 타임아웃
    #[status(504)]
    #[error("{0}")]
    GatewayTimeout(String),

    // sassry 오류 코드 시작 1000 ~
    #[status(401)]
    #[value(1001)]
    #[error("Missing JWT token in cookies")]
    MissingJwtToken,

    #[status(401)]
    #[value(1002)]
    #[error(transparent)]
    InvalidJwt(anyhow::Error),

    #[status(401)]
    #[value(1003)]
    #[error("JWT expired")]
    ExpiredJwt,
}

impl axum::response::IntoResponse for HttpError {
    fn into_response(self) -> axum::response::Response {
        let (code, value) = self.to_response();
        match StatusCode::from_u16(code) {
            Ok(status) => (status, axum::Json(value)).into_response(),
            Err(code) => {
                let str = value.to_string();
                let (_, err) = HttpError::Internal(anyhow::anyhow!(
                    "Invalid status code({:?}) : {}",
                    code,
                    str
                ))
                .to_response();
                warn!("Invalid status code({:?}) : {}", code, str);
                (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(err)).into_response()
            }
        }
    }
}
