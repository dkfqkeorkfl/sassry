use axum::{
    extract::FromRequestParts,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cassry::{
    chrono::Utc,
    secrecy::{ExposeSecret, SecretString},
    *,
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use uuid::Uuid;

use tokio::sync::RwLock;

/// 토큰 타입 구분 (access/refresh 등)
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum TokenType {
    #[serde(rename = "N")]
    None,
    #[serde(rename = "A")]
    Access,
    #[serde(rename = "R")]
    Refresh,
    #[serde(rename = "E")]
    Etc(String),
}

impl Default for TokenType {
    fn default() -> Self {
        TokenType::None
    }
}

#[derive(Debug, Error)]
pub enum ClaimsError {
    #[error("Missing cookie header")]
    MissingCookieHeader,

    #[error("Missing JWT token in cookies")]
    MissingJwtToken,

    #[error("JWT verification failed: {0}")]
    InvalidJwt(String),

    #[error("Internal server error")]
    Internal(anyhow::Error),
}

impl IntoResponse for ClaimsError {
    fn into_response(self) -> Response {
        match &self {
            ClaimsError::MissingCookieHeader
            | ClaimsError::MissingJwtToken
            | ClaimsError::InvalidJwt(_) => {
                warn!("Claims error: {}", self);
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }

            ClaimsError::Internal(err) => {
                warn!("Claims internal error: {:?}", err);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal server error".to_string(),
                )
                    .into_response()
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClaimsCore<T> {
    pub jti: String, // 토큰 고유 ID
    pub from: String,

    pub exp: i64,              // 만료 시간 (timestamp)
    pub iat: i64,              // 발급 시간 (timestamp)
    pub token_type: TokenType, // access/refresh 등

    pub sub: u64,  // 사용자 ID
    pub role: u64, // 권한(플래그)

    // refresh 토큰은 아래 정보를 세션에 담아야 한다.
    // pub aud: u64,              // 대상자 (클라이언트 ID 등)
    // pub nbf: usize,            // 시작 시간 (timestamp)
    // pub env: Environment,      // 실행환경
    // pub iss: u64,              // 발급자 (서비스 ID 등)
    // pub user_agent: String,    // 실행환경 (예: dev, prod)
    pub extra: std::marker::PhantomData<T>,
}

pub type ClaimsFromAccess = ClaimsCore<()>;
pub type ClaimsForExchange = ClaimsCore<u64>;
pub type ClaimsRefresh = ClaimsCore<i64>;

impl From<ClaimsFromAccess> for ClaimsForExchange {
    fn from(claims: ClaimsFromAccess) -> Self {
        Self {
            jti: claims.jti,
            from: claims.from,
            exp: claims.exp,
            iat: claims.iat,
            token_type: claims.token_type,
            sub: claims.sub,
            role: claims.role,
            extra: std::marker::PhantomData::default(),
        }
    }
}

impl Default for ClaimsFromAccess {
    fn default() -> Self {
        let now = Utc::now().timestamp();

        Self {
            jti: Uuid::new_v4().to_string(),
            from: Default::default(),
            iat: now,
            exp: now,
            token_type: TokenType::default(),
            sub: Default::default(),
            role: Default::default(),
            extra: std::marker::PhantomData::default(),
        }
    }
}

impl<S> FromRequestParts<S> for ClaimsFromAccess
where
    S: Send + Sync,
{
    type Rejection = ClaimsError;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl core::future::Future<Output = Result<Self, Self::Rejection>> {
        async move {
            // 쿠키에서 JWT 토큰 추출
            let cookies = parts
                .headers
                .get("cookie")
                .and_then(|cookie_header| cookie_header.to_str().ok())
                .ok_or(ClaimsError::MissingCookieHeader)?;

            // 쿠키 문자열에서 JWT 토큰 찾기
            let jwt_token = cookies
                .split(';')
                .find_map(|cookie| {
                    let cookie = cookie.trim();
                    if cookie.starts_with("access=") {
                        Some(cookie[4..].to_string())
                    } else {
                        None
                    }
                })
                .ok_or(ClaimsError::MissingJwtToken)?;

            let jwt_manager = parts
                .extensions
                .get::<Arc<JwtIssuer>>()
                .ok_or_else(|| ClaimsError::Internal(anyhow::anyhow!("JwtManager not found")))?;

            jwt_manager
                .verify_jwt(&jwt_token, None)
                .await
                .map_err(|e| ClaimsError::InvalidJwt(e.to_string()))
        }
    }
}

impl<S> FromRequestParts<S> for ClaimsForExchange
where
    S: Send + Sync,
{
    type Rejection = ClaimsError;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl core::future::Future<Output = Result<Self, Self::Rejection>> {
        async move {
            // 쿠키에서 JWT 토큰 추출
            let cookies = parts
                .headers
                .get("cookie")
                .and_then(|cookie_header| cookie_header.to_str().ok())
                .ok_or(ClaimsError::MissingCookieHeader)?;

            // 쿠키 문자열에서 JWT 토큰 찾기
            let jwt_token = cookies
                .split(';')
                .find_map(|cookie| {
                    let cookie = cookie.trim();
                    if cookie.starts_with("access=") {
                        Some(cookie[4..].to_string())
                    } else {
                        None
                    }
                })
                .ok_or(ClaimsError::MissingJwtToken)?;

            let jwt_manager = parts
                .extensions
                .get::<Arc<JwtIssuer>>()
                .ok_or_else(|| ClaimsError::Internal(anyhow::anyhow!("JwtManager not found")))?;

            let mut validation = Validation::default();
            validation.validate_exp = false;
            jwt_manager
                .verify_jwt(&jwt_token, Some(validation))
                .await
                .map(|claims| claims.into())
                .map_err(|e| ClaimsError::InvalidJwt(e.to_string()))
        }
    }
}

pub struct Inner {
    secret: SecretString,
    name: String,
    access_ttl: chrono::Duration,
    refresh_ttl: chrono::Duration,
}

impl Inner {
    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(
        name: String,
        secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            secret,
            name,
            access_ttl,
            refresh_ttl,
        }
    }

    /// 비밀키 교환 (이전 비밀키를 반환)
    pub fn exchange_secret(&mut self, new_secret: SecretString) -> SecretString {
        let old_secret = std::mem::replace(&mut self.secret, new_secret);
        old_secret
    }

    /// 현재 비밀키 가져오기 (읽기 전용)
    pub fn get_secret(&self) -> SecretString {
        self.secret.clone()
    }

    /// JWT 토큰 생성 (HS256) - 클레임 구조체를 직접 받아 생성
    pub fn generate_jwt(&self, claims: &ClaimsFromAccess) -> anyhow::Result<String> {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(self.secret.expose_secret().as_bytes()),
        )
        .map_err(anyhow::Error::from)
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub fn verify_jwt(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<ClaimsFromAccess> {
        decode::<ClaimsFromAccess>(
            token,
            &DecodingKey::from_secret(self.secret.expose_secret().as_bytes()),
            &validation.unwrap_or_default(),
        )
        .map(|token_data| token_data.claims)
        .map_err(anyhow::Error::from)
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub fn login(
        &self,
        uid: u64,
        role: u64,
        user_agent: String,
    ) -> anyhow::Result<(String, ClaimsRefresh)> {
        let now = Utc::now();
        let refresh_claims = ClaimsRefresh {
            jti: Uuid::new_v4().to_string(),
            from: user_agent,
            iat: now.timestamp(),
            exp: (now + self.refresh_ttl).timestamp(),
            token_type: TokenType::Refresh,
            sub: uid,
            role: role,
            extra: Default::default(),
        };

        let access_claims = ClaimsFromAccess {
            jti: Uuid::new_v4().to_string(),
            from: refresh_claims.jti.clone(),
            iat: now.timestamp(),
            exp: (now + self.access_ttl).timestamp(),
            token_type: TokenType::Access,
            sub: uid,
            role: role,
            extra: Default::default(),
        };

        let access_token = self.generate_jwt(&access_claims)?;
        Ok((access_token, refresh_claims))
    }

    /// Refresh Token 검증 및 Access Token 재발급
    /// 1. refresh_token이 JWT로서 유효한지 먼저 검증
    /// 2. (선택) DB/Redis 등에서 추가 검증(블랙리스트, 만료, 사용자 상태 등)
    /// 3. 검증 통과 시 새로운 access_token 발급
    /// 실제 서비스에서는 refresh_token 재사용 방지, 블랙리스트, 만료, 사용자 상태 등 섬세한 보안 정책 필요
    pub fn refresh_access_token(
        &self,
        access_token: &ClaimsForExchange,
        refresh_token: &ClaimsRefresh,
        user_agent: String,
    ) -> anyhow::Result<(String, ClaimsRefresh)> {
        if access_token.exp < chrono::Utc::now().timestamp()
            || access_token.sub != refresh_token.sub
            || access_token.from != refresh_token.jti
            || refresh_token.from != user_agent.to_string()
        {
            return Err(anyhowln!("Invalid token type"));
        }

        self.login(access_token.sub, access_token.role, user_agent)
    }
}

pub struct JwtIssuer {
    inner: RwLock<Inner>,
}

impl JwtIssuer {
    pub fn new(
        name: String,
        secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            inner: Inner::new(name, secret, access_ttl, refresh_ttl).into(),
        }
    }

    pub async fn exchange_secret(&self, new_secret: SecretString) -> SecretString {
        let mut inner = self.inner.write().await;
        inner.exchange_secret(new_secret)
    }

    pub async fn get_secret(&self) -> SecretString {
        let inner = self.inner.read().await;
        inner.get_secret()
    }

    pub async fn generate_jwt(&self, claims: &ClaimsFromAccess) -> anyhow::Result<String> {
        let inner = self.inner.read().await;
        inner.generate_jwt(claims)
    }

    pub async fn verify_jwt(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<ClaimsFromAccess> {
        let inner = self.inner.read().await;
        inner.verify_jwt(token, validation)
    }

    pub async fn login(
        &self,
        uid: u64,
        role: u64,
        user_agent: String,
    ) -> anyhow::Result<(String, ClaimsRefresh)> {
        let inner = self.inner.read().await;
        inner.login(uid, role, user_agent)
    }

    pub async fn refresh_access_token(
        &self,
        access_token: &ClaimsForExchange,
        refresh_token: &ClaimsRefresh,
        user_agent: String,
    ) -> anyhow::Result<(String, ClaimsRefresh)> {
        let inner = self.inner.read().await;
        inner.refresh_access_token(access_token, refresh_token, user_agent)
    }
}
