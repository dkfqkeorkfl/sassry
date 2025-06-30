use axum::{
    extract::FromRequestParts,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cassry::{
    chrono::Utc,
    ring::hmac,
    secrecy::{ExposeSecret, SecretString},
    *,
};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
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
            ClaimsError::MissingCookieHeader | ClaimsError::MissingJwtToken => {
                debug!("Claims error: {}", self);
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }

            ClaimsError::InvalidJwt(err) => {
                debug!("{} : {}", self, err);
                (StatusCode::UNAUTHORIZED, err.to_string()).into_response()
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
    pub nbf: i64,              // 시작 시간 (timestamp)
    pub token_type: TokenType, // access/refresh 등

    pub sub: u64,  // 사용자 ID
    pub role: u64, // 권한(플래그)

    // refresh 토큰은 아래 정보를 세션에 담아야 한다.
    // pub aud: u64,              // 대상자 (클라이언트 ID 등)

    // pub env: Environment,      // 실행환경
    // pub iss: u64,              // 발급자 (서비스 ID 등)
    // pub user_agent: String,    // 실행환경 (예: dev, prod)
    pub extra: std::marker::PhantomData<T>,
}

pub mod detail {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct AccessTag;

    #[derive(Debug, Serialize, Deserialize, Clone)]
    pub struct RefreshTag;
}

pub type ClaimsFromAccess = ClaimsCore<detail::AccessTag>;
pub type ClaimsRefresh = ClaimsCore<detail::RefreshTag>;

impl Default for ClaimsFromAccess {
    fn default() -> Self {
        let now = Utc::now().timestamp();

        Self {
            jti: Uuid::new_v4().to_string(),
            from: Default::default(),
            iat: now,
            exp: now,
            nbf: now,
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
            let cookies = parts
                .extensions
                .get::<tower_cookies::Cookies>()
                .ok_or(ClaimsError::MissingCookieHeader)?;
            let cookie = cookies
                .get(JwtIssuer::get_cookie_name())
                .ok_or(ClaimsError::MissingCookieHeader)?;
            let jwt_manager = parts
                .extensions
                .get::<Arc<JwtIssuer>>()
                .ok_or_else(|| ClaimsError::Internal(anyhow::anyhow!("JwtManager not found")))?;

            let cookie_value = cookie.value();
            println!("cookie_value: {}", cookie_value);
            jwt_manager
                .verify_jwt(&cookie_value, None)
                .await
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

    pub fn compute_hash(&self, data: &str) -> anyhow::Result<String> {
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.secret.expose_secret().as_bytes());
        let tag = hmac::sign(&key, data.as_bytes());
        Ok(hex::encode(tag.as_ref()))
    }

    /// JWT 토큰 생성 (HS256) - 클레임 구조체를 직접 받아 생성
    pub fn generate_jwt<T: Serialize>(&self, claims: &T) -> anyhow::Result<String> {
        encode(
            &Header::default(),
            claims,
            &EncodingKey::from_secret(self.secret.expose_secret().as_bytes()),
        )
        .map_err(anyhow::Error::from)
    }

    pub fn verify<T: DeserializeOwned>(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<T> {
        decode::<T>(
            token,
            &DecodingKey::from_secret(self.secret.expose_secret().as_bytes()),
            &validation.unwrap_or_default(),
        )
        .map(|token_data| token_data.claims)
        .map_err(anyhow::Error::from)
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub fn verify_jwt(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<ClaimsFromAccess> {
        self.verify::<ClaimsFromAccess>(token, validation)
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub fn login(
        &self,
        uid: u64,
        role: u64,
        user_agent: String,
    ) -> anyhow::Result<(ClaimsFromAccess, ClaimsRefresh)> {
        let now = Utc::now();
        let timestamp = now.timestamp();
        let refresh_claims = ClaimsRefresh {
            jti: Uuid::new_v4().to_string(),
            from: user_agent,
            iat: timestamp,
            exp: (now + self.refresh_ttl).timestamp(),
            nbf: timestamp,
            token_type: TokenType::Refresh,
            sub: uid,
            role: role,
            extra: Default::default(),
        };

        let access_claims = ClaimsFromAccess {
            jti: Uuid::new_v4().to_string(),
            from: refresh_claims.jti.clone(),
            iat: timestamp,
            exp: (now + self.access_ttl).timestamp(),
            nbf: timestamp,
            token_type: TokenType::Access,
            sub: uid,
            role: role,
            extra: Default::default(),
        };

        Ok((access_claims, refresh_claims))
    }

    pub fn refresh_access_token(
        &self,
        refresh_token: &ClaimsRefresh,
        user_agent: String,
    ) -> anyhow::Result<(ClaimsFromAccess, ClaimsRefresh)> {
        if refresh_token.from != user_agent {
            return Err(anyhowln!("Invalid token type"));
        }

        let now = Utc::now();
        let timestamp = now.timestamp();
        let refresh_claims = ClaimsRefresh {
            jti: Uuid::new_v4().to_string(),
            from: user_agent,
            iat: timestamp,
            exp: (now + self.refresh_ttl).timestamp(),
            nbf: refresh_token.nbf,
            token_type: TokenType::Refresh,
            sub: refresh_token.sub,
            role: refresh_token.role,
            extra: Default::default(),
        };

        let access_claims = ClaimsFromAccess {
            jti: Uuid::new_v4().to_string(),
            from: refresh_claims.jti.clone(),
            iat: timestamp,
            exp: (now + self.access_ttl).timestamp(),
            nbf: refresh_token.nbf,
            token_type: TokenType::Access,
            sub: refresh_token.sub,
            role: refresh_token.role,
            extra: Default::default(),
        };

        Ok((access_claims, refresh_claims))
    }
}

pub struct JwtIssuer {
    inner: RwLock<Inner>,
}

impl JwtIssuer {
    pub fn get_cookie_name() -> &'static str {
        "access"
    }

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

    pub async fn generate_jwt<T: Serialize>(&self, claims: &T) -> anyhow::Result<String> {
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
    ) -> anyhow::Result<(ClaimsFromAccess, ClaimsRefresh)> {
        let inner = self.inner.read().await;
        inner.login(uid, role, user_agent)
    }

    pub async fn refresh_access_token(
        &self,
        refresh_token: &ClaimsRefresh,
        user_agent: String,
    ) -> anyhow::Result<(ClaimsFromAccess, ClaimsRefresh)> {
        let inner = self.inner.read().await;
        inner.refresh_access_token(refresh_token, user_agent)
    }

    pub async fn compute_hash(&self, data: &str) -> anyhow::Result<String> {
        let inner = self.inner.read().await;
        inner.compute_hash(data)
    }

    pub async fn get_name(&self) -> String {
        let inner = self.inner.read().await;
        inner.name.clone()
    }

    pub async fn verify<T: DeserializeOwned>(&self, data: &str, validation: Option<Validation>) -> anyhow::Result<T> {
        let inner = self.inner.read().await;
        inner.verify::<T>(data, validation)
    }
}
