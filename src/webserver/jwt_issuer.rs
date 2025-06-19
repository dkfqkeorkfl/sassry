use axum::{extract::FromRequestParts, http::StatusCode, response::{IntoResponse, Response}};
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};
use thiserror::Error;
use cassry::{
    secrecy::{ExposeSecret, SecretString},
    *,
};

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

    pub exp: usize,            // 만료 시간 (timestamp)
    pub iat: usize,            // 발급 시간 (timestamp)
    pub token_type: TokenType, // access/refresh 등

    pub sub: u64,              // 사용자 ID
    pub role: u64,             // 권한(플래그)

    // refresh 토큰은 아래 정보를 세션에 담아야 한다.
    // pub aud: u64,              // 대상자 (클라이언트 ID 등)
    // pub nbf: usize,            // 시작 시간 (timestamp)
    // pub env: Environment,      // 실행환경
    // pub iss: u64,              // 발급자 (서비스 ID 등)
    // pub user_agent: String,    // 실행환경 (예: dev, prod)
    pub extra: std::marker::PhantomData<T>,
}

pub type ClaimsFromAccess = ClaimsCore<()>;
pub type ClaimsForRefresh = ClaimsCore<u64>;

impl From<ClaimsFromAccess> for ClaimsForRefresh {
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
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;

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

impl<S> FromRequestParts<S> for ClaimsForRefresh
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
    issuer: u64,
    env: String,
}

impl Inner {
    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(issuer: u64, env: String, secret: SecretString) -> Self {
        Self {
            secret,
            issuer,
            env,
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
    pub fn login(&self) -> anyhow::Result<(String, String)> {
        let mut refresh_claims = ClaimsFromAccess::default();
        refresh_claims.token_type = TokenType::Refresh;

        let mut access_claims = refresh_claims.clone();
        access_claims.token_type = TokenType::Access;
        access_claims.from = refresh_claims.jti.clone();

        let access_token = self.generate_jwt(&access_claims)?;
        let refresh_token = self.generate_jwt(&refresh_claims)?;

        Ok((access_token, refresh_token))
    }

    /// Refresh Token 검증 및 Access Token 재발급
    /// 1. refresh_token이 JWT로서 유효한지 먼저 검증
    /// 2. (선택) DB/Redis 등에서 추가 검증(블랙리스트, 만료, 사용자 상태 등)
    /// 3. 검증 통과 시 새로운 access_token 발급
    /// 실제 서비스에서는 refresh_token 재사용 방지, 블랙리스트, 만료, 사용자 상태 등 섬세한 보안 정책 필요
    pub fn refresh_access_token(
        &self,
        access_token: &ClaimsForRefresh,
        refresh_token: &ClaimsFromAccess,
    ) -> anyhow::Result<(String, String)> {
        // 1. JWT 구조 및 서명, 만료 등 1차 검증
        //let claims = self.jwt_manager.read().await.verify_jwt(refresh_token, None).await?;
        // 2. 토큰 타입이 refresh인지 확인
        // if claims.token_type != TokenType::Refresh {
        //     return Err(anyhowln!("Invalid token type"));
        // }
        // 3. (선택) DB/Redis 등에서 refresh_token 추가 검증 (블랙리스트, 만료, 사용자 상태 등)
        // TODO: self.token_manager.verify_refresh_token(&claims.sub.to_string(), refresh_token) 등 추가
        // 4. 검증 통과 시 새로운 access_token 발급
        let mut refresh_claims = ClaimsFromAccess::default();
        refresh_claims.token_type = TokenType::Refresh;
        let mut access_claims = refresh_claims.clone();
        access_claims.token_type = TokenType::Access;
        access_claims.from = refresh_claims.jti.clone();

        let new_access_token = self.generate_jwt(&access_claims)?;
        let new_refresh_token = self.generate_jwt(&refresh_claims)?;
        // (선택) refresh_token 재사용 방지 정책이 있다면, 새 refresh_token도 발급 및 저장
        // self.token_manager.store_tokens(&claims.sub.to_string(), &new_access_token, refresh_token);
        Ok((new_access_token, new_refresh_token))
    }
}

pub struct JwtIssuer {
    inner: RwLock<Inner>,
}

impl JwtIssuer {
    pub fn new(secret: SecretString, issuer: u64, env: String) -> Self {
        Self {
            inner: Inner::new(issuer, env, secret).into(),
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

    pub async fn login(&self) -> anyhow::Result<(String, String)> {
        let inner = self.inner.read().await;
        inner.login()
    }

    pub async fn refresh_access_token(
        &self,
        access_token: &ClaimsForRefresh,
        refresh_token: &ClaimsFromAccess,
    ) -> anyhow::Result<(String, String)> {
        let inner = self.inner.read().await;
        inner.refresh_access_token(access_token, refresh_token)
    }
}
