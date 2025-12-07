use super::error::HttpError;
use axum::extract::FromRequestParts;
use cassry::{
    chrono::Utc,
    ring::hmac,
    secrecy::{ExposeSecret, SecretString},
    tokio::sync::RwLock,
    *,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Validation};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

/// 토큰 타입 구분 (access/refresh 등)
// #[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
// pub enum TokenType {
//     #[serde(rename = "N")]
//     None,
//     #[serde(rename = "A")]
//     Access,
//     #[serde(rename = "R")]
//     Refresh,
//     #[serde(rename = "E")]
//     Etc(String),
// }

// impl Default for TokenType {
//     fn default() -> Self {
//         TokenType::None
//     }
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SocketClaims {
    pub sub: u64,
    pub role: u64,
    pub aud: String,
    pub from: String,

    pub rnbf: i64, //refresh가 시작되는 시간.
    pub nbf: i64,  // 로그인 시간(timestamp)
    pub exp: i64,  // 만료 시간 (timestamp)
    pub iat: i64,  // 발급 시간 (timestamp)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AccessClaimsFrame<T> {
    pub jti: String, // 토큰 고유 ID
    pub from: String,

    pub nbf: i64, // 로그인 시간 (timestamp)
    pub iat: i64, // 발급 시간 (timestamp)
    pub exp: i64, // 만료 시간 (timestamp)

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

pub type AccessClaims = AccessClaimsFrame<detail::AccessTag>;
pub type RefreshClaims = AccessClaimsFrame<detail::RefreshTag>;

impl Default for AccessClaims {
    fn default() -> Self {
        let now = Utc::now().timestamp();

        Self {
            jti: Uuid::new_v4().to_string(),
            from: Default::default(),
            iat: now,
            exp: now,
            nbf: now,
            sub: Default::default(),
            role: Default::default(),
            extra: std::marker::PhantomData::default(),
        }
    }
}

impl<S> FromRequestParts<S> for AccessClaims
where
    S: Send + Sync,
{
    type Rejection = HttpError;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl core::future::Future<Output = Result<Self, Self::Rejection>> {
        async move {
            let cookie = parts
                .extensions
                .get::<tower_cookies::Cookies>()
                .and_then(|cookies| cookies.get(AccessIssuer::get_cookie_name()))
                .ok_or(HttpError::MissingJwtToken)?;
            let jwt_manager = parts
                .extensions
                .get::<Arc<AccessIssuer>>()
                .ok_or(anyhow::anyhow!("JwtManager not found"))?;

            let mut validation = Validation::default();
            validation.validate_exp = false;
            let claims = jwt_manager
                .verify_jwt(&cookie.value(), Some(validation))
                .await
                .map_err(|e| HttpError::InvalidJwt(e))?;
            if chrono::Utc::now().timestamp() > claims.exp {
                return Err(HttpError::ExpiredJwt);
            }

            Ok(claims)
        }
    }
}

pub struct JwtIssuer {
    secret: SecretString,
}

impl JwtIssuer {
    pub fn get_secret(&self) -> SecretString {
        self.secret.clone()
    }

    pub fn compute_hash(&self, data: &str) -> anyhow::Result<String> {
        let key = hmac::Key::new(hmac::HMAC_SHA256, self.secret.expose_secret().as_bytes());
        let tag = hmac::sign(&key, data.as_bytes());
        Ok(hex::encode(tag))
    }

    pub fn generate_jwt<T: Serialize>(&self, claims: &T) -> anyhow::Result<String> {
        jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
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
        jsonwebtoken::decode::<T>(
            token,
            &DecodingKey::from_secret(self.secret.expose_secret().as_bytes()),
            &validation.unwrap_or_default(),
        )
        .map(|token_data| token_data.claims)
        .map_err(anyhow::Error::from)
    }
}

pub struct AccessIssuerImpl {
    name : String,
    isser: JwtIssuer,
    access_ttl: chrono::Duration,
    refresh_ttl: chrono::Duration,
}

impl AccessIssuerImpl {
    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn set_access_ttl(&mut self, access_ttl: chrono::Duration) {
        self.access_ttl = access_ttl;
    }
    pub fn get_access_ttl(&self) -> &chrono::Duration {
        &self.access_ttl
    }

    
    pub fn get_refresh_ttl(&self) -> &chrono::Duration {
        &self.refresh_ttl
    }
    pub fn set_refresh_ttl(&mut self, refresh_ttl: chrono::Duration) {
        self.refresh_ttl = refresh_ttl;
    }
    

    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(
        name: String,
        secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            name,
            isser: JwtIssuer { secret },
            access_ttl,
            refresh_ttl,
        }
    }

    pub fn generate_jwt(&self, claims: &AccessClaims) -> anyhow::Result<String> {
        self.isser.generate_jwt(claims)
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub fn verify_jwt(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<AccessClaims> {
        self.isser.verify::<AccessClaims>(token, validation)
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub fn login(&self, uid: u64, role: u64, from: String) -> (AccessClaims, RefreshClaims) {
        let now = Utc::now();
        let timestamp = now.timestamp();
        let refresh_claims = RefreshClaims {
            jti: Uuid::new_v4().to_string(),
            from,
            iat: timestamp,
            exp: (now + self.refresh_ttl).timestamp(),
            nbf: timestamp,
            sub: uid,
            role: role,
            extra: Default::default(),
        };

        let access_claims = AccessClaims {
            jti: Uuid::new_v4().to_string(),
            from: refresh_claims.jti.clone(),
            iat: timestamp,
            exp: (now + self.access_ttl).timestamp(),
            nbf: timestamp,
            sub: uid,
            role: role,
            extra: Default::default(),
        };

        (access_claims, refresh_claims)
    }

    pub fn refresh_access_token(
        &self,
        refresh_clams: &RefreshClaims,
        access_claims: &AccessClaims,
    ) -> anyhow::Result<(AccessClaims, RefreshClaims)> {
        if access_claims.from != refresh_clams.jti || access_claims.sub != refresh_clams.sub {
            return Err(anyhow::anyhow!(
                "Invalid refresh token: from={}, sub={}",
                access_claims.from,
                access_claims.sub
            ));
        }

        let now = Utc::now();
        let timestamp = now.timestamp();
        let refresh_claims = RefreshClaims {
            jti: Uuid::new_v4().to_string(),
            from: refresh_clams.from.clone(),
            iat: timestamp,
            exp: (now + self.refresh_ttl).timestamp(),
            nbf: refresh_clams.nbf,
            sub: refresh_clams.sub,
            role: refresh_clams.role,
            extra: Default::default(),
        };

        let access_claims = AccessClaims {
            jti: Uuid::new_v4().to_string(),
            from: refresh_claims.jti.clone(),
            iat: timestamp,
            exp: (now + self.access_ttl).timestamp(),
            nbf: refresh_clams.nbf,
            sub: refresh_clams.sub,
            role: refresh_clams.role,
            extra: Default::default(),
        };

        Ok((access_claims, refresh_claims))
    }
}

#[derive(Clone)]
pub struct AccessIssuer {
    isser: RwArc<AccessIssuerImpl>,
}

impl AccessIssuer {
    pub async fn get_name(&self) -> String {
        self.isser.read().await.get_name().clone()
    }
    
    pub async fn get_access_ttl(&self) -> chrono::Duration {
        self.isser.read().await.get_access_ttl().clone()
    }
    pub async fn set_access_ttl(&mut self, access_ttl: chrono::Duration) {
        self.isser.write().await.set_access_ttl(access_ttl);
    }

    pub async fn get_refresh_ttl(&self) -> chrono::Duration {
        self.isser.read().await.get_refresh_ttl().clone()
    }
    pub async fn set_refresh_ttl(&mut self, refresh_ttl: chrono::Duration) {
        self.isser.write().await.set_refresh_ttl(refresh_ttl);
    }

    pub const fn get_cookie_name() -> &'static str {
        "access"
    }

    pub const fn get_session_sub_name() -> &'static str {
        "sub"
    }

    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(
        name: String,
        secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            isser: Arc::new(RwLock::new(AccessIssuerImpl {
                name,
                isser: JwtIssuer { secret },
                access_ttl,
                refresh_ttl,
            })),
        }
    }

    pub async fn generate_jwt(&self, claims: &AccessClaims) -> anyhow::Result<String> {
        self.isser.read().await.generate_jwt(claims)
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub async fn verify_jwt(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<AccessClaims> {
        self.isser.read().await.verify_jwt(token, validation)
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub async fn login(&self, uid: u64, role: u64, from: String) -> (AccessClaims, RefreshClaims) {
        self.isser.read().await.login(uid, role, from)
    }

    pub async fn refresh_access_token(
        &self,
        refresh_clams: &RefreshClaims,
        access_claims: &AccessClaims,
    ) -> anyhow::Result<(AccessClaims, RefreshClaims)> {
        self.isser
            .read()
            .await
            .refresh_access_token(refresh_clams, access_claims)
    }
}
