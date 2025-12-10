use super::error::HttpError;
use axum::{extract::FromRequestParts, http::HeaderMap};
use base64::engine::Engine;
use cassry::{
    chrono::Utc,
    ring::hmac,
    secrecy::{ExposeSecret, SecretString},
    *,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Validation};
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;
use tower_cookies::Cookies;
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

#[derive(Debug, Default, Clone, bincode::Encode, bincode::Decode)]
pub struct UserPayload {
    pub uid: u64,
    pub role: u64,
    pub nick: String,

    pub login_at: i64,
    pub payload_creation: i64,
}

impl Serialize for UserPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let config = bincode::config::standard();
        let encoded = bincode::encode_to_vec(self, config).map_err(|e| {
            serde::ser::Error::custom(format!("bincode serialization failed: {}", e))
        })?;
        let base64_str = base64::engine::general_purpose::STANDARD.encode(&encoded);
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for UserPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = bincode::config::standard();
        let base64_str = String::deserialize(deserializer)?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&base64_str)
            .map_err(|e| serde::de::Error::custom(format!("base64 decoding failed: {}", e)))?;
        bincode::decode_from_slice(&decoded, config)
            .map(|(result, _)| result)
            .map_err(|e| serde::de::Error::custom(format!("bincode deserialization failed: {}", e)))
    }
}

#[derive(Debug, Default, Clone, bincode::Encode, bincode::Decode)]
pub struct AccessPayload {
    pub uid: u64,
    pub created_at: i64,
}

impl From<&UserPayload> for AccessPayload {
    fn from(payload: &UserPayload) -> Self {
        Self {
            uid: payload.uid,
            created_at: payload.payload_creation,
        }
    }
}

impl Serialize for AccessPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let config = bincode::config::standard();
        let encoded = bincode::encode_to_vec(self, config).map_err(|e| {
            serde::ser::Error::custom(format!("bincode serialization failed: {}", e))
        })?;
        let base64_str = base64::engine::general_purpose::STANDARD.encode(&encoded);
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for AccessPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = bincode::config::standard();
        let base64_str = String::deserialize(deserializer)?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&base64_str)
            .map_err(|e| serde::de::Error::custom(format!("base64 decoding failed: {}", e)))?;
        bincode::decode_from_slice(&decoded, config)
            .map(|(result, _)| result)
            .map_err(|e| serde::de::Error::custom(format!("bincode deserialization failed: {}", e)))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserRefreshClaims {
    /// 토큰 고유 ID
    pub jti: String,
    /// 토큰의 만료 시간(초 단위)
    pub exp: i64,
    /// 발급자
    pub iss: String,
    /// 사용자 ID
    pub sub: String,
    /// 발급 시간
    pub iat: i64,

    /// 환경
    pub env: String,

    pub role: u64,
    pub failed: usize,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserClaimsFrame<T> {
    /// 토큰 고유 ID
    pub jti: String,
    /// 토큰의 만료 시간(초 단위)
    pub exp: i64,

    /// 발급자
    pub derived_from: String,
    pub payload: T,
}

pub type UserAccessClaims = UserClaimsFrame<AccessPayload>;
pub type UserCsrfClaims = UserClaimsFrame<UserPayload>;

impl<S> FromRequestParts<S> for UserAccessClaims
where
    S: Send + Sync,
{
    type Rejection = HttpError;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl core::future::Future<Output = Result<Self, Self::Rejection>> {
        async move {
            let cookies = parts
                .extensions
                .get::<tower_cookies::Cookies>()
                .ok_or(HttpError::MissingJwtToken)?;
            let jwt_manager = parts
                .extensions
                .get::<AccessIssuer>()
                .ok_or(anyhow::anyhow!("JwtManager not found"))?;

            let mut validation = Validation::default();
            validation.validate_exp = false;
            let claims = jwt_manager
                .export_access_claims_from(cookies, Some(validation))
                .await?;
            if chrono::Utc::now().timestamp() > claims.exp {
                return Err(HttpError::ExpiredJwt);
            }

            Ok(claims)
        }
    }
}

impl<S> FromRequestParts<S> for UserCsrfClaims
where
    S: Send + Sync,
{
    type Rejection = HttpError;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl core::future::Future<Output = Result<Self, Self::Rejection>> {
        async move {
            let cookies = parts
                .extensions
                .get::<tower_cookies::Cookies>()
                .ok_or(HttpError::MissingJwtToken)?;
            let jwt_manager = parts
                .extensions
                .get::<AccessIssuer>()
                .ok_or(anyhow::anyhow!("JwtManager not found"))?;

            let mut validation = Validation::default();
            validation.validate_exp = false;
            let claims_pair = jwt_manager
                .export_claims_pair_from(cookies, &parts.headers, Some(validation))
                .await?;

            // 만료 시간 확인
            if chrono::Utc::now().timestamp() > claims_pair.csrf_claims.exp {
                return Err(HttpError::ExpiredJwt);
            }

            Ok(claims_pair.csrf_claims)
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IssuedClaims {
    pub refresh_claims: UserRefreshClaims,
    pub access_token: String,
    pub csrf_token: String,
}

pub struct ClaimsPair {
    pub access_claims: UserAccessClaims,
    pub csrf_claims: UserCsrfClaims,
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
    name: String,
    access_isser: JwtIssuer,
    csrf_isser: JwtIssuer,
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
        access_secret: SecretString,
        csrf_secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            name,
            access_isser: JwtIssuer {
                secret: access_secret,
            },
            csrf_isser: JwtIssuer {
                secret: csrf_secret,
            },
            access_ttl,
            refresh_ttl,
        }
    }

    pub fn verify_jwt_access(
        &self,
        access_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<UserAccessClaims> {
        self.access_isser
            .verify::<UserAccessClaims>(access_token, validation)
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub fn verify_jwt_pair(
        &self,
        access_token: &str,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<ClaimsPair> {
        let access_claims = self
            .access_isser
            .verify::<UserAccessClaims>(access_token, validation.clone())?;
        let csrf_claims = self
            .csrf_isser
            .verify::<UserCsrfClaims>(csrf_token, validation)?;
        if csrf_claims.derived_from != access_claims.jti {
            return Err(anyhow::anyhow!(
                "Invalid csrf token: from={}, jti={}",
                csrf_claims.derived_from,
                access_claims.jti
            ));
        }
        Ok(ClaimsPair {
            access_claims,
            csrf_claims,
        })
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub fn login(
        &self,
        uid: u64,
        role: u64,
        nick: String,
        password: String,
        user_agent: String,
    ) -> anyhow::Result<IssuedClaims> {
        let now = Utc::now();
        let user_payload = UserPayload {
            uid,
            role,
            nick,
            login_at: now.timestamp_millis(),
            payload_creation: now.timestamp_millis(),
        };
        let access_payload = AccessPayload {
            uid,
            created_at: now.timestamp_millis(),
        };

        let refresh_claims = UserRefreshClaims {
            jti: Uuid::new_v4().to_string(),
            exp: (now + self.refresh_ttl).timestamp(),
            iss: self.name.clone(),
            sub: uid.to_string(),
            iat: now.timestamp(),
            env: user_agent,
            role: role,
            failed: 0,
            password: password,
        };

        let access_claims = UserAccessClaims {
            jti: Uuid::new_v4().to_string(),
            exp: (now + self.access_ttl).timestamp(),
            derived_from: refresh_claims.jti.clone(),
            payload: access_payload,
        };

        let csrf_claims = self.generate_csrf_token(&access_claims, user_payload);
        Ok(IssuedClaims {
            refresh_claims,
            access_token: self.access_isser.generate_jwt(&access_claims)?,
            csrf_token: self.csrf_isser.generate_jwt(&csrf_claims)?,
        })
    }

    pub fn generate_csrf_token(
        &self,
        access_claims: &UserAccessClaims,
        user_payload: UserPayload,
    ) -> UserCsrfClaims {
        UserCsrfClaims {
            jti: Uuid::new_v4().to_string(),
            exp: access_claims.exp,
            derived_from: access_claims.jti.clone(),
            payload: user_payload,
        }
    }

    pub fn refresh_access_token(
        &self,
        prev_access_claims: &UserAccessClaims,
        prev_refresh_clams: UserRefreshClaims,
        mut user_payload: UserPayload,
    ) -> anyhow::Result<IssuedClaims> {
        if prev_access_claims.payload.uid != user_payload.uid
            || prev_access_claims.derived_from != prev_refresh_clams.jti
        {
            return Err(anyhow::anyhow!(
                "Invalid refresh token: uid={}",
                prev_access_claims.payload.uid
            ));
        }

        let now = Utc::now();
        user_payload.payload_creation = now.timestamp_millis();
        let refresh_claims = UserRefreshClaims {
            jti: Uuid::new_v4().to_string(),
            exp: (now + self.refresh_ttl).timestamp(),

            iss: self.name.clone(),
            sub: prev_refresh_clams.sub,
            iat: now.timestamp(),
            env: prev_refresh_clams.env,
            role: prev_refresh_clams.role,
            failed: prev_refresh_clams.failed,
            password: prev_refresh_clams.password,
        };

        let access_claims = UserAccessClaims {
            jti: Uuid::new_v4().to_string(),
            exp: (now + self.access_ttl).timestamp(),
            derived_from: refresh_claims.jti.clone(),
            payload: (&user_payload).into(),
        };

        let csrf_claims = self.generate_csrf_token(&access_claims, user_payload);
        Ok(IssuedClaims {
            refresh_claims,
            access_token: self.access_isser.generate_jwt(&access_claims)?,
            csrf_token: self.csrf_isser.generate_jwt(&csrf_claims)?,
        })
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
        access_secret: SecretString,
        csrf_secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            isser: Arc::new(
                AccessIssuerImpl::new(name, access_secret, csrf_secret, access_ttl, refresh_ttl)
                    .into(),
            ),
        }
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub async fn login(
        &self,
        uid: u64,
        role: u64,
        nick: String,
        password: String,
        user_agent: String,
    ) -> anyhow::Result<IssuedClaims> {
        self.isser
            .read()
            .await
            .login(uid, role, nick, password, user_agent)
    }

    pub async fn refresh_access_token(
        &self,
        prev_access_claims: &UserAccessClaims,
        prev_refresh_clams: UserRefreshClaims,
        user_payload: UserPayload,
    ) -> anyhow::Result<IssuedClaims> {
        self.isser.read().await.refresh_access_token(
            &prev_access_claims,
            prev_refresh_clams,
            user_payload,
        )
    }

    pub async fn export_access_claims_from(
        &self,
        cookies: &Cookies,
        validation: Option<Validation>,
    ) -> Result<UserAccessClaims, HttpError> {
        let access_token = cookies
            .get(AccessIssuer::get_cookie_name())
            .ok_or(HttpError::MissingJwtToken)?;
        let access_claims = self
            .isser
            .read()
            .await
            .verify_jwt_access(access_token.value(), validation)
            .map_err(|e| HttpError::InvalidJwt(e))?;
        Ok(access_claims)
    }

    pub async fn export_claims_pair_from_bearer(
        &self,
        cookies: &Cookies,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> Result<ClaimsPair, HttpError> {
        let access_token = cookies
            .get(AccessIssuer::get_cookie_name())
            .ok_or(HttpError::MissingJwtToken)?;

        self.isser
            .read()
            .await
            .verify_jwt_pair(access_token.value(), csrf_token, validation)
            .map_err(|e| HttpError::InvalidJwt(e))
    }

    pub async fn export_claims_pair_from(
        &self,
        cookies: &Cookies,
        headers: &HeaderMap,
        validation: Option<Validation>,
    ) -> Result<ClaimsPair, HttpError> {
        let authorization = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|header| header.to_str().ok())
            .ok_or(HttpError::MissingJwtToken)?;
        let csrf_token = authorization
            .strip_prefix("Bearer ")
            .ok_or(HttpError::InvalidJwt(anyhow::anyhow!(
                "Invalid Authorization header format"
            )))?;

        self.export_claims_pair_from_bearer(cookies, csrf_token, validation)
            .await
    }
}
