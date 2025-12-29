use super::error::HttpError;
use axum::{extract::FromRequestParts, http::HeaderMap};
use cassry::{
    base64::Engine,
    chrono::{DateTime, Utc},
    moka::future::Cache,
    secrecy::{ExposeSecret, SecretString},
    *,
};
use jsonwebtoken::{DecodingKey, EncodingKey, Validation};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr, TimestampSeconds};
use std::{net::IpAddr, sync::Arc, u64};
use tower_cookies::Cookies;
use uuid::Uuid;

pub struct LoginParams {
    pub uid: i64,
    pub password: String,
    pub role: i64,

    pub nick: String,
    pub login_ip: IpAddr,
}

pub trait DerivedFrom {
    fn get_derived_from(&self) -> Uuid;
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserPayload {
    pub derived_from: [u8; 16],

    pub uid: i64,
    pub role: i64,
    pub nick: String,

    #[serde(with = "serialization::ipaddr_bytes")]
    pub login_ip: IpAddr,
    pub login_at: i64,
    pub payload_created_at: i64,
}

impl DerivedFrom for UserPayload {
    fn get_derived_from(&self) -> Uuid {
        Uuid::from_bytes(self.derived_from.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPayload {
    pub derived_from: [u8; 16],

    pub uid: i64,
    pub payload_created_at: i64,
}

impl DerivedFrom for AccessPayload {
    fn get_derived_from(&self) -> Uuid {
        Uuid::from_bytes(self.derived_from.clone())
    }
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserRefreshClaims {
    /// 토큰 고유 ID
    #[serde(rename = "_id")]
    #[serde_as(as = "DisplayFromStr")]
    pub jti: Uuid,
    /// 토큰의 만료 시간(초 단위)
    #[serde_as(as = "TimestampSeconds")]
    pub exp: DateTime<Utc>,
    /// 발급자
    pub iss: String,

    /// 사용자 ID
    pub sub: i64,
    /// 발급 시간
    pub iat: i64,

    /// 환경
    #[serde_as(as = "DisplayFromStr")]
    pub env: IpAddr,

    pub role: i64,
    pub failed: usize,
    pub password: String,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UserClaimsFrame<T: DerivedFrom + Serialize + DeserializeOwned> {
    /// 토큰 고유 ID
    #[serde_as(as = "DisplayFromStr")]
    pub jti: Uuid,
    /// 토큰의 만료 시간(
    #[serde_as(as = "TimestampSeconds")]
    pub exp: DateTime<Utc>,

    #[serde(with = "serialization::postcard_base64")]
    pub payload: T,
}

impl<T: DerivedFrom + Serialize + DeserializeOwned> DerivedFrom for UserClaimsFrame<T> {
    fn get_derived_from(&self) -> Uuid {
        self.payload.get_derived_from()
    }
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
                .get::<TokenIssuer>()
                .ok_or(anyhow::anyhow!("JwtManager not found"))?;

            let mut validation = Validation::default();
            validation.validate_exp = false;
            let claims = jwt_manager
                .extract_access_claims_from(cookies, Some(validation))
                .await?;
            if chrono::Utc::now() > claims.exp {
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
                .get::<TokenIssuer>()
                .ok_or(anyhow::anyhow!("JwtManager not found"))?;

            let mut validation = Validation::default();
            validation.validate_exp = false;
            let claims_pair = jwt_manager
                .extract_claims_pair_from(cookies, &parts.headers, Some(validation))
                .await?;

            // 만료 시간 확인
            if chrono::Utc::now() > claims_pair.csrf_claims.exp {
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
    pub csrf_dump: String,
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

    pub fn generate_jwt_with_nonce<T: Serialize>(
        &self,
        claims: &T,
        nonce: &[u8],
    ) -> anyhow::Result<String> {
        let arr = self.secret.expose_secret().as_bytes().try_into()?;
        let mut hasher = blake3::Hasher::new_keyed(&arr);
        hasher.update(nonce);
        let hash = hasher.finalize();

        jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            claims,
            &EncodingKey::from_secret(hash.as_bytes()),
        )
        .map_err(anyhow::Error::from)
    }

    pub fn verify_with_nonce<T: DeserializeOwned>(
        &self,
        token: &str,
        nonce: &[u8],
        validation: Option<Validation>,
    ) -> anyhow::Result<T> {
        let arr = self.secret.expose_secret().as_bytes().try_into()?;
        let mut hasher = blake3::Hasher::new_keyed(&arr);
        hasher.update(nonce);
        let hash = hasher.finalize();

        jsonwebtoken::decode::<T>(
            token,
            &DecodingKey::from_secret(hash.as_bytes()),
            &validation.unwrap_or_default(),
        )
        .map(|token_data| token_data.claims)
        .map_err(anyhow::Error::from)
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

pub struct TokenIssuerImpl {
    name: String,
    access_isser: JwtIssuer,
    csrf_isser: JwtIssuer,

    csrf_dump_key: SecretString,
    access_ttl: chrono::Duration,
    refresh_ttl: chrono::Duration,
    blocked_refreshs: Cache<Uuid, ()>,
}

impl TokenIssuerImpl {
    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn update(&mut self, access_ttl: chrono::Duration, refresh_ttl: chrono::Duration) {
        self.access_ttl = access_ttl;
        self.refresh_ttl = refresh_ttl;
    }

    pub fn get_access_ttl(&self) -> &chrono::Duration {
        &self.access_ttl
    }

    pub fn get_refresh_ttl(&self) -> &chrono::Duration {
        &self.refresh_ttl
    }

    pub async fn insert_block(&self, jti: &Uuid) {
        self.blocked_refreshs.insert(jti.clone(), ()).await;
    }

    pub fn contains_block(&self, jti: &Uuid) -> bool {
        self.blocked_refreshs.contains_key(jti)
    }

    pub fn generate_dump_nonce(
        &self,
        access_claims: &UserAccessClaims,
        addr: &IpAddr,
    ) -> anyhow::Result<Vec<u8>> {
        let addr = postcard::to_stdvec(addr)?;
        let bytes = [access_claims.jti.as_bytes(), addr.as_slice()].concat();
        let nonce = util::hmac_blake3_with_len(
            self.csrf_dump_key.expose_secret().as_bytes().try_into()?,
            bytes.as_slice(),
            12,
        )?;

        Ok(nonce)
    }

    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(
        name: String,
        access_secret: SecretString,
        csrf_secret: SecretString,
        csrf_dump_key: SecretString,
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
            csrf_dump_key,
            access_ttl,
            refresh_ttl,
            blocked_refreshs: moka::future::CacheBuilder::new(u64::MAX)
                .time_to_live(std::time::Duration::from_secs(
                    access_ttl.num_seconds() as u64
                ))
                .build(),
        }
    }

    pub fn verify_jwt_access(
        &self,
        access_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<UserAccessClaims> {
        self.access_isser
            .verify::<UserAccessClaims>(access_token, validation)
            .and_then(|claims| {
                if self.contains_block(&claims.payload.get_derived_from()) {
                    return Err(anyhow::anyhow!("Blocked refresh token"));
                }
                Ok(claims)
            })
    }

    pub fn verify_jwt_csrf(
        &self,
        access_claims: &UserAccessClaims,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<UserCsrfClaims> {
        self.csrf_isser.verify_with_nonce::<UserCsrfClaims>(
            csrf_token,
            access_claims.jti.as_bytes(),
            validation,
        )
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub fn verify_jwt_pair(
        &self,
        access_token: &str,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<ClaimsPair> {
        let access_claims = self.verify_jwt_access(access_token, validation.clone())?;
        let csrf_claims = self.verify_jwt_csrf(&access_claims, csrf_token, validation)?;
        if self.contains_block(&access_claims.jti) {
            return Err(anyhow::anyhow!("Invalid access token"));
        }
        Ok(ClaimsPair {
            access_claims,
            csrf_claims,
        })
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub fn login(&self, params: LoginParams) -> anyhow::Result<IssuedClaims> {
        let now = Utc::now();
        let refresh_jti = Uuid::new_v4();
        let refresh_claims = UserRefreshClaims {
            jti: refresh_jti,
            exp: now + self.refresh_ttl,
            iss: self.name.clone(),
            sub: params.uid,
            iat: now.timestamp(),
            env: params.login_ip,
            role: params.role,
            failed: 0,
            password: params.password,
        };

        let access_payload = AccessPayload {
            derived_from: refresh_jti.into_bytes(),
            uid: params.uid,
            payload_created_at: now.timestamp_millis(),
        };

        let access_jti = Uuid::new_v4();
        let access_claims = UserAccessClaims {
            jti: access_jti,
            exp: now + self.access_ttl,
            payload: access_payload,
        };

        let user_payload = UserPayload {
            derived_from: access_jti.into_bytes(),
            uid: params.uid,
            role: params.role,
            nick: params.nick,
            login_ip: params.login_ip,
            login_at: now.timestamp_millis(),
            payload_created_at: now.timestamp_millis(),
        };

        let (csrf_token, csrf_dump) = self.generate_csrf_token(&access_claims, user_payload)?;
        Ok(IssuedClaims {
            refresh_claims,
            access_token: self.access_isser.generate_jwt(&access_claims)?,
            csrf_token,
            csrf_dump,
        })
    }

    pub fn decrypt_csrf_dump(
        &self,
        access_claims: &UserAccessClaims,
        addr: &IpAddr,
        csrf_dump: &str,
    ) -> anyhow::Result<String> {
        let nonce = self.generate_dump_nonce(access_claims, addr)?;
        let encrypted = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(csrf_dump)?;
        let decrypted = util::decrypt_str_by_aes_gcm_128(
            access_claims.jti.as_bytes(),
            nonce.as_slice(),
            encrypted,
        )?;

        Ok(decrypted.expose_secret().to_string())
    }

    pub fn generate_csrf_token(
        &self,
        access_claims: &UserAccessClaims,
        user_payload: UserPayload,
    ) -> anyhow::Result<(String, String)> {
        let csrf_claims = UserCsrfClaims {
            jti: Uuid::new_v4(),
            exp: access_claims.exp,
            payload: user_payload,
        };

        let csrf_token = self
            .csrf_isser
            .generate_jwt_with_nonce(&csrf_claims, access_claims.jti.as_bytes())?;
        let nonce = self.generate_dump_nonce(access_claims, &csrf_claims.payload.login_ip)?;
        let encrypted = util::encrypt_str_by_aes_gcm_128(
            access_claims.jti.as_bytes(),
            nonce.as_slice(),
            &csrf_token,
        )?;
        Ok((
            csrf_token,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&encrypted),
        ))
    }

    pub fn refresh_access_token(
        &self,
        prev_access_claims: &UserAccessClaims,
        prev_refresh_clams: UserRefreshClaims,
        mut user_payload: UserPayload,
    ) -> anyhow::Result<IssuedClaims> {
        if prev_access_claims.payload.uid != user_payload.uid
            || prev_access_claims.payload.get_derived_from() != prev_refresh_clams.jti
        {
            return Err(anyhow::anyhow!(
                "Invalid refresh token: uid={}",
                prev_access_claims.payload.uid
            ));
        }

        let now = Utc::now();

        let refresh_jti = Uuid::new_v4();
        let refresh_claims = UserRefreshClaims {
            jti: refresh_jti,
            exp: now + self.refresh_ttl,

            iss: self.name.clone(),
            sub: prev_refresh_clams.sub,
            iat: now.timestamp(),
            env: prev_refresh_clams.env,
            role: prev_refresh_clams.role,
            failed: prev_refresh_clams.failed,
            password: prev_refresh_clams.password,
        };

        let access_payload = AccessPayload {
            derived_from: refresh_jti.into_bytes(),
            uid: user_payload.uid,
            payload_created_at: now.timestamp_millis(),
        };
        let access_jti = Uuid::new_v4();
        let access_claims = UserAccessClaims {
            jti: access_jti.clone(),
            exp: now + self.access_ttl,
            payload: access_payload,
        };

        user_payload.derived_from = access_jti.into_bytes();
        user_payload.payload_created_at = now.timestamp_millis();
        let (csrf_token, csrf_dump) = self.generate_csrf_token(&access_claims, user_payload)?;
        Ok(IssuedClaims {
            refresh_claims,
            access_token: self.access_isser.generate_jwt(&access_claims)?,
            csrf_token,
            csrf_dump,
        })
    }
}

#[derive(Clone)]
pub struct TokenIssuer {
    isser: RwArc<TokenIssuerImpl>,
}

impl TokenIssuer {
    pub async fn get_name(&self) -> String {
        self.isser.read().await.get_name().clone()
    }

    pub async fn update(&mut self, access_ttl: chrono::Duration, refresh_ttl: chrono::Duration) {
        self.isser.write().await.update(access_ttl, refresh_ttl);
    }
    pub async fn get_access_ttl(&self) -> chrono::Duration {
        self.isser.read().await.get_access_ttl().clone()
    }

    pub async fn get_refresh_ttl(&self) -> chrono::Duration {
        self.isser.read().await.get_refresh_ttl().clone()
    }

    pub const fn get_cookie_name() -> &'static str {
        "access"
    }

    pub const fn get_session_sub_name() -> &'static str {
        "sub"
    }

    pub async fn insert_block(&self, jti: &Uuid) {
        self.isser.write().await.insert_block(jti).await;
    }

    pub async fn contains_block(&self, jti: &Uuid) -> bool {
        self.isser.read().await.contains_block(jti)
    }

    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(
        name: String,
        access_secret: SecretString,
        csrf_secret: SecretString,
        csrf_dump_key: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
    ) -> Self {
        Self {
            isser: Arc::new(
                TokenIssuerImpl::new(
                    name,
                    access_secret,
                    csrf_secret,
                    csrf_dump_key,
                    access_ttl,
                    refresh_ttl,
                )
                .into(),
            ),
        }
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub async fn login(&self, params: LoginParams) -> anyhow::Result<IssuedClaims> {
        self.isser.read().await.login(params)
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

    pub async fn verify_jwt_csrf(
        &self,
        access_claims: &UserAccessClaims,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<UserCsrfClaims> {
        self.isser
            .read()
            .await
            .verify_jwt_csrf(access_claims, csrf_token, validation)
    }

    pub async fn extract_access_claims_from(
        &self,
        cookies: &Cookies,
        validation: Option<Validation>,
    ) -> Result<UserAccessClaims, HttpError> {
        let access_token = cookies
            .get(TokenIssuer::get_cookie_name())
            .ok_or(HttpError::MissingJwtToken)?;
        let access_claims = self
            .isser
            .read()
            .await
            .verify_jwt_access(access_token.value(), validation)
            .map_err(|e| HttpError::InvalidJwt(e))?;
        Ok(access_claims)
    }

    pub async fn extract_claims_pair_from_bearer(
        &self,
        cookies: &Cookies,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> Result<ClaimsPair, HttpError> {
        let access_token = cookies
            .get(TokenIssuer::get_cookie_name())
            .ok_or(HttpError::MissingJwtToken)?;

        self.isser
            .read()
            .await
            .verify_jwt_pair(access_token.value(), csrf_token, validation)
            .map_err(|e| HttpError::InvalidJwt(e))
    }

    pub async fn extract_claims_pair_from(
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

        self.extract_claims_pair_from_bearer(cookies, csrf_token, validation)
            .await
    }

    pub async fn decrypt_csrf_dump(
        &self,
        access_claims: &UserAccessClaims,
        addr: &IpAddr,
        csrf_dump: &str,
    ) -> anyhow::Result<String> {
        self.isser
            .read()
            .await
            .decrypt_csrf_dump(access_claims, addr, csrf_dump)
    }
}
