use super::error::HttpError;
use axum::{extract::FromRequestParts, http::HeaderMap};
use bson::{oid::ObjectId, serde_helpers::datetime::FromChrono04DateTime};
use cassry::{
    base64::Engine,
    chrono::{DateTime, Local, Utc},
    moka::future::Cache,
    secrecy::{ExposeSecret, SecretSlice, SecretString},
    tokio::sync::RwLock,
    *,
};
use derive_more::{Display, From, Into};
use jsonwebtoken::{DecodingKey, EncodingKey, Validation};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr, TimestampSeconds, TimestampMilliSeconds, FromInto};
use std::{net::IpAddr, sync::Arc, u64};
use tower_cookies::Cookies;
use uuid::Uuid;

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Display, From, Into)]
#[display("{}", _0)]
pub struct UidKey(#[serde_as(as = "DisplayFromStr")] i64);
impl UidKey {
    pub fn value(&self) -> &i64 {
        &self.0
    }
}

impl Into<String> for UidKey {
    fn into(self) -> String {
        self.0.to_string()
    }
}

pub struct LoginParams {
    pub uid: UidKey,
    pub password: String,
    pub role: i64,

    pub nick: String,
    pub login_ip: IpAddr,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsrfPayload {
    #[serde(with = "uuid::serde::compact")]
    pub derived_from: uuid::Uuid,

    #[serde_as(as = "FromInto<i64>")]
    pub uid: UidKey,
    pub role: i64,
    pub nick: String,

    #[serde(with = "serialization::ipaddr_bytes")]
    pub login_ip: IpAddr,

    #[serde_as(as = "TimestampMilliSeconds")]
    pub login_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessPayload {
    #[serde(with = "uuid::serde::compact")]
    pub derived_from: uuid::Uuid,

    #[serde_as(as = "FromInto<i64>")]
    pub uid: UidKey,
    #[serde_as(as = "TimestampMilliSeconds")]
    pub login_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasRefreshmutable {
    pub failed: usize,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasRefreshImmutable {
    /// 발급자
    pub iss: String,
    pub role: i64,
    pub password: String,

    #[serde_as(as = "DisplayFromStr")]
    pub ip: IpAddr,

    #[serde_as(as = "FromChrono04DateTime")]
    pub created_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasRefreshClaims {
    // #[serde(default)]
    // #[serde(skip)]
    // #[serde(rename = "_id")]
    // pub id: Option<ObjectId>,

    /// 토큰 고유 ID
    pub jti: Uuid,

    /// 토큰의 만료 시간
    #[serde_as(as = "FromChrono04DateTime")]
    pub exp: DateTime<Utc>,
    /// 발급자
    pub iss: String,

    /// 사용자 ID
    pub sub: UidKey,
    /// 발급 시간
    #[serde_as(as = "FromChrono04DateTime")]
    pub iat: DateTime<Utc>,

    /// 환경
    #[serde_as(as = "DisplayFromStr")]
    pub env: IpAddr,

    pub role: i64,
    pub failed: usize,
    pub password: String,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasClaimsFrame<T: Serialize + DeserializeOwned> {
    /// 토큰 고유 ID
    pub jti: Uuid,
    /// 토큰의 만료 시간(jwt는 초단위 지원)
    #[serde_as(as = "TimestampSeconds")]
    pub exp: DateTime<Utc>,

    #[serde(with = "serialization::postcard_base64")]
    pub payload: T,
}

pub type SasAccessClaims = SasClaimsFrame<AccessPayload>;
pub type SasCsrfClaims = SasClaimsFrame<CsrfPayload>;

impl<S> FromRequestParts<S> for SasAccessClaims
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

impl<S> FromRequestParts<S> for SasCsrfClaims
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
    pub refresh_claims: SasRefreshClaims,
    pub access_token: String,
    pub csrf_token: String,
    pub csrf_dump: String,
}

pub struct ClaimsPair {
    pub access_claims: SasAccessClaims,
    pub csrf_claims: SasCsrfClaims,
}


pub struct KyInfo {
    pub created_at: DateTime<Local>,
    pub secret: SecretSlice<u8>,
}

pub struct JwtIssuer {
    secret: SecretString,
    keys: Cache<String, Arc<KyInfo>>,
    main: RwLock<String>,
}

impl JwtIssuer {
    pub fn get_secret(&self) -> SecretString {
        self.secret.clone()
    }

    pub async fn insert_key(&self, created_at: DateTime<Local>, origin: &str) -> anyhow::Result<()> {
        let arr = self.secret.expose_secret().as_bytes().try_into()?;
        let mut hasher = blake3::Hasher::new_keyed(&arr);
        hasher.update(origin.as_bytes());
        let hash = hasher.finalize();
        let secret = secrecy::SecretSlice::new(hash.as_bytes().to_vec().into());

        let key = {
            let bytes = postcard::to_stdvec(&created_at.timestamp_millis())?;
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
        };
        
        self.keys.insert(key.clone(), Arc::new(KyInfo { created_at, secret })).await;
        let mut locked = self.main.write().await;
        if let Some(prev) = self.keys.get(locked.as_str()).await {
            if prev.created_at < created_at {
                *locked = key;
            }
        }
        else {
            *locked = key;
        }

        Ok(())
    }

    pub async fn get_key(&self) -> Option<(String, Arc<KyInfo>)> {
        let locked = self.main.read().await;
        self.keys.get(locked.as_str()).await.map(|x| (locked.clone(), x))
    }

    pub async fn generate_jwt_with_nonce<T: Serialize>(
        &self,
        claims: &T,
        nonce: &[u8],
    ) -> anyhow::Result<String> {
        let (key, info) = self.get_key().await.ok_or(anyhow::anyhow!("Key not found"))?;
        let mut header = jsonwebtoken::Header::default();
        header.kid = Some(key);

        let mut hasher = blake3::Hasher::new_keyed(&info.secret.expose_secret().try_into()?);
        hasher.update(nonce);
        let hash = hasher.finalize();

        jsonwebtoken::encode(
            &header,
            claims,
            &EncodingKey::from_secret(hash.as_bytes()),
        )
        .map_err(anyhow::Error::from)
    }

    pub async fn verify_with_nonce<T: DeserializeOwned>(
        &self,
        token: &str,
        nonce: &[u8],
        validation: Option<Validation>,
    ) -> anyhow::Result<T> {
        let kid = jsonwebtoken::decode_header(token)?.kid.ok_or(anyhow::anyhow!("Key not found"))?;
        let info = self.keys.get(&kid).await.ok_or(anyhow::anyhow!("Key not found"))?;

        let mut hasher = blake3::Hasher::new_keyed(&info.secret.expose_secret().try_into()?);
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

    pub async fn generate_jwt<T: Serialize>(&self, claims: &T) -> anyhow::Result<String> {
        let (key, info) = self.get_key().await.ok_or(anyhow::anyhow!("Key not found"))?;
        let mut header = jsonwebtoken::Header::default();
        header.kid = Some(key);
        jsonwebtoken::encode(
            &header,
            claims,
            &EncodingKey::from_secret(info.secret.expose_secret()),
        )
        .map_err(anyhow::Error::from)
    }

    pub async fn verify<T: DeserializeOwned>(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<T> {
        let kid = jsonwebtoken::decode_header(token)?.kid.ok_or(anyhow::anyhow!("Key not found"))?;
        let info = self.keys.get(&kid).await.ok_or(anyhow::anyhow!("Key not found"))?;

        jsonwebtoken::decode::<T>(
            token,
            &DecodingKey::from_secret(info.secret.expose_secret()),
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
    pub async fn insert_key(&self, key: DateTime<Local>, origin: &str) -> anyhow::Result<()> {
        self.csrf_isser.insert_key(key, origin).await?;
        self.access_isser.insert_key(key, origin).await
    }

    pub fn get_name(&self) -> &String {
        &self.name
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
        access_claims: &SasAccessClaims,
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
                keys: moka::future::CacheBuilder::new(u64::MAX)
                    .time_to_live(std::time::Duration::from_secs(
                        refresh_ttl.num_seconds() as u64
                    ))
                    .build(),
                main: RwLock::new(String::new()),
            },
            csrf_isser: JwtIssuer {
                secret: csrf_secret,
                keys: moka::future::CacheBuilder::new(u64::MAX)
                    .time_to_live(std::time::Duration::from_secs(
                        refresh_ttl.num_seconds() as u64
                    ))
                    .build(),
                main: RwLock::new(String::new()),
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

    pub async fn verify_jwt_access(
        &self,
        access_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<SasAccessClaims> {
        self.access_isser
            .verify::<SasAccessClaims>(access_token, validation).await
            .and_then(|claims| {
                if self.contains_block(&claims.payload.derived_from) {
                    return Err(anyhow::anyhow!("Blocked refresh token"));
                }
                Ok(claims)
            })
    }

    pub async fn verify_jwt_csrf(
        &self,
        access_claims: &SasAccessClaims,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<SasCsrfClaims> {
        self.csrf_isser.verify_with_nonce::<SasCsrfClaims>(
            csrf_token,
            access_claims.jti.as_bytes(),
            validation,
        ).await
    }

    /// JWT 토큰 검증 (유효성, 만료 등)
    pub async fn verify_jwt_pair(
        &self,
        access_token: &str,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<ClaimsPair> {
        let access_claims = self.verify_jwt_access(access_token, validation.clone()).await?;
        let csrf_claims = self.verify_jwt_csrf(&access_claims, csrf_token, validation).await?;
        if self.contains_block(&access_claims.jti) {
            return Err(anyhow::anyhow!("Invalid access token"));
        }
        Ok(ClaimsPair {
            access_claims,
            csrf_claims,
        })
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub async fn login(&self, params: LoginParams) -> anyhow::Result<IssuedClaims> {
        let now = Utc::now();
        let refresh_jti = Uuid::now_v7();
        let refresh_claims = SasRefreshClaims {
            jti: refresh_jti,
            exp: now + self.refresh_ttl,
            iss: self.name.clone(),
            sub: params.uid.clone(),
            iat: now,
            env: params.login_ip,
            role: params.role,
            failed: 0,
            password: params.password,
        };

        let access_payload = AccessPayload {
            derived_from: refresh_jti,
            uid: params.uid.clone(),
            login_at: now,
        };

        let access_jti = Uuid::now_v7();
        let access_claims = SasAccessClaims {
            jti: access_jti,
            exp: now + self.access_ttl,
            payload: access_payload,
        };

        let user_payload = CsrfPayload {
            derived_from: access_jti,
            uid: params.uid.clone(),
            role: params.role,
            nick: params.nick,
            login_ip: params.login_ip,
            login_at: now,
        };

        let (csrf_token, csrf_dump) = self.generate_csrf_token(&access_claims, user_payload).await?;
        Ok(IssuedClaims {
            refresh_claims,
            access_token: self.access_isser.generate_jwt(&access_claims).await?,
            csrf_token,
            csrf_dump,
        })
    }

    pub fn decrypt_csrf_dump(
        &self,
        access_claims: &SasAccessClaims,
        addr: &IpAddr,
        csrf_dump: &str,
    ) -> anyhow::Result<String> {
        let nonce = self.generate_dump_nonce(access_claims, addr)?;
        let encrypted = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(csrf_dump)?;
        let decrypted = util::decrypt_str_by_aes_gcm_128(
            access_claims.jti.as_bytes().try_into()?,
            nonce.as_slice().try_into()?,
            encrypted,
        )?;

        Ok(decrypted.expose_secret().to_string())
    }

    pub async fn generate_csrf_token(
        &self,
        access_claims: &SasAccessClaims,
        user_payload: CsrfPayload,
    ) -> anyhow::Result<(String, String)> {
        let csrf_claims = SasCsrfClaims {
            jti: Uuid::new_v4(),
            exp: access_claims.exp,
            payload: user_payload,
        };

        let csrf_token = self
            .csrf_isser
            .generate_jwt_with_nonce(&csrf_claims, access_claims.jti.as_bytes()).await?;
        let nonce = self.generate_dump_nonce(access_claims, &csrf_claims.payload.login_ip)?;
        let encrypted = util::encrypt_str_by_aes_gcm_128(
            access_claims.jti.as_bytes().try_into()?,
            nonce.as_slice().try_into()?,
            &csrf_token,
        )?;
        Ok((
            csrf_token,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&encrypted),
        ))
    }

    pub async fn refresh_access_token(
        &self,
        prev_access_claims: &SasAccessClaims,
        prev_refresh_clams: SasRefreshClaims,
        mut user_payload: CsrfPayload,
    ) -> anyhow::Result<IssuedClaims> {
        if prev_access_claims.payload.uid != user_payload.uid
            || prev_access_claims.payload.derived_from != prev_refresh_clams.jti
        {
            return Err(anyhow::anyhow!(
                "Invalid refresh token: uid={}",
                prev_access_claims.payload.uid
            ));
        }

        let now = Utc::now();

        let refresh_jti = Uuid::now_v7();
        let refresh_claims = SasRefreshClaims {
            jti: refresh_jti,
            exp: now + self.refresh_ttl,

            iss: self.name.clone(),
            sub: prev_refresh_clams.sub,
            iat: now,
            env: prev_refresh_clams.env,
            role: prev_refresh_clams.role,
            failed: prev_refresh_clams.failed,
            password: prev_refresh_clams.password,
        };

        let access_payload = AccessPayload {
            derived_from: refresh_jti,
            uid: user_payload.uid.clone(),
            login_at: user_payload.login_at.clone(),
        };
        let access_jti = Uuid::now_v7();
        let access_claims = SasAccessClaims {
            jti: access_jti.clone(),
            exp: now + self.access_ttl,
            payload: access_payload,
        };

        user_payload.derived_from = access_jti;
        let (csrf_token, csrf_dump) = self.generate_csrf_token(&access_claims, user_payload).await?;
        Ok(IssuedClaims {
            refresh_claims,
            access_token: self.access_isser.generate_jwt(&access_claims).await?,
            csrf_token,
            csrf_dump,
        })
    }
}

#[derive(Clone)]
pub struct TokenIssuer {
    isser: Arc<TokenIssuerImpl>,
}

impl TokenIssuer {
    pub async fn get_name(&self) -> String {
        self.isser.get_name().clone()
    }

    pub async fn get_access_ttl(&self) -> chrono::Duration {
        self.isser.get_access_ttl().clone()
    }

    pub async fn get_refresh_ttl(&self) -> chrono::Duration {
        self.isser.get_refresh_ttl().clone()
    }

    pub const fn get_cookie_name() -> &'static str {
        "access"
    }

    pub const fn get_session_sub_name() -> &'static str {
        "sub"
    }

    pub async fn insert_key(&self, key: DateTime<Local>, origin: &str) -> anyhow::Result<()> {
        self.isser.insert_key(key, origin).await
    }

    pub async fn insert_block(&self, jti: &Uuid) {
        self.isser.insert_block(jti).await;
    }

    pub async fn contains_block(&self, jti: &Uuid) -> bool {
        self.isser.contains_block(jti)
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
        self.isser.login(params).await
    }

    pub async fn refresh_access_token(
        &self,
        prev_access_claims: &SasAccessClaims,
        prev_refresh_clams: SasRefreshClaims,
        user_payload: CsrfPayload,
    ) -> anyhow::Result<IssuedClaims> {
        self.isser.refresh_access_token(
            &prev_access_claims,
            prev_refresh_clams,
            user_payload,
        ).await
    }

    pub async fn verify_jwt_csrf(
        &self,
        access_claims: &SasAccessClaims,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<SasCsrfClaims> {
        self.isser
            .verify_jwt_csrf(access_claims, csrf_token, validation).await
    }

    pub async fn extract_access_claims_from(
        &self,
        cookies: &Cookies,
        validation: Option<Validation>,
    ) -> Result<SasAccessClaims, HttpError> {
        let access_token = cookies
            .get(TokenIssuer::get_cookie_name())
            .ok_or(HttpError::MissingJwtToken)?;
        let access_claims = self
            .isser
            .verify_jwt_access(access_token.value(), validation).await
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
            .verify_jwt_pair(access_token.value(), csrf_token, validation).await
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
        access_claims: &SasAccessClaims,
        addr: &IpAddr,
        csrf_dump: &str,
    ) -> anyhow::Result<String> {
        self.isser
            .decrypt_csrf_dump(access_claims, addr, csrf_dump)
    }
}
