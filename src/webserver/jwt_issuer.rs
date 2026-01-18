use super::error::HttpError;
use axum::{extract::FromRequestParts, http::HeaderMap};
use axum_extra::headers::UserAgent;
use axum_client_ip::ClientIp;
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
use serde_with::{serde_as, DisplayFromStr, FromInto, TimestampMilliSeconds, TimestampSeconds};
use std::{sync::Arc, u64};
use tower_cookies::{Cookie, Cookies};
use uuid::Uuid;

use super::ser;

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
    pub ip: ClientIp,
    pub user_agent: UserAgent,
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SasAccessPayload {
    #[serde(with = "uuid::serde::compact")]
    pub derived_from: uuid::Uuid,

    #[serde_as(as = "FromInto<i64>")]
    pub uid: UidKey,
    pub role: i64,
    pub nick: String,

    #[serde_as(as = "TimestampMilliSeconds")]
    pub login_at: DateTime<Utc>,

    #[serde_as(as = "TimestampMilliSeconds")]
    pub origin_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasRefreshmutable {
    pub failed: usize,
    #[serde_as(as = "FromInto<ser::DisplayerIP>")]
    pub ip: ClientIp,

    #[serde_as(as = "DisplayFromStr")]
    pub user_agent: UserAgent,

    #[serde_as(as = "FromChrono04DateTime")]
    pub updated_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasRefreshImmutable {
    /// 최초 발급자
    pub origin_iss: String,
    pub role: i64,
    pub password: String,

    #[serde_as(as = "FromInto<ser::DisplayerIP>")]
    pub origin_ip: ClientIp,
    #[serde_as(as = "DisplayFromStr")]
    pub origin_user_agent: UserAgent,

    // access payload, csrf payload의 origin_at
    #[serde_as(as = "FromChrono04DateTime")]
    pub created_at: DateTime<Utc>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasRefreshClaims {
    #[serde(default)]
    #[serde(skip)]
    #[serde(rename = "_id")]
    pub id: Option<ObjectId>,

    /// 토큰 고유 ID
    #[serde_as(as = "DisplayFromStr")]
    pub jti: Uuid,

    /// 토큰의 만료 시간
    #[serde_as(as = "FromChrono04DateTime")]
    pub exp: DateTime<Utc>,

    /// 사용자 ID
    pub sub: UidKey,

    /// 발급자
    pub iss: String,
    /// 대상 서비스
    pub aud: String,

    /// 발급 시간
    #[serde_as(as = "FromChrono04DateTime")]
    pub iat: DateTime<Utc>,

    /// additional datas
    pub immutable: Arc<SasRefreshImmutable>,
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mutable: Option<SasRefreshmutable>,
}

pub trait CsrfKeyProvider {
    fn csrf_origin(&self) -> &[u8];
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SasAccessClaims {
    /// 토큰 고유 ID
    #[serde_as(as = "DisplayFromStr")]
    pub jti: Uuid,
    /// 토큰의 만료 시간(jwt는 초단위 지원)
    #[serde_as(as = "TimestampSeconds")]
    pub exp: DateTime<Utc>,

    #[serde(with = "serialization::postcard_base64")]
    pub payload: Arc<SasAccessPayload>,
}

impl CsrfKeyProvider for SasAccessClaims {
    fn csrf_origin(&self) -> &[u8] {
        self.jti.as_bytes()
    }
}

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
                .extract_access_claims_with_csrf_headers(cookies, &parts.headers, Some(validation))
                .await?;
            if chrono::Utc::now() > claims.exp {
                return Err(HttpError::ExpiredJwt);
            }

            Ok(claims)
        }
    }
}

fn build_access_cookie(token: String, ttl: &chrono::Duration) -> anyhow::Result<Cookie<'static>> {
    let date = Utc::now() + *ttl;
    let expires =
        tower_cookies::cookie::time::OffsetDateTime::from_unix_timestamp(date.timestamp())?;
    Ok(Cookie::build((TokenIssuer::ACCESS_COOKIE_NAME, token))
        .expires(expires)
        .same_site(tower_cookies::cookie::SameSite::Lax)
        .secure(true)
        .http_only(true)
        .path("/")
        .build())
}

fn build_csrf_cookie(token: String, ttl: &chrono::Duration) -> anyhow::Result<Cookie<'static>> {
    let date = Utc::now() + *ttl;
    let expires =
        tower_cookies::cookie::time::OffsetDateTime::from_unix_timestamp(date.timestamp())?;
    Ok(Cookie::build((TokenIssuer::CSRF_COOKIE_NAME, token))
        .expires(expires)
        .same_site(tower_cookies::cookie::SameSite::Lax)
        .secure(true)
        .http_only(false)
        .path("/")
        .build())
}

#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CsrfToken(#[serde(with = "serialization::postcard_base64")] pub Vec<u8>);

impl CsrfToken {
    pub fn verify(&self, csrf: &str) -> bool {
        serde_json::from_str::<CsrfToken>(csrf)
            .map(|token| token == *self)
            .unwrap_or(false)
    }

    pub fn to_string(&self) -> anyhow::Result<String> {
        serde_json::to_string(self).map_err(anyhow::Error::from)
    }
}

#[derive(Debug, Clone)]
pub struct IssuedClaims {
    pub refresh_claims: SasRefreshClaims,
    pub access_cookie: Cookie<'static>,
    pub csrf_cookie: Cookie<'static>,
}

impl IssuedClaims {
    pub fn apply_to_cookies(self, cookies: &Cookies) -> SasRefreshClaims {
        cookies.add(self.access_cookie);
        cookies.add(self.csrf_cookie);
        self.refresh_claims
    }
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

    pub async fn insert_key(
        &self,
        created_at: DateTime<Local>,
        origin: &str,
    ) -> anyhow::Result<()> {
        let arr = self.secret.expose_secret().as_bytes().try_into()?;
        let mut hasher = blake3::Hasher::new_keyed(&arr);
        hasher.update(origin.as_bytes());
        let hash = hasher.finalize();
        let secret = secrecy::SecretSlice::new(hash.as_bytes().to_vec().into());

        let key = {
            let bytes = postcard::to_stdvec(&created_at.timestamp_millis())?;
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
        };

        self.keys
            .insert(key.clone(), Arc::new(KyInfo { created_at, secret }))
            .await;
        let mut locked = self.main.write().await;
        if let Some(prev) = self.keys.get(locked.as_str()).await {
            if prev.created_at < created_at {
                *locked = key;
            }
        } else {
            *locked = key;
        }

        Ok(())
    }

    pub async fn get_key(&self) -> Option<(String, Arc<KyInfo>)> {
        let locked = self.main.read().await;
        self.keys
            .get(locked.as_str())
            .await
            .map(|x| (locked.clone(), x))
    }

    pub async fn generate_jwt<T: Serialize + CsrfKeyProvider>(
        &self,
        claims: &T,
    ) -> anyhow::Result<(String, CsrfToken)> {
        let (key, info) = self
            .get_key()
            .await
            .ok_or(anyhow::anyhow!("Key not found"))?;
        let mut header = jsonwebtoken::Header::default();
        header.kid = Some(key);
        let jwt = jsonwebtoken::encode(
            &header,
            claims,
            &EncodingKey::from_secret(info.secret.expose_secret()),
        )
        .map_err(anyhow::Error::from)?;

        let mut hasher = blake3::Hasher::new_keyed(&info.secret.expose_secret().try_into()?);
        hasher.update(claims.csrf_origin());
        let hash = hasher.finalize();
        let csrf = CsrfToken(hash.as_bytes().to_vec());
        Ok((jwt, csrf))
    }

    pub async fn verify<T: DeserializeOwned>(
        &self,
        token: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<T> {
        let kid = jsonwebtoken::decode_header(token)?
            .kid
            .ok_or(anyhow::anyhow!("Kid not found"))?;
        let info = self
            .keys
            .get(&kid)
            .await
            .ok_or(anyhow::anyhow!("Key not found"))?;

        jsonwebtoken::decode::<T>(
            token,
            &DecodingKey::from_secret(info.secret.expose_secret()),
            &validation.unwrap_or_default(),
        )
        .map(|token_data| token_data.claims)
        .map_err(anyhow::Error::from)
    }

    pub async fn verify_with_csrf<T: DeserializeOwned + CsrfKeyProvider>(
        &self,
        token: &str,
        csrf: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<T> {
        let kid = jsonwebtoken::decode_header(token)?
            .kid
            .ok_or(anyhow::anyhow!("Kid not found"))?;
        let info = self
            .keys
            .get(&kid)
            .await
            .ok_or(anyhow::anyhow!("Key not found"))?;

        let claims = jsonwebtoken::decode::<T>(
            token,
            &DecodingKey::from_secret(info.secret.expose_secret()),
            &validation.unwrap_or_default(),
        )
        .map(|token_data| token_data.claims)
        .map_err(anyhow::Error::from)?;

        let mut hasher = blake3::Hasher::new_keyed(&info.secret.expose_secret().try_into()?);
        hasher.update(claims.csrf_origin());
        let hash = hasher.finalize();
        let helper = CsrfToken(hash.as_bytes().to_vec());
        if !helper.verify(csrf) {
            return Err(anyhow::anyhow!("Invalid CSRF token"));
        }

        Ok(claims)
    }
}

pub struct TokenIssuerImpl {
    name: String,
    access_isser: JwtIssuer,

    access_ttl: chrono::Duration,
    refresh_ttl: chrono::Duration,
    csrf_ttl: chrono::Duration,
    blocked_refreshs: Cache<Uuid, ()>,
}

impl TokenIssuerImpl {
    pub async fn insert_key(&self, key: DateTime<Local>, origin: &str) -> anyhow::Result<()> {
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

    /// 새로운 JwtManager 인스턴스 생성
    pub fn new(
        name: String,
        access_secret: SecretString,
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
        session_timeout: chrono::Duration,
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
            access_ttl,
            refresh_ttl,
            csrf_ttl: access_ttl + session_timeout,
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
            .verify::<SasAccessClaims>(access_token, validation)
            .await
            .and_then(|claims| {
                if self.contains_block(&claims.payload.derived_from) {
                    return Err(anyhow::anyhow!("Blocked refresh token"));
                }
                Ok(claims)
            })
    }

    pub async fn verify_jwt_access_with_csrf(
        &self,
        access_token: &str,
        csrf: &str,
        validation: Option<Validation>,
    ) -> anyhow::Result<SasAccessClaims> {
        self.access_isser
            .verify_with_csrf::<SasAccessClaims>(access_token, csrf, validation)
            .await
            .and_then(|claims| {
                if self.contains_block(&claims.payload.derived_from) {
                    return Err(anyhow::anyhow!("Blocked refresh token"));
                }
                Ok(claims)
            })
    }

    pub async fn generate_tokens(
        &self,
        refresh_claims: SasRefreshClaims,
        access_claims: SasAccessClaims,
    ) -> anyhow::Result<IssuedClaims> {
        let (access_token, csrf) = self.access_isser.generate_jwt(&access_claims).await?;
        let access_cookie = build_access_cookie(access_token, &self.refresh_ttl)?;

        let csrf_cookie = build_csrf_cookie(csrf.to_string()?, &self.csrf_ttl)?;
        Ok(IssuedClaims {
            refresh_claims,
            access_cookie,
            csrf_cookie,
        })
    }

    /// 로그인 시도 (아이디/비밀번호 검증은 실제 구현 필요)
    pub async fn login(&self, params: LoginParams) -> anyhow::Result<IssuedClaims> {
        let now = Utc::now();
        let refresh_jti = util::datetime_to_uuid7(now)?;
        let refresh_claims = SasRefreshClaims {
            id: None,
            jti: refresh_jti,
            exp: now + self.refresh_ttl,
            iss: self.name.clone(),
            aud: self.name.clone(),
            sub: params.uid.clone(),
            iat: now,
            immutable: Arc::new(SasRefreshImmutable {
                origin_iss: self.name.clone(),
                role: params.role,
                password: params.password,
                origin_ip: params.ip,
                origin_user_agent: params.user_agent,
                created_at: now,
            }),
            mutable: None,
        };

        let user_payload = SasAccessPayload {
            derived_from: refresh_jti,
            uid: params.uid.clone(),
            role: params.role,
            nick: params.nick,
            origin_at: now,
            login_at: now,
        };

        let access_claims = SasAccessClaims {
            jti: Uuid::now_v7(),
            exp: now + self.access_ttl,
            payload: user_payload.into(),
        };

        self.generate_tokens(refresh_claims, access_claims).await
    }

    pub async fn refresh_access_token(
        &self,
        prev_refresh_clams: SasRefreshClaims,
        prev_payload: &Arc<SasAccessPayload>,
        client_ip: ClientIp,
        user_agent: UserAgent,
    ) -> anyhow::Result<IssuedClaims> {
        if prev_refresh_clams.sub != prev_payload.uid
            || prev_payload.derived_from != prev_refresh_clams.jti
        {
            return Err(anyhow::anyhow!(
                "Invalid refresh token: uid={}",
                prev_payload.uid
            ));
        }

        let now = Utc::now();
        let refresh_jti = util::datetime_to_uuid7(now)?;
        let refresh_claims = SasRefreshClaims {
            id: None,
            jti: refresh_jti,
            exp: now + self.refresh_ttl,

            iss: self.name.clone(),
            aud: prev_refresh_clams.aud,
            sub: prev_refresh_clams.sub,
            iat: now,
            immutable: prev_refresh_clams.immutable.clone(),
            mutable: SasRefreshmutable {
                failed: 0,
                ip: client_ip,
                user_agent: user_agent,
                updated_at: now,
            }
            .into(),
        };

        let mut user_payload = prev_payload.as_ref().clone();
        user_payload.derived_from = refresh_jti;
        let access_claims = SasAccessClaims {
            jti: Uuid::now_v7(),
            exp: now + self.access_ttl,
            payload: user_payload.into(),
        };

        self.generate_tokens(refresh_claims, access_claims).await
    }
}

#[derive(Clone)]
pub struct TokenIssuer {
    isser: Arc<TokenIssuerImpl>,
}

impl TokenIssuer {
    pub const ACCESS_COOKIE_NAME: &'static str = "access";
    pub const CSRF_COOKIE_NAME: &'static str = "sas-csrf";

    pub async fn get_name(&self) -> String {
        self.isser.get_name().clone()
    }

    pub async fn get_access_ttl(&self) -> chrono::Duration {
        self.isser.get_access_ttl().clone()
    }

    pub async fn get_refresh_ttl(&self) -> chrono::Duration {
        self.isser.get_refresh_ttl().clone()
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
        access_ttl: chrono::Duration,
        refresh_ttl: chrono::Duration,
        session_timeout: chrono::Duration,
    ) -> Self {
        Self {
            isser: Arc::new(
                TokenIssuerImpl::new(
                    name,
                    access_secret,
                    access_ttl,
                    refresh_ttl,
                    session_timeout,
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
        prev_refresh_clams: SasRefreshClaims,
        prev_payload: &Arc<SasAccessPayload>,
        client_ip: ClientIp,
        user_agent: UserAgent,
    ) -> anyhow::Result<IssuedClaims> {
        self.isser
            .refresh_access_token(prev_refresh_clams, prev_payload, client_ip, user_agent)
            .await
    }

    pub async fn extract_access_claims(
        &self,
        cookies: &Cookies,
        validation: Option<Validation>,
    ) -> Result<SasAccessClaims, HttpError> {
        let access_token = cookies
            .get(TokenIssuer::ACCESS_COOKIE_NAME)
            .ok_or(HttpError::MissingJwtToken)?;
        let access_claims = self
            .isser
            .verify_jwt_access(access_token.value(), validation)
            .await
            .map_err(|e| HttpError::InvalidJwt(e))?;
        Ok(access_claims)
    }

    pub async fn extract_access_claims_with_csrf(
        &self,
        cookies: &Cookies,
        csrf: &str,
        validation: Option<Validation>,
    ) -> Result<SasAccessClaims, HttpError> {
        let access_token = cookies
            .get(TokenIssuer::ACCESS_COOKIE_NAME)
            .ok_or(HttpError::MissingJwtToken)?;
        let access_claims = self
            .isser
            .verify_jwt_access_with_csrf(access_token.value(), csrf, validation)
            .await
            .map_err(|e| HttpError::InvalidJwt(e))?;
        Ok(access_claims)
    }

    pub async fn is_valid_csrf_headers<'a>(
        &self,
        cookies: &'a Cookies,
        headers: &HeaderMap,
    ) -> Result<Cookie<'a>, HttpError> {
        let csrf_token_from_header = headers
            .get("X-CSRF-Token")
            .and_then(|header| header.to_str().ok())
            .ok_or(HttpError::MissingCsrfToken)?;

        self.is_valid_csrf_str(cookies, csrf_token_from_header)
            .await
    }

    pub async fn is_valid_csrf_str<'a>(
        &self,
        cookies: &'a Cookies,
        csrf_token: &str,
    ) -> Result<Cookie<'a>, HttpError> {
        let csrf_token_from_cookie = cookies
            .get(TokenIssuer::CSRF_COOKIE_NAME)
            .ok_or(HttpError::MissingCsrfToken)?;

        if csrf_token_from_cookie.value() != csrf_token {
            return Err(HttpError::InvalidCsrfToken);
        }

        Ok(csrf_token_from_cookie)
    }

    pub async fn extract_access_claims_with_csrf_headers(
        &self,
        cookies: &Cookies,
        headers: &HeaderMap,
        validation: Option<Validation>,
    ) -> Result<SasAccessClaims, HttpError> {
        let cookie = self.is_valid_csrf_headers(cookies, headers).await?;

        self.extract_access_claims_with_csrf(cookies, cookie.value(), validation)
            .await
    }

    pub async fn extract_access_claims_with_csrf_str(
        &self,
        cookies: &Cookies,
        csrf_token: &str,
        validation: Option<Validation>,
    ) -> Result<SasAccessClaims, HttpError> {
        let cookie = self.is_valid_csrf_str(cookies, csrf_token).await?;

        self.extract_access_claims_with_csrf(cookies, cookie.value(), validation)
            .await
    }

    pub fn remove_cookies(cookies: &Cookies) {
        let access = Cookie::build((TokenIssuer::ACCESS_COOKIE_NAME, ""))
            .expires(tower_cookies::cookie::time::OffsetDateTime::from_unix_timestamp(0).unwrap())
            .same_site(tower_cookies::cookie::SameSite::Lax)
            .secure(true)
            .http_only(true)
            .path("/")
            .build();

        let csrf = Cookie::build((TokenIssuer::CSRF_COOKIE_NAME, ""))
            .expires(tower_cookies::cookie::time::OffsetDateTime::from_unix_timestamp(0).unwrap())
            .same_site(tower_cookies::cookie::SameSite::Lax)
            .secure(true)
            .http_only(false)
            .path("/")
            .build();
        cookies.remove(access);
        cookies.remove(csrf);
    }
}
