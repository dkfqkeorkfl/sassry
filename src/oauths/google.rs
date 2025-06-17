use axum::response::Redirect;
use chrono::{DateTime, Duration, Utc};
use oauth2::{
    basic::BasicClient, AuthUrl, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EndpointNotSet, EndpointSet, PkceCodeVerifier, RedirectUrl, RevocationErrorResponseType,
    RevocationUrl, Scope, StandardErrorResponse, StandardRevocableToken,
    StandardTokenIntrospectionResponse, StandardTokenResponse, TokenResponse, TokenUrl,
};

use cassry::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleProfile {
    pub id: String,
    pub name: String,
    pub email: String,
    pub picture: Option<String>,
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Deserialize)]
pub struct AuthRequest {
    code: String,
    state: String,
}

struct Inner {
    client: Option<
        oauth2::Client<
            StandardErrorResponse<oauth2::basic::BasicErrorResponseType>,
            StandardTokenResponse<oauth2::EmptyExtraTokenFields, oauth2::basic::BasicTokenType>,
            StandardTokenIntrospectionResponse<
                oauth2::EmptyExtraTokenFields,
                oauth2::basic::BasicTokenType,
            >,
            StandardRevocableToken,
            StandardErrorResponse<RevocationErrorResponseType>,
            EndpointSet,
            EndpointNotSet,
            EndpointNotSet,
            EndpointSet,
            EndpointSet,
        >,
    >,
    request: oauth2::reqwest::Client,
}

impl Inner {
    fn new() -> anyhow::Result<Self> {
        let request = oauth2::reqwest::ClientBuilder::new()
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        let s = Self {
            client: None,
            request: request,
        };

        Ok(s)
    }

    fn init(
        &mut self,
        client_id: &str,
        client_secret: &str,
        redirect_url: &str,
    ) -> anyhow::Result<()> {
        let auth_url = AuthUrl::new("https://accounts.google.com/o/oauth2/auth".into())?;
        let token_url = TokenUrl::new("https://oauth2.googleapis.com/token".into())?;
        let redirect_url = RedirectUrl::new(redirect_url.into())?;
        let revocation_url = RevocationUrl::new("https://oauth2.googleapis.com/revoke".into())?;

        let client = BasicClient::new(ClientId::new(client_id.into()))
            .set_client_secret(ClientSecret::new(client_secret.into()))
            .set_auth_uri(auth_url)
            .set_token_uri(token_url)
            .set_redirect_uri(redirect_url)
            .set_revocation_url(revocation_url);

        self.client = Some(client);

        Ok(())
    }

    async fn login(&self) -> anyhow::Result<(Redirect, PkceCodeVerifier)> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhowln!("OAuth client not initialized"))?;

        let (pkce_code_challenge, pkce_code_verifier) =
            oauth2::PkceCodeChallenge::new_random_sha256();

        let (auth_url, _csrf_token) = client
            .authorize_url(CsrfToken::new_random)
            .add_scope(Scope::new("openid".into()))
            .add_scope(Scope::new("profile".into()))
            .add_scope(Scope::new("email".into()))
            .set_pkce_challenge(pkce_code_challenge)
            .url();

        Ok((Redirect::temporary(auth_url.as_str()), pkce_code_verifier))
    }

    async fn auth_callback(
        &self,
        auth_request: AuthRequest,
        pkce_code_verifier: PkceCodeVerifier,
    ) -> anyhow::Result<GoogleProfile> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhowln!("OAuth client not initialized"))?;

        let token_result = client
            .exchange_code(AuthorizationCode::new(auth_request.code))
            .set_pkce_verifier(pkce_code_verifier)
            .request_async(&self.request)
            .await?;

        let user_info = self
            .request
            .get("https://www.googleapis.com/oauth2/v2/userinfo")
            .bearer_auth(token_result.access_token().secret())
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // token_result.set_expires_in(None).;
        let profile = GoogleProfile {
            id: user_info["id"].as_str().unwrap_or("Unknown").to_string(),
            name: user_info["name"].as_str().unwrap_or("Unknown").to_string(),
            email: user_info["email"].as_str().unwrap_or("Unknown").to_string(),
            picture: user_info["picture"].as_str().map(String::from),
            access_token: token_result.access_token().secret().to_string(),
            refresh_token: token_result.refresh_token().map(|t| t.secret().to_string()),
            expires_at: Utc::now()
                + Duration::seconds(
                    token_result
                        .expires_in()
                        .map(|e| e.as_secs() as i64)
                        .unwrap_or(3600),
                ),
        };

        Ok(profile)
    }

    async fn refresh_token(&self, mut profile: GoogleProfile) -> anyhow::Result<GoogleProfile> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhowln!("OAuth client not initialized"))?;

        if profile.expires_at <= Utc::now() {
            if let Some(refresh_token) = &profile.refresh_token {
                let token_result = client
                    .exchange_refresh_token(&oauth2::RefreshToken::new(refresh_token.clone()))
                    .request_async(&self.request)
                    .await?;

                profile.access_token = token_result.access_token().secret().to_string();
                profile.refresh_token =
                    token_result.refresh_token().map(|t| t.secret().to_string());
                profile.expires_at = Utc::now()
                    + Duration::seconds(
                        token_result
                            .expires_in()
                            .map(|e| e.as_secs() as i64)
                            .unwrap_or(3600),
                    );
            }
        }

        Ok(profile)
    }
}

#[derive(Clone)]
pub struct GoogleOAuth {
    inner: Arc<RwLock<Inner>>,
}

impl GoogleOAuth {
    pub fn get_provider(&self) -> &str {
        "google"
    }

    pub fn new() -> anyhow::Result<Self> {
        let inner = Inner::new()?;
        Ok(Self {
            inner: Arc::new(RwLock::new(inner)),
        })
    }

    pub async fn init(
        &self,
        client_id: &str,
        client_secret: &str,
        redirect_url: &str,
    ) -> anyhow::Result<()> {
        let mut inner = self.inner.write().await;
        inner.init(client_id, client_secret, redirect_url)
    }

    pub async fn login(&self) -> anyhow::Result<(Redirect, PkceCodeVerifier)> {
        let inner = self.inner.read().await;
        inner.login().await
    }

    pub async fn auth_callback(
        &self,
        auth_request: AuthRequest,
        pkce_code_verifier: PkceCodeVerifier,
    ) -> anyhow::Result<GoogleProfile> {
        let inner = self.inner.read().await;
        inner.auth_callback(auth_request, pkce_code_verifier).await
    }

    pub async fn refresh_token(&self, profile: GoogleProfile) -> anyhow::Result<GoogleProfile> {
        let inner = self.inner.read().await;
        inner.refresh_token(profile).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_google_oauth_init() {
        let oauth = GoogleOAuth::new().unwrap();
        assert!(oauth
            .init(
                "test_client_id",
                "test_client_secret",
                "http://localhost:3000/callback"
            )
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_google_oauth_uninitialized() {
        let oauth = GoogleOAuth::new().unwrap();
        assert!(matches!(
            oauth.login().await,
            Err(e) if e.to_string().contains("OAuth client not initialized")
        ));
    }
}
