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
    pub code: String,
    pub state: String,
}

pub struct GoogleOAuth {
    client: oauth2::Client<
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
    request: oauth2::reqwest::Client,
}

impl GoogleOAuth {
    pub fn get_provider(&self) -> &'static str {
        "google"
    }

    pub fn new(client_id: &str, client_secret: &str, redirect_url: &str) -> anyhow::Result<Self> {
        let request = oauth2::reqwest::ClientBuilder::new()
            .redirect(oauth2::reqwest::redirect::Policy::none())
            .build()?;

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

        let s = Self {
            client: client,
            request: request,
        };

        Ok(s)
    }

    pub async fn login(&self) -> anyhow::Result<(Redirect, PkceCodeVerifier)> {
        let (pkce_code_challenge, pkce_code_verifier) =
            oauth2::PkceCodeChallenge::new_random_sha256();

        let (auth_url, _csrf_token) = self
            .client
            .authorize_url(CsrfToken::new_random)
            .add_scope(Scope::new("openid".into()))
            .add_scope(Scope::new("profile".into()))
            .add_scope(Scope::new("email".into()))
            .set_pkce_challenge(pkce_code_challenge)
            .url();

        Ok((Redirect::temporary(auth_url.as_str()), pkce_code_verifier))
    }

    pub async fn auth_callback(
        &self,
        auth_request: AuthRequest,
        pkce_code_verifier: PkceCodeVerifier,
    ) -> anyhow::Result<GoogleProfile> {
        let token_result = self
            .client
            .exchange_code(AuthorizationCode::new(auth_request.code))
            .set_pkce_verifier(pkce_code_verifier)
            .request_async(&self.request)
            .await?;

        let txt = self
            .request
            .get("https://www.googleapis.com/oauth2/v2/userinfo")
            .bearer_auth(token_result.access_token().secret())
            .send()
            .await?
            .text()
            .await?;
        let user_info = serde_json::from_str::<serde_json::Value>(&txt)?;

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

    pub async fn refresh_token(&self, mut profile: GoogleProfile) -> anyhow::Result<GoogleProfile> {
        if profile.expires_at <= Utc::now() {
            if let Some(refresh_token) = &profile.refresh_token {
                let token_result = self
                    .client
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