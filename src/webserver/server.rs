use axum::{
    ServiceExt, extract::State, http::{StatusCode, Uri}, response::IntoResponse
};

use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use cassry::*;
use tower::{Layer, Service};
use axum::response::Response;
use axum::http::Request;
use serde::{Deserialize, Serialize};
// https://www.runit.cloud/2020/04/https-ssl.html

/// Axum 미들웨어 설정 구조체
/// 모든 필드는 Option으로 선언되어 있어 선택적으로 설정 가능
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareConfig {
    /// 서버 포트 설정
    #[serde(default)]
    pub server: Option<ServerConfig>,
    
    /// TLS 인증서 설정
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    
    /// 로깅 설정
    #[serde(default)]
    pub logging: Option<LoggingConfig>,
    
    /// JWT 및 세션 TTL 설정
    #[serde(default)]
    pub jwt: Option<JwtConfig>,
    
    /// 타임아웃 설정
    #[serde(default)]
    pub timeout: Option<TimeoutConfig>,
    
    /// 미들웨어 설정
    #[serde(default)]
    pub middleware: Option<MiddlewareSettings>,
    
    /// 컴프레션 설정
    #[serde(default)]
    pub compression: Option<CompressionConfig>,
    
    /// CORS 설정
    #[serde(default)]
    pub cors: Option<CorsConfig>,
    
    /// 보안 헤더 설정
    #[serde(default)]
    pub security_headers: Option<SecurityHeadersConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default)]
    pub http_port: Option<u16>,
    
    #[serde(default)]
    pub https_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    #[serde(default)]
    pub cert_file: Option<String>,
    
    #[serde(default)]
    pub key_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default)]
    pub log_yaml_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    #[serde(default)]
    pub access_ttl: Option<i64>,
    
    #[serde(default)]
    pub refresh_ttl: Option<i64>,
    
    #[serde(default)]
    pub session_ttl: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeoutConfig {
    #[serde(default)]
    pub request_timeout: Option<u64>,
    
    #[serde(default)]
    pub connection_timeout: Option<u64>,
    
    #[serde(default)]
    pub read_timeout: Option<u64>,
    
    #[serde(default)]
    pub write_timeout: Option<u64>,
    
    #[serde(default)]
    pub keep_alive_timeout: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MiddlewareSettings {
    #[serde(default)]
    pub request_body_limit: Option<u64>,
    
    #[serde(default)]
    pub rate_limit: Option<RateLimitConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    #[serde(default)]
    pub per_second: Option<u64>,
    
    #[serde(default)]
    pub burst_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    #[serde(default)]
    pub enable: Option<bool>,
    
    #[serde(default)]
    pub min_size: Option<u64>,
    
    #[serde(default)]
    pub quality: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfig {
    #[serde(default)]
    pub max_age: Option<u64>,
    
    #[serde(default)]
    pub allowed_origins: Option<Vec<String>>,
    
    #[serde(default)]
    pub allowed_methods: Option<Vec<String>>,
    
    #[serde(default)]
    pub allowed_headers: Option<Vec<String>>,
    
    #[serde(default)]
    pub allow_credentials: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityHeadersConfig {
    #[serde(default)]
    pub x_frame_options: Option<String>,
    
    #[serde(default)]
    pub x_content_type_options: Option<String>,
    
    #[serde(default)]
    pub hsts: Option<HstsConfig>,
    
    #[serde(default)]
    pub csp_policy: Option<String>,
    
    #[serde(default)]
    pub referrer_policy: Option<String>,
    
    #[serde(default)]
    pub permissions_policy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HstsConfig {
    #[serde(default)]
    pub max_age: Option<u64>,
    
    #[serde(default)]
    pub include_subdomains: Option<bool>,
}

pub struct Param {
    pub addr: IpAddr,
    pub cert: String,
    pub key: String,

    pub http_port: u16,
    pub https_port: u16,

    pub eject: chrono::Duration,
    pub ping_interval: chrono::Duration,
}

// StatusCode가 200이 아닐 때 로깅하는 레이어
#[derive(Clone)]
pub struct Non200StatusLoggingLayer;

impl<S> Layer<S> for Non200StatusLoggingLayer {
    type Service = Non200StatusLoggingService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        Non200StatusLoggingService {
            inner: std::sync::Arc::new(tokio::sync::Mutex::new(inner)),
        }
    }
}

pub struct Non200StatusLoggingService<S> {
    inner: std::sync::Arc<tokio::sync::Mutex<S>>,
}

impl<S> Clone for Non200StatusLoggingService<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<S, B> Service<Request<B>> for Non200StatusLoggingService<S>
where
    S: Service<Request<B>, Response = Response> + Send + 'static,
    S::Future: Send + 'static,
    B: Send + 'static,
{
    type Response = Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        // 이 레이어는 단순히 로깅만 하므로 항상 준비되어 있다고 가정
        // 실제 poll_ready 체크는 call에서 내부 서비스를 호출할 때 수행됨
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<B>) -> Self::Future {
        let path = req.uri().path().to_string();
        let method = req.method().clone();
        let inner = self.inner.clone();
        
        Box::pin(async move {
            let mut inner_guard = inner.lock().await;
            let response = inner_guard.call(req).await?;
            let status = response.status();
            
            if status != StatusCode::OK {
                error!(
                    "Non-200 status code: {} {} -> {}",
                    method,
                    path,
                    status.as_u16()
                );
            }
            
            Ok(response)
        })
    }
}

pub struct Server {
    config: Arc<Param>,
}

impl Server {
    fn get_config(&self) -> Arc<Param> {
        self.config.clone()
    }
    
    async fn redirect_http_to_https(ports: Arc<Param>) -> anyhow::Result<()> {
        fn make_https(host: String, uri: Uri, ports: Arc<Param>) -> anyhow::Result<Uri> {
            let mut parts = uri.into_parts();

            parts.scheme = Some(axum::http::uri::Scheme::HTTPS);

            if parts.path_and_query.is_none() {
                parts.path_and_query = Some("/".parse().unwrap());
            }

            let https_host =
                host.replace(&ports.http_port.to_string(), &ports.https_port.to_string());
            parts.authority = Some(https_host.parse()?);

            Ok(Uri::from_parts(parts)?)
        }

        let router = axum::Router::new()
            .fallback(
                |Host(host): Host, uri: Uri, State(state): State<Arc<Param>>| async move {
                    match make_https(host, uri, state) {
                        Ok(uri) => Ok(axum::response::Redirect::permanent(&uri.to_string())),
                        Err(error) => {
                            cassry::error!(
                                "failed to convert URI to HTTPS : {}",
                                error.to_string()
                            );
                            Err(StatusCode::BAD_REQUEST)
                        }
                    }
                },
            )
            .with_state(ports.clone());

        let addr = SocketAddr::new(ports.addr.clone(), ports.http_port.clone());
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let ret = axum::serve(listener, router.into_make_service()).await?;
        cassry::debug!("http is listening on {:?}", addr);
        Ok(ret)
    }

    pub async fn new(param: Param, router: axum::Router) -> anyhow::Result<Self> {
        let service = tower::ServiceBuilder::new() // 초당 10 요청
            .timeout(std::time::Duration::from_secs(5))
            .load_shed()
            .service(router.into_make_service_with_connect_info::<SocketAddr>());
        
        let config = Arc::new(param);
        let addr = SocketAddr::new(config.addr.clone(), config.https_port.clone());
        let cloned_addr = addr.clone();
        let acceptor = RustlsConfig::from_pem_file(&config.cert, &config.key).await?;
        
        tokio::spawn(Server::redirect_http_to_https(config.clone()));
        tokio::spawn(async move {
            if let Err(e) = axum_server::bind_rustls(cloned_addr, acceptor)
                .serve(service)
                .await
            {
                cassry::error!("occur error in server : {}", e.to_string());
            }
        });

        cassry::info!("success that open webserver : addr({})", addr.to_string());
        Ok(Server { config: config })
    }
}
