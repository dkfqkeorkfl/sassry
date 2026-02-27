use axum::{
    extract::State,
    http::{HeaderMap, StatusCode, Uri},
};

use axum_server::tls_rustls::RustlsConfig;
use serde_with::{serde_as, DurationSeconds};

use std::sync::Arc;
use std::{
    net::{IpAddr, SocketAddr},
    str::FromStr,
};

use axum::http::{HeaderName, HeaderValue, Method};
use cassry::*;
use serde::{Deserialize, Serialize};
use tower_governor::{governor::GovernorConfigBuilder, GovernorLayer};
use tower_http::{
    compression::{
        predicate::{NotForContentType, SizeAbove},
        CompressionLayer, Predicate,
    },
    cors::CorsLayer,
    limit::RequestBodyLimitLayer,
    normalize_path::NormalizePathLayer,
    sensitive_headers::{SetSensitiveRequestHeadersLayer, SetSensitiveResponseHeadersLayer},
    set_header::SetResponseHeaderLayer,
    trace::TraceLayer,
};

#[derive(Debug, thiserror::Error)]
#[error("Server errors: {errors:?}")]
pub struct ServerErrors {
    pub errors: Vec<anyhow::Error>,
}
// https://www.runit.cloud/2020/04/https-ssl.html
/// Axum 미들웨어 설정 구조체
/// 모든 필드는 Option으로 선언되어 있어 선택적으로 설정 가능
/// #[serde_as]
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MiddlewareConfig {
    /// 로깅 활성화 여부
    #[serde(default)]
    pub log: Option<LogConfig>,

    /// 요청 타임아웃 (초 단위로 직렬화)
    /// JSON에서는 숫자(초)로 저장되고, std::time::Duration으로 변환됨
    /// none : 24시간
    #[serde_as(as = "Option<DurationSeconds<f64>>")]
    pub timeout: Option<std::time::Duration>,

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
pub struct LogConfig {
    #[serde(default)]
    pub sensitive_request: Vec<String>,

    #[serde(default)]
    pub sensitive_response: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// HTTP 포트 (리다이렉트용)
    pub socket_addr: SocketAddr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpsConfig {
    pub addr: IpAddr,
    /// HTTPS 포트 (실제 서비스 포트)
    pub https_port: u16,

    /// SSL 인증서 파일 경로
    pub cert_file: String,

    /// SSL 개인키 파일 경로
    pub key_file: String,

    /// HTTP 포트 (리다이렉트용)
    #[serde(default)]
    pub http_port: Option<u16>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ServerConfig {
    Http(HttpConfig),
    Https(HttpsConfig),
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
    /// IP당 초당 허용 요청 수
    pub per_second: u64,

    /// 최대 버스트 크기
    #[serde(default)]
    pub burst_size: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    /// 압축 최소 크기 (바이트) - 이 크기 이상의 응답만 압축
    pub min_size: u16,

    /// 압축 품질 (1-11) - 높을수록 압축률이 높지만 CPU 사용량 증가
    #[serde(default)]
    pub gzip: Option<bool>,

    #[serde(default)]
    pub deflate: Option<bool>,

    #[serde(default)]
    pub br: Option<bool>,
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
    /// HSTS max-age 값 (초)
    pub max_age: u64,

    /// includeSubDomains 플래그
    #[serde(default)]
    pub include_subdomains: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Param {
    pub addr: IpAddr,
    pub cert: String,
    pub key: String,

    pub http_port: u16,
    pub https_port: u16,

    pub eject: chrono::Duration,
    pub ping_interval: chrono::Duration,
}

type TaskHandle = tokio::task::JoinHandle<Result<(), std::io::Error>>;
pub struct Server {
    server_config: ServerConfig,
    middleware_config: MiddlewareConfig,
    handle: axum_server::Handle<SocketAddr>,
    tasks: Vec<TaskHandle>,
}

impl Server {
    pub fn get_middleware_config(&self) -> &MiddlewareConfig {
        &self.middleware_config
    }

    pub fn get_server_config(&self) -> &ServerConfig {
        &self.server_config
    }

    /// 로깅 미들웨어 적용
    fn apply_logging(mut router: axum::Router, log: &LogConfig) -> anyhow::Result<axum::Router> {
        cassry::info!("[apply_logging] TraceLayer applied");
        router = router.layer(TraceLayer::new_for_http());
        if !log.sensitive_request.is_empty() {
            let headers = log
                .sensitive_request
                .iter()
                .map(|s| HeaderName::from_str(s))
                .collect::<Result<Vec<HeaderName>, _>>()
                .map_err(|e| anyhowln!("failed to parse header name: {}", e))?;

            router = router.layer(SetSensitiveRequestHeadersLayer::new(headers))
        }

        if !log.sensitive_response.is_empty() {
            let headers = log
                .sensitive_response
                .iter()
                .map(|s| HeaderName::from_str(s))
                .collect::<Result<Vec<HeaderName>, _>>()
                .map_err(|e| anyhowln!("failed to parse header name: {}", e))?;

            router = router.layer(SetSensitiveResponseHeadersLayer::new(headers))
        }
        Ok(router)
    }

    /// 타임아웃 Duration을 그대로 사용 (이미 std::time::Duration)
    fn apply_timeout(timeout: Option<std::time::Duration>) -> std::time::Duration {
        let result = timeout.unwrap_or(std::time::Duration::from_secs(60 * 60 * 24));
        cassry::info!(
            "[apply_timeout] timeout configured: {} seconds",
            result.as_secs()
        );
        result
    }

    /// 미들웨어 설정 적용 (RequestBodyLimit, RateLimit)
    fn apply_middleware(
        mut router: axum::Router,
        middleware: &MiddlewareSettings,
    ) -> anyhow::Result<axum::Router> {
        // RequestBodyLimitLayer
        if let Some(limit) = middleware.request_body_limit {
            router = router.layer(RequestBodyLimitLayer::new(limit as usize));
            cassry::info!(
                "[apply_middleware] RequestBodyLimitLayer applied: {} bytes",
                limit
            );
        }

        // Rate Limiting (GovernorLayer)
        if let Some(rate_limit) = &middleware.rate_limit {
            let governor_conf = if let Some(burst_size) = rate_limit.burst_size {
                let governor_conf = GovernorConfigBuilder::default()
                    .per_second(rate_limit.per_second)
                    .burst_size(burst_size)
                    .finish()
                    .ok_or(anyhowln!("failed to build GovernorConfig"))?;
                cassry::info!(
                    "[apply_middleware] GovernorLayer applied: per_second={}, burst_size={}",
                    rate_limit.per_second,
                    burst_size
                );
                governor_conf
            } else {
                let governor_conf = GovernorConfigBuilder::default()
                    .per_second(rate_limit.per_second)
                    .finish()
                    .ok_or(anyhowln!("failed to build GovernorConfig"))?;
                cassry::info!(
                    "[apply_middleware] GovernorLayer applied: per_second={}",
                    rate_limit.per_second,
                );
                governor_conf
            };

            router = router.layer(GovernorLayer::new(governor_conf));
        }

        Ok(router)
    }

    /// 컴프레션 미들웨어 적용
    fn apply_compression(router: axum::Router, conf: &CompressionConfig) -> axum::Router {
        if conf.min_size < 1024 {
            cassry::warn!(
                "[apply_compression] min_size is too small: {}",
                conf.min_size
            );
        }

        let min_size = if !(0..u16::MAX).contains(&conf.min_size) {
            cassry::error!(
                "[apply_compression] min_size is too large: {}",
                conf.min_size
            );
            u16::MAX
        } else {
            conf.min_size
        };

        // SizeAbove는 u16을 기대하므로 변환 (최대 65535 바이트까지)
        let predicate = SizeAbove::new(min_size)
            .and(NotForContentType::GRPC)
            .and(NotForContentType::IMAGES)
            .and(NotForContentType::SSE);
        let compression_layer = CompressionLayer::new()
            .gzip(conf.gzip.unwrap_or(false))
            .deflate(conf.deflate.unwrap_or(false))
            .br(conf.br.unwrap_or(false))
            .compress_when(predicate);

        cassry::info!(
            "[apply_compression] CompressionLayer applied: min_size={} bytes, gzip={}, deflate={}, br={}",
            min_size,
            conf.gzip.unwrap_or(false),
            conf.deflate.unwrap_or(false),
            conf.br.unwrap_or(false)
        );

        router.layer(compression_layer)
    }

    /// CORS 미들웨어 적용
    fn apply_cors(router: axum::Router, cors: &CorsConfig) -> axum::Router {
        let mut cors_layer = CorsLayer::new();

        // Allowed Origins
        if let Some(origins) = &cors.allowed_origins {
            if !origins.is_empty() {
                let parsed_origins: Result<Vec<_>, _> =
                    origins.iter().map(|o| o.parse::<HeaderValue>()).collect();
                if let Ok(parsed) = parsed_origins {
                    cors_layer = cors_layer.allow_origin(parsed);
                    cassry::info!("[apply_cors] allowed_origins applied: {:?}", origins);
                }
            }
        }

        // Allowed Methods
        if let Some(methods) = &cors.allowed_methods {
            let parsed_methods: Result<Vec<_>, _> =
                methods.iter().map(|m| m.parse::<Method>()).collect();
            if let Ok(parsed) = parsed_methods {
                cors_layer = cors_layer.allow_methods(parsed);
                cassry::info!("[apply_cors] allowed_methods applied: {:?}", methods);
            }
        }

        // Allowed Headers
        if let Some(headers) = &cors.allowed_headers {
            let parsed_headers: Result<Vec<_>, _> =
                headers.iter().map(|h| h.parse::<HeaderName>()).collect();
            if let Ok(parsed) = parsed_headers {
                cors_layer = cors_layer.allow_headers(parsed);
                cassry::info!("[apply_cors] allowed_headers applied: {:?}", headers);
            }
        }

        // Allow Credentials
        if let Some(allow_creds) = cors.allow_credentials {
            cors_layer = cors_layer.allow_credentials(allow_creds);
            cassry::info!("[apply_cors] allow_credentials applied: {}", allow_creds);
        }

        // Max Age
        if let Some(max_age) = cors.max_age {
            cors_layer = cors_layer.max_age(std::time::Duration::from_secs(max_age));
            cassry::info!("[apply_cors] max_age applied: {} seconds", max_age);
        }

        router.layer(cors_layer)
    }

    /// 보안 헤더 미들웨어 적용
    fn apply_security_headers(
        mut router: axum::Router,
        security: &SecurityHeadersConfig,
    ) -> axum::Router {
        // X-Frame-Options
        if let Some(x_frame) = &security.x_frame_options {
            if let Ok(header_value) = HeaderValue::from_str(x_frame) {
                router = router.layer(SetResponseHeaderLayer::overriding(
                    axum::http::header::X_FRAME_OPTIONS,
                    header_value,
                ));
                cassry::info!(
                    "[apply_security_headers] X-Frame-Options applied: {}",
                    x_frame
                );
            }
        }

        // X-Content-Type-Options
        if let Some(x_content_type) = &security.x_content_type_options {
            if let Ok(header_value) = HeaderValue::from_str(x_content_type) {
                router = router.layer(SetResponseHeaderLayer::overriding(
                    axum::http::header::X_CONTENT_TYPE_OPTIONS,
                    header_value,
                ));
                cassry::info!(
                    "[apply_security_headers] X-Content-Type-Options applied: {}",
                    x_content_type
                );
            }
        }

        // HSTS
        if let Some(hsts) = &security.hsts {
            let hsts_value = if hsts.include_subdomains.unwrap_or(false) {
                format!("max-age={}; includeSubDomains", hsts.max_age)
            } else {
                format!("max-age={}", hsts.max_age)
            };
            if let Ok(header_value) = HeaderValue::from_str(&hsts_value) {
                router = router.layer(SetResponseHeaderLayer::overriding(
                    axum::http::header::STRICT_TRANSPORT_SECURITY,
                    header_value,
                ));
                cassry::info!(
                    "[apply_security_headers] HSTS applied: max_age={}, include_subdomains={:?}",
                    hsts.max_age,
                    hsts.include_subdomains
                );
            }
        }

        // CSP Policy
        if let Some(csp) = &security.csp_policy {
            if let Ok(header_value) = HeaderValue::from_str(csp) {
                router = router.layer(SetResponseHeaderLayer::overriding(
                    axum::http::header::CONTENT_SECURITY_POLICY,
                    header_value,
                ));
                cassry::info!("[apply_security_headers] CSP Policy applied: {}", csp);
            }
        }

        // Referrer Policy
        if let Some(referrer) = &security.referrer_policy {
            if let Ok(header_value) = HeaderValue::from_str(referrer) {
                router = router.layer(SetResponseHeaderLayer::overriding(
                    axum::http::header::REFERRER_POLICY,
                    header_value,
                ));
                cassry::info!(
                    "[apply_security_headers] Referrer-Policy applied: {}",
                    referrer
                );
            }
        }

        // Permissions Policy
        if let Some(permissions) = &security.permissions_policy {
            if let Ok(header_value) = HeaderValue::from_str(permissions) {
                router = router.layer(SetResponseHeaderLayer::overriding(
                    HeaderName::from_static("permissions-policy"),
                    header_value,
                ));
                cassry::info!(
                    "[apply_security_headers] Permissions-Policy applied: {}",
                    permissions
                );
            }
        }

        router
    }

    async fn redirect_http_to_https2(
        handle: axum_server::Handle<SocketAddr>,
        config: Arc<HttpsConfig>,
    ) -> anyhow::Result<tokio::task::JoinHandle<Result<(), std::io::Error>>> {
        fn make_https(host: &str, uri: Uri, ports: Arc<HttpsConfig>) -> anyhow::Result<Uri> {
            let mut parts = uri.into_parts();

            parts.scheme = Some(axum::http::uri::Scheme::HTTPS);

            if parts.path_and_query.is_none() {
                parts.path_and_query = Some("/".parse().unwrap());
            }

            let https_host = host.replace(
                &ports.http_port.unwrap().to_string(),
                &ports.https_port.to_string(),
            );
            parts.authority = Some(https_host.parse()?);

            Ok(Uri::from_parts(parts)?)
        }

        let router = axum::Router::new()
            .fallback(
                |hearders: HeaderMap, uri: Uri, State(state): State<Arc<HttpsConfig>>| async move {
                    let host = hearders
                        .get(axum::http::header::HOST)
                        .map(|h| h.to_str().ok())
                        .flatten();
                    if host.is_none() {
                        return Err(StatusCode::BAD_REQUEST);
                    }

                    match make_https(host.unwrap(), uri, state) {
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
            .with_state(config.clone());

        let handle = tokio::spawn(async move {
            let addr = SocketAddr::new(config.addr.clone(), config.http_port.unwrap());
            axum_server::bind(addr)
                .handle(handle)
                .serve(router.into_make_service())
                .await
        });

        Ok(handle)
    }

    /// MiddlewareConfig를 사용하여 서버를 생성하는 새로운 함수
    /// ServerConfig enum과 MiddlewareConfig를 사용하여 모든 미들웨어를 구성
    pub async fn new(
        server_config: ServerConfig,
        middleware_config: MiddlewareConfig,
        mut router: axum::Router,
    ) -> anyhow::Result<Self> {
        if let Some(log) = &middleware_config.log {
            router = Self::apply_logging(router, log)?;
        }

        if let Some(middleware) = &middleware_config.middleware {
            router = Self::apply_middleware(router, middleware)?;
        }

        if let Some(compression) = &middleware_config.compression {
            router = Self::apply_compression(router, compression);
        }

        if let Some(cors) = &middleware_config.cors {
            router = Self::apply_cors(router, cors);
        }

        if let Some(security) = &middleware_config.security_headers {
            router = Self::apply_security_headers(router, security);
        }

        // Server 헤더 제거 (보안)
        router = router
            .layer(SetResponseHeaderLayer::overriding(
                axum::http::header::SERVER,
                HeaderValue::from_static(""),
            ))
            .layer(NormalizePathLayer::trim_trailing_slash());

        // ServiceBuilder 설정
        let service_builder = tower::ServiceBuilder::new()
            .timeout(Self::apply_timeout(middleware_config.timeout))
            .load_shed();

        // 서비스 빌더에 추가 설정 적용
        let service =
            service_builder.service(router.into_make_service_with_connect_info::<SocketAddr>());

        let handle = axum_server::Handle::new();
        let copied_handle = handle.clone();
        let shutdown_task = tokio::spawn(async move {
            cassry::util::shutdown_signal().await?;
            cassry::info!("Received termination signal shutting down");
            copied_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
            Ok(())
        });

        let copied_handle = handle.clone();
        // ServerConfig에서 설정 추출
        let server_tasks = match &server_config {
            ServerConfig::Http(config) => {
                let addr = config.socket_addr.clone();
                let http_task = tokio::spawn(async move {
                    axum_server::bind(addr.clone())
                        .handle(copied_handle)
                        .serve(service)
                        .await
                });
                vec![http_task]
            }
            ServerConfig::Https(config) => {
                let addr = SocketAddr::new(config.addr, config.https_port);
                let acceptor =
                    RustlsConfig::from_pem_file(&config.cert_file, &config.key_file).await?;
                let https_task = tokio::spawn(async move {
                    axum_server::bind_rustls(addr.clone(), acceptor)
                        .handle(copied_handle.clone())
                        .serve(service)
                        .await
                });

                if config.http_port.is_some() {
                    let config = config.clone();
                    let http_task =
                        Server::redirect_http_to_https2(handle.clone(), Arc::new(config)).await?;
                    vec![https_task, http_task]
                } else {
                    vec![https_task]
                }
            }
        };

        let mut tasks = vec![shutdown_task];
        tasks.extend(server_tasks.into_iter());

        cassry::info!(
            "success that open webserver with MiddlewareConfig : {:?}",
            server_config
        );
        Ok(Server {
            server_config: server_config,
            middleware_config: middleware_config,
            handle: handle,
            tasks,
        })
    }

    pub async fn run_until_shutdown(self) -> Result<(), ServerErrors> {
        let mut results = Vec::new();
        let mut tasks = self.tasks;
        let mut shutdown_task_id = if let Some(shutdown_task) = tasks.get(0) {
            Some(shutdown_task.id())
        } else {
            None
        };

        while !tasks.is_empty() {
            let (result, _idx, remaining) = futures::future::select_all(tasks).await;
            tasks = remaining;

            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if let Some(id) = shutdown_task_id {
                        if let Some(idx) = tasks.iter().position(|t| t.id() == id) {
                            tasks.remove(idx).abort();
                        }
                        shutdown_task_id = None;
                    }

                    results.push(e.into());
                    // 서버 에러 → 나머지도 함께 종료
                    self.handle
                        .graceful_shutdown(Some(std::time::Duration::from_secs(10)));
                }
                Err(e) => {
                    if let Some(id) = shutdown_task_id {
                        if let Some(idx) = tasks.iter().position(|t| t.id() == id) {
                            tasks.remove(idx).abort();
                        }
                        shutdown_task_id = None;
                    }

                    results.push(e.into());
                    self.handle
                        .graceful_shutdown(Some(std::time::Duration::from_secs(10)));
                }
            }
        }

        if results.is_empty() {
            Ok(())
        } else {
            Err(ServerErrors { errors: results })
        }
    }
}
