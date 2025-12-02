use axum::{
    extract::State,
    http::{StatusCode, Uri}, response::IntoResponse,
};

use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use cassry::*;
use tower::{Layer, Service};
use axum::response::Response;
use axum::http::Request;
// https://www.runit.cloud/2020/04/https-ssl.html

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
        let service = tower::ServiceBuilder::new()
            .rate_limit(10, std::time::Duration::from_secs(1)) // 초당 10 요청
            .timeout(std::time::Duration::from_secs(5))
            .load_shed()
            .service(router.into_make_service());

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
