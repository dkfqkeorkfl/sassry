use axum::{
    extract::State,
    http::{StatusCode, Uri},
};

use axum_extra::extract::Host;
use axum_server::tls_rustls::RustlsConfig;

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use cassry::*;
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

pub struct Server {
    config: Arc<Param>,
}

impl Server {
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
        if let Err(e) = axum_server::bind_rustls(cloned_addr, acceptor)
            .serve(service)
            .await
        {
            cassry::error!("occur error in server : {}", e.to_string());
        }
        Server::redirect_http_to_https(config.clone()).await?;
        cassry::info!("success that open webserver : addr({})", addr.to_string());
        Ok(Server { config: config })
    }
}
