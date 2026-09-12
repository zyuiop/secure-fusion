#![deny(
    unused_must_use,
    unreachable_code,
    unreachable_patterns,
    unused_imports,
    dead_code,
    irrefutable_let_patterns,
    unused_unsafe,
    unused_mut,
    unused_variables
)]
#![warn(unused_lifetimes, redundant_lifetimes)]
#![deny(clippy::perf)]

mod config;

#[cfg(feature = "tracing")]
mod tracing;

use crate::config::{MySqlProxyConfig, load_config};
use async_trait::async_trait;
use common::{
    AuthenticationHandler, Backend, Frontend, LoginError, LoginMethod, ProxyImplementation,
};
use env_logger::Env;
use mysql_backend::MySqlBackend;
use mysql_frontend::{MySqlFrontend, MySqlFrontendConfig};
use proxy::DataFusionProxy;
use proxy::session::DataFusionSession;
use std::sync::Arc;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();

    let MySqlProxyConfig {
        backend_config,
        crypto_config,
    } = load_config("config.toml");

    #[cfg(feature = "tracing")]
    let telemetry_server = if let Ok(endpoint) = std::env::var("TRACING_ENDPOINT") {
        crate::tracing::init_tracing(&endpoint)
    } else {
        None
    };

    #[cfg(feature = "tracing")]
    if telemetry_server.is_none() {
        crate::tracing::mute_tracing()
    }

    let backend = DataFusionProxy::new(
        // TODO(database-adaptability)
        MySqlBackend::new(backend_config).await.unwrap(),
        crypto_config.init_key_manager(),
        #[cfg(feature = "tracing")]
        telemetry_server.is_some(),
    );
    let srv = ProxyServer {
        inner_server: backend,
        inner_auth: Arc::new(StubAuthBackend),
    };
    let srv = Arc::new(srv);

    // TODO: support multiple frontends
    // Init frontend(s)
    let frontend = MySqlFrontend::new(
        MySqlFrontendConfig {
            port: None,
            host: None,
        },
        srv.clone(),
    );

    #[cfg(feature = "tracing")]
    if let Some(telemetry_server) = telemetry_server {
        ctrlc::set_handler(move || {
            log::info!("Sending last telemetry events...");
            telemetry_server
                .shutdown_with_timeout(std::time::Duration::from_secs(10))
                .expect("failed to cleanup telemetry");
        })
        .expect("Error setting Ctrl-C handler");
    }

    // Start frontend(s)
    frontend.start_listening();
}

struct ProxyServer<B: Backend> {
    inner_server: DataFusionProxy<B>,
    inner_auth: Arc<StubAuthBackend>,
}

#[async_trait]
impl<B: Backend + Send + Sync + 'static> ProxyImplementation for ProxyServer<B> {
    type SessionType = DataFusionSession<B::SessionType>;
    type AuthenticationHandler = StubAuthBackend;

    async fn new_session(&self, initial_database: Option<&str>) -> Self::SessionType {
        self.inner_server.new_session(initial_database).await
    }

    fn authentication_handler(&self) -> Arc<Self::AuthenticationHandler> {
        self.inner_auth.clone()
    }
}

struct StubAuthBackend;

#[async_trait::async_trait]
impl AuthenticationHandler for StubAuthBackend {
    async fn try_login(
        &self,
        _username: &str,
        _login_method: LoginMethod,
    ) -> Result<(), LoginError> {
        // TODO
        Ok(())
    }
}
