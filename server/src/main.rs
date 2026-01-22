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

use crate::config::{MySqlProxyConfig, load_config};
use async_trait::async_trait;
use chacha20poly1305::ChaCha20Poly1305;
use common::{
    AuthenticationHandler, Backend, Frontend, LoginError, LoginMethod, ProxyImplementation,
};
use crypto::LongTermKeyManager;
use crypto::key_manager::MasterKeyHmacSha256KeyManager;
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
        secret_key,
    } = load_config("config.toml");

    let backend = DataFusionProxy::new(
        // TODO(database-adaptability)
        MySqlBackend::new(backend_config).await.unwrap(),
        LongTermKeyManager::new(Box::new(
            MasterKeyHmacSha256KeyManager::<ChaCha20Poly1305>::from_hex_key(&secret_key),
        )),
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

    // Start frontend(s)
    frontend.start_listening().await;
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
