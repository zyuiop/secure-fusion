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

pub(crate) mod client_side_helper;
pub(crate) mod command_phase;
pub(crate) mod connection_phase;
pub(crate) mod server;
pub(crate) mod status;

use common::ProxyImplementation;
use std::sync::Arc;

pub struct MySqlFrontendConfig {
    pub port: Option<u16>,
    pub host: Option<String>,
    // More settings for authentication and stuff
}

pub struct MySqlFrontend<T: ProxyImplementation> {
    config: MySqlFrontendConfig,
    handler: Arc<T>,
}

impl<T: ProxyImplementation> MySqlFrontend<T> {
    pub fn new(config: MySqlFrontendConfig, handler: Arc<T>) -> MySqlFrontend<T> {
        Self { config, handler }
    }
}
