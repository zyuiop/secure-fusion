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

use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

mod arrow;
pub mod cipher;
pub mod config;
pub mod encrypted_column_meta;
pub mod error;
pub mod identifiers;
pub mod key_management;
pub mod planning;
pub mod row_id;

pub use key_management::context::*;
pub use key_management::*;

pub type LongTermKeyManager = Box<dyn PrincipalKeyManager>;

pub trait KeyManagerGetter {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager>;
}

impl KeyManagerGetter for &SessionConfig {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager> {
        self.get_extension().expect("no crypto engine configured!")
    }
}

impl KeyManagerGetter for &SessionState {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager> {
        self.config().get_long_term_keys_manager()
    }
}

impl KeyManagerGetter for Arc<TaskContext> {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager> {
        self.session_config().get_long_term_keys_manager()
    }
}
