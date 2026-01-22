use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use futures_util::lock::Mutex;
use mysql_async::Conn;
use std::sync::Arc;

pub trait ConnGetter {
    fn get_conn(&self) -> Arc<Mutex<Conn>>;
}

impl ConnGetter for SessionConfig {
    fn get_conn(&self) -> Arc<Mutex<Conn>> {
        self.get_extension()
            .expect("no MySQL connection configured!")
    }
}

impl ConnGetter for SessionState {
    fn get_conn(&self) -> Arc<Mutex<Conn>> {
        self.config().get_conn()
    }
}

impl ConnGetter for Arc<TaskContext> {
    fn get_conn(&self) -> Arc<Mutex<Conn>> {
        self.session_config().get_conn()
    }
}
