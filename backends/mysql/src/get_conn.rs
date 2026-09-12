use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use futures_util::lock::Mutex;
use mysql_async::Conn;
use std::sync::Arc;

pub trait ConnGetter {
    fn get_conn(&self) -> Arc<Mutex<Conn>>;

    #[cfg(feature = "index-search")]
    fn get_index_search_client(
        &self,
    ) -> Option<Arc<index_search_protocol::IndexSearchServerClient>>;
}

impl ConnGetter for SessionConfig {
    fn get_conn(&self) -> Arc<Mutex<Conn>> {
        self.get_extension()
            .expect("no MySQL connection configured!")
    }

    #[cfg(feature = "index-search")]
    fn get_index_search_client(
        &self,
    ) -> Option<Arc<index_search_protocol::IndexSearchServerClient>> {
        self.get_extension()
    }
}

impl ConnGetter for SessionState {
    fn get_conn(&self) -> Arc<Mutex<Conn>> {
        self.config().get_conn()
    }

    #[cfg(feature = "index-search")]
    fn get_index_search_client(
        &self,
    ) -> Option<Arc<index_search_protocol::IndexSearchServerClient>> {
        self.config().get_index_search_client()
    }
}

impl ConnGetter for Arc<TaskContext> {
    fn get_conn(&self) -> Arc<Mutex<Conn>> {
        self.session_config().get_conn()
    }

    #[cfg(feature = "index-search")]
    fn get_index_search_client(
        &self,
    ) -> Option<Arc<index_search_protocol::IndexSearchServerClient>> {
        self.session_config().get_index_search_client()
    }
}
