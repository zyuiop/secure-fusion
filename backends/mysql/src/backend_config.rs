use crate::store::{MetadataStore, StoreToDisk};
use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use mysql_async::Conn;
use std::sync::Arc;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct BackendConfig {
    /// If true, new columns are automatically encrypted, unless they are of an INT datatype
    pub encrypt_by_default: bool,

    /// If true, identifiers are normalized
    pub normalize_identifiers: bool,

    pub mysql_connect_string: String,

    pub metadata_store: MetadataStoreConfig,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub(super) enum MetadataStoreConfig {
    Disk(StoreToDisk),
}

impl From<&'_ MetadataStoreConfig> for MetadataStore {
    fn from(value: &'_ MetadataStoreConfig) -> Self {
        match value {
            MetadataStoreConfig::Disk(disk) => disk.clone().into(),
        }
    }
}

impl BackendConfig {
    pub async fn get_conn(&self) -> mysql_async::Result<Conn> {
        Conn::from_url(&self.mysql_connect_string).await
    }
}

impl Default for BackendConfig {
    fn default() -> BackendConfig {
        BackendConfig {
            normalize_identifiers: false,
            encrypt_by_default: true,
            mysql_connect_string: "mysql://user:password@host:3306/".into(),
            metadata_store: MetadataStoreConfig::Disk(StoreToDisk::default()),
        }
    }
}

pub trait GetConfig {
    fn get_backend_config(&self) -> Arc<BackendConfig>;
}

impl GetConfig for SessionConfig {
    fn get_backend_config(&self) -> Arc<BackendConfig> {
        self.get_extension().expect("no backend config set!")
    }
}

impl GetConfig for SessionState {
    fn get_backend_config(&self) -> Arc<BackendConfig> {
        self.config().get_backend_config()
    }
}

impl GetConfig for Arc<TaskContext> {
    fn get_backend_config(&self) -> Arc<BackendConfig> {
        self.session_config().get_backend_config()
    }
}
