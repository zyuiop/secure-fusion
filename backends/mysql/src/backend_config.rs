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

    #[cfg(feature = "index-search")]
    pub index_search: Option<IndexSearchSatelliteConfig>,

    #[serde(default)]
    pub row_binding_aad: bool,

    pub df_hash_join_max_pushdown_values: Option<usize>,

    pub df_hash_join_max_pushdown_size_per_value: Option<usize>,

    pub df_repartition_joins: Option<bool>,

    pub df_target_parallelism: Option<usize>,

    pub df_default_filter_selectivity: Option<u8>,

    pub df_batch_size: Option<usize>,
}

#[cfg(feature = "index-search")]
#[derive(serde::Deserialize, serde::Serialize)]
pub struct IndexSearchSatelliteConfig {
    pub host: String,
    pub port: u16,
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
            row_binding_aad: false,

            #[cfg(feature = "index-search")]
            index_search: None,

            df_hash_join_max_pushdown_values: None,
            df_hash_join_max_pushdown_size_per_value: None,
            df_repartition_joins: None,
            df_target_parallelism: None,
            df_default_filter_selectivity: None,
            df_batch_size: None,
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
