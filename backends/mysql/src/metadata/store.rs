use crate::errors::MySqlResult;
use crate::metadata::{SerializableEncryptedSchemaMeta, SerializableEncryptedTableMeta};
use crate::providers::table_provider::MySqlTableProvider;
use async_trait::async_trait;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use datafusion::sql::ResolvedTableReference;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::sync::Arc;

#[async_trait]
pub trait MetadataStoreLocation {
    async fn read(&self, schema: &str) -> MySqlResult<SerializableEncryptedSchemaMeta>;
    async fn write(
        &self,
        schema: &str,
        contents: &SerializableEncryptedSchemaMeta,
    ) -> MySqlResult<()>;
}

#[derive(Clone)]
pub struct MetadataStore {
    store_location: Arc<dyn MetadataStoreLocation + Send + Sync>,
}

impl MetadataStore {
    pub async fn read_metadata(
        &self,
        schema: &str,
    ) -> MySqlResult<SerializableEncryptedSchemaMeta> {
        self.store_location.read(schema).await
    }

    pub async fn write_metadata(
        &self,
        schema: &str,
        meta: &SerializableEncryptedSchemaMeta,
    ) -> MySqlResult<()> {
        self.store_location.write(schema, meta).await
    }

    pub async fn update_metadata<F>(&self, schema: &str, updater: F) -> MySqlResult<()>
    where
        F: FnOnce(&mut SerializableEncryptedSchemaMeta) -> MySqlResult<()>,
    {
        let mut meta = self.read_metadata(schema).await?;
        updater(&mut meta)?;
        self.write_metadata(schema, &meta).await
    }

    pub async fn save_metadata_for_table(
        &self,
        table_name: &ResolvedTableReference,
        table: Arc<MySqlTableProvider>,
    ) -> MySqlResult<()> {
        let meta = table.encryption_metadata();
        let serializable = SerializableEncryptedTableMeta::from(meta);
        self.update_metadata(&table_name.schema, |meta| {
            meta.encrypted_tables
                .insert(table_name.table.to_string(), serializable);
            Ok(())
        })
        .await
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct StoreToDisk {
    path: String,
}

impl Default for StoreToDisk {
    fn default() -> Self {
        Self {
            path: ".".to_string(),
        }
    }
}

impl From<StoreToDisk> for MetadataStore {
    fn from(value: StoreToDisk) -> Self {
        Self {
            store_location: Arc::new(value),
        }
    }
}

impl StoreToDisk {
    fn read_raw(&self, schema: &str) -> MySqlResult<Option<String>> {
        let mut open_options = OpenOptions::new();
        let open_options = open_options.read(true).write(false);

        let Some(f) = open_options
            .open(format!("{}/meta_{schema}.toml", self.path))
            .ok()
        else {
            return Ok(None);
        };

        let mut contents = String::new();
        let mut reader = BufReader::new(f);
        reader.read_to_string(&mut contents).unwrap(); // TODO error handling

        Ok(Some(contents))
    }

    fn write_raw(&self, schema: &str, contents: &str) -> MySqlResult<()> {
        let mut f = File::create(format!("{}/meta_{schema}.toml", self.path)).unwrap();
        f.write_all(contents.as_bytes()).unwrap();
        Ok(())
    }
}

#[async_trait]
impl MetadataStoreLocation for StoreToDisk {
    async fn read(&self, schema: &str) -> MySqlResult<SerializableEncryptedSchemaMeta> {
        let data = self.read_raw(schema)?;

        match data {
            None => Ok(SerializableEncryptedSchemaMeta::default()),
            Some(data) => Ok(toml::from_str::<SerializableEncryptedSchemaMeta>(&data)
                .expect("cannot deserialize metadata")),
        }
    }

    async fn write(
        &self,
        schema: &str,
        contents: &SerializableEncryptedSchemaMeta,
    ) -> MySqlResult<()> {
        let serialized = toml::to_string(contents)?;
        self.write_raw(schema, &serialized)
    }
}

pub trait StoreGetter {
    fn try_get_store(&self) -> Option<Arc<MetadataStore>>;

    fn get_store(&self) -> Arc<MetadataStore> {
        self.try_get_store()
            .expect("server runtime state is invalid: MetadataStore not found")
    }
}

impl StoreGetter for &SessionConfig {
    fn try_get_store(&self) -> Option<Arc<MetadataStore>> {
        self.get_extension()
    }
}

impl StoreGetter for &SessionState {
    fn try_get_store(&self) -> Option<Arc<MetadataStore>> {
        self.config().try_get_store()
    }
}

impl StoreGetter for Arc<TaskContext> {
    fn try_get_store(&self) -> Option<Arc<MetadataStore>> {
        self.session_config().try_get_store()
    }
}
