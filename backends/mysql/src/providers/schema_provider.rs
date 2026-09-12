use crate::providers::dummy_source::DummySource;
use crate::providers::table_provider::MySqlTableProvider;
use async_trait::async_trait;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::{DataFusionError, exec_err};
use rustc_hash::FxHashMap;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub(crate) struct MySqlSchemaProvider {
    tables: RwLock<FxHashMap<String, Arc<MySqlTableProvider>>>,
    dummy_table: Arc<DummySource>,
}

impl MySqlSchemaProvider {
    pub fn new(tables: FxHashMap<String, Arc<MySqlTableProvider>>) -> Arc<Self> {
        Arc::new(Self {
            tables: RwLock::new(tables),
            dummy_table: Arc::new(DummySource::default()),
        })
    }

    pub fn mysql_table(&self, name: &str) -> Option<Arc<MySqlTableProvider>> {
        self.tables.try_read().unwrap().get(name).cloned()
    }

    pub fn register_mysql_table(
        &self,
        name: String,
        table: Arc<MySqlTableProvider>,
    ) -> datafusion::common::Result<Arc<MySqlTableProvider>> {
        let mut lock = self.tables.try_write().unwrap();

        if lock.contains_key(&name) {
            return exec_err!("Table {name} already exists")?;
        }

        lock.insert(name, table.clone());
        Ok(table)
    }

    pub fn take_mysql_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<MySqlTableProvider>>> {
        let mut lock = self.tables.try_write().unwrap();
        Ok(lock.remove(name))
    }

    pub fn replace_mysql_table(
        &self,
        name: String,
        table: Arc<MySqlTableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let mut lock = self.tables.try_write().unwrap();
        lock.insert(name, table.clone());
        Ok(Some(table))
    }
}

#[async_trait]
impl SchemaProvider for MySqlSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        let lock = self.tables.try_read().unwrap();
        lock.keys().cloned().collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if name == "dual" {
            return Ok(Some(self.dummy_table.clone()));
        }

        let lock = self.tables.try_read().unwrap();
        Ok(lock
            .get(name)
            .cloned()
            .map::<Arc<dyn TableProvider>, _>(|v| v))
    }

    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        unimplemented!("use the register_mysql_table function instead")
    }

    fn deregister_table(
        &self,
        name: &str,
    ) -> datafusion::common::Result<Option<Arc<dyn TableProvider>>> {
        let mut lock = self.tables.try_write().unwrap();
        Ok(lock.remove(name).map::<Arc<dyn TableProvider>, _>(|v| v))
    }

    fn table_exist(&self, name: &str) -> bool {
        let lock = self.tables.try_read().unwrap();
        lock.contains_key(name)
    }
}
