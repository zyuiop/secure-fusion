use crate::providers::dummy_source::DummySource;
use crate::providers::table_provider::MySqlTableProvider;
use async_trait::async_trait;
use datafusion::arrow::datatypes::Field;
use datafusion::catalog::{SchemaProvider, TableProvider};
use datafusion::common::DataFusionError;
use mysql_async::Conn;
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
            return Err(DataFusionError::Execution("Table already exists".into()));
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

    pub async fn force_create_indexable_column(
        &self,
        table_name: &str,
        conn: &mut Conn,
    ) -> datafusion::common::Result<Field> {
        let mut lock = self.tables.try_write().unwrap();
        let table = lock.remove(table_name).ok_or(
            DataFusionError::Internal(format!("Invalid code path: tried to force indexable column creation on non-existing table {table_name}"))
        )?;

        let mut table = Arc::unwrap_or_clone(table.clone());
        table.create_indexable_column(conn).await?;

        let column = table
            .get_indexable_column()
            .expect("infaillible: indexable column has been created in the previous operation");

        lock.insert(table_name.to_string(), Arc::new(table));

        Ok(column)
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
