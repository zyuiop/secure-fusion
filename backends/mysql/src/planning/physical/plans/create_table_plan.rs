use super::ddl_physical_plan::{DdlPlan, DdlPlanImpl};
use crate::backend_config::BackendConfig;
use crate::metadata::{EncryptedTableMeta, SerializableEncryptedTableMeta};
use crate::providers::build_primary_key;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use crate::providers::table_provider::MySqlTableProvider;
use crate::store::StoreGetter;
use datafusion::common::ResolvedTableReference;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::sqlparser::ast::{CreateTable, Statement as SqlStatement};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct CreateTablePlan {
    catalog: Arc<MySqlCatalogProvider>,
    table_reference: ResolvedTableReference,
    table_def: Arc<MySqlTableProvider>,
    serializable_metadata: SerializableEncryptedTableMeta,
}

impl CreateTablePlan {
    pub fn new_plan(
        catalog: Arc<MySqlCatalogProvider>,
        table_reference: ResolvedTableReference,
        mut table: Box<CreateTable>,
        config: &BackendConfig,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let primary_key = build_primary_key(&table);
        let encrypted_meta =
            EncryptedTableMeta::build_from_statement(&mut table, &primary_key, config)?;

        // Forward table creation instruction unmodified
        let statement = SqlStatement::CreateTable((*table).clone());

        let table_def = MySqlTableProvider::from_columns(
            table_reference.clone().into(),
            table.columns.clone(),
            primary_key.clone(),
            encrypted_meta,
        );

        let serializable_metadata: SerializableEncryptedTableMeta =
            table_def.encryption_metadata().into();

        let extra_impl = Self {
            catalog,
            table_reference,
            table_def: Arc::new(table_def),
            serializable_metadata,
        };

        Ok(Arc::new(DdlPlan::new(statement, Arc::new(extra_impl))))
    }
}

#[async_trait::async_trait]
impl DdlPlanImpl for CreateTablePlan {
    fn name(&self) -> &'static str {
        "drop_table"
    }

    async fn execute_extra(&self, context: Arc<TaskContext>) -> datafusion::common::Result<()> {
        let schema = self
            .catalog
            .mysql_schema(&self.table_reference.schema)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "schema not found {}",
                    self.table_reference.schema
                ))
            })?;

        schema.register_mysql_table(
            self.table_reference.table.to_string(),
            self.table_def.clone(),
        )?;

        // Update metadata
        context
            .get_store()
            .update_metadata(&self.table_reference.schema, |meta| {
                meta.encrypted_tables.insert(
                    self.table_reference.table.to_string(),
                    self.serializable_metadata.clone(),
                );
                Ok(())
            })
            .await?;

        Ok(())
    }
}
