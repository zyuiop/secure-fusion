use super::super::super::unparser;
use super::ddl_physical_plan::{DdlPlan, DdlPlanImpl};
use crate::providers::catalog_provider::MySqlCatalogProvider;
use crate::store::StoreGetter;
use async_trait::async_trait;
use datafusion::catalog::CatalogProvider;
use datafusion::common::{DataFusionError, ResolvedTableReference};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::sqlparser::ast::{ObjectType, Statement as SqlStatement};
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

#[derive(Debug)]
pub struct DropTablePlan {
    catalog: Arc<MySqlCatalogProvider>,
    if_exists: bool,
    table: ResolvedTableReference,
}

impl DropTablePlan {
    pub fn new_plan(
        catalog: Arc<MySqlCatalogProvider>,
        table: ResolvedTableReference,
        if_exists: bool,
    ) -> Arc<dyn ExecutionPlan> {
        let statement = SqlStatement::Drop {
            table: None,
            object_type: ObjectType::Table,
            if_exists,
            names: vec![unparser::object_name_from_resolved(&table)],
            purge: false,     /* Not supported on MySQL */
            cascade: false,   /* TODO, carry over? */
            restrict: false,  /* TODO, carry over? */
            temporary: false, /* TODO, carry over? */
        };

        let extra_impl = Self {
            catalog,
            table,
            if_exists,
        };

        Arc::new(DdlPlan::new(statement, Arc::new(extra_impl)))
    }
}

#[async_trait]
impl DdlPlanImpl for DropTablePlan {
    fn name(&self) -> &'static str {
        "drop_table"
    }

    async fn execute_extra(&self, context: Arc<TaskContext>) -> datafusion::common::Result<()> {
        let Some(schema) = self.catalog.schema(&self.table.schema) else {
            if !self.if_exists {
                return Err(DataFusionError::Execution(format!(
                    "Schema does not exist: {}",
                    &self.table.schema
                )));
            }
            return Ok(());
        };

        let result = schema.deregister_table(&self.table.table)?;

        context
            .get_store()
            .update_metadata(&self.table.schema, |meta| {
                meta.encrypted_tables.remove(&self.table.table.to_string());
                Ok(())
            })
            .await?;

        if result.is_none() && !self.if_exists {
            return Err(DataFusionError::Execution(format!(
                "Table does not exist: {}",
                self.table
            )));
        }

        Ok(())
    }
}
