use super::ddl_physical_plan::{DdlPlan, DdlPlanImpl};
use crate::providers::catalog_provider::MySqlCatalogProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::sqlparser::ast::{
    ObjectName, ObjectNamePart, Statement as SqlStatement,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::{Ident, ObjectType};
use std::sync::Arc;

#[derive(Debug)]
pub struct DropSchemaPlan {
    catalog: Arc<MySqlCatalogProvider>,
    if_exists: bool,
    name: String,
}

impl DropSchemaPlan {
    pub fn new_plan(
        catalog: Arc<MySqlCatalogProvider>,
        if_exists: bool,
        name: String,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Forward table creation instruction unmodified
        let statement = SqlStatement::Drop {
            if_exists,
            names: vec![ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
                name.clone(),
            ))])],
            object_type: ObjectType::Database,

            cascade: false,
            restrict: false,
            purge: false,
            temporary: false,
            table: None,
        };

        let extra_impl = Self {
            catalog,
            if_exists,
            name,
        };

        Ok(Arc::new(DdlPlan::new(statement, Arc::new(extra_impl))))
    }
}

#[async_trait::async_trait]
impl DdlPlanImpl for DropSchemaPlan {
    fn name(&self) -> &'static str {
        "drop_database"
    }

    async fn execute_extra(&self, _context: Arc<TaskContext>) -> datafusion::common::Result<()> {
        if self.catalog.mysql_schema(&self.name).is_none() {
            return if self.if_exists {
                Ok(())
            } else {
                Err(DataFusionError::Execution(
                    "Schema does not exist!".to_string(),
                ))
            };
        }

        self.catalog.deregister_schema(&self.name, false)?;

        Ok(())
    }
}
