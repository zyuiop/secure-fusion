use super::ddl_physical_plan::{DdlPlan, DdlPlanImpl};
use crate::providers::catalog_provider::MySqlCatalogProvider;
use crate::providers::schema_provider::MySqlSchemaProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::sqlparser::ast::{
    ObjectName, ObjectNamePart, Statement as SqlStatement,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::Ident;
use std::sync::Arc;

#[derive(Debug)]
pub struct CreateSchemaPlan {
    catalog: Arc<MySqlCatalogProvider>,
    if_not_exists: bool,
    name: String,
}

impl CreateSchemaPlan {
    pub fn new_plan(
        catalog: Arc<MySqlCatalogProvider>,
        if_not_exists: bool,
        name: String,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // Forward table creation instruction unmodified
        let statement = SqlStatement::CreateDatabase {
            db_name: ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name.clone()))]),
            if_not_exists,

            // Default values
            location: None,
            managed_location: None,
            or_replace: false,
            transient: false,
            clone: None,
            data_retention_time_in_days: None,
            max_data_extension_time_in_days: None,
            external_volume: None,
            catalog: None,
            replace_invalid_characters: None,
            default_ddl_collation: None,
            storage_serialization_policy: None,
            comment: None,
            catalog_sync: None,
            catalog_sync_namespace_mode: None,
            catalog_sync_namespace_flatten_delimiter: None,
            with_tags: None,
            with_contacts: None,
            default_charset: None,
            default_collation: None,
        };

        let extra_impl = Self {
            catalog,
            if_not_exists,
            name,
        };

        Ok(Arc::new(DdlPlan::new(statement, Arc::new(extra_impl))))
    }
}

#[async_trait::async_trait]
impl DdlPlanImpl for CreateSchemaPlan {
    fn name(&self) -> &'static str {
        "create_database"
    }

    async fn execute_extra(&self, _context: Arc<TaskContext>) -> datafusion::common::Result<()> {
        if self.catalog.mysql_schema(&self.name).is_some() {
            return if self.if_not_exists {
                Ok(())
            } else {
                Err(DataFusionError::Execution(
                    "Schema already exists!".to_string(),
                ))
            };
        }

        MySqlCatalogProvider::register_schema(
            self.catalog.as_ref(),
            self.name.clone(),
            MySqlSchemaProvider::new(Default::default()),
        );

        Ok(())
    }
}
