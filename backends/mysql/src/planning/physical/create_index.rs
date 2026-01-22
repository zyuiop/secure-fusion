use crate::metadata::{ColumnName, EncryptedIndexConfigurationVariant};
use crate::planning::physical::MySqlPhysicalPlanner;
use crate::planning::physical::plans::noop::NoOpDdlPlan;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::CreateIndex;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::Expr;
use log::warn;
use std::sync::Arc;

impl MySqlPhysicalPlanner {
    fn extract_column_names(ci: &CreateIndex) -> datafusion::common::Result<Vec<ColumnName>> {
        ci.columns
            .iter()
            .map(|expr| {
                match expr.expr {
                    Expr::Column(ref ident) => Ok(ident.name.clone()),
                    ref other => {
                        // TODO: for some expressions, it actually may make sense to forward to the index!
                        // This would trivially enable case insensitive indices, prefix indices, concat indices, ...
                        plan_err!("Unsupported index expression: {:?}", other)
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }

    pub(super) async fn plan_create_index(
        &self,
        session_state: &SessionState,
        ci: &CreateIndex,
        catalog: Arc<MySqlCatalogProvider>,
        _next: &dyn PhysicalPlanner,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // 1. Is the index an encrypted one?
        // We abuse the USING clause to avoid having to modify the sql parser too much
        let Some(using) = &ci.using else {
            warn!("ignored CREATE INDEX statement with no USING clause (should be forwarded)");
            return Ok(NoOpDdlPlan::new());
            // todo!("forward create index as is (check if column is encrypted first?)");
        };

        let having = using.to_lowercase();
        if having == "btree" || having == "hash" {
            // MySQL basic
            warn!("ignored CREATE INDEX statement with basic USING clause (should be forwarded)");
            return Ok(NoOpDdlPlan::new());
            // todo!("forward create index as is (check if column is encrypted first?)");
        }

        let resolved_table_name = ci.table.clone().resolve(
            "__ignored",
            &session_state
                .config_options()
                .catalog
                .default_schema
                .clone(),
        );

        let table_name = ci.table.table();
        let table = catalog
            .mysql_schema(&resolved_table_name.schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("No such schema {}", resolved_table_name.schema))
            })?
            .mysql_table(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("No such table {}", table_name)))?;

        let columns = Self::extract_column_names(ci)?;
        let index_name = ci
            .name
            .clone()
            .unwrap_or_else(|| format!("{table_name}_{having}_{}", columns.join("-")));

        let index_variant = EncryptedIndexConfigurationVariant::parse_from_sql(
            index_name,
            columns,
            table.clone(),
            &having,
        )?;
        let plan = index_variant
            .build(table_name, table.get_indexable_column_type().clone())
            .create_index_plan(resolved_table_name, session_state)
            .await?;
        Ok(plan)
    }
}
