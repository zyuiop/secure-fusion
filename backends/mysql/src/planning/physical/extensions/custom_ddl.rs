use crate::backend_config::GetConfig;
use crate::get_catalog::CatalogGetter;
use crate::planning::logical::custom_ddl::{CustomDdlLogicalPlan, DdlOperation};
use crate::planning::physical::plans::alter_table_plan::AlterTablePlan;
use crate::planning::physical::plans::create_schema_plan::CreateSchemaPlan;
use crate::planning::physical::plans::create_table_plan::CreateTablePlan;
use crate::planning::physical::plans::drop_schema_plan::DropSchemaPlan;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

pub struct CustomDdlExtensionPlanner;

impl CustomDdlExtensionPlanner {
    async fn plan_node(
        &self,
        node: &DdlOperation,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let catalog = session_state.get_catalog();

        match node {
            DdlOperation::CreateTable(reference, ct) => {
                let ct = ct.clone();
                let reference = reference.clone();

                CreateTablePlan::new_plan(
                    catalog,
                    reference,
                    ct,
                    session_state.get_backend_config().as_ref(),
                )
            }
            DdlOperation::AlterTable {
                operations,
                table,
                if_exists,
            } => AlterTablePlan::new_plan(
                catalog,
                table.clone(),
                *if_exists,
                operations,
                session_state.get_backend_config().as_ref(),
            ),
            DdlOperation::CreateDatabase {
                if_not_exists,
                db_name,
            } => CreateSchemaPlan::new_plan(catalog, *if_not_exists, db_name.clone()),
            DdlOperation::DropDatabase { db_name, if_exists } => {
                DropSchemaPlan::new_plan(catalog, *if_exists, db_name.clone())
            }
        }
    }
}

#[async_trait]
impl ExtensionPlanner for CustomDdlExtensionPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node): Option<&CustomDdlLogicalPlan> = node.as_any().downcast_ref() else {
            return Ok(None);
        };

        let result = self.plan_node(&node.operation, session_state).await?;

        Ok(Some(result))
    }
}
