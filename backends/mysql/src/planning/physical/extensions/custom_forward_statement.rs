use crate::planning::logical::custom_forward_statement::ForwardStatement;
use crate::planning::physical::plans::mysql_raw_exec_plan::MySqlRawExecPlan;
use crate::planning::physical::plans::mysql_raw_query_plan::MySqlRawQueryPlan;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

pub struct CustomForwardStatementPlanner;

#[async_trait]
impl ExtensionPlanner for CustomForwardStatementPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node): Option<&ForwardStatement> = node.as_any().downcast_ref() else {
            return Ok(None);
        };

        let plan: Arc<dyn ExecutionPlan> = if node.return_updated_count() {
            Arc::new(MySqlRawExecPlan::new(node.statement()))
        } else {
            Arc::new(MySqlRawQueryPlan::new(
                node.statement(),
                Arc::new(node.schema().as_arrow().clone()),
            ))
        };

        Ok(Some(plan))
    }
}
