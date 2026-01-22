use crate::planning::logical::custom_txcontrol::CustomTransactionControl;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

pub struct CustomTxControlPlanner;

#[async_trait]
impl ExtensionPlanner for CustomTxControlPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node): Option<&CustomTransactionControl> = node.as_any().downcast_ref() else {
            return Ok(None);
        };

        let plan = node.inner.to_plan();
        Ok(Some(plan))
    }
}
