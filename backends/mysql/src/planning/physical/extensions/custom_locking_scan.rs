use crate::planning::logical::custom_locking_scan::CustomLockingScan;
use crate::planning::physical::locking::transform_select_with_locking;
use async_trait::async_trait;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

pub struct CustomLockingScanPlanner;

#[async_trait]
impl ExtensionPlanner for CustomLockingScanPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(lock_scan): Option<&CustomLockingScan> = node.as_any().downcast_ref() else {
            return Ok(None);
        };

        assert_eq!(physical_inputs.len(), 1);

        let node = Arc::clone(&physical_inputs[0]);
        let node = transform_select_with_locking(node, lock_scan.lock_type())?;
        Ok(Some(node))
    }
}
