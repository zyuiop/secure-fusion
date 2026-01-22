use crate::planning::physical::plans::mysql_scan_plan::MySqlScanPlan;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::sqlparser::ast::LockType;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Transforms the given node up, trying to locate a [MySqlScanPlan] node to add locking metadata to it
pub fn transform_select_with_locking(
    node: Arc<dyn ExecutionPlan>,
    locking: LockType,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let node = node.transform_up(|node| {
        let Some(plan) = node.as_ref().as_any().downcast_ref::<MySqlScanPlan>() else {
            return Ok(Transformed::no(node));
        };

        Ok(match plan.with_locking(locking) {
            None => Transformed::no(node),
            Some(node) => Transformed::yes(Arc::new(node)),
        })
    });

    Ok(node?.data)
}
