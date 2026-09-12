use crate::planning::physical::plans::mysql_scan_plan::MySqlScanPlan;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::sqlparser::ast::LockType;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub fn transform_select(
    node: Arc<dyn ExecutionPlan>,
    transform: impl Fn(&mut MySqlScanPlan) -> datafusion::common::error::Result<()>,
) -> datafusion::common::error::Result<Arc<dyn ExecutionPlan>> {
    let node = node.transform_up(|node| {
        let Some(plan) = node.as_any().downcast_ref::<MySqlScanPlan>() else {
            return Ok(Transformed::no(node));
        };

        let mut owned = plan.clone();
        transform(&mut owned)?;
        Ok(Transformed::yes(Arc::new(owned)))
    });

    Ok(node?.data)
}

/// Transforms the given node up, trying to locate a [MySqlScanPlan] node to add locking metadata to it
pub fn transform_select_with_locking(
    node: Arc<dyn ExecutionPlan>,
    locking: LockType,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    transform_select(node, |plan| {
        plan.set_lock_type(locking);
        Ok(())
    })
}
