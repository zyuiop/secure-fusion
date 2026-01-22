use crate::planning::logical::custom_sort_scan::PushedDownSort;
use crate::planning::physical::plans::mysql_scan_plan::MySqlScanPlan;
use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use std::sync::Arc;

pub struct CustomScanPlanner;

#[async_trait]
impl ExtensionPlanner for CustomScanPlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(custom_scan): Option<&PushedDownSort> = node.as_any().downcast_ref() else {
            return Ok(None);
        };

        assert_eq!(physical_inputs.len(), 1);

        let node = Arc::clone(&physical_inputs[0]);

        let node = node.transform_up(|node| {
            let Some(node) = node.as_ref().as_any().downcast_ref::<MySqlScanPlan>() else {
                return Ok(Transformed::no(node));
            };

            // Physical sorting expressions required for ordering property
            let physical_sort_expr = custom_scan
                .sort_expressions
                .iter()
                .map(|sort| {
                    let physical_expr = planner.create_physical_expr(
                        &sort.expr,
                        custom_scan.child.schema(),
                        session_state,
                    )?;

                    Ok(PhysicalSortExpr::new(
                        physical_expr,
                        SortOptions {
                            descending: !sort.asc,
                            nulls_first: sort.nulls_first,
                        },
                    ))
                })
                .collect::<datafusion::common::Result<_>>()?;

            let node = node.with_sort(custom_scan.sort_expressions.clone(), physical_sort_expr);

            Ok(Transformed::yes(Arc::new(node)))
        })?;

        if node.transformed {
            Ok(Some(node.data))
        } else {
            Ok(None)
        }
    }
}
