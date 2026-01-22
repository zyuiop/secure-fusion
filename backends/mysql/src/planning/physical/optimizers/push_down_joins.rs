use crate::planning::physical::plans::mysql_scan_plan::MySqlScanPlan;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use log::warn;
use std::sync::Arc;

#[derive(Debug)]
pub struct PushDownJoins;

fn extract_mysql_plan(plan: &dyn ExecutionPlan) -> Option<&MySqlScanPlan> {
    plan.as_any().downcast_ref::<MySqlScanPlan>().or_else(|| {
        let coal = plan.as_any().downcast_ref::<CoalesceBatchesExec>()?;
        coal.input().as_any().downcast_ref::<MySqlScanPlan>()
    })
}

impl PushDownJoins {
    fn transform_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(join) = plan.as_any().downcast_ref::<HashJoinExec>() else {
            return Ok(Transformed::no(plan));
        };

        if join.filter.is_some() {
            warn!("join with filter cannot be optimized");

            let displayable = DisplayableExecutionPlan::new(plan.as_ref());
            warn!("plan: {}", displayable.one_line());

            return Ok(Transformed::no(plan));
        }

        let mode = join.join_type.clone();
        if !MySqlScanPlan::supports_join_mode(&mode) {
            warn!("Unsupported join mode {mode} cannot be optimized");
            return Ok(Transformed::no(plan));
        }

        // We want both children to be mysql scan plans
        let Some(left) = extract_mysql_plan(join.left.as_ref()) else {
            return Ok(Transformed::no(plan));
        };
        let Some(right) = extract_mysql_plan(join.right.as_ref()) else {
            return Ok(Transformed::no(plan));
        };

        let Some(equijoin_where) = join
            .on
            .iter()
            .map(|(left, right)| {
                let left = left.as_any().downcast_ref::<Column>().ok_or(())?;
                let right = right.as_any().downcast_ref::<Column>().ok_or(())?;

                Ok((left.name().to_string(), right.name().to_string()))
            })
            .collect::<Result<Vec<(String, String)>, ()>>()
            .ok()
        else {
            warn!(
                "Equijoin could not be transformed because left or right column is not an expression"
            );
            let displayable = DisplayableExecutionPlan::new(plan.as_ref());
            warn!("plan: {}", displayable.one_line());

            return Ok(Transformed::no(plan));
        };

        let new_plan = left.join(
            mode,
            right,
            equijoin_where.into_iter(),
            &join.projection,
            join.schema(),
        )?;
        let new_plan: Arc<dyn ExecutionPlan> = Arc::new(new_plan);

        Ok(Transformed::yes(new_plan))
    }
}

impl PhysicalOptimizerRule for PushDownJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_up(|plan| self.transform_plan(plan))?;
        Ok(result.data)
    }

    fn name(&self) -> &str {
        "push_down_join"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
