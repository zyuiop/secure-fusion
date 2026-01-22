use datafusion::common::DFSchema;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::optimizer::AnalyzerRule;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct EliminateDummyTable;

impl AnalyzerRule for EliminateDummyTable {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let Transformed { data, .. } = plan.transform_up(|mut plan| {
            let LogicalPlan::TableScan(ref mut scan) = plan else {
                return Ok(Transformed::no(plan));
            };

            if scan.table_name.table() != "dual" {
                return Ok(Transformed::no(plan));
            }

            Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                EmptyRelation {
                    produce_one_row: true,
                    schema: Arc::new(DFSchema::empty()),
                },
            )))
        })?;

        Ok(data)
    }

    fn name(&self) -> &str {
        "eliminate_dummy_tables"
    }
}
