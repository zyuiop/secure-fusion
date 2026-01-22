use datafusion::common::{DFSchemaRef, plan_err};
use datafusion::logical_expr::expr::Sort;
use datafusion::logical_expr::{Expr, LogicalPlan, SortExpr, UserDefinedLogicalNodeCore};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Represents a sort operation that will be pushed down to the Scan below
/// Because of DataFusion logical plan structure, we must create this "fake" logical node, which will
/// be merged with the Scan during physical planning.
/// The correct thing to do would be to create an alternative Scan node, but we would lose all default optimizations which assume LogicalNode::Scan
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct PushedDownSort {
    pub child: Arc<LogicalPlan>,
    pub sort_expressions: Vec<SortExpr>,
}

impl UserDefinedLogicalNodeCore for PushedDownSort {
    fn name(&self) -> &str {
        "custom_sort_scan"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.child]
    }

    fn schema(&self) -> &DFSchemaRef {
        self.child.schema()
    }

    fn expressions(&self) -> Vec<Expr> {
        self.sort_expressions
            .iter()
            .map(|expr| expr.expr.clone())
            .collect()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str("CustomSortScan: ")?;
        for ex in self.sort_expressions.iter() {
            write!(f, "{}", ex)?;
        }
        Ok(())
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        mut inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        if exprs.len() != self.sort_expressions.len() {
            plan_err!("Cannot change number of expressions in step!")?
        };

        if inputs.len() != 1 {
            plan_err!("Must have exactly one input!")?
        }

        Ok(Self {
            child: Arc::new(inputs.remove(0)),
            sort_expressions: self
                .sort_expressions
                .iter()
                .zip(exprs)
                .map(|(original, new_expr)| Sort {
                    expr: new_expr,
                    asc: original.asc,
                    nulls_first: original.nulls_first,
                })
                .collect(),
        })
    }

    fn supports_limit_pushdown(&self) -> bool {
        // We want limits to be pushed down to the scan when able
        true
    }
}
