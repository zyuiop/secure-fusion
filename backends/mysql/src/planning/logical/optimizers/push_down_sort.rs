use crate::get_catalog::TableGetter;
use crate::planning::logical::custom_sort_scan::PushedDownSort;
use crate::providers::table_provider::MySqlTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::common::DataFusionError;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{Extension, LogicalPlan, Sort, TableSource};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::prelude::Expr;
use log::warn;
use rustc_hash::FxHashSet;
use std::sync::Arc;

#[derive(Debug)]
#[allow(dead_code)]
pub struct PushDownSort;

impl PushDownSort {
    fn extract_scan_source(
        sort: &Sort,
    ) -> datafusion::common::Result<Option<Arc<dyn TableSource>>> {
        let mut scan_source = None;

        sort.input.apply(|node| {
            // TODO: this logic will NOT work for JOINS! We must be smarter
            match node {
                LogicalPlan::Join(_) => {
                    warn!("push_down_sort encountered a join and stopped");
                    scan_source = None;
                    Ok(TreeNodeRecursion::Stop)
                }
                LogicalPlan::Extension(ext)
                    if ext.node.as_any().downcast_ref::<PushDownSort>().is_some() =>
                {
                    // Because `apply` is Top-Down, we will always encounter a pushdown before a
                    // scan, if it is present. Therefore, if we stop, scan_source and scan_schema
                    // should still be empty
                    scan_source = None;
                    Ok(TreeNodeRecursion::Stop)
                }
                LogicalPlan::TableScan(scan) => {
                    let _ = scan_source.insert(scan.source.clone());
                    Ok(TreeNodeRecursion::Stop)
                }
                _ => Ok(TreeNodeRecursion::Continue),
            }
        })?;

        let Some(scan_source) = scan_source else {
            return Ok(None);
        };

        Ok(Some(scan_source))
    }

    fn can_forward_expr(
        expr: &Expr,
        column_names: &FxHashSet<String>,
        scan_table: &MySqlTableProvider,
    ) -> bool {
        match expr {
            Expr::Column(c) => {
                if let Some(ref relation) = c.relation {
                    if relation.table() != scan_table.table_reference.table() {
                        // Different table
                        return false;
                    }
                }

                if !column_names.contains(&c.name) {
                    false
                } else if scan_table.is_encrypted(&c.name) {
                    false
                } else {
                    true
                }
            }
            Expr::Alias(alias) => Self::can_forward_expr(&alias.expr, column_names, scan_table),
            Expr::ScalarVariable(_, _) => true, // makes no sense
            Expr::Literal(_, _) => true,        // makes no sense either
            Expr::BinaryExpr(bin_expr) => {
                Self::can_forward_expr(&bin_expr.left, column_names, scan_table)
                    && Self::can_forward_expr(&bin_expr.right, column_names, scan_table)
            }
            Expr::Like(like) | Expr::SimilarTo(like) => {
                Self::can_forward_expr(&like.expr, column_names, scan_table)
                    && Self::can_forward_expr(&like.pattern, column_names, scan_table)
            }
            Expr::IsNotNull(expr)
            | Expr::IsNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsUnknown(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsNotFalse(expr)
            | Expr::IsNotUnknown(expr)
            | Expr::Negative(expr)
            | Expr::Not(expr) => Self::can_forward_expr(expr, column_names, scan_table),
            Expr::Between(btw) => {
                Self::can_forward_expr(&btw.expr, column_names, scan_table)
                    && Self::can_forward_expr(&btw.low, column_names, scan_table)
                    && Self::can_forward_expr(&btw.high, column_names, scan_table)
            }
            Expr::Case(case) => {
                let expr = case
                    .expr
                    .as_ref()
                    .map(|v| Self::can_forward_expr(&v, column_names, scan_table))
                    .unwrap_or(true);
                let elze = case
                    .else_expr
                    .as_ref()
                    .map(|v| Self::can_forward_expr(&v, column_names, scan_table))
                    .unwrap_or(true);

                expr && elze
                    && case.when_then_expr.iter().all(|(left, right)| {
                        Self::can_forward_expr(&left, column_names, scan_table)
                            && Self::can_forward_expr(&right, column_names, scan_table)
                    })
            }
            Expr::Cast(cast) => Self::can_forward_expr(&cast.expr, column_names, scan_table),
            Expr::TryCast(cast) => Self::can_forward_expr(&cast.expr, column_names, scan_table),
            Expr::ScalarFunction(_) => false, // we may want to parse this one but it's more complicated
            Expr::AggregateFunction(_) | Expr::WindowFunction(_) => false, // This NEVER makes sense right?
            Expr::InList(_) => false,
            Expr::Exists(_) => false,
            Expr::InSubquery(_) => false,
            Expr::ScalarSubquery(_) => false,
            Expr::Wildcard { .. } => false,
            Expr::GroupingSet(_) => false,
            Expr::Placeholder(_) => false,
            Expr::OuterReferenceColumn(_, _) => false,
            Expr::Unnest(_) => false,
        }
    }
}

impl OptimizerRule for PushDownSort {
    fn name(&self) -> &str {
        "PushDownSort"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>, DataFusionError> {
        let LogicalPlan::Sort(ref sort) = plan else {
            return Ok(Transformed::no(plan));
        };

        let Some(scan_source) = Self::extract_scan_source(sort)? else {
            return Ok(Transformed::no(plan));
        };

        let Some(scan_source) = scan_source.as_mysql_opt() else {
            return Ok(Transformed::no(plan));
        };

        let column_names = scan_source
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let can_push: Vec<_> = sort
            .expr
            .iter()
            .filter(|expr| Self::can_forward_expr(&expr.expr, &column_names, scan_source))
            .cloned()
            .collect();

        if can_push.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let sort_child = PushedDownSort {
            child: sort.input.clone(),
            sort_expressions: can_push,
        };

        let new_sort = LogicalPlan::Sort(Sort {
            input: Arc::new(LogicalPlan::Extension(Extension {
                node: Arc::new(sort_child),
            })),
            fetch: sort.fetch.clone(),
            expr: sort.expr.clone(),
        });

        Ok(Transformed::yes(new_sort))
    }
}
