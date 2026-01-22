use crate::planning::physical::decrypt::DECRYPT_PSEUDOFUNC_NAME;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::common::{DFSchemaRef, DataFusionError};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{
    Expr, LogicalPlan, Projection, TableProviderFilterPushDown, TableScan,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rustc_hash::FxHashSet;
use std::fmt::Debug;
use std::mem;
use std::sync::Arc;

#[derive(Debug)]
pub struct PushDownDecryptionFilterRule;

/// A small visitor that returns true if it finds the encryption pseudo-function
struct EncryptedExpressionsExtractor<'a> {
    input_schema: DFSchemaRef,
    input: &'a Vec<Expr>,
    child_visitor: ChildExpressionVisitor,
}

struct ChildExpressionVisitor {
    result: bool,
}

impl TreeNodeVisitor<'_> for ChildExpressionVisitor {
    type Node = Expr;

    fn f_up(&mut self, expr: &'_ Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        match expr {
            Expr::ScalarFunction(f) if f.name() == DECRYPT_PSEUDOFUNC_NAME => {
                self.result = true;
                Ok(TreeNodeRecursion::Stop)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

impl<'a> TreeNodeVisitor<'_> for EncryptedExpressionsExtractor<'a> {
    type Node = Expr;

    fn f_up(&mut self, expr: &'_ Self::Node) -> datafusion::common::Result<TreeNodeRecursion> {
        match expr {
            Expr::Column(column) => {
                let column = self.input_schema.index_of_column(&column)?;
                let input = &self.input[column];
                input.visit(&mut self.child_visitor)
            }
            _ => Ok(TreeNodeRecursion::Continue),
        }
    }
}

impl<'a> EncryptedExpressionsExtractor<'a> {
    fn new(input_schema: DFSchemaRef, input: &'a Vec<Expr>) -> Self {
        Self {
            input_schema,
            input,
            child_visitor: ChildExpressionVisitor { result: false },
        }
    }

    fn finish(&mut self) -> bool {
        mem::replace(&mut self.child_visitor.result, false)
    }

    fn visit_and_finish(&mut self, expr: &Expr) -> datafusion::common::Result<bool> {
        expr.visit(self)?;
        Ok(self.finish())
    }
}

struct ReplaceInputInPredicate<'a> {
    input_schema: DFSchemaRef,
    input: &'a Vec<Expr>,
}

impl<'a> TreeNodeRewriter for ReplaceInputInPredicate<'a> {
    type Node = Expr;

    fn f_up(&mut self, node: Self::Node) -> datafusion::common::Result<Transformed<Self::Node>> {
        match node {
            Expr::Column(column) => {
                let column = self.input_schema.index_of_column(&column)?;
                let input = &self.input[column];

                // Remove encryption function in input
                Transformed::yes(input.clone()).transform_data(|input| {
                    input.transform_up(|expr| match expr {
                        Expr::ScalarFunction(mut f) if f.name() == DECRYPT_PSEUDOFUNC_NAME => {
                            Ok(Transformed::yes(f.args.remove(0)))
                        }
                        // Remove aliases to simplify logic later on
                        Expr::Alias(alias) => Ok(Transformed::yes(*alias.expr)),
                        _ => Ok(Transformed::no(expr)),
                    })
                })
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

impl OptimizerRule for PushDownDecryptionFilterRule {
    fn name(&self) -> &str {
        "pushdown_decryption_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>, DataFusionError> {
        // We're looking for a simple Filter > Project w/ Decrypt > Scan
        let LogicalPlan::Filter(filter) = &plan else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::Projection(project) = filter.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };

        let LogicalPlan::TableScan(table_scan) = project.input.as_ref() else {
            return Ok(Transformed::no(plan));
        };

        let mut replacer =
            EncryptedExpressionsExtractor::new(Arc::clone(&project.schema), &project.expr);

        let filter_predicates = split_conjunction(&filter.predicate);
        let columns_with_encrypted_data = filter_predicates
            .iter()
            .cloned()
            .filter(|node| replacer.visit_and_finish(node).unwrap())
            .collect::<Vec<_>>();

        if columns_with_encrypted_data.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let filters_with_pushdown = table_scan
            .source
            .supports_filters_pushdown(columns_with_encrypted_data.as_slice())?;

        let mut replacer = ReplaceInputInPredicate {
            input_schema: Arc::clone(&project.schema),
            input: &project.expr,
        };

        let mut supported_filters = columns_with_encrypted_data
            .into_iter()
            .zip(filters_with_pushdown)
            .filter(|(_, res)| res != &TableProviderFilterPushDown::Unsupported)
            .map(|(pred, _)| {
                // Remove pseudo-function from passed down filters
                let pred = pred.clone();
                Ok(pred.rewrite(&mut replacer)?.data)
            })
            .collect::<Result<FxHashSet<_>, DataFusionError>>()?;

        if supported_filters.is_empty() {
            return Ok(Transformed::no(plan));
        }

        for filter in table_scan.filters.iter() {
            if !supported_filters.contains(filter) {
                // Small optimization: only clone if filter is not already there
                supported_filters.insert(filter.clone());
            }
        }

        let new_scan = LogicalPlan::TableScan(TableScan {
            filters: supported_filters.into_iter().collect(),
            source: table_scan.source.clone(),
            table_name: table_scan.table_name.clone(),
            projection: table_scan.projection.clone(),
            projected_schema: table_scan.projected_schema.clone(),
            fetch: table_scan.fetch.clone(),
        });

        let new_project = LogicalPlan::Projection(Projection::try_new_with_schema(
            project.expr.clone(),
            Arc::new(new_scan),
            project.schema.clone(),
        )?);

        let LogicalPlan::Filter(mut new_filter) = plan else {
            unreachable!()
        };
        new_filter.input = Arc::new(new_project);

        Ok(Transformed::yes(LogicalPlan::Filter(new_filter)))
    }
}
