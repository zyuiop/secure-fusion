use crate::ast_expr_ext::AstExprExt;
use crate::filtering::indexable_filter;
use crate::filtering::indexable_filter::{
    ColumnOrTuple, ColumnWithCast, EqualityOperator, IndexableFilterExpr,
};
use crate::metadata::kw_search_func::{KwSearchArgs, KwSearchUdf, SearchMode};
use crypto::planning::physical::decrypt::DecryptUdf;
use datafusion::common::ScalarValue;
use datafusion::logical_expr;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::{Expr, Operator};
use log::warn;
use rustc_hash::FxHashSet;

pub type IndexableLogicalExpr<'a> = IndexableFilterExpr<&'a logical_expr::Expr>;

fn as_scalar_value_logical(expr: &Expr) -> Option<ScalarValue> {
    match expr {
        Expr::Literal(lit, _) => Some(lit.clone()),
        Expr::Alias(e) => as_scalar_value_logical(&e.expr),
        Expr::Cast(e) => {
            let v = as_scalar_value_logical(&e.expr)?;
            v.cast_to(&e.data_type).ok()
        }
        Expr::TryCast(e) => {
            let v = as_scalar_value_logical(&e.expr)?;
            v.cast_to(&e.data_type).ok()
        }
        // Complex expressions should already have been simplified
        _ => None,
    }
}

fn as_column_logical(expr: &Expr) -> Option<ColumnWithCast> {
    match DecryptUdf::eliminate_decrypt_in_expr(expr).unwrap() {
        Expr::Column(col) => Some(ColumnWithCast::column(col)),
        Expr::Alias(e) => as_column_logical(&e.expr),
        Expr::Cast(e) => as_column_logical(&e.expr).map(|v| v.cast(&e.data_type)),
        Expr::TryCast(e) => as_column_logical(&e.expr).map(|v| v.try_cast(&e.data_type)),
        _ => None,
    }
}

fn as_columns_logical(expr: &Expr) -> Option<ColumnOrTuple> {
    as_column_logical(expr).map(ColumnOrTuple::Column)
}

impl<'a> From<&'a logical_expr::Expr> for IndexableLogicalExpr<'a> {
    fn from(expr: &'a Expr) -> Self {
        match DecryptUdf::eliminate_decrypt_in_expr(expr).unwrap() {
            Expr::Alias(inner) => inner.expr.as_ref().into(),
            Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
            | Expr::IsUnknown(_)
            | Expr::Unnest(_)
            | Expr::Case(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::OuterReferenceColumn(_, _)
            | Expr::Wildcard { .. }
            | Expr::Exists(_)
            | Expr::WindowFunction(_)
            | Expr::AggregateFunction(_)
            | Expr::GroupingSet(_)
            | Expr::SetComparison(_)
            | Expr::Column(_)
            | Expr::Placeholder(_)
            | Expr::SimilarTo(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::Negative(_)
            | Expr::IsNotUnknown(_) => Self::Other(expr),

            expr @ Expr::Between(between) => {
                let Some(col) = as_column_logical(&between.expr) else {
                    return Self::Other(expr);
                };

                let Some(low) = as_scalar_value_logical(&between.low) else {
                    return Self::Other(expr);
                };

                let Some(high) = as_scalar_value_logical(&between.high) else {
                    return Self::Other(expr);
                };

                Self::Between(col, low, high)
            }

            expr @ Expr::BinaryExpr(bin) => {
                // One of the two arms may be a scalar value
                match bin.op {
                    Operator::And => Self::And(
                        Box::new(bin.left.as_ref().into()),
                        Box::new(bin.right.as_ref().into()),
                    ),
                    Operator::Or => Self::Or(
                        Box::new(bin.left.as_ref().into()),
                        Box::new(bin.right.as_ref().into()),
                    ),
                    other => {
                        let Ok(eq_op) = EqualityOperator::try_from(&other) else {
                            return Self::Other(expr);
                        };

                        let value_left = as_scalar_value_logical(&bin.left);
                        // Should we invert the operator?
                        let eq_op = if value_left.is_some() {
                            eq_op.inverse_order()
                        } else {
                            eq_op
                        };

                        let Some(value) = value_left.or(as_scalar_value_logical(&bin.right)) else {
                            return Self::Other(expr);
                        };
                        let Some(column) =
                            as_column_logical(&bin.left).or(as_column_logical(&bin.right))
                        else {
                            return Self::Other(expr);
                        };

                        Self::Eq(column, eq_op, value)
                    }
                }
            }

            Expr::Like(v) => {
                let Some(column) = as_column_logical(&v.expr) else {
                    return Self::Other(expr);
                };

                let Some(pattern) = as_scalar_value_logical(&v.pattern) else {
                    return Self::Other(expr);
                };

                let ScalarValue::Utf8(Some(pattern)) = pattern else {
                    return Self::Other(expr);
                };

                Self::KwSearchLike(column, pattern)
            }
            Expr::InList(in_list) => {
                let Some(column) = as_columns_logical(&in_list.expr) else {
                    return Self::Other(expr);
                };

                let Some(values) = in_list
                    .list
                    .iter()
                    .map(as_scalar_value_logical)
                    .collect::<Option<FxHashSet<_>>>()
                else {
                    return Self::Other(expr);
                };

                Self::InList(column, values)
            }
            Expr::ScalarFunction(sf) if sf.func.name() == KwSearchUdf::KW_SEARCH_UDF_NAME => {
                let KwSearchArgs {
                    search_columns,
                    search_string,
                    search_mode,
                } = match KwSearchUdf::split_args(&sf.args) {
                    Ok(v) => v,
                    Err(e) => {
                        warn!("invalid kw_search args: {e}");
                        return Self::Other(expr);
                    }
                };

                match search_mode {
                    SearchMode::Boolean => Self::KwMatch {
                        columns: search_columns.clone(),
                        search_string,
                    },
                }
            }
            Expr::Not(e) | Expr::IsFalse(e) | Expr::IsNotTrue(e) => {
                Self::Not(Box::new(e.as_ref().into()))
            }

            Expr::IsTrue(e) | Expr::IsNotFalse(e) => {
                // TODO: in practice, these are different semantics (re. null values), but let's for now ignore this
                e.as_ref().into()
            }

            Expr::Cast(inner) => inner.expr.as_ref().into(),
            Expr::TryCast(inner) => inner.expr.as_ref().into(),

            other => Self::Other(other),
        }
    }
}

impl<'a> From<IndexableLogicalExpr<'a>> for ast::Expr {
    fn from(value: IndexableLogicalExpr<'a>) -> Self {
        match value {
            IndexableLogicalExpr::And(l, r) => ast::Expr::from(*l).and(ast::Expr::from(*r)),
            IndexableLogicalExpr::Or(l, r) => ast::Expr::from(*l).or(ast::Expr::from(*r)),
            IndexableLogicalExpr::Not(e) => ast::Expr::from(*e).not(),
            IndexableLogicalExpr::Eq(c, op, v) => ast::Expr::from(c).binop(
                indexable_filter::unparser().scalar_to_sql(&v).unwrap(),
                op.into(),
            ),
            IndexableLogicalExpr::Between(c, v1, v2) => ast::Expr::Between {
                expr: Box::new(c.into()),
                negated: false,
                low: Box::new(indexable_filter::unparser().scalar_to_sql(&v1).unwrap()),
                high: Box::new(indexable_filter::unparser().scalar_to_sql(&v2).unwrap()),
            },
            IndexableLogicalExpr::InList(c, v) => {
                let unparser = indexable_filter::unparser();
                let list = v
                    .iter()
                    .map(|v| unparser.scalar_to_sql(v).unwrap())
                    .collect::<Vec<_>>();

                ast::Expr::InList {
                    expr: Box::new(ast::Expr::from(c)),
                    list,
                    negated: false,
                }
            }
            IndexableLogicalExpr::KwSearchLike(c, v) => ast::Expr::Like {
                negated: false,
                expr: Box::new(ast::Expr::from(c)),
                pattern: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(v).into())),
                any: false,
                escape_char: None,
            },
            IndexableLogicalExpr::KwMatch { .. } => unimplemented!("kw_match"),
            IndexableLogicalExpr::Other(e) => {
                let unparser = indexable_filter::unparser();
                unparser.expr_to_sql(e).unwrap()
            }
        }
    }
}
