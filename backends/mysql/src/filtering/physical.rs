use crate::ast_expr_ext::AstExprExt;
use crate::filtering::indexable_filter;
use crate::filtering::indexable_filter::{
    ColumnOrTuple, ColumnWithCast, EqualityOperator, IndexableFilterExpr, unparser,
};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::Expr;
use datafusion::physical_expr;
use datafusion::physical_expr::PhysicalExpr;
use rustc_hash::FxHashSet;

fn as_scalar_value_physical(expr: &dyn PhysicalExpr) -> Option<ScalarValue> {
    use datafusion::physical_expr::expressions;

    let any = expr.as_any();

    if let Some(expr) = any.downcast_ref::<expressions::Literal>() {
        Some(expr.value().clone())
    } else if let Some(expr) = any.downcast_ref::<expressions::CastExpr>() {
        let v = as_scalar_value_physical(expr.expr().as_ref())?;
        v.cast_to(expr.cast_type()).ok()
    } else if let Some(expr) = any.downcast_ref::<expressions::TryCastExpr>() {
        let v = as_scalar_value_physical(expr.expr().as_ref())?;
        v.cast_to(expr.cast_type()).ok()
    } else {
        None
    }
}

fn as_column_physical(expr: &dyn PhysicalExpr) -> Option<ColumnWithCast> {
    use datafusion::physical_expr::expressions;

    let any = expr.as_any();
    if let Some(expr) = any.downcast_ref::<expressions::Column>() {
        Some(ColumnWithCast::raw(expr.name()))
    } else if let Some(expr) = any.downcast_ref::<expressions::CastExpr>() {
        as_column_physical(expr.expr().as_ref()).map(|v| v.cast(expr.cast_type()))
    } else if let Some(expr) = any.downcast_ref::<expressions::TryCastExpr>() {
        as_column_physical(expr.expr().as_ref()).map(|v| v.try_cast(expr.cast_type()))
    } else {
        None
    }
}

fn as_columns_physical(expr: &dyn PhysicalExpr) -> Option<ColumnOrTuple> {
    if let Some(rec) = as_column_physical(expr) {
        Some(ColumnOrTuple::Column(rec))
    } else if let Some(sf) = expr
        .as_any()
        .downcast_ref::<physical_expr::ScalarFunctionExpr>()
        && sf.name() == "struct"
    {
        // Hash Filter will sometimes emit "tuples" of values, but will wrap them in a weird "struct" function
        // MySQL supports (a, b) IN ((1, 2), (3, 4), ...) so we just rewrite it this way
        let columns = sf
            .args()
            .iter()
            .map(|arg| Some(as_column_physical(arg.as_ref())?.into()))
            .collect::<Option<Vec<_>>>()?;

        Some(ColumnOrTuple::Tuple(columns))
    } else {
        None
    }
}

/// Converts a scalar value to an expression, with a special treatment for structs
/// This is used to handle the InList case with multiple columns
fn scalar_or_struct_to_expr(value: &ScalarValue) -> Option<Expr> {
    if let ScalarValue::Struct(struct_array) = value {
        let sub_expressions = struct_array
            .columns()
            .iter()
            .map(|column| {
                let value = ScalarValue::try_from_array(column.as_ref(), 0).ok()?;
                scalar_or_struct_to_expr(&value)
            })
            .collect::<Option<Vec<Expr>>>()?;

        Some(Expr::Tuple(sub_expressions))
    } else {
        unparser().scalar_to_sql(value).ok()
    }
}

impl<'a> From<&'a dyn PhysicalExpr> for IndexablePhysicalExpr<'a> {
    fn from(expr: &'a dyn PhysicalExpr) -> Self {
        use crypto::planning::physical::decrypt::DecryptUdf;
        use datafusion::logical_expr::Operator;
        use datafusion::physical_expr::{ScalarFunctionExpr, expressions};

        let any = expr.as_any();
        if let Some(binary) = any.downcast_ref::<expressions::BinaryExpr>() {
            let left = binary.left().as_ref();
            let right = binary.right().as_ref();

            match binary.op() {
                Operator::And => Self::And(Box::new(left.into()), Box::new(right.into())),
                Operator::Or => Self::Or(Box::new(left.into()), Box::new(right.into())),
                other => {
                    let Ok(eq_op) = EqualityOperator::try_from(other) else {
                        return Self::Other(expr);
                    };

                    let value_left = as_scalar_value_physical(left);
                    // Should we invert the operator?
                    let eq_op = if value_left.is_some() {
                        eq_op.inverse_order()
                    } else {
                        eq_op
                    };

                    let Some(value) = value_left.or(as_scalar_value_physical(right)) else {
                        return Self::Other(expr);
                    };
                    let Some(column) = as_column_physical(left).or(as_column_physical(right))
                    else {
                        return Self::Other(expr);
                    };

                    Self::Eq(column, eq_op, value)
                }
            }
        } else if let Some(like) = any.downcast_ref::<expressions::LikeExpr>() {
            let Some(column) = as_column_physical(like.expr().as_ref()) else {
                return Self::Other(expr);
            };

            let Some(pattern) = as_scalar_value_physical(like.pattern().as_ref()) else {
                return Self::Other(expr);
            };

            let ScalarValue::Utf8(Some(pattern)) = pattern else {
                return Self::Other(expr);
            };

            Self::KwSearchLike(column, pattern)
        } else if let Some(inlist) = any.downcast_ref::<expressions::InListExpr>() {
            let Some(column) = as_columns_physical(inlist.expr().as_ref()) else {
                return Self::Other(expr);
            };

            let Some(values) = inlist
                .list()
                .iter()
                .map(|v| as_scalar_value_physical(v.as_ref()))
                .collect::<Option<FxHashSet<_>>>()
            else {
                return Self::Other(expr);
            };

            Self::InList(column, values)
        } else if let Some(inner) = any.downcast_ref::<expressions::NotExpr>() {
            Self::Not(Box::new(inner.arg().as_ref().into()))
        } else if let Some(sf) = any.downcast_ref::<ScalarFunctionExpr>() {
            // TODO: KW_SEARCH? maybe?
            if sf.fun().name() == DecryptUdf::DECRYPT_UDF_NAME {
                // Strip decryption function as it may prevent filters from being detected
                let arg = sf
                    .args()
                    .get(0)
                    .expect("encountered decryption function with no argument");
                arg.as_ref().into()
            } else {
                Self::Other(sf)
            }
        } else if let Some(inner) = any.downcast_ref::<expressions::CastExpr>() {
            inner.expr().as_ref().into()
        } else if let Some(inner) = any.downcast_ref::<expressions::TryCastExpr>() {
            inner.expr().as_ref().into()
        } else {
            Self::Other(expr)
        }
    }
}

impl<'a> TryFrom<IndexablePhysicalExpr<'a>> for ast::Expr {
    type Error = &'a dyn PhysicalExpr;

    fn try_from(value: IndexablePhysicalExpr<'a>) -> Result<Self, Self::Error> {
        match value {
            IndexablePhysicalExpr::And(l, r) => {
                Ok(ast::Expr::try_from(*l)?.and(ast::Expr::try_from(*r)?))
            }
            IndexablePhysicalExpr::Or(l, r) => {
                Ok(ast::Expr::try_from(*l)?.or(ast::Expr::try_from(*r)?))
            }
            IndexablePhysicalExpr::Not(e) => Ok(ast::Expr::try_from(*e)?.not()),
            IndexablePhysicalExpr::Eq(c, op, v) => Ok(ast::Expr::from(c).binop(
                indexable_filter::unparser().scalar_to_sql(&v).unwrap(),
                op.into(),
            )),
            IndexableFilterExpr::Between(c, low, high) => Ok(ast::Expr::Between {
                expr: Box::new(c.into()),
                negated: false,
                low: Box::new(indexable_filter::unparser().scalar_to_sql(&low).unwrap()),
                high: Box::new(indexable_filter::unparser().scalar_to_sql(&high).unwrap()),
            }),
            IndexablePhysicalExpr::InList(c, v) => {
                let list = v
                    .iter()
                    .map(|v| scalar_or_struct_to_expr(v).unwrap())
                    .collect::<Vec<_>>();

                Ok(ast::Expr::InList {
                    expr: Box::new(ast::Expr::from(c)),
                    list,
                    negated: false,
                })
            }
            IndexablePhysicalExpr::KwSearchLike(c, v) => Ok(ast::Expr::Like {
                negated: false,
                expr: Box::new(ast::Expr::from(c)),
                pattern: Box::new(ast::Expr::Value(ast::Value::SingleQuotedString(v).into())),
                any: false,
                escape_char: None,
            }),
            IndexablePhysicalExpr::KwMatch { .. } => unimplemented!("kw_match"),
            IndexablePhysicalExpr::Other(e) => Err(e),
        }
    }
}

pub type IndexablePhysicalExpr<'a> = IndexableFilterExpr<&'a dyn PhysicalExpr>;
