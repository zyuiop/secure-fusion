mod inverted_index;

pub use inverted_index::inverted_index::InvertedIndexConfig;

use datafusion::common::{Column, ScalarValue, TableReference};
use datafusion::logical_expr::Expr;

pub fn expr_to_column<'a>(table_ref: &TableReference, expr: &'a Expr) -> Option<&'a Column> {
    match expr {
        Expr::Alias(e) => expr_to_column(table_ref, e.expr.as_ref()),
        Expr::Column(c)
            if c.relation.is_none()
                || c.relation
                    .as_ref()
                    .is_some_and(|tr| tr.table() == table_ref.table()) =>
        {
            Some(c)
        }
        Expr::Cast(e) => expr_to_column(table_ref, e.expr.as_ref()),
        Expr::TryCast(e) => expr_to_column(table_ref, e.expr.as_ref()),
        _ => None,
    }
}

pub fn expr_to_value(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(sv, _) => Some(sv),
        Expr::Alias(e) => expr_to_value(e.expr.as_ref()),
        Expr::Cast(e) => expr_to_value(e.expr.as_ref()),
        Expr::TryCast(e) => expr_to_value(e.expr.as_ref()),
        _ => None,
    }
}
