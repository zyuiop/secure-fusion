use datafusion::logical_expr::sqlparser::ast::{SelectItem, SetExpr};
use datafusion::sql::sqlparser::ast::{Expr, Ident, Query};

const fn needs_aliasing(expr: &Expr) -> bool {
    matches!(expr, Expr::Exists { .. } | Expr::Value(_))
}

/// Rewrites a plan to set the column names to what MySQL would define
pub fn rewrite_projection(query: &mut Query) {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return;
    };

    select
        .projection
        .iter_mut()
        .for_each(|select_item| match select_item {
            SelectItem::UnnamedExpr(ex) if needs_aliasing(ex) => {
                let alias = Ident::with_quote('"', ex.to_string());
                *select_item = SelectItem::ExprWithAlias {
                    expr: ex.clone(),
                    alias,
                }
            }
            _ => {}
        });
}
