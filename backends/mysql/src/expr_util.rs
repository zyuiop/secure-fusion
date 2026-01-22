use datafusion::sql::sqlparser::ast::{Expr, Visit, Visitor};
use rustc_hash::FxHashSet;
use std::ops::ControlFlow;

struct ColumnsVisitor(FxHashSet<String>);

impl Visitor for ColumnsVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(i) => {
                self.0.insert(i.value.clone());
            }
            Expr::CompoundIdentifier(i) => {
                self.0.insert(i.last().unwrap().value.clone());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}

pub fn identifiers_in_expr(expr: &Expr) -> FxHashSet<String> {
    let mut visitor = ColumnsVisitor(FxHashSet::default());
    let _ = expr.visit(&mut visitor);
    visitor.0
}
