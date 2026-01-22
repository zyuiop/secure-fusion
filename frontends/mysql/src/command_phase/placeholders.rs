use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{Value, VisitorMut};
use std::ops::ControlFlow;

const POSITIONAL_PLACEHOLDER_PREFIX: &str = "$";

pub struct ParametersToDatafusionVisitor(usize);

impl ParametersToDatafusionVisitor {
    pub fn new() -> Self {
        ParametersToDatafusionVisitor(0)
    }
}

impl VisitorMut for ParametersToDatafusionVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &mut ast::Expr) -> ControlFlow<Self::Break> {
        match expr {
            ast::Expr::Value(inner) => {
                match &mut inner.value {
                    Value::Placeholder(v) => {
                        self.0 += 1;
                        *v = format!("{POSITIONAL_PLACEHOLDER_PREFIX}{}", self.0);
                    }
                    _ => {}
                }
                ControlFlow::Continue(())
            }
            _ => ControlFlow::Continue(()),
        }
    }
}
