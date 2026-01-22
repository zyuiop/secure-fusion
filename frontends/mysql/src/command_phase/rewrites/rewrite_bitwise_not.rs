use datafusion::sql::sqlparser::ast::{Expr, Statement};
use datafusion::sql::sqlparser::ast::{
    Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, ObjectName,
    ObjectNamePart, UnaryOperator, VisitMut, VisitorMut,
};
use std::ops::ControlFlow;

struct UnsupportedOpsRewriter;

fn make_function(name: &str, arg: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(name.into())]),
        args: FunctionArguments::List(FunctionArgumentList {
            clauses: vec![],
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(arg))],
            duplicate_treatment: None,
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
    })
}

impl VisitorMut for UnsupportedOpsRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::UnaryOp {
                expr: param,
                op: UnaryOperator::BitwiseNot,
            } => {
                *expr = make_function("bitwise_not", *param.clone());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }
}
/// Rewrites a plan to set the column names to what MySQL would define
pub fn rewrite_unsupported_ops(query: &mut Statement) {
    let _ = query.visit(&mut UnsupportedOpsRewriter);
}
