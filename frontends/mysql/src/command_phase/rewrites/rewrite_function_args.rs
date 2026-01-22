use datafusion::sql::sqlparser::ast::{
    DictionaryField, Expr, FunctionArg, FunctionArgExpr, FunctionArgumentClause, FunctionArguments,
    Ident, Statement, ValueWithSpan, VisitMut, VisitorMut,
};
use std::mem;
use std::ops::ControlFlow;

struct FunctionArgsRewriter;

impl VisitorMut for FunctionArgsRewriter {
    type Break = ();

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };

        let FunctionArguments::List(argslist) = &mut function.args else {
            return ControlFlow::Continue(());
        };

        if argslist.clauses.is_empty() {
            // Shortcut
            return ControlFlow::Continue(());
        }

        let clauses = mem::take(&mut argslist.clauses);
        for clause in clauses.into_iter() {
            match clause {
                FunctionArgumentClause::Separator(sep) => {
                    argslist
                        .args
                        .push(FunctionArg::Unnamed(FunctionArgExpr::Expr(
                            Expr::Dictionary(vec![DictionaryField {
                                key: Ident::new("separator"),
                                value: Box::new(Expr::Value(ValueWithSpan::from(sep))),
                            }]), /* Expr::Value(
                                     ValueWithSpan::from(
                                         Value::DollarQuotedString(
                                             DollarQuotedString {
                                                 tag: Some("separator".to_string()),
                                                 value: sep.to_string()
                                             }
                                         )
                                     )
                                 )*/
                        )));
                }
                _ => {
                    argslist.clauses.push(clause);
                }
            }
        }

        ControlFlow::Continue(())
    }
}

/// Rewrites a plan to set the column names to what MySQL would define
pub fn rewrite_function_args(query: &mut Statement) {
    let _ = query.visit(&mut FunctionArgsRewriter);
}
