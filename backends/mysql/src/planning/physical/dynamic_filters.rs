use crate::ast_expr_ext::AstExprExt;
use crate::filtering::resolver::ResolveIndexResult;
use crate::metadata::DynamicFilter;
use crate::providers::table_provider::MySqlTableProvider;
use async_trait::async_trait;
use common::profile;
use crypto::KeyManagerGetter;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::Expr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct DynamicFilterInterop(
    pub Arc<DynamicFilterPhysicalExpr>,
    pub Arc<MySqlTableProvider>,
);

#[async_trait]
impl DynamicFilter for DynamicFilterInterop {
    async fn execute_filter(&self, context: Arc<TaskContext>) -> datafusion::common::Result<Expr> {
        self.0.wait_complete().await;
        let current_arc = self.0.current()?;

        if let Some(parsed) = profile!("convert_filter_and_resolve", {
            let ResolveIndexResult {
                forwarded_static, ..
            } = self
                .1
                .index_resolver()
                .resolve_indices_for_physical_filters(
                    &context.get_long_term_keys_manager(),
                    &[current_arc.as_ref()],
                )?;
            forwarded_static.into_iter().reduce(|a, b| a.and(b))
        }) {
            Ok(parsed)
        } else {
            Ok(Expr::Value(ast::Value::Boolean(true).into()))
        }
    }
}
