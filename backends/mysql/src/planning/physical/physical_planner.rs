use crate::get_catalog::{CatalogGetter, TableGetter};
use crate::planning::physical::extensions;
use crate::planning::physical::plans::drop_table_plan::DropTablePlan;
use crate::planning::physical::plans::transaction_control_plan::TransactionControl;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use async_trait::async_trait;
use datafusion::common::exec_err;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::{
    DdlStatement, DmlStatement, LogicalPlan, Statement, TransactionConclusion, WriteOp,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};
use log::warn;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct MySqlPhysicalPlanner {
    base: DefaultPhysicalPlanner,
}

impl Debug for MySqlPhysicalPlanner {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("MySqlPhysicalPlanner")
    }
}

impl Default for MySqlPhysicalPlanner {
    fn default() -> Self {
        let extensions = extensions::get_extensions();
        Self {
            base: DefaultPhysicalPlanner::with_extension_planners(extensions),
        }
    }
}

#[async_trait]
impl QueryPlanner for MySqlPhysicalPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let base_plan = self
            .do_create_physical_plan(logical_plan, session_state)
            .await?;

        // replace_decryptions_in_plan(base_plan, &session_state)
        Ok(base_plan)
    }
}

impl MySqlPhysicalPlanner {
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all))]
    async fn do_create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        match logical_plan {
            // LogicalPlan::Dml(_) => {}
            LogicalPlan::Ddl(ddl_statement) => {
                let catalog = session_state.get_catalog();
                self.handle_ddl(session_state, ddl_statement, catalog, &self.base)
                    .await
            }
            LogicalPlan::Dml(DmlStatement {
                target,
                op: WriteOp::Delete,
                input,
                ..
            }) => {
                if let Some(mysql_table) = target.as_mysql_opt() {
                    let input = self.base.create_physical_plan(input, session_state).await?;
                    mysql_table.delete_from(session_state, input).await
                } else {
                    exec_err!("Table source can't be downcasted to MySqlTableProvider")
                }
            }
            LogicalPlan::Dml(DmlStatement {
                target,
                op: WriteOp::Update,
                input,
                ..
            }) => {
                if let Some(mysql_table) = target.as_mysql_opt() {
                    let input = self.base.create_physical_plan(input, session_state).await?;
                    mysql_table.update(session_state, input).await
                } else {
                    exec_err!("Table source can't be downcasted to MySqlTableProvider")
                }
            }
            lp @ LogicalPlan::Statement(stmt) => {
                match stmt {
                    // TODO: determine where the SetTransaction Isolation Level comes
                    Statement::TransactionStart(_) => Ok(TransactionControl::Start.to_plan()),
                    Statement::TransactionEnd(tx_end) => match tx_end.conclusion {
                        TransactionConclusion::Commit => Ok(TransactionControl::Commit.to_plan()),
                        TransactionConclusion::Rollback => {
                            Ok(TransactionControl::Rollback.to_plan())
                        }
                    },
                    Statement::SetVariable(set_var) if set_var.variable == "autocommit" => {
                        Ok(TransactionControl::SetAutocommit(set_var.value == "1").to_plan())
                    }
                    _ => self.base.create_physical_plan(lp, session_state).await, // Statement::SetVariable(set_var) if set_var.variable == "autocommit" => {}
                }
            }
            other => self.base.create_physical_plan(other, session_state).await,
        }
    }

    async fn handle_ddl(
        &self,
        session_state: &SessionState,
        ddl_statement: &DdlStatement,
        catalog: Arc<MySqlCatalogProvider>,
        next: &dyn PhysicalPlanner,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        match ddl_statement {
            DdlStatement::DropTable(dt) => {
                let reference = dt.name.clone().resolve(
                    "__ignored",
                    &session_state
                        .config_options()
                        .catalog
                        .default_schema
                        .clone(),
                );
                Ok(DropTablePlan::new_plan(catalog, reference, dt.if_exists))
            }
            DdlStatement::CreateIndex(ci) => {
                self.plan_create_index(session_state, ci, catalog, next)
                    .await
            }
            other => {
                warn!("DDL statement handled at physical planning: {other:?}");
                todo!("ddl unimplemented")
            }
        }
    }
}
