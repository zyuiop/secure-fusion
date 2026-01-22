pub mod analyzers;
mod context;
pub(super) mod custom_ddl;
pub(super) mod custom_forward_statement;
pub(super) mod custom_locking_scan;
pub(super) mod custom_sort_scan;
pub(super) mod custom_txcontrol;
mod delete;
mod insert;
mod normalize_ident;
pub mod optimizers;
mod update;

pub use insert::DUPLICATE_VALUE_PFX;
pub use update::{CURRENT_VALUE_PREFIX, FILTER_PREFIX};

use crate::get_catalog::CatalogGetter;
use crate::metadata::kw_search_func::rewrite_kw_search;
use crate::planning::logical::custom_ddl::{CustomDdlLogicalPlan, DdlOperation};
use crate::planning::logical::custom_forward_statement::ForwardStatement;
use crate::planning::logical::custom_locking_scan::CustomLockingScan;
use crate::planning::logical::custom_txcontrol::TransactionControl;
use crate::planning::unparser;
use async_trait::async_trait;
use common::statement::ParsedStatement;
use common::{HandlerResult, LogicalPrePlanner, default_statement_to_plan};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{DFSchema, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::sqlparser::ast::{Expr, ObjectName, ObjectType, Statement, Value};
use datafusion::logical_expr::{EmptyRelation, Extension, LogicalPlan};
use datafusion::optimizer::OptimizerConfig;
use datafusion::sql::sqlparser::ast::{Ident, LockTable, Set, TransactionMode, VisitMut};
use std::sync::Arc;

pub struct MySqlLogicalPlanner;

impl MySqlLogicalPlanner {
    pub(super) fn make_empty_plan() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        })
    }
}

#[async_trait]
impl LogicalPrePlanner for MySqlLogicalPlanner {
    async fn statement_to_plan(
        &self,
        statement: ParsedStatement,
        session_state: &SessionState,
    ) -> HandlerResult<LogicalPlan> {
        // Handle DML statements
        match statement {
            ParsedStatement::Statement(mut statement) => {
                if session_state
                    .config_options()
                    .sql_parser
                    .enable_ident_normalization
                {
                    let _ = <Statement as VisitMut>::visit(
                        &mut statement,
                        &mut normalize_ident::Normalizer,
                    );
                }

                match statement {
                    Statement::CreateTable(ct) => {
                        let obj_name = unparser::table_name_to_ref(&ct.name);
                        let obj_name = obj_name.resolve(
                            &session_state.options().catalog.default_catalog,
                            &session_state.options().catalog.default_schema,
                        );

                        Ok(CustomDdlLogicalPlan::plan_for(DdlOperation::CreateTable(
                            obj_name,
                            Box::new(ct),
                        )))
                    }
                    Statement::AlterTable {
                        if_exists,
                        name,
                        operations,
                        ..
                    } => {
                        let table = unparser::table_name_to_ref(&name);
                        let table = table.resolve(
                            &session_state.options().catalog.default_catalog,
                            &session_state.options().catalog.default_schema,
                        );

                        Ok(CustomDdlLogicalPlan::plan_for(DdlOperation::AlterTable {
                            table,
                            operations,
                            if_exists,
                        }))
                    }
                    Statement::CreateDatabase {
                        db_name,
                        if_not_exists,
                        ..
                    } => Ok(CustomDdlLogicalPlan::plan_for(
                        DdlOperation::CreateDatabase {
                            db_name: db_name.0[0].as_ident().unwrap().value.clone(),
                            if_not_exists,
                        },
                    )),
                    Statement::LockTables { tables } => {
                        // Remap lock tables (TODO)
                        let tables = tables.into_iter().flat_map(
                            |LockTable {
                                 table,
                                 lock_type,
                                 alias,
                             }| {
                                let resolved_table = session_state
                                    .get_catalog()
                                    .mysql_schema(session_state.default_schema())
                                    .and_then(|schema| schema.mysql_table(&table.value));

                                match resolved_table {
                                    None => {
                                        // Weird, the table should exist, but forward as is...
                                        vec![LockTable {
                                            table,
                                            lock_type,
                                            alias,
                                        }]
                                    }
                                    Some(tbl) => {
                                        let mut output = tbl
                                            .linked_table_names()
                                            .into_iter()
                                            .map(|other_table| LockTable {
                                                table: Ident::new(other_table),
                                                lock_type: lock_type.clone(),
                                                alias: alias.clone(),
                                            })
                                            .collect::<Vec<_>>();

                                        output.push(LockTable {
                                            table,
                                            lock_type,
                                            alias,
                                        });
                                        output
                                    }
                                }
                            },
                        );
                        Ok(ForwardStatement::new_with_dml_schema(
                            Statement::LockTables {
                                tables: tables.collect(),
                            },
                        ))
                    }
                    s @ Statement::UnlockTables => Ok(ForwardStatement::new_with_dml_schema(s)),
                    Statement::Drop {
                        object_type,
                        if_exists,
                        mut names,
                        ..
                    } if object_type == ObjectType::Database => {
                        let name = names.remove(0); // Only one name taken into account

                        let db_name = name.0[0].as_ident().unwrap().value.clone();
                        Ok(CustomDdlLogicalPlan::plan_for(DdlOperation::DropDatabase {
                            db_name,
                            if_exists,
                        }))
                    }
                    Statement::ShowCreate { obj_type, obj_name } => {
                        let obj_name = unparser::table_name_to_ref(&obj_name);
                        let obj_name = obj_name.resolve(
                            &session_state.options().catalog.default_catalog,
                            &session_state.options().catalog.default_schema,
                        );

                        Ok(ForwardStatement::show_create(obj_type, obj_name))
                    }
                    st @ Statement::ShowStatus { .. } => Ok(ForwardStatement::show_status(st)),
                    Statement::Delete(delete) => self.plan_delete(delete, session_state).await,
                    upd @ Statement::Update { .. } => self.plan_update(upd, session_state).await,
                    Statement::Insert(ins) => self.plan_insert(ins, session_state).await,
                    Statement::Set(Set::SetTransaction { modes, .. }) => {
                        let mode = modes.first();

                        if let Some(TransactionMode::IsolationLevel(level)) = mode {
                            Ok(TransactionControl::isolation_level(*level)?.to_logical_node())
                        } else {
                            plan_err!("Invalid SET_TRANSACTION command")?
                        }
                    }
                    Statement::Set(Set::SingleAssignment {
                        variable: ObjectName(parts),
                        values,
                        ..
                    }) if !parts.is_empty()
                        && parts[0].as_ident().is_some()
                        && parts[0].as_ident().unwrap().value == "autocommit" =>
                    {
                        let value = &values[0];
                        if let Expr::Value(value) = value {
                            let value = match &value.value {
                                Value::Number(v, _) => v == "1",
                                Value::Boolean(v) => *v,
                                _ => plan_err!("cannot parse AUTOCOMMIT value")?,
                            };

                            Ok(TransactionControl::SetAutocommit(value).to_logical_node())
                        } else {
                            plan_err!("missing AUTOCOMMIT value")?
                        }
                    }
                    Statement::Query(ref mut query) if !query.locks.is_empty() => {
                        rewrite_kw_search(query.as_mut());

                        let lock_type = query.locks.first().cloned().unwrap().lock_type;
                        let base_statement =
                            default_statement_to_plan(statement, session_state).await?;

                        // TODO: tag the _FOR UPDATE_ part
                        let new_statement = base_statement.transform_up(|node| match node {
                            scan @ LogicalPlan::TableScan(_) => {
                                let new_plan = CustomLockingScan(scan, lock_type.try_into()?);
                                Ok(Transformed::yes(LogicalPlan::Extension(Extension {
                                    node: Arc::new(new_plan),
                                })))
                            }
                            other => Ok(Transformed::no(other)),
                        })?;

                        Ok(new_statement.data)
                    }
                    Statement::Query(ref mut query) => {
                        rewrite_kw_search(query.as_mut());
                        default_statement_to_plan(statement, session_state).await
                    }

                    /* Statement::Drop { .. } => {
                        todo!("mysql_backend: drop")
                    }
                    Statement::AlterTable { .. } => {
                        todo!("mysql_backend: alter table")
                    }
                    Statement::AlterIndex { .. } => {
                        todo!("mysql_backend: alter index")
                    }
                    Statement::CreateIndex(ci) => {
                        todo!("mysql_backend: create index")
                    }*/
                    // TODO: plan for other DML statements
                    // ACTUALLY, most of them are handled by the default planner already
                    statement => default_statement_to_plan(statement, session_state).await,
                }
            }
            s => unimplemented!("unknown statement kind {s:?}"),
        }
    }
}
