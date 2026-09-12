use crate::metadata::{ColumnName, EncryptedIndexConfigurationVariant};
use crate::planning::physical::MySqlPhysicalPlanner;
use crate::planning::physical::plans::mysql_raw_exec_plan::MySqlRawExecPlan;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use datafusion::common::{ResolvedTableReference, plan_datafusion_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::CreateIndex;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::PhysicalPlanner;
use datafusion::prelude::Expr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{
    Ident, IndexColumn, IndexType, ObjectName, OrderByExpr, OrderByOptions,
};
use datafusion::sql::unparser::expr_to_sql;
use futures_util::TryStreamExt;
use std::sync::Arc;

impl MySqlPhysicalPlanner {
    fn extract_column_names(ci: &CreateIndex) -> datafusion::common::Result<Vec<ColumnName>> {
        ci.columns
            .iter()
            .map(|expr| {
                match expr.expr {
                    Expr::Column(ref ident) => Ok(ident.name.clone()),
                    ref other => {
                        // TODO: for some expressions, it actually may make sense to forward to the index!
                        // This would trivially enable case insensitive indices, prefix indices, concat indices, ...
                        plan_err!("Unsupported index expression: {:?}", other)
                    }
                }
            })
            .collect::<Result<Vec<_>, _>>()
    }

    pub(super) async fn plan_create_index(
        &self,
        session_state: &SessionState,
        ci: &CreateIndex,
        catalog: Arc<MySqlCatalogProvider>,
        _next: &dyn PhysicalPlanner,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let resolved_table_name = ci.table.clone().resolve(
            "def",
            &session_state
                .config_options()
                .catalog
                .default_schema
                .clone(),
        );

        // 1. Is the index an encrypted one?
        // We abuse the USING clause to avoid having to modify the sql parser too much
        let Some(using) = &ci.using else {
            return self.try_forward_create_index(resolved_table_name, ci, catalog);
        };

        let having = using.to_lowercase();
        if having == "btree" || having == "hash" {
            // MySQL basic
            return self.try_forward_create_index(resolved_table_name, ci, catalog);
        }

        let table_name = ci.table.table();
        let schema = catalog
            .mysql_schema(&resolved_table_name.schema)
            .ok_or_else(|| {
                DataFusionError::Plan(format!("No such schema {}", resolved_table_name.schema))
            })?;

        let table = schema
            .mysql_table(table_name)
            .ok_or_else(|| DataFusionError::Plan(format!("No such table {}", table_name)))?;

        let columns = Self::extract_column_names(ci)?;
        let index_name = ci
            .name
            .clone()
            .unwrap_or_else(|| format!("{table_name}_{having}_{}", columns.join("-")));

        let index_variant = EncryptedIndexConfigurationVariant::parse_from_sql(
            index_name,
            columns,
            table.clone(),
            &having,
        )?;

        let table =
            if index_variant.requires_rowid() && table.get_row_id_column().is_none() {
                let (create_plan, _) = table.create_rowid_column(session_state).await?.ok_or(
                    plan_datafusion_err!(
                        "inconsistent state for row_id column: does not exist, yet cannot create"
                    ),
                )?;

                // Can we really execute a plan as part of planning...?
                let context = session_state.task_ctx();
                let result = create_plan.execute(0, context)?;
                let _ = result.try_collect::<Vec<_>>().await?;

                // Re-obtain the table as it has changed
                schema
                    .mysql_table(table_name)
                    .ok_or_else(|| DataFusionError::Plan(format!("No such table {}", table_name)))?
            } else {
                table
            };

        let plan = index_variant
            .build(table.table_reference(), table.get_row_id_column())
            .create_index_plan(table.table_reference(), session_state)
            .await?;
        Ok(plan)
    }

    fn try_forward_create_index(
        &self,
        resolved_table_name: ResolvedTableReference,
        ci: &CreateIndex,
        catalog: Arc<MySqlCatalogProvider>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let ast_plan = ast::CreateIndex {
            name: ci
                .name
                .as_ref()
                .map(|v| ObjectName::from(Ident::from(v.as_str()))),
            table_name: ObjectName::from(vec![
                Ident::from(resolved_table_name.schema.as_ref()),
                Ident::from(resolved_table_name.table.as_ref()),
            ]),
            using: ci
                .using
                .as_ref()
                .map(|using| IndexType::Custom(Ident::from(using.as_str()))),
            columns: ci
                .columns
                .iter()
                .map(|sort_expr| {
                    Ok(IndexColumn {
                        column: OrderByExpr {
                            expr: expr_to_sql(&sort_expr.expr)?,
                            options: OrderByOptions {
                                asc: Some(sort_expr.asc),
                                nulls_first: None,
                            },
                            with_fill: None,
                        },
                        operator_class: None,
                    })
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?,
            unique: ci.unique,
            if_not_exists: ci.if_not_exists,
            include: vec![],
            nulls_distinct: None,
            with: vec![],
            predicate: None,
            index_options: vec![],
            alter_options: vec![],
            concurrently: false,
        };
        let fwd_plan = Arc::new(MySqlRawExecPlan::new(ast::Statement::CreateIndex(ast_plan)));

        let Some(table) = catalog
            .mysql_schema(&resolved_table_name.schema)
            .and_then(|schema| schema.mysql_table(&resolved_table_name.table))
        else {
            // Table or schema not managed by us -> forward
            return Ok(fwd_plan);
        };

        // Verify the columns
        for expr in ci.columns.iter() {
            let columns = expr.expr.column_refs();
            for column in columns {
                if table.is_encrypted(column.name()) {
                    plan_err!(
                        "Cannot create an index over column {} in table {resolved_table_name}: column is encrypted",
                        column.name()
                    )?
                }
            }
        }

        Ok(fwd_plan)
    }
}
