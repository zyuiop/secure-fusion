use super::ddl_physical_plan::{DdlPlan, DdlPlanImpl};
use crate::backend_config::BackendConfig;
use crate::metadata::EncryptedColumnMeta;
use crate::planning::physical::plans::mysql_raw_exec_plan::MySqlRawExecPlan;
use crate::planning::physical::plans::noop::NoOpDdlPlan;
use crate::planning::unparser::object_name_from_resolved;
use crate::providers::catalog_provider::MySqlCatalogProvider;
use crate::providers::schema_provider::MySqlSchemaProvider;
use crate::providers::table_provider::{ColumnDefault, MySqlTableProvider};
use crate::store::StoreGetter;
use datafusion::arrow::datatypes::{FieldRef, Schema};
use datafusion::catalog::TableProvider;
use datafusion::common::{
    ResolvedTableReference, exec_err, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::sqlparser::ast::helpers::attached_token::AttachedToken;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::ast::{AlterTable, AlterTableOperation, Statement};
use datafusion::sql::sqlparser::tokenizer::{Token, TokenWithSpan};
use std::fmt::Debug;
use std::iter::once;
use std::sync::Arc;

#[derive(Debug)]
pub struct AlterTablePlan {
    resolved_table_reference: ResolvedTableReference,
    schema_provider: Arc<MySqlSchemaProvider>,
    extra_ops: Vec<Arc<dyn AlterTableOp>>,
}

impl AlterTablePlan {
    fn is_intercepted(op: &AlterTableOperation) -> bool {
        matches!(
            op,
            AlterTableOperation::AddColumn { .. }
                | AlterTableOperation::DropColumn { .. }
                | AlterTableOperation::DropIndex { .. }
                | AlterTableOperation::AddConstraint { .. }
        )
    }

    fn can_forward_safely(op: &AlterTableOperation) -> bool {
        matches!(op, AlterTableOperation::AutoIncrement { .. })
    }

    fn build_forwarded_statement(
        resolved_table_reference: &ResolvedTableReference,
        operations: Vec<AlterTableOperation>,
    ) -> Statement {
        Statement::AlterTable(AlterTable {
            operations,
            name: object_name_from_resolved(resolved_table_reference),
            if_exists: false,
            location: None,
            only: false,
            on_cluster: None,
            table_type: None,
            end_token: AttachedToken(TokenWithSpan::wrap(Token::EOF)),
        })
    }

    pub fn new_plan(
        catalog: Arc<MySqlCatalogProvider>,
        table_reference: ResolvedTableReference,
        if_exists: bool,
        operations: &Vec<AlterTableOperation>,
        config: &BackendConfig,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let schema = catalog
            .mysql_schema(table_reference.schema.as_ref())
            .ok_or_else(|| plan_datafusion_err!("no such schema {}", table_reference.schema))?;

        let existing_table = schema.mysql_table(table_reference.table.as_ref());

        let Some(existing_table) = existing_table else {
            return if if_exists {
                Ok(NoOpDdlPlan::new())
            } else {
                plan_err!("no such table {}", table_reference.table)
            };
        };

        // Transform operations
        let (intercept, operations): (Vec<_>, Vec<_>) =
            operations.iter().cloned().partition(Self::is_intercepted);

        let operations = operations
            .into_iter()
            .map(|op| {
                if Self::can_forward_safely(&op) {
                    Ok(op)
                } else {
                    plan_err!("Unsupported ALTER TABLE operation: {op}")
                }
            })
            .collect::<Result<Vec<_>, _>>()?;

        if intercept.is_empty() {
            // Forward request
            return Ok(Arc::new(MySqlRawExecPlan::new(
                Self::build_forwarded_statement(&table_reference, operations),
            )));
        }

        let primary_key = existing_table.primary_key().as_slice();

        let mut extra_operations: Vec<Arc<dyn AlterTableOp>> = Vec::new();
        let mut forwarded_operations = operations;

        for mut intercepted in intercept.into_iter() {
            match &mut intercepted {
                AlterTableOperation::AddColumn { column_def, .. } => {
                    // Check if the column is encrypted
                    let meta =
                        EncryptedColumnMeta::try_from_column(column_def, primary_key, config)?;
                    let field = MySqlTableProvider::build_field_from_column_def(
                        table_reference.table.as_ref(),
                        column_def,
                        meta.as_ref(),
                    )?;
                    let field = Arc::new(field);
                    let default = MySqlTableProvider::build_column_default(column_def, &field)?;
                    extra_operations.push(Arc::new(AddColumn(field, meta, default)));
                    forwarded_operations.push(intercepted);
                }
                AlterTableOperation::DropColumn { column_names, .. } => {
                    for column in column_names {
                        let column = column.value.to_string();

                        if existing_table
                            .encryption_metadata()
                            .indices()
                            .iter()
                            .any(|index| index.tracked_columns().contains(&column))
                        {
                            plan_err!("Cannot drop column {column}: is used in an index.")?;
                        };

                        extra_operations.push(Arc::new(DropColumn(column)))
                    }

                    forwarded_operations.push(intercepted);
                }
                AlterTableOperation::DropIndex { .. } => {
                    not_impl_err!("TODO: Drop Index")?;
                }
                AlterTableOperation::AddConstraint { .. } => {
                    not_impl_err!("TODO: Add Index")?;
                }
                _ => unreachable!("did you forget to modify the is_intercepted function?"),
            }
        }

        Ok(Arc::new(DdlPlan::new(
            Self::build_forwarded_statement(&table_reference, forwarded_operations),
            Arc::new(AlterTablePlan {
                schema_provider: schema,
                resolved_table_reference: table_reference,
                extra_ops: extra_operations,
            }),
        )))
    }
}

trait AlterTableOp: Send + Sync + Debug {
    fn execute_extra(&self, table: &mut MySqlTableProvider) -> datafusion::common::Result<()>;
}

#[async_trait::async_trait]
impl DdlPlanImpl for AlterTablePlan {
    fn name(&self) -> &'static str {
        "alter_table"
    }

    async fn execute_extra(&self, context: Arc<TaskContext>) -> datafusion::common::Result<()> {
        let Some(table) = self
            .schema_provider
            .take_mysql_table(self.resolved_table_reference.table.as_ref())?
        else {
            exec_err!(
                "Table {} does not exist in local schema",
                self.resolved_table_reference.table
            )?
        };

        let mut table = Arc::unwrap_or_clone(table);
        let original_table = table.clone();

        let result: datafusion::common::Result<()> = {
            for task in self.extra_ops.iter() {
                task.execute_extra(&mut table)?;
            }
            Ok(())
        };

        match result {
            Ok(_) => {
                let table = Arc::new(table);
                self.schema_provider
                    .register_mysql_table(
                        self.resolved_table_reference.table.to_string(),
                        Arc::clone(&table),
                    )
                    .expect("failed to insert back table!");

                context
                    .get_store()
                    .save_metadata_for_table(&self.resolved_table_reference, table)
                    .await
                    .expect("failed to insert save metadata to disk!");

                Ok(())
            }
            Err(fwd) => {
                // Revert changes
                self.schema_provider
                    .register_mysql_table(
                        self.resolved_table_reference.table.to_string(),
                        Arc::new(original_table),
                    )
                    .expect("failed to insert back table!");
                Err(fwd.into())
            }
        }
    }
}

#[derive(Debug)]
struct DropColumn(String);

impl AlterTableOp for DropColumn {
    fn execute_extra(&self, table: &mut MySqlTableProvider) -> datafusion::common::Result<()> {
        table.encryption_metadata_mut().remove_column(&self.0);
        let schema = Schema::new(
            table
                .schema()
                .fields
                .iter()
                .filter(|field| field.name() != &self.0)
                .cloned()
                .collect::<Vec<_>>(),
        );
        table.update_schema(Arc::new(schema));
        table.columns_defaults.remove(&self.0);

        Ok(())
    }
}

#[derive(Debug)]
struct AddColumn(FieldRef, Option<EncryptedColumnMeta>, Option<ColumnDefault>);

impl AlterTableOp for AddColumn {
    fn execute_extra(&self, table: &mut MySqlTableProvider) -> datafusion::common::Result<()> {
        if let Some(meta) = self.1.as_ref() {
            table
                .encryption_metadata_mut()
                .add_column(self.0.name().clone(), meta.clone());
        }

        if let Some(default) = self.2.as_ref() {
            table
                .columns_defaults
                .insert(self.0.name().clone(), default.clone());
        }

        let fields = table
            .schema()
            .fields
            .iter()
            .cloned()
            .chain(once(self.0.clone()))
            .collect::<Vec<_>>();

        table.update_schema(Arc::new(Schema::new(fields)));
        Ok(())
    }
}
