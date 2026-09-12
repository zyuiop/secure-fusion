use crate::MySqlTableProvider;
use crate::get_catalog::CatalogGetter;
use crate::get_conn::ConnGetter;
use crate::planning::logical::FILTER_PREFIX;
use crate::sinks::mysql_dml_sink::MySqlDmlSink;
use crate::sinks::sink::RecordBatchSinkExec;
use crate::store::StoreGetter;
use common::dml::{DML_SCHEMA, DmlResult};
use crypto::row_id::RowIdColumn;
use crypto::{IdentifierContext, KeyManagerGetter};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{exec_datafusion_err, exec_err, plan_datafusion_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::sqlparser::ast::ColumnDef;
use datafusion::physical_expr;
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::ResolvedTableReference;
use futures_util::TryStreamExt;
use futures_util::stream::once;
use log::info;
use mysql_async::prelude::Queryable;
use std::any::Any;
use std::fmt::Formatter;
use std::iter;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct CreateRowidPlan {
    row_id: RowIdColumn,
    column_def: ColumnDef,
    resolved_table_reference: ResolvedTableReference,
    fill_table_plan: Arc<dyn ExecutionPlan>,
    props: Arc<PlanProperties>,
}

impl CreateRowidPlan {
    pub async fn create_row_id(
        table: &MySqlTableProvider,
        session: &dyn Session,
        row_id_column: RowIdColumn,
        column_def: ColumnDef,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&DML_SCHEMA)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        // Build the update plan
        let table_schema = table.schema();
        let primary_key_schema = table
            .primary_key()
            .iter()
            .map(|col| table_schema.field_with_name(col).cloned())
            .collect::<Result<Vec<_>, _>>()?;

        let primary_key_schema = Schema::new(primary_key_schema);

        let scan_plan = table
            .scan_and_decrypt_all(session, primary_key_schema, &[], None)
            .await?;
        let scan_schema = scan_plan.schema();

        let filter_project =
            scan_schema
                .fields
                .iter()
                .enumerate()
                .map(|(pos, field)| ProjectionExpr {
                    expr: Arc::new(physical_expr::expressions::Column::new(field.name(), pos)),
                    alias: format!("{FILTER_PREFIX}{}", field.name()),
                });

        let crypto = session
            .config()
            .get_long_term_keys_manager()
            .get_identifier_generator(&IdentifierContext::RowIdColumn {
                table_context: table.table_reference().clone(),
            });

        let compute_rowid = row_id_column
            .compute_rowid(crypto, scan_schema.as_ref())?
            .ok_or(exec_datafusion_err!(
                "failed to create an expression to generate row_id column"
            ))?;

        let project = iter::once(compute_rowid)
            .chain(filter_project)
            .collect::<Vec<_>>();

        // Build the projection execution
        let projection_exec = ProjectionExec::try_new(project, scan_plan)?;

        // Finally, build the update sink
        let sink = MySqlDmlSink::update(
            table.table_reference().clone(),
            projection_exec.schema().clone(),
        )?;
        let update_plan = Arc::new(RecordBatchSinkExec::new(
            Arc::new(projection_exec),
            Arc::new(sink),
        ));

        // We can now build ourself
        Ok(Arc::new(Self {
            row_id: row_id_column,
            fill_table_plan: update_plan,
            column_def,
            props,
            resolved_table_reference: table.table_reference().clone(),
        }))
    }
}

impl DisplayAs for CreateRowidPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} query={}", self.name(), self.column_def)
    }
}

impl CreateRowidPlan {
    async fn create_column(&self, task: Arc<TaskContext>) -> datafusion::common::Result<()> {
        let conn = task.get_conn();
        let mut conn = conn.try_lock().expect("failed to lock DB connection");

        conn.query_drop(format!(
            "ALTER TABLE {}.{} \
                ADD COLUMN {} NULL,\
                ADD UNIQUE INDEX ({})",
            self.resolved_table_reference.schema,
            self.resolved_table_reference.table,
            self.column_def,
            self.column_def.name
        ))
        .await
        .map_err(|e| exec_datafusion_err!("Failed to create the indexable column: {e}"))?;

        Ok(())
    }

    async fn rollback(&self, task: Arc<TaskContext>) -> datafusion::common::Result<()> {
        let conn = task.get_conn();
        let mut conn = conn.try_lock().expect("failed to lock DB connection");

        conn.query_drop(format!(
            "ALTER TABLE {}.{} DROP COLUMN {}",
            self.resolved_table_reference.schema,
            self.resolved_table_reference.table,
            self.column_def,
        ))
        .await
        .map_err(|e| exec_datafusion_err!("Failed to drop the indexable column: {e}"))?;

        Ok(())
    }

    async fn finish_column_setup(&self, task: Arc<TaskContext>) -> datafusion::common::Result<()> {
        let conn = task.get_conn();
        let mut conn = conn.try_lock().expect("failed to lock DB connection");

        conn.query_drop(format!(
            "ALTER TABLE {}.{} MODIFY {} NOT NULL",
            self.resolved_table_reference.schema,
            self.resolved_table_reference.table,
            self.column_def,
        ))
        .await
        .map_err(|e| exec_datafusion_err!("Failed to update the indexable column: {e}"))?;

        Ok(())
    }

    async fn save_updated_metadata(
        &self,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<()> {
        let schema_provider = context
            .session_config()
            .get_catalog()
            .mysql_schema(self.resolved_table_reference.schema.as_ref())
            .ok_or_else(|| {
                plan_datafusion_err!("no such schema {}", self.resolved_table_reference.schema)
            })?;

        // Steal table from schema provider
        let Some(table) =
            schema_provider.take_mysql_table(self.resolved_table_reference.table.as_ref())?
        else {
            exec_err!(
                "Table {} does not exist in local schema",
                self.resolved_table_reference.table
            )?
        };

        // Modify table
        let mut table = Arc::unwrap_or_clone(table);
        table.encryption_metadata_mut().row_id_column = Some(self.row_id.clone());

        // Reinsert table in schema provider
        let table = Arc::new(table);
        schema_provider
            .register_mysql_table(
                self.resolved_table_reference.table.to_string(),
                Arc::clone(&table),
            )
            .expect("failed to insert back table!");

        // Save updated metadata
        context
            .get_store()
            .save_metadata_for_table(&self.resolved_table_reference, table)
            .await
            .expect("failed to insert save metadata to disk!");

        Ok(())
    }
}

impl ExecutionPlan for CreateRowidPlan {
    fn name(&self) -> &str {
        "CreateRowId"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.fill_table_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            plan_err!("invalid children count for CreateRowId")?
        }

        Ok(Arc::new(Self {
            props: self.props.clone(),
            column_def: self.column_def.clone(),
            row_id: self.row_id.clone(),
            fill_table_plan: children[0].clone(),
            resolved_table_reference: self.resolved_table_reference.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);

        info!(
            "Creating RowId column on table {}",
            self.resolved_table_reference
        );

        let cloned_self = self.clone();
        let future = async move {
            cloned_self.create_column(context.clone()).await?;

            info!("Created RowId column, setting initial values...");

            let result = {
                let underlying = cloned_self
                    .fill_table_plan
                    .execute(partition, context.clone())?;
                let _ = underlying.try_collect::<Vec<_>>().await?;

                cloned_self.finish_column_setup(context.clone()).await?;
                cloned_self.save_updated_metadata(context.clone()).await?;

                info!("Created RowId column");

                Ok(RecordBatch::from(DmlResult::empty()))
            };

            if result.is_err() {
                cloned_self.rollback(context).await?;
            }

            result
        };

        let schema = self.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            once(future),
        )))
    }
}
