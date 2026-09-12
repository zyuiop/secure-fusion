// TODO: maybe we want a common plan for the index creation part?

use crate::get_catalog::CatalogGetter;
use crate::get_conn::ConnGetter;
use crate::metadata;
use crate::metadata::{EncryptedIndex, IndexInsertStrategy, SerializableEncryptedTableMeta};
use crate::planning::logical::FILTER_PREFIX;
use crate::providers::schema_provider::MySqlSchemaProvider;
use crate::sinks::mysql_dml_sink::MySqlDmlSink;
use crate::sinks::sink::RecordBatchSinkExec;
use crate::store::StoreGetter;
use common::dml::{DML_SCHEMA, DmlResult};
use common::profile;
use crypto::KeyManagerGetter;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DataFusionError, ResolvedTableReference, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::physical_expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures_util::{TryStreamExt, stream};
use mysql_async::prelude::Queryable;
use std::any::Any;
use std::fmt::Formatter;
use std::iter::once;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CreateColumnarIndexPlan {
    /// The plan that will be used to set index values for existing rows
    compute_initial_index_plan: Arc<dyn ExecutionPlan>,
    schema_provider: Arc<MySqlSchemaProvider>,
    table: ResolvedTableReference,
    config: Arc<dyn EncryptedIndex>,
    properties: Arc<PlanProperties>,

    index_column_type: String,
    index_column_name: Arc<str>,
}

async fn create_index_compute_plan(
    resolved_table_ref: ResolvedTableReference,
    session: &SessionState,
    index: Arc<dyn EncryptedIndex>,
    indexed_column: String,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let table = session
        .mysql_schema_for_ref(&(resolved_table_ref.clone().into()))
        .expect("table not found");

    let projected_schema = table
        .primary_key()
        .iter()
        .cloned()
        .chain(once(indexed_column))
        .map(|col_name| table.schema().field_with_name(&col_name).cloned())
        .collect::<Result<Vec<_>, _>>()?;

    // Returns a scan for the indexed column and the index column of the table
    let decrypted_table_scan = table
        .scan_and_decrypt_all(session, Schema::new(projected_schema), &[], None)
        .await?;

    let select_schema = decrypted_table_scan.schema();

    let IndexInsertStrategy::AddColumns(mut add_columns) = index.insert(
        table.as_ref(),
        session.get_long_term_keys_manager(),
        select_schema.clone(),
    )?
    else {
        unreachable!("Columnar Indexes use the AddColumns strategy")
    };
    let index_column = add_columns.remove(0);

    // Build the full projection array, with the PK columns prefixed to be understood correctly by the update sink
    let project = once(index_column);
    let other_columns = select_schema
        .fields()
        .iter()
        .take(select_schema.fields.len() - 1) // don't change the blind index column
        .enumerate()
        .map(|(pos, field)| ProjectionExpr {
            expr: Arc::new(physical_expr::expressions::Column::new(field.name(), pos)),
            alias: format!("{FILTER_PREFIX}{}", field.name()),
        });
    let projection_columns: Vec<ProjectionExpr> = project.chain(other_columns).collect();

    // Build the projection execution
    let projection_exec = ProjectionExec::try_new(projection_columns, decrypted_table_scan)?;

    // Finally, build the update sink
    let sink = MySqlDmlSink::update(resolved_table_ref, projection_exec.schema().clone())?;
    let update_plan = Arc::new(RecordBatchSinkExec::new(
        Arc::new(projection_exec),
        Arc::new(sink),
    ));

    Ok(update_plan)
}

impl CreateColumnarIndexPlan {
    /// Creates a plan to create a blind index
    /// This requires a physical_planner parameter to create the sub-plan that will provision the index
    pub async fn blind_index(
        table_ref: &ResolvedTableReference,
        session: &SessionState,
        index: Arc<metadata::blind_index::BlindIndex>,
    ) -> datafusion::common::Result<Self> {
        let sub_plan = create_index_compute_plan(
            table_ref.clone(),
            session,
            index.clone(),
            index.indexed_column.to_string(),
        )
        .await?;

        Ok(Self::new(
            table_ref,
            session,
            sub_plan,
            format!("CHAR({})", index.size_bits.div_ceil(8) * 2),
            index.index_column_name.clone(),
            index,
        ))
    }

    /// Creates a plan to create a range index
    /// This requires a physical_planner parameter to create the sub-plan that will provision the index
    pub async fn range_index(
        table_ref: &ResolvedTableReference,
        session: &SessionState,
        index: Arc<metadata::indices::range_queries::RangeIndex>,
    ) -> datafusion::common::Result<Self> {
        let sub_plan = create_index_compute_plan(
            table_ref.clone(),
            session,
            index.clone(),
            index.indexed_column.to_string(),
        )
        .await?;

        Ok(Self::new(
            table_ref,
            session,
            sub_plan,
            "INT UNSIGNED".to_string(),
            index.index_column_name.clone(),
            index,
        ))
    }

    fn new(
        table_ref: &ResolvedTableReference,
        session: &SessionState,
        compute_initial_index_plan: Arc<dyn ExecutionPlan>,
        index_column_type: String,
        index_column_name: Arc<str>,
        index: Arc<dyn EncryptedIndex>,
    ) -> Self {
        Self {
            compute_initial_index_plan,
            config: index.clone(),
            schema_provider: session
                .get_catalog()
                .mysql_schema(&table_ref.schema)
                .unwrap(),
            table: table_ref.clone(),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&DML_SCHEMA)),
                Partitioning::RoundRobinBatch(1),
                EmissionType::Final,
                Boundedness::Bounded,
            )),
            index_column_type,
            index_column_name,
        }
    }

    async fn execute_async(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<()> {
        let conn = context.get_conn();
        let mut conn = conn.lock().await;

        // 0. Lock full table
        profile!(
            "lock table for write",
            conn.query_drop(format!("LOCK TABLE {} WRITE", self.table.table))
                .await
                .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?
        );

        // 1. Create column on table (nullable)
        // 2. Create index on column
        profile!(
            "add column",
            conn.query_drop(format!(
                "ALTER TABLE {} \
        ADD COLUMN {} {} NULL, \
        ADD INDEX {} ({})",
                self.table.table,
                &self.index_column_name,
                &self.index_column_type,
                &self.index_column_name,
                &self.index_column_name
            ))
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?
        );

        // 3. Execute underlying plan
        drop(conn); // We need to release the connection lock for the next plan to work
        let stream = self
            .compute_initial_index_plan
            .execute(partition, context.clone())?;
        let _ = stream.try_collect::<Vec<_>>().await?;

        // 4. Make column not null (TODO: should we, actually?)
        let conn = context.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop(format!(
            "ALTER TABLE {} MODIFY COLUMN {} {} NOT NULL",
            self.table.table, &self.index_column_name, &self.index_column_type,
        ))
        .await
        .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        // 5. Unlock table
        conn.query_drop("UNLOCK TABLES")
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        // 6. Update table metadata
        let table = self
            .schema_provider
            .mysql_table(&self.table.table)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("table not found {}", self.table.table))
            })?;

        let mut table = Arc::unwrap_or_clone(table);
        let meta = table.encryption_metadata_mut();
        meta.add_index(self.config.clone());

        let serialized_meta: SerializableEncryptedTableMeta =
            SerializableEncryptedTableMeta::from(&*meta);
        context
            .get_store()
            .update_metadata(&self.table.schema, |meta| {
                meta.encrypted_tables
                    .insert(self.table.table.to_string(), serialized_meta);
                Ok(())
            })
            .await?;

        let _ = self
            .schema_provider
            .replace_mysql_table(self.table.table.to_string(), Arc::new(table))?;

        Ok(())
    }
}

impl DisplayAs for CreateColumnarIndexPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CreateColumnarIndex")
    }
}

impl ExecutionPlan for CreateColumnarIndexPlan {
    fn name(&self) -> &str {
        "CreateColumnarIndex"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.compute_initial_index_plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            plan_err!("CreateColumnarIndexPlan requires exactly one child")?;
        }

        let CreateColumnarIndexPlan {
            schema_provider,
            compute_initial_index_plan: _,
            table,
            config,
            properties,
            index_column_type,
            index_column_name,
        } = Arc::unwrap_or_clone(self);
        let new_plan = CreateColumnarIndexPlan {
            config,
            table,
            properties,
            compute_initial_index_plan: children.remove(0),
            schema_provider,
            index_column_type,
            index_column_name,
        };

        Ok(Arc::new(new_plan))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let schema = SchemaRef::new(Schema::empty());

        let cloned_self = self.clone();
        let future_result = async move {
            cloned_self
                .execute_async(partition, context)
                .await
                .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

            Ok(DmlResult::empty().into())
        };

        let result = stream::once(future_result);
        let stream_adapter = RecordBatchStreamAdapter::new(schema, result);

        Ok(Box::pin(stream_adapter))
    }
}
