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
use crypto::cipher::AssociatedData;
use crypto::key_manager::KeyManager;
use crypto::planning::physical::decrypt::DecryptExpr;
use crypto::{CipherContext, KeyManagerGetter};
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::common::{DataFusionError, ResolvedTableReference, plan_err};
use datafusion::datasource::TableProvider;
use datafusion::execution::{SendableRecordBatchStream, SessionState, TaskContext};
use datafusion::physical_expr;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::sql::TableReference;
use futures_util::{TryStreamExt, stream};
use mysql_async::prelude::Queryable;
use std::any::Any;
use std::fmt::Formatter;
use std::iter;
use std::iter::once;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct CreateBlindIndexPlan {
    /// The plan that will be used to set index values for existing rows
    compute_initial_index_plan: Arc<dyn ExecutionPlan>,
    schema_provider: Arc<MySqlSchemaProvider>,
    table: ResolvedTableReference,
    config: Arc<metadata::blind_index::BlindIndex>,
    properties: PlanProperties,
}

pub async fn create_index_compute_plan(
    table_ref: ResolvedTableReference,
    session: &SessionState,
    index: Arc<metadata::blind_index::BlindIndex>,
) -> Arc<dyn ExecutionPlan> {
    let table_ref = TableReference::partial(table_ref.schema.clone(), table_ref.table.clone());

    let table = session
        .mysql_schema_for_ref(&table_ref)
        .expect("table not found");
    let table_schema = table.schema();
    let source_index_position = table_schema
        .index_of(&index.column)
        .expect("index column not found");

    // Build the projection array (indices of the columns to query in the table)
    // First column is the indexed column
    // Other columns are the primary key for the table
    let pk = table.primary_key();
    let mut projection = Vec::with_capacity(pk.len() + 1);
    projection.push(source_index_position);
    projection.extend(pk.iter().map(|col_name| {
        table_schema
            .index_of(col_name)
            .expect("primary key column not found")
    }));

    // Retrieve the metadata for the indexed column, and build the cipher and AAD for the decryption
    let column_meta = table.encryption_metadata().column(&index.column).unwrap();
    let keys_manager = session.get_long_term_keys_manager();
    let cipher = keys_manager.get_cipher(&CipherContext::TableColumn {
        table_name: table_ref.table(),
        column_name: index.column.as_ref(),
    });
    let aad = AssociatedData::column_with_type(
        table_ref.table(),
        index.column.as_ref(),
        &column_meta.original_type,
    );

    // Build the base scan plan to have a schema to work on
    let table_scan_plan =
        TableProvider::scan(table.as_ref(), session, Some(&projection), &[], None)
            .await
            .unwrap();

    // Build the expression used to compute the blind index column
    let index_column_expr = Arc::new(physical_expr::expressions::Column::new(&index.column, 0));
    let decrypted_index_column = DecryptExpr::new(index_column_expr, cipher, aad);
    let decrypted_index_projection =
        ProjectionExpr::new(Arc::new(decrypted_index_column), index.column.clone());

    let pk_projection = table_scan_plan.schema();
    let pk_projection = pk_projection
        .fields()
        .iter()
        .enumerate()
        .skip(1)
        .map(|(pos, field)| {
            ProjectionExpr::new(
                Arc::new(Column::new(field.name(), pos)),
                field.name().clone(),
            )
        });
    let projection = once(decrypted_index_projection).chain(pk_projection);

    let table_scan_plan = Arc::new(ProjectionExec::try_new(projection, table_scan_plan).unwrap());

    let select_schema = table_scan_plan.schema();

    let index_insert_strat = index
        .insert(
            table.as_ref(),
            session.get_long_term_keys_manager(),
            select_schema.clone(),
        )
        .unwrap();

    let IndexInsertStrategy::AddColumns(mut add_columns) = index_insert_strat else {
        unreachable!("blindindex uses the AddColumns strategy")
    };
    let blind_index_column = add_columns.remove(0);

    // Build the full projection array, with the PK columns prefixed to be understood correctly by the update sink
    let project = iter::once(blind_index_column);
    let other_columns = select_schema
        .fields()
        .iter()
        .skip(1)
        .enumerate()
        .map(|(pos, field)| ProjectionExpr {
            expr: Arc::new(physical_expr::expressions::Column::new(
                field.name(),
                pos + 1,
            )),
            alias: format!("{FILTER_PREFIX}{}", field.name()),
        });
    let projection_columns: Vec<ProjectionExpr> = project.chain(other_columns).collect();

    // Build the projection execution
    let projection_exec = ProjectionExec::try_new(projection_columns, table_scan_plan).unwrap();

    // Finally, build the update sink
    let sink = MySqlDmlSink::update(table_ref.clone(), projection_exec.schema().clone()).unwrap();
    let update_plan = Arc::new(RecordBatchSinkExec::new(
        Arc::new(projection_exec),
        Arc::new(sink),
    ));

    update_plan
}

impl CreateBlindIndexPlan {
    /// Creates a plan to create a blind index
    /// This requires a physical_planner parameter to create the sub-plan that will provision the index
    pub async fn new(
        table_ref: ResolvedTableReference,
        session: &SessionState,
        index: Arc<metadata::blind_index::BlindIndex>,
    ) -> Self {
        let sub_plan = create_index_compute_plan(table_ref.clone(), session, index.clone()).await;

        Self {
            compute_initial_index_plan: sub_plan,
            config: index,
            schema_provider: session
                .get_catalog()
                .mysql_schema(&table_ref.schema)
                .unwrap(),
            table: table_ref,
            properties: PlanProperties::new(
                EquivalenceProperties::new(Arc::clone(&DML_SCHEMA)),
                Partitioning::RoundRobinBatch(session.config().target_partitions()),
                EmissionType::Final,
                Boundedness::Bounded,
            ),
        }
    }

    async fn execute_async(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<()> {
        let conn = context.get_conn();
        let mut conn = conn.try_lock().unwrap();

        // 0. Lock full table
        conn.query_drop(format!("LOCK TABLE {} WRITE", self.table.table))
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        // 1. Create column on table (nullable)
        // 2. Create index on column
        conn.query_drop(format!(
            "ALTER TABLE {} \
        ADD COLUMN {} CHAR({}) NULL, \
        ADD INDEX {} ({})",
            self.table.table,
            &self.config.index_column_name,
            self.config.size_bits.div_ceil(8) * 2,
            &self.config.index_column_name,
            &self.config.index_column_name
        ))
        .await
        .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        // 2.5. Update table metadata
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

        // 3. Execute underlying plan
        drop(conn); // We need to release the connection lock for the next plan to work
        let stream = self
            .compute_initial_index_plan
            .execute(partition, context.clone())?;
        let _ = stream.try_collect::<Vec<_>>().await?;

        // 4. Make column not null
        let conn = context.get_conn();
        let mut conn = conn.try_lock().unwrap();
        conn.query_drop(format!(
            "ALTER TABLE {} MODIFY COLUMN {} CHAR({}) NOT NULL",
            self.table.table,
            &self.config.index_column_name,
            self.config.size_bits.div_ceil(8) * 2,
        ))
        .await
        .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        // 5. Unlock table
        conn.query_drop("UNLOCK TABLES")
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        Ok(())
    }
}

impl DisplayAs for CreateBlindIndexPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CreateBlindIndex")
    }
}

impl ExecutionPlan for CreateBlindIndexPlan {
    fn name(&self) -> &str {
        "CreateBlindIndex"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
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
            plan_err!("CreateBlindIndexPlan requires exactly one child")?;
        }

        let CreateBlindIndexPlan {
            schema_provider,
            compute_initial_index_plan: _,
            table,
            config,
            properties,
        } = Arc::unwrap_or_clone(self);
        let new_plan = CreateBlindIndexPlan {
            config,
            table,
            properties,
            compute_initial_index_plan: children.remove(0),
            schema_provider,
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
