use crate::get_conn::ConnGetter;
use crate::sinks::sink::RecordBatchSink;
use crate::sinks::utils::map_rows_to_params_reordered;
use async_trait::async_trait;
use common::dml::DmlResult;
use common::profile;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DataFusionError, exec_err};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::WriteOp;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use mysql_async::prelude::{BatchQuery, Queryable, WithParams};
use std::fmt::Formatter;
use std::ops::DerefMut;
use std::sync::Arc;

#[derive(Debug)]
pub struct MySqlDmlSink {
    /// For debug purposes
    pub(super) operation: WriteOp,

    pub(super) expected_input_schema: SchemaRef,

    /// The columns projection, in terms of position in the input schema.
    /// The updates must appear first, and filters last.
    /// Columns must appear in the same order as in the query string.
    pub(super) columns_projection: Vec<usize>,
    pub(super) query_string: String,

    pub(super) report_last_insert: bool,
}

#[cfg(feature = "log-dml-queries")]
macro_rules! build_log_outgoing_values {
    ($mapped: expr, $expected_schema: expr) => {{
        use mysql_common::params::Params;

        let collected_expr = $mapped.collect::<Vec<_>>();

        log::info!("Send values batch. Schema: {}", $expected_schema);

        for e in &collected_expr {
            match e {
                Params::Empty => log::info!("[Row] (empty)"),
                Params::Named(param_map) => {
                    log::info!("[Row] {{");
                    for (field, value) in param_map.iter() {
                        let field = String::from_utf8(field.clone()).unwrap();
                        log::info!("[...]     {}: {:?}", field, value);
                    }
                    log::info!("[...] }}");
                }
                Params::Positional(positional) => {
                    let mapped = positional.iter().zip($expected_schema.fields().iter());

                    log::info!("[Row] {{");
                    for (value, field) in mapped {
                        log::info!("[...]     {}: {:?}", field.name(), value);
                    }
                    log::info!("[...] }}");
                }
            }
            log::info!("[Row]",)
        }

        collected_expr.into_iter()
    }};
}

#[cfg(not(feature = "log-dml-queries"))]
macro_rules! build_log_outgoing_values {
    ($mapped: expr, $_: expr) => {
        $mapped
    };
}

impl DisplayAs for MySqlDmlSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MySqlDmlSink[{}]({}, input={})",
            self.operation, self.query_string, self.expected_input_schema
        )
    }
}

#[async_trait]
impl RecordBatchSink for MySqlDmlSink {
    async fn handle_batches(
        &self,
        data: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<DmlResult> {
        let conn = context.get_conn();
        let mut conn = conn.try_lock().unwrap();

        #[cfg(feature = "log-outgoing-queries")]
        log::info!("Sending: {}", &self.query_string);

        let stmt = profile!(
            "prepare_statement",
            conn.prep(&self.query_string)
                .await
                .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?
        );

        let mut num_inserted = 0u64;
        for batch in data.into_iter() {
            if batch.schema() != self.expected_input_schema {
                exec_err!(
                    "Batch schema differs from expected input schema. Expected: {}, got: {}",
                    self.expected_input_schema,
                    batch.schema()
                )?;
            }

            num_inserted += batch.num_rows() as u64;
            let mapped =
                map_rows_to_params_reordered(batch, Some(self.columns_projection.as_slice()))
                    .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

            let mapped = build_log_outgoing_values!(mapped, &self.expected_input_schema);

            profile!(
                "exec_batch",
                stmt.clone()
                    .with(mapped)
                    .batch(conn.deref_mut())
                    .await
                    .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?
            );
        }

        let mut dml_result = DmlResult::new(num_inserted);
        if self.report_last_insert
            && let Some(last_insert_id) = conn.last_insert_id()
        {
            dml_result = dml_result.with_last_insert_id(last_insert_id)
        }

        Ok(dml_result)
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.expected_input_schema
    }
}
