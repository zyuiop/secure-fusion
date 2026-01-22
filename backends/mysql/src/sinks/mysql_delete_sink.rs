use super::sink::RecordBatchSink;
use crate::get_conn::ConnGetter;
use crate::sinks::utils::map_rows_to_params;
use async_trait::async_trait;
use common::dml::DmlResult;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use datafusion::sql::TableReference;
use mysql_async::prelude::{BatchQuery, Queryable, WithParams};
use std::fmt::{Debug, Formatter};
use std::ops::DerefMut;
use std::sync::Arc;

pub struct MySqlDeleteSink {
    target_table: TableReference,
    primary_key_schema: SchemaRef,
}

impl Debug for MySqlDeleteSink {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MySqlDeleteSink(target_table: {:?}, primary_key_schema: {:?})",
            self.target_table, self.primary_key_schema
        )
    }
}

impl MySqlDeleteSink {
    pub(crate) fn new(target_table: TableReference, primary_key_schema: SchemaRef) -> Self {
        Self {
            target_table,
            primary_key_schema,
        }
    }
}

impl DisplayAs for MySqlDeleteSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "MySqlDeleteSink table={} schema={}",
            self.target_table, self.primary_key_schema
        )
    }
}

#[async_trait]
impl RecordBatchSink for MySqlDeleteSink {
    async fn handle_batches(
        &self,
        data: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<DmlResult> {
        let conn = context.get_conn();
        let mut conn = conn.try_lock().unwrap();

        let filters = self
            .primary_key_schema
            .fields()
            .iter()
            .map(|field| format!("{} = ?", field.name()))
            .collect::<Vec<_>>()
            .join(" AND ");

        let query = format!(r#"DELETE FROM {} WHERE {filters}"#, &self.target_table);

        #[cfg(feature = "log-outgoing-queries")]
        log::info!("Sending: {query}");

        let stmt = conn
            .prep(query)
            .await
            .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;

        let mut num_deleted = 0;
        for batch in data.into_iter() {
            num_deleted += batch.num_rows() as u64;
            let rows = map_rows_to_params(batch)
                .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
            let s = stmt.clone();
            s.with(rows)
                .batch(conn.deref_mut())
                .await
                .map_err(|conn_err| DataFusionError::External(Box::new(conn_err)))?;
        }

        Ok(DmlResult::new(num_deleted))
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.primary_key_schema
    }
}
