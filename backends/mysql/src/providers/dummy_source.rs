use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::common::plan_err;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// A dummy table, used for queries in the form `SELECT 1 FROM dual`.
///
/// This is used when computation is done in the projection phase but no actual source is selected.
/// It should be removed by an analyzer step during logical planning, any call to `scan` is therefore a mistake.
///
/// See [https://en.wikipedia.org/wiki/DUAL_table]
#[derive(Debug)]
pub struct DummySource {
    schema: SchemaRef,
}

impl Default for DummySource {
    fn default() -> Self {
        Self {
            schema: SchemaRef::new(Schema::empty()),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for DummySource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        plan_err!(
            "dummy-source should not be scanned, did you disable the eliminate-dummy-table rule?"
        )
    }
}
