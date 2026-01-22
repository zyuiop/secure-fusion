use crate::errors::MySqlBackendErrorInner;
use crate::get_conn::ConnGetter;
use common::dml::{DML_SCHEMA, DmlResult};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::sqlparser::ast;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures_util::stream::once;
use mysql_async::prelude::Queryable;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) struct MySqlRawExecPlan {
    query: ast::Statement,
    props: PlanProperties,
}

impl DisplayAs for MySqlRawExecPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} query={}", self.name(), self.query,)
    }
}

impl MySqlRawExecPlan {
    pub(crate) fn new(stmt: ast::Statement) -> Self {
        let project_schema = Arc::clone(&DML_SCHEMA);
        Self {
            query: stmt,
            props: PlanProperties::new(
                EquivalenceProperties::new(project_schema),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }
    }
}

impl ExecutionPlan for MySqlRawExecPlan {
    fn name(&self) -> &str {
        MySqlRawExecPlan::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.props.eq_properties.schema().clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This plan is a source for rows, it can never have child plans
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        // TODO: limit pushdown will require this
        if _children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::External(Box::new(
                MySqlBackendErrorInner::PhysicalPlanningError(
                    "MySQL plan cannot have children".to_string(),
                ),
            )))
        }
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let conn = context.get_conn();
        let query = self.query.clone();

        let result = async move {
            let mut underlying = conn.lock().await;
            underlying
                .query_drop(query.to_string())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let record = DmlResult::new_with_insert_id(
                underlying.affected_rows(),
                underlying.last_insert_id(),
            );

            Ok(RecordBatch::from(record))
        };

        let schema = self.schema();
        let result = once(result);

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, result)))
    }

    fn supports_limit_pushdown(&self) -> bool {
        // logical planning pushes the limit already, but if we don't report pushdown compatibility physical planning re-adds a limit step on top
        true
    }
}
