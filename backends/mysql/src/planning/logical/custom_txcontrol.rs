use datafusion::common::{DFSchema, DFSchemaRef, plan_err};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::sqlparser::ast::TransactionIsolationLevel;
use mysql_async::IsolationLevel;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::sync::Arc;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum TransactionControl {
    Start,
    Commit,
    Rollback,
    SetAutocommit(bool),
    SetIsolationLevel(IsolationLevel),
}

impl TransactionControl {
    pub fn isolation_level(level: TransactionIsolationLevel) -> datafusion::common::Result<Self> {
        let equivalent = match level {
            TransactionIsolationLevel::ReadUncommitted => IsolationLevel::ReadUncommitted,
            TransactionIsolationLevel::ReadCommitted => IsolationLevel::ReadCommitted,
            TransactionIsolationLevel::RepeatableRead => IsolationLevel::RepeatableRead,
            TransactionIsolationLevel::Serializable => IsolationLevel::Serializable,
            TransactionIsolationLevel::Snapshot => {
                plan_err!("Transaction isolation level Snapshot is not supported")?
            }
        };

        Ok(Self::SetIsolationLevel(equivalent))
    }

    pub fn to_logical_node(self) -> LogicalPlan {
        let node_base = CustomTransactionControl {
            inner: self,
            schema: DFSchemaRef::new(DFSchema::empty()),
        };

        LogicalPlan::Extension(Extension {
            node: Arc::new(node_base),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CustomTransactionControl {
    pub inner: TransactionControl,
    schema: DFSchemaRef,
}

impl PartialOrd for CustomTransactionControl {
    fn partial_cmp(&self, _other: &Self) -> Option<Ordering> {
        None
    }
}

impl UserDefinedLogicalNodeCore for CustomTransactionControl {
    fn name(&self) -> &str {
        "TransactionControl"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "TransactionControl({:?})", self.inner)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        assert!(exprs.is_empty());
        assert!(inputs.is_empty());

        Ok(self.clone())
    }
}
