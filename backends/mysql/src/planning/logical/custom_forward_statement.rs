use crate::planning::unparser;
use common::dml::DML_SCHEMA_LOGICAL;
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{DFSchema, DFSchemaRef, ResolvedTableReference};
use datafusion::logical_expr::expr::Placeholder;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{ShowCreateObject, Value, Visitor};
use datafusion::logical_expr::{Expr, Extension, LogicalPlan, UserDefinedLogicalNodeCore};
use datafusion::sql::sqlparser::ast::{Delete, Statement, Visit, VisitMut, VisitorMut};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::MySqlDialect;
use std::cmp::Ordering;
use std::fmt::Formatter;
use std::ops::ControlFlow;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ForwardStatement {
    inner: Statement,
    schema: DFSchemaRef,
    found_placeholders: Vec<Expr>,
    return_updated_count: bool,
}

impl PartialOrd for ForwardStatement {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.inner.partial_cmp(&other.inner)
    }
}

struct PlaceholderFinder {
    expressions: Vec<Expr>,
}

impl Visitor for PlaceholderFinder {
    type Break = ();

    fn pre_visit_value(&mut self, value: &Value) -> ControlFlow<Self::Break> {
        if let Value::Placeholder(name) = value {
            self.expressions
                .push(Expr::Placeholder(Placeholder::new(name.clone(), None)));
        }
        ControlFlow::Continue(())
    }
}

struct PlaceholderReplacer {
    values: Vec<Expr>,
    new_placeholders: Vec<Expr>,
}

impl PlaceholderReplacer {
    fn new(mut values: Vec<Expr>) -> PlaceholderReplacer {
        values.reverse();
        Self {
            values,
            new_placeholders: vec![],
        }
    }
}

impl VisitorMut for PlaceholderReplacer {
    type Break = ();

    fn pre_visit_value(&mut self, value: &mut Value) -> ControlFlow<Self::Break> {
        if let Value::Placeholder(_) = value {
            let next_value = self.values.pop();

            if let Some(next_value) = next_value {
                let unparsed = Unparser::new(&MySqlDialect {})
                    .expr_to_sql(&next_value)
                    .unwrap();

                if let ast::Expr::Value(value_with_span) = unparsed {
                    *value = value_with_span.value.clone();
                }
            }
        }

        // Is the value still a placeholder after replacement?
        if let Value::Placeholder(name) = value {
            self.new_placeholders
                .push(Expr::Placeholder(Placeholder::new(name.clone(), None)));
        }

        ControlFlow::Continue(())
    }
}

impl UserDefinedLogicalNodeCore for ForwardStatement {
    fn name(&self) -> &str {
        "ForwardStatement"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        self.found_placeholders.clone()
    }

    fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ForwardStatement({})", self.inner)
    }

    fn with_exprs_and_inputs(
        &self,
        exprs: Vec<Expr>,
        _inputs: Vec<LogicalPlan>,
    ) -> datafusion::common::Result<Self> {
        let mut new_statement = self.inner.clone();
        let mut visitor = PlaceholderReplacer::new(exprs);
        let _ = VisitMut::visit(&mut new_statement, &mut visitor);

        Ok(Self {
            inner: new_statement,
            schema: self.schema.clone(),
            found_placeholders: visitor.new_placeholders,
            return_updated_count: self.return_updated_count,
        })
    }
}

impl ForwardStatement {
    pub fn new(stmt: Statement, schema: Vec<Field>) -> Self {
        let schema = DFSchemaRef::new(
            DFSchema::new_with_metadata(
                schema
                    .into_iter()
                    .map(|field| (None, Arc::new(field)))
                    .collect(),
                Default::default(),
            )
            .unwrap(),
        );
        Self::new_with_schema(stmt, schema)
    }

    pub fn new_with_schema(stmt: Statement, schema: DFSchemaRef) -> Self {
        let mut placeholder_finder = PlaceholderFinder {
            expressions: vec![],
        };
        let _ = Visit::visit(&stmt, &mut placeholder_finder);

        Self {
            inner: stmt,
            found_placeholders: placeholder_finder.expressions,
            return_updated_count: false,
            schema,
        }
    }

    pub fn new_wrapped(stmt: Statement, schema: Vec<Field>) -> LogicalPlan {
        LogicalPlan::Extension(Extension {
            node: Arc::new(Self::new(stmt, schema)),
        })
    }

    pub fn new_with_dml_schema(stmt: Statement) -> LogicalPlan {
        let mut placeholder_finder = PlaceholderFinder {
            expressions: vec![],
        };
        let _ = Visit::visit(&stmt, &mut placeholder_finder);

        LogicalPlan::Extension(Extension {
            node: Arc::new(Self {
                inner: stmt,
                found_placeholders: placeholder_finder.expressions,
                return_updated_count: true,
                schema: Arc::clone(&DML_SCHEMA_LOGICAL),
            }),
        })
    }

    pub fn statement(&self) -> Statement {
        self.inner.clone()
    }

    pub fn return_updated_count(&self) -> bool {
        self.return_updated_count
    }

    pub fn show_create(
        obj_type: ShowCreateObject,
        obj_name: ResolvedTableReference,
    ) -> LogicalPlan {
        let stmt = Statement::ShowCreate {
            obj_name: unparser::object_name_from_resolved(&obj_name),
            obj_type,
        };

        Self::new_wrapped(
            stmt,
            vec![
                Field::new("Table", DataType::Utf8, false),
                Field::new("Create Table", DataType::Utf8, false),
            ],
        )
    }

    pub fn delete(statement: Delete) -> LogicalPlan {
        Self::new_with_dml_schema(Statement::Delete(statement))
    }

    pub fn show_status(statement: Statement) -> LogicalPlan {
        Self::new_wrapped(
            statement,
            vec![
                Field::new("Variable_name", DataType::Utf8, false),
                Field::new("Value", DataType::Utf8, false),
            ],
        )
    }
}
