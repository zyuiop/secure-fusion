use crate::errors::MySqlBackendErrorInner;
use crate::get_conn::ConnGetter;
use crate::metadata::ColumnName;
use crate::planning::unparser::{object_name_from_resolved, object_name_matches_resolved};
use async_trait::async_trait;
use common::profile;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::datatypes::{FieldRef, Fields, Schema};
use datafusion::common::{JoinType, exec_datafusion_err, exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::SortExpr;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::{
    JoinConstraint, OrderByOptions, TableFactor, ValueWithSpan, WildcardAdditionalOptions,
};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::ResolvedTableReference;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Ident, JoinOperator, LimitClause, LockType, OrderBy, OrderByExpr,
    OrderByKind, Select, SelectItem, SetExpr, TableAlias, Value,
};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::ast::{
    DerivedRelationBuilder, QueryBuilder, RelationBuilder, SelectBuilder, TableRelationBuilder,
    TableWithJoinsBuilder,
};
use datafusion::sql::unparser::dialect::MySqlDialect;
use futures_util::{TryStreamExt, stream};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct MySqlScanPlan {
    props: PlanProperties,

    scan: ScanHierarchyComponent,
    project_schema: SchemaRef,

    fetch: Option<usize>,
    sort: Vec<SortExpr>,
    lock_type: Option<LockType>,
}

impl DisplayAs for MySqlScanPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} schema=", self.name(),)?;

        for field in self.project_schema.fields() {
            write!(f, "{}:{} ", field.name(), field.data_type())?;
        }

        Ok(())
    }
}

impl MySqlScanPlan {
    pub fn new_from_schema(
        table: &ResolvedTableReference,
        columns: SchemaRef,
        fetch: Option<usize>,
        static_filter: Option<Expr>,
        dynamic_filters: Vec<Arc<dyn DynamicFilter>>,
    ) -> Self {
        let table = {
            let mut table_rel = TableRelationBuilder::default();
            table_rel.name(object_name_from_resolved(table));
            let mut rel = RelationBuilder::default();
            rel.table(table_rel);
            let mut table_with_join = TableWithJoinsBuilder::default();
            table_with_join.relation(rel);
            table_with_join
        };

        let projection = columns
            .fields()
            .iter()
            .map(|field| SelectItem::UnnamedExpr(ast::Expr::Identifier(Ident::new(field.name()))))
            .collect();

        let selection = SelectBuilder::default()
            .push_from(table)
            .projection(projection)
            .selection(static_filter)
            .build()
            .expect("simple select should be buildable");

        Self::new_query(selection, columns, fetch, dynamic_filters)
    }

    pub fn new_query(
        query: ast::Select,
        schema_ref: SchemaRef,
        fetch: Option<usize>,
        dynamic_filters: Vec<Arc<dyn DynamicFilter>>,
    ) -> Self {
        Self {
            scan: ScanHierarchyComponent::Leaf {
                base_query: Box::new(query),
                dynamic_filters,
            },

            props: PlanProperties::new(
                EquivalenceProperties::new(schema_ref.clone()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),

            project_schema: schema_ref,
            fetch,

            lock_type: None,
            sort: vec![],
        }
    }

    fn set_schema(&mut self, schema_ref: SchemaRef) {
        self.project_schema = schema_ref;
        self.refresh_props();
    }

    fn refresh_props(&mut self) {
        self.props = PlanProperties::new(
            EquivalenceProperties::new(self.project_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }

    #[inline]
    pub fn supports_join_mode(mode: &JoinType) -> bool {
        matches!(mode, JoinType::Inner | JoinType::Left)
    }

    pub fn join<JoinIter: Iterator<Item = (ColumnName, ColumnName)>>(
        &self,
        mode: JoinType,
        other: &MySqlScanPlan,
        equijoin: JoinIter,
        project: &Option<Vec<usize>>,
        project_schema: SchemaRef,
    ) -> datafusion::common::Result<Self> {
        if !Self::supports_join_mode(&mode) {
            plan_err!("Unsupported join mode: {mode}")?
        }

        // Some things make no sense for a join
        if !self.sort.is_empty() || !other.sort.is_empty() {
            plan_err!("Cannot join two plans when one of them is sorted!")?
        }

        if self.fetch.is_some() || other.fetch.is_some() {
            plan_err!("Cannot join two plans when one of them has a LIMIT clause")?
        }

        if self.lock_type.is_some()
            && other.lock_type.is_some()
            && self.lock_type != other.lock_type
        {
            plan_err!("Cannot join two plans with different lock type")?
        }

        let left_alias = Ident::new("t1");
        let right_alias = Ident::new("t2");
        let right_first_index = self.schema().fields.len();

        let equijoin_where = equijoin
            .map(|(left, right)| Expr::BinaryOp {
                left: Box::new(Expr::CompoundIdentifier(vec![
                    left_alias.clone(),
                    Ident::new(left),
                ])),
                right: Box::new(Expr::CompoundIdentifier(vec![
                    right_alias.clone(),
                    Ident::new(right),
                ])),
                op: BinaryOperator::Eq,
            })
            .reduce(|left, right| Expr::BinaryOp {
                left: Box::new(left),
                right: Box::new(right),
                op: BinaryOperator::And,
            });

        let project_columns = project
            .as_ref()
            .map(|projection| {
                projection
                    .iter()
                    .map(|&col_offset| {
                        if col_offset >= right_first_index {
                            let col_offset = col_offset - right_first_index;

                            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                                right_alias.clone(),
                                Ident::new(other.schema().field(col_offset).name()),
                            ]))
                        } else {
                            SelectItem::UnnamedExpr(Expr::CompoundIdentifier(vec![
                                left_alias.clone(),
                                Ident::new(self.schema().field(col_offset).name()),
                            ]))
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or(vec![SelectItem::Wildcard(
                WildcardAdditionalOptions::default(),
            )]);

        let right = other.scan.clone();

        let mut mutable_self = self.clone();
        mutable_self.scan = ScanHierarchyComponent::Join {
            left: (left_alias, Box::new(mutable_self.scan)),
            right: (right_alias, Box::new(right)),
            filter: Box::new(equijoin_where),
            mode,
            project_columns,
        };
        mutable_self.props = PlanProperties::new(
            EquivalenceProperties::new(project_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        mutable_self.project_schema = project_schema;
        if mutable_self.lock_type.is_none() && other.lock_type.is_some() {
            mutable_self.lock_type = other.lock_type.clone();
        }

        Ok(mutable_self)
    }

    pub fn with_locking(&self, lock_type: LockType) -> Option<Self> {
        if self.lock_type.is_some_and(|lock| lock == lock_type) {
            None
        } else {
            let mut me_cloned = self.clone();
            me_cloned.lock_type = Some(lock_type);
            Some(me_cloned)
        }
    }

    pub fn add_select_column(
        &mut self,
        table: &ResolvedTableReference,
        field: FieldRef,
        alias: Option<&String>,
    ) {
        let new_aliased_name = self.scan.add_select_column(table, field.name(), alias);

        let new_aliased_name = new_aliased_name
            .filter(|aliased_name| {
                aliased_name != field.name() && alias.is_none_or(|alias| alias != aliased_name)
            })
            .or_else(|| alias.cloned());

        let aliased_field = if let Some(alias) = new_aliased_name {
            let field = Arc::unwrap_or_clone(field).with_name(alias);
            Arc::new(field)
        } else {
            field
        };

        let new_schema = self
            .schema()
            .fields()
            .iter()
            .cloned()
            .chain(iter::once(aliased_field))
            .collect::<Fields>();

        self.set_schema(SchemaRef::new(Schema::new(new_schema)))
    }

    pub fn with_sort(&self, mut sort: Vec<SortExpr>, physical_sort: Vec<PhysicalSortExpr>) -> Self {
        let mut cloned_self = self.clone();
        cloned_self.sort.append(&mut sort);

        let mut equivalence_props = cloned_self.props.eq_properties.clone();
        equivalence_props.add_ordering(physical_sort);

        cloned_self.props = cloned_self.props.with_eq_properties(equivalence_props);
        cloned_self
    }
}

#[async_trait]
pub trait DynamicFilter: Send + Sync + Debug {
    async fn execute_filter(
        &self,
        filter: Expr,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<Expr>;
}

#[derive(Debug, Clone)]
pub enum ScanHierarchyComponent {
    Leaf {
        base_query: Box<Select>,
        dynamic_filters: Vec<Arc<dyn DynamicFilter>>, // TODO: dynamic filters!
    },
    Join {
        left: (Ident, Box<ScanHierarchyComponent>),
        mode: JoinType,
        right: (Ident, Box<ScanHierarchyComponent>),
        filter: Box<Option<Expr>>,
        project_columns: Vec<SelectItem>,
    },
}

impl ScanHierarchyComponent {
    fn has_table(&self, table: &ResolvedTableReference) -> bool {
        match self {
            ScanHierarchyComponent::Leaf { base_query, .. } => {
                base_query.from.iter().any(|tbl_with_join| {
                    let relation_is_table = match &tbl_with_join.relation {
                        TableFactor::Table { name, .. } => {
                            object_name_matches_resolved(name, table)
                        }
                        _ => false,
                    };

                    relation_is_table
                        || tbl_with_join.joins.iter().any(|join| match &join.relation {
                            TableFactor::Table { name, .. } => {
                                object_name_matches_resolved(name, table)
                            }
                            _ => false,
                        })
                })
            }
            ScanHierarchyComponent::Join {
                left: (_, left),
                right: (_, right),
                ..
            } => left.has_table(table) || right.has_table(table),
        }
    }

    /// Adds a column to the selection and returns the new aliased name
    fn add_select_column(
        &mut self,
        table: &ResolvedTableReference,
        column_name: &ColumnName,
        alias: Option<&String>,
    ) -> Option<String> {
        if self.has_table(table) {
            match self {
                ScanHierarchyComponent::Leaf { base_query, .. } => {
                    base_query.projection.push(SelectItem::ExprWithAlias {
                        expr: Expr::Identifier(Ident::new(column_name)),
                        alias: Ident::new(alias.unwrap_or(column_name)),
                    });

                    Some(alias.unwrap_or(column_name).clone())
                }
                ScanHierarchyComponent::Join {
                    left: (li, left),
                    right: (ri, right),
                    project_columns,
                    ..
                } => {
                    // Is it on left or right side?
                    let (actual_alias, source_table) = if left.has_table(table) {
                        let alias = left.add_select_column(table, column_name, alias);
                        (alias, li.clone())
                    } else {
                        let alias = right.add_select_column(table, column_name, alias);
                        (alias, ri.clone())
                    };
                    let source_table_name = source_table.value.clone();

                    project_columns.push(SelectItem::ExprWithAlias {
                        expr: Expr::CompoundIdentifier(vec![source_table, Ident::new(column_name)]),
                        alias: Ident::new(alias.unwrap_or(column_name)),
                    });

                    actual_alias.map(|s| format!("{source_table_name}.{s}"))
                }
            }
        } else {
            None
        }
    }

    async fn to_query_inner(
        &self,
        ctx: Arc<TaskContext>,
    ) -> datafusion::error::Result<Box<ast::Query>> {
        match self {
            ScanHierarchyComponent::Leaf {
                base_query,
                dynamic_filters,
            } => {
                let mut base_query = base_query.clone();
                if !dynamic_filters.is_empty() {
                    if let Some(selection) = base_query.selection.take() {
                        let mut selection = selection;
                        for filter in dynamic_filters {
                            selection = filter.execute_filter(selection, ctx.clone()).await?;
                        }
                        base_query.selection = Some(selection);
                    }
                }

                let q = QueryBuilder::default()
                    .body(Box::new(SetExpr::Select(base_query)))
                    .build()?;

                Ok(Box::new(q))
            }
            ScanHierarchyComponent::Join {
                left: (left_alias, left_query),
                mode,
                right: (right_alias, right_query),
                project_columns,
                filter,
            } => {
                let mut left = DerivedRelationBuilder::default();
                left.subquery(Box::pin(left_query.to_query_inner(ctx.clone())).await?)
                    .alias(Some(TableAlias {
                        name: left_alias.clone(),
                        columns: vec![],
                    }))
                    .lateral(false);
                let mut left_rel_builder = RelationBuilder::default();
                left_rel_builder.derived(left);

                let mut right = DerivedRelationBuilder::default();
                right
                    .subquery(Box::pin(right_query.to_query_inner(ctx.clone())).await?)
                    .alias(Some(TableAlias {
                        name: right_alias.clone(),
                        columns: vec![],
                    }))
                    .lateral(false);
                let mut right_rel_builder = RelationBuilder::default();
                right_rel_builder.derived(right);

                let join_filter = filter
                    .clone()
                    .ok_or_else(|| exec_datafusion_err!("No filter for join!"));

                let mut relation_builder = TableWithJoinsBuilder::default();
                relation_builder.relation(left_rel_builder);
                relation_builder.push_join(ast::Join {
                    relation: right_rel_builder.build()?.unwrap(),
                    join_operator: match mode {
                        JoinType::Inner => JoinOperator::Inner(JoinConstraint::On(join_filter?)),
                        JoinType::Left => JoinOperator::Left(JoinConstraint::On(join_filter?)),
                        e => exec_err!("unexpected join mode {e}")?,
                    },
                    global: false,
                });

                let mut qb = SelectBuilder::default();
                qb.push_from(relation_builder);
                qb.projection(project_columns.clone());

                let select = qb.build()?;

                let q = QueryBuilder::default()
                    .body(Box::new(SetExpr::Select(Box::new(select))))
                    .build()?;

                Ok(Box::new(q))
            }
        }
    }
    async fn to_query(
        &self,
        ctx: Arc<TaskContext>,
        fetch: &Option<usize>,
        lock_type: &Option<LockType>,
        sort: &Vec<SortExpr>,
    ) -> datafusion::error::Result<Box<ast::Query>> {
        let mut base_query = self.to_query_inner(ctx).await?;

        if let Some(fetch) = fetch {
            base_query.limit_clause = Some(LimitClause::LimitOffset {
                limit: Some(Expr::Value(ValueWithSpan::from(Value::Number(
                    fetch.to_string(),
                    false,
                )))),
                offset: None,
                limit_by: vec![],
            });
        }

        if let Some(lock_type) = lock_type {
            base_query.locks = vec![ast::LockClause {
                lock_type: lock_type.clone(),
                of: None,
                nonblock: None,
            }];
        }

        if sort.len() > 0 {
            let unparser = Unparser::new(&MySqlDialect {});
            let order_by_expr = sort
                .into_iter()
                .map(|SortExpr { expr, asc, .. }| {
                    let expr = unparser.expr_to_sql(&expr)?;
                    let options = OrderByOptions {
                        asc: Some(*asc),
                        nulls_first: None, // Some(nulls_first),
                    };

                    Ok(OrderByExpr {
                        expr,
                        options,
                        with_fill: None,
                    })
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;

            base_query.order_by = Some(OrderBy {
                interpolate: None,
                kind: OrderByKind::Expressions(order_by_expr),
            });
        }

        Ok(base_query)
    }
}

impl ExecutionPlan for MySqlScanPlan {
    fn name(&self) -> &str {
        MySqlScanPlan::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.project_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        // This plan is a source for rows, it can never have child plans
        vec![]
    }

    fn supports_limit_pushdown(&self) -> bool {
        // logical planning pushes the limit already, but if we don't report pushdown compatibility physical planning re-adds a limit step on top
        // true
        false // TODO
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

    #[cfg(feature = "old-scan-converter")]
    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        use datafusion_table_providers::sql::db_connection_pool::dbconnection::AsyncDbConnection;
        use futures_util::TryStreamExt;
        use futures_util::stream::once;

        let conn = context.get_conn();
        let schema = self.schema();
        let query = self.query.clone();

        let result = async move {
            // This closure must never use `self`, otherwise it introduces lifetime problems.
            // All needed values must be cloned upfront.
            profile!(
                "query_arrow",
                conn.query_arrow(&query.to_string(), &[], Some(schema))
                    .await
                    .map_err(DataFusionError::External)
            )
        };

        let schema = self.schema();
        let result = once(result).try_flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, result)))
    }

    #[cfg(not(feature = "old-scan-converter"))]
    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let schema = self.schema();

        let task_context = context.clone();
        let scan = self.scan.clone();
        let fetch = self.fetch.clone();
        let lock_type = self.lock_type.clone();
        let sort = self.sort.clone();

        let async_block = async move {
            let query = profile!(
                "to_query",
                scan.to_query(task_context.clone(), &fetch, &lock_type, &sort)
                    .await
            )?;
            let conn = task_context.get_conn();

            #[cfg(not(feature = "log-outgoing-queries"))]
            log::trace!("Query: {query}");

            #[cfg(feature = "log-outgoing-queries")]
            log::info!("Outgoing query: {query}");

            profile!(
                "query_arrow",
                crate::arrow_helper::query_arrow(
                    conn,
                    &query.to_string(),
                    schema,
                    context.session_config().batch_size()
                )
            )
        };

        let result = stream::once(async_block).try_flatten();
        let stream_adapter = RecordBatchStreamAdapter::new(self.schema(), result);

        Ok(Box::pin(stream_adapter))
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if self.fetch != limit {
            let mut new_me = self.clone();
            new_me.fetch = limit;
            Some(Arc::new(new_me))
        } else {
            None
        }
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch.clone()
    }
}
