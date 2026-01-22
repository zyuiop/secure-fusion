//! https://dev.mysql.com/doc/refman/9.2/en/group-by-handling.html
//!
//! In SQL:1999 it is allowed to provide a column in the projection even if it does not appear
//! in the GROUP BY clause, under the condition that the column is "functionnally dependent" on the
//! group by clause.

use datafusion::catalog::TableProvider;
use datafusion::common::{
    Constraint, DataFusionError, TableReference, not_impl_err, plan_datafusion_err, plan_err,
};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName,
    ObjectNamePart, Query, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableFactor,
    TableWithJoins, Value, Visit, VisitMut, Visitor, VisitorMut,
};
use nohash_hasher::{IntMap, IntSet, IsEnabled};
use rustc_hash::{FxHashMap, FxHashSet};
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::{ControlFlow, DerefMut};
use std::slice;
use std::sync::Arc;

struct AggregatesRewriter {
    // Contains a set of all tables which are legal to use in the selection clause
    fully_matched_tables: IntSet<TableProviderMapKey>,
    resolved_tables: FxHashMap<TableReference, TableProviderMapKey>,
    group_by_col: FxHashSet<(TableProviderMapKey, usize)>,

    subquery_depth: usize,
    was_rewritten: bool,
}

#[inline]
const fn is_subquery(expr: &Expr) -> bool {
    matches!(expr, Expr::Subquery(_) | Expr::InSubquery { .. })
}

impl AggregatesRewriter {
    fn new(
        fully_matched_tables: IntSet<TableProviderMapKey>,
        group_by_col: FxHashSet<(TableProviderMapKey, usize)>,
        resolved_tables: FxHashMap<TableReference, TableProviderMapKey>,
    ) -> Self {
        Self {
            fully_matched_tables,
            resolved_tables,
            group_by_col,

            subquery_depth: 0,
            was_rewritten: false,
        }
    }

    fn get_reset_was_rewritten(&mut self) -> bool {
        let was_rewritten = self.was_rewritten;
        self.was_rewritten = false;
        self.subquery_depth = 0;

        was_rewritten
    }
}

impl VisitorMut for AggregatesRewriter {
    type Break = DataFusionError;

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if is_subquery(expr) {
            self.subquery_depth += 1;
        }

        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        if self.subquery_depth > 0 {
            if is_subquery(expr) {
                self.subquery_depth -= 1;
            }

            return ControlFlow::Continue(());
        }

        let Some(resolved_column) =
            resolve_table_for_expr(expr, &self.resolved_tables, "projection")
        else {
            return ControlFlow::Continue(());
        };

        let should_wrap = resolved_column.map(|(table, column)| {
            self.fully_matched_tables.contains(&table)
                && !self.group_by_col.contains(&(table, column))
        });

        match should_wrap {
            Ok(true) => {
                self.was_rewritten = true;

                // We need to rewrite the expression to introduce a function call.
                let original = expr.clone();
                *expr = Expr::Function(Function {
                    name: ObjectName(vec![ObjectNamePart::Identifier("nth_value".into())]),
                    uses_odbc_syntax: false,
                    parameters: FunctionArguments::None,
                    args: FunctionArguments::List(FunctionArgumentList {
                        duplicate_treatment: None,
                        clauses: vec![],
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(original)),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::Number("1".to_string(), false).into(),
                            ))),
                        ],
                    }),
                    filter: None,
                    null_treatment: None,
                    over: None,
                    within_group: vec![],
                });
                ControlFlow::Continue(())
            }
            Ok(false) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(e),
        }
    }
}

pub async fn rewrite_aggregates(
    query: &mut Query,
    session: &SessionContext,
) -> datafusion::common::Result<()> {
    // Only target queries that have a GROUP BY clause (otherwise, we don't care)
    let SetExpr::Select(sel) = query.body.deref_mut() else {
        return Ok(());
    };

    let GroupByExpr::Expressions(expr, _) = &sel.group_by else {
        return Ok(());
    };

    // Only process raw columns (for now?)
    let group_by_columns = expr
        .iter()
        .filter_map(|expr| match expr {
            Expr::Identifier(ident) => Some(compound_identifier_to_table_reference_and_column(
                slice::from_ref(ident),
            )),
            Expr::CompoundIdentifier(ident) => Some(
                compound_identifier_to_table_reference_and_column(&ident[..]),
            ),
            _ => None,
        })
        .collect::<Vec<_>>();

    // Ensure we have at least one GROUP BY column to rewrite
    if group_by_columns.is_empty() {
        return Ok(());
    }

    // Resolve tables in query to verify the grouping expressions
    let (resolved_tables, join_conditions) = simple_resolve_tables(session, &sel.from).await?;

    // Resolve each column in group by
    let group_by_columns = group_by_columns
        .into_iter()
        .map(|(table_ref, column_name)| {
            resolve_table(
                &table_ref,
                &column_name,
                &resolved_tables,
                "group by clause",
            )
        })
        .collect::<Result<FxHashSet<_>, _>>()?;

    // What columns are allowed?
    // To know this, we need to extract the schema of the tables for which the entire primary key is
    // present in the group by clause.
    // We can use an intmap here because we know we're hashing an address
    let mut columns_by_table = IntMap::default();
    for (table, column_index) in &group_by_columns {
        // we can already skip tables that have no primary key constainta
        if table.0.constraints().is_none() {
            continue;
        }

        columns_by_table
            .entry(table.clone())
            .or_insert_with(IntSet::default)
            .insert(*column_index);
    }

    for (table, column) in join_conditions.get_columns_in_conditions() {
        columns_by_table
            .entry(table.clone())
            .or_insert_with(IntSet::default)
            .insert(*column);
    }

    // This set will contain the list of tables for which all columns are safe to use in the projection
    let mut fully_matched_tables = IntSet::default();
    'top_loop: for (table, columns_in_group_by) in columns_by_table.into_iter() {
        // Is the full primary key present?
        let Some(constraints) = table.0.constraints() else {
            continue; // Should be unreachable
        };

        for constraint in constraints.iter() {
            let columns_in_constraint = match constraint {
                Constraint::PrimaryKey(columns) | Constraint::Unique(columns) => columns,
            };

            if columns_in_constraint
                .iter()
                .all(|column| columns_in_group_by.contains(column))
            {
                // This constraint is matched - we know
                fully_matched_tables.insert(table);
                continue 'top_loop;
            }
        }
    }

    // Rewrite/resolve wildcards
    let mut wildcards = sel
        .projection
        .iter()
        .enumerate()
        .filter_map(|(index, project)| match project {
            SelectItem::QualifiedWildcard(qual, _) => {
                let new_columns = resolve_wildcard(qual, &resolved_tables);
                let result = new_columns.map(|vec| (index, vec));
                Some(result)
            }
            _ => None,
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Rewrite wildcards from last to first
    while let Some((pos, mut new_select_items)) = wildcards.pop() {
        sel.projection.remove(pos);

        // Insert items from last to first
        // This is sadly quite inefficient as we shift all elements each time
        while let Some(item) = new_select_items.pop() {
            sel.projection.insert(pos, item);
        }
    }

    let mut rewriter =
        AggregatesRewriter::new(fully_matched_tables, group_by_columns, resolved_tables);

    for project in sel.projection.iter_mut() {
        let initial_name = if let SelectItem::UnnamedExpr(e) = project {
            match e {
                Expr::CompoundIdentifier(identifier) => identifier.last().cloned(),
                Expr::Identifier(ident) => Some(ident.clone()),
                _ => None,
            }
        } else {
            None
        };

        let result = project.visit(&mut rewriter);
        let was_rewritten = rewriter.get_reset_was_rewritten();

        if let Some(err) = result.break_value() {
            Err(err)?;
        }

        if was_rewritten && let Some(alias) = initial_name {
            let SelectItem::UnnamedExpr(expr) = project.clone() else {
                unreachable!();
            };

            *project = SelectItem::ExprWithAlias { expr, alias }
        }
    }

    #[cfg(not(feature = "log_queries"))]
    log::trace!("Rewritten aggregate query to: {query}");

    #[cfg(feature = "log_queries")]
    log::info!("Rewritten aggregate query to: {query}");

    Ok(())
}

fn resolve_wildcard(
    wildcard: &SelectItemQualifiedWildcardKind,
    resolved_tables: &FxHashMap<TableReference, TableProviderMapKey>,
) -> datafusion::common::Result<Vec<SelectItem>> {
    match wildcard {
        SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
            let table_ref = object_name_to_table_reference(object_name).ok_or_else(|| {
                plan_datafusion_err!("could not parse table reference {object_name}")
            })?;

            let existing_table = resolved_tables.get(&table_ref).ok_or_else(|| {
                plan_datafusion_err!("could not resolve table reference {table_ref}")
            })?;

            let mut table_ref_ident: Vec<Ident> = vec![];
            if let Some(schema) = table_ref.schema() {
                table_ref_ident.push(schema.into());
            };
            table_ref_ident.push(table_ref.table().into());

            let result = existing_table
                .0
                .schema()
                .fields()
                .iter()
                .map(|field| {
                    let mut identifier = table_ref_ident.clone();
                    identifier.push(field.name().as_str().into());

                    SelectItem::ExprWithAlias {
                        expr: Expr::CompoundIdentifier(identifier),
                        alias: field.name().as_str().into(),
                    }
                })
                .collect::<Vec<_>>();

            Ok(result)
        }
        other => not_impl_err!("unimplemented wildcard kind: {other}")?,
    }
}

fn resolve_table_for_expr(
    expr: &Expr,
    resolved_tables: &FxHashMap<TableReference, TableProviderMapKey>,
    clause: &str,
) -> Option<datafusion::common::Result<(TableProviderMapKey, usize)>> {
    match expr {
        Expr::Identifier(ident) => Some(resolve_table_for_identifier(
            slice::from_ref(ident),
            resolved_tables,
            clause,
        )),
        Expr::CompoundIdentifier(ident) => Some(resolve_table_for_identifier(
            &ident[..],
            resolved_tables,
            clause,
        )),
        _ => None,
    }
}

fn resolve_table_for_identifier(
    identifier: &[Ident],
    resolved_tables: &FxHashMap<TableReference, TableProviderMapKey>,
    clause: &str,
) -> datafusion::common::Result<(TableProviderMapKey, usize)> {
    let (refer, col) = compound_identifier_to_table_reference_and_column(identifier);
    resolve_table(&refer, &col, resolved_tables, clause)
}

fn resolve_table(
    table_ref: &Option<TableReference>,
    column_name: &String,
    resolved_tables: &FxHashMap<TableReference, TableProviderMapKey>,
    clause: &str,
) -> datafusion::common::Result<(TableProviderMapKey, usize)> {
    let res = if let Some(table_ref) = table_ref {
        // Simple case: just take the resolved table!
        let table = resolved_tables
            .get(&table_ref)
            .ok_or_else(|| plan_datafusion_err!("Unknown table {table_ref} in {clause}"))?
            .clone();

        let column_index = table.0.schema().index_of(&column_name)?;

        (table, column_index)
    } else {
        let table_candidates = resolved_tables
            .iter()
            .filter_map(|(table_reference, table)| {
                if let Ok(column_pos) = table.0.schema().index_of(column_name) {
                    Some((table_reference, table, column_pos))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let table_candidates = if table_candidates.len() == 2 {
            // If we have two candidates, but the two are equal [minus a schema], return a vector with only one
            let (schema, table, column) = &table_candidates[0];

            if schema.resolved_eq(table_candidates[1].0) {
                vec![(*schema, *table, *column)]
            } else {
                table_candidates
            }
        } else {
            table_candidates
        };

        if table_candidates.len() == 1 {
            let (_, table, pos) = table_candidates[0];
            (table.clone(), pos)
        } else if table_candidates.is_empty() {
            plan_err!("No table found for field {column_name} in {clause}")?
        } else {
            let candidates = table_candidates
                .iter()
                .map(|(tref, _, _)| tref.to_quoted_string())
                .collect::<Vec<_>>()
                .join(", ");
            plan_err!(
                "Ambiguous field {column_name} in {clause}. Can refer to any of the following matching tables: {candidates}"
            )?
        }
    };

    Ok(res)
}

/// A wrapper for TableProviders that compares based on address only
#[derive(Clone)]
struct TableProviderMapKey(Arc<dyn TableProvider>);

impl Debug for TableProviderMapKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TableProvider({:x})", Arc::as_ptr(&self.0).addr())
    }
}

/// SAFETY: Requires implementing Hash with a single call to write_u...
impl IsEnabled for TableProviderMapKey {}

impl Hash for TableProviderMapKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(Arc::as_ptr(&self.0).addr());
    }
}

impl PartialEq for TableProviderMapKey {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for TableProviderMapKey {}

struct JoinFilters(FxHashSet<(TableProviderMapKey, usize)>);

impl JoinFilters {
    fn new() -> Self {
        Self(FxHashSet::default())
    }

    fn add_condition(&mut self, condition: (TableProviderMapKey, usize)) {
        self.0.insert(condition);
    }

    fn get_columns_in_conditions(&self) -> &FxHashSet<(TableProviderMapKey, usize)> {
        &self.0
    }
}

struct JoinFilterCollector<'a> {
    expressions: JoinFilters,
    table_names: &'a FxHashMap<TableReference, TableProviderMapKey>,
}

impl<'a> JoinFilterCollector<'a> {
    fn new(table_names: &'a FxHashMap<TableReference, TableProviderMapKey>) -> Self {
        Self {
            table_names,
            expressions: JoinFilters::new(),
        }
    }

    fn finish(self) -> JoinFilters {
        self.expressions
    }

    fn visit_expr_faillible(&mut self, expr: &Expr) -> datafusion::common::Result<()> {
        let Expr::BinaryOp { left, right, op } = expr else {
            return Ok(());
        };

        if op != &BinaryOperator::Eq {
            return Ok(());
        }

        // Coerce left and right into columns
        let resolved_left = resolve_table_for_expr(left.as_ref(), &self.table_names, "JOIN ON");
        if let Some(resolved_left) = resolved_left {
            self.expressions.add_condition(resolved_left?)
        }

        let resolved_right = resolve_table_for_expr(right.as_ref(), &self.table_names, "JOIN ON");
        if let Some(resolved_right) = resolved_right {
            self.expressions.add_condition(resolved_right?)
        }

        Ok(())
    }
}

impl<'a> Visitor for JoinFilterCollector<'a> {
    type Break = DataFusionError;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match self.visit_expr_faillible(expr) {
            Ok(_) => ControlFlow::Continue(()),
            Err(e) => ControlFlow::Break(e),
        }
    }
}

async fn simple_resolve_tables(
    session: &SessionContext,
    from: &[TableWithJoins],
) -> datafusion::common::Result<(FxHashMap<TableReference, TableProviderMapKey>, JoinFilters)> {
    let mut table_names = FxHashMap::default();

    let mut add_table = |reference: TableReference, table_provider: TableProviderMapKey| {
        // We should not have full table references
        assert!(!matches!(reference, TableReference::Full { .. }));

        // Insert an unqualified reference first
        if let (TableReference::Partial { table, .. }) = &reference {
            table_names.insert(
                TableReference::Bare {
                    table: Arc::clone(table),
                },
                table_provider.clone(),
            );
        }

        table_names.insert(reference, table_provider);
    };

    let mut join_constraints = Vec::new();
    for tbl_with_join in from {
        for join in &tbl_with_join.joins {
            let Some((reference, relation)) = resolve_table_factor(&join.relation, session).await?
            else {
                continue;
            };

            add_table(reference, relation);

            // ON may contain "equivalence" properties, e.g. two columns from two relations are equivalent
            let Some(constraints) = (match &join.join_operator {
                JoinOperator::Join(ct) | JoinOperator::Inner(ct) | JoinOperator::Left(ct) => {
                    Some(ct)
                }
                _ => None,
            }) else {
                continue;
            };

            let JoinConstraint::On(expr) = constraints else {
                continue;
            };

            join_constraints.push(expr.clone());
        }

        if let Some((reference, relation)) =
            resolve_table_factor(&tbl_with_join.relation, session).await?
        {
            add_table(reference, relation);
        };
    }

    let mut equivalence_visitor = JoinFilterCollector::new(&table_names);
    for constraint in join_constraints.into_iter() {
        let r = <Expr as Visit>::visit(&constraint, &mut equivalence_visitor);
        if let ControlFlow::Break(e) = r {
            return Err(e);
        }
    }
    let equivalences = equivalence_visitor.finish();

    Ok((table_names, equivalences))
}

async fn resolve_table_factor(
    table_factor: &TableFactor,
    session: &SessionContext,
) -> datafusion::common::Result<Option<(TableReference, TableProviderMapKey)>> {
    let TableFactor::Table { name, alias, .. } = table_factor else {
        return Ok(None);
    };

    let Some(mut table_reference) = object_name_to_table_reference(&name) else {
        return Ok(None);
    };

    let table = session.table_provider(table_reference.clone()).await?;
    let table = TableProviderMapKey(table);

    if let Some(alias) = alias {
        // If there is an alias, modify the returned relation
        table_reference = TableReference::Bare {
            table: alias.name.value.as_str().into(),
        };
    }

    Ok(Some((table_reference, table)))
}

fn object_name_to_table_reference(object_name: &ObjectName) -> Option<TableReference> {
    match &object_name.0[..] {
        [
            ..,
            ObjectNamePart::Identifier(schema),
            ObjectNamePart::Identifier(table),
        ] => Some(TableReference::Partial {
            schema: schema.value.as_str().into(),
            table: table.value.as_str().into(),
        }),
        [ObjectNamePart::Identifier(table)] => Some(TableReference::Bare {
            table: table.value.as_str().into(),
        }),
        _ => None,
    }
}

fn compound_identifier_to_table_reference_and_column(
    compound_identifier: &[Ident],
) -> (Option<TableReference>, String) {
    match &compound_identifier {
        [.., schema, table, column] => (
            Some(TableReference::Partial {
                schema: schema.value.as_str().into(),
                table: table.value.as_str().into(),
            }),
            column.value.clone(),
        ),
        [table, column] => (
            Some(TableReference::Bare {
                table: table.value.as_str().into(),
            }),
            column.value.clone(),
        ),
        [column] => (None, column.value.clone()),
        _ => panic!("empty identifier"),
    }
}
