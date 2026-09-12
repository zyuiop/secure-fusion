use crate::filtering::indexable_filter::{
    ColumnOrTuple, ColumnWithCast, EqualityOperator, IndexSelectivity, SupportOptions,
};
use crate::filtering::logical::IndexableLogicalExpr;
use crate::filtering::physical::IndexablePhysicalExpr;
use crate::metadata::{DynamicFilter, EncryptedTableMeta, IndexQueryStrategy};
use crate::providers::table_provider::{MySqlTableProvider, TableStatistics};
use crypto::LongTermKeyManager;
use crypto::row_id::RowIdColumn;
use datafusion::common::stats::Precision;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::sql::ResolvedTableReference;
use datafusion::sql::sqlparser::ast;
use std::convert::identity;
use std::sync::Arc;

pub(crate) struct ResolveIndexResult {
    /// When a filter is entirely supported and no longer needs to be handled by a second filter
    /// node, it's index (from the original `filters` array) is added here.
    /// Currently, this is only true for filters that can be forwarded as-is.
    #[allow(unused)]
    pub(crate) fully_supported_filters: Vec<usize>,

    /// Filters that should be statically forwarded to the server
    pub(crate) forwarded_static: Vec<ast::Expr>,

    /// TODO
    pub(crate) forwarded_dynamic: Vec<Arc<dyn DynamicFilter>>,

    #[allow(unused)]
    pub(crate) index_selectivity: IndexSelectivity,
}

pub struct IndexResolver<'a> {
    parent: &'a MySqlTableProvider,
    table_ref: &'a ResolvedTableReference,
    table_metadata: &'a EncryptedTableMeta,
    row_id: Option<&'a RowIdColumn>,
}

impl<'a> From<&'a MySqlTableProvider> for IndexResolver<'a> {
    fn from(table_provider: &'a MySqlTableProvider) -> Self {
        Self {
            parent: table_provider,
            table_metadata: table_provider.encryption_metadata(),
            table_ref: table_provider.table_reference(),
            row_id: table_provider.get_row_id_column(),
        }
    }
}

impl<'a> IndexResolver<'a> {
    fn logical_support_options(&self) -> SupportOptions<&Expr> {
        SupportOptions {
            supports_not: true,
            supports_or: true,
            is_supported: Box::new(|filter, stats| self.can_forward_exact_logical(filter, stats)),
        }
    }

    fn physical_support_options(&self) -> SupportOptions<&dyn PhysicalExpr> {
        SupportOptions {
            supports_not: true,
            supports_or: true,
            is_supported: Box::new(|filter, stats| self.can_forward_exact_physical(filter, stats)),
        }
    }

    /// Returns true if the passed expression can be directly forwarded to the server
    fn can_forward_exact_logical(
        &self,
        logical_expr: &IndexableLogicalExpr,
        statistics: &TableStatistics,
    ) -> Option<IndexSelectivity> {
        let columns_with_num = match logical_expr {
            IndexableLogicalExpr::Eq(c, op, _) => {
                vec![(
                    c.clone(),
                    if op == &EqualityOperator::Eq {
                        Precision::Exact(1usize)
                    } else {
                        Precision::Absent
                    },
                )]
            }
            IndexableLogicalExpr::InList(ColumnOrTuple::Column(c), vec) => {
                vec![(c.clone(), Precision::Exact(vec.len()))]
            }
            IndexableLogicalExpr::KwSearchLike(c, _) => vec![(c.clone(), Precision::Absent)],

            IndexableLogicalExpr::KwMatch { columns, .. } => {
                columns
                    .iter()
                    .map(ColumnWithCast::column)
                    .map(|column| (column, Precision::Absent)) // no size information - also, we don't care, we're building an encrypted database
                    .collect()
            }
            IndexableLogicalExpr::InList(ColumnOrTuple::Tuple(columns), vec) => {
                let size_info = Precision::Exact(vec.len());
                columns.iter().map(|col| (col.clone(), size_info)).collect()
            }
            IndexableLogicalExpr::Other(expr) => expr
                .column_refs()
                .iter()
                .map(|v| (ColumnWithCast::column(*v), Precision::Absent))
                .collect(),
            _ => return None,
        };

        let (can_fw, n_rows) = columns_with_num.iter().fold(
            (true, statistics.num_rows()),
            |(can_fw, num_rows), (column, selectivity_multiplier)| {
                if !can_fw {
                    // simple forward of the existing result
                    return (false, num_rows);
                }

                let is_same_table = (&column.column.relation).as_ref().is_none_or(|tbl| {
                    tbl.table() == self.table_ref.table.as_ref()
                        && tbl
                            .schema()
                            .is_none_or(|schema| schema == self.table_ref.schema.as_ref())
                });

                let can_fw = is_same_table
                    && !self
                        .table_metadata
                        .is_column_encrypted(column.column.name());

                if !can_fw {
                    // Again, no need to compute anything in this case
                    return (false, num_rows);
                }

                let Some(multiplier) = selectivity_multiplier.get_value() else {
                    // Assume 20% by default
                    return (true, num_rows.with_estimated_selectivity(0.2));
                };

                let Some(column_selectivity) = statistics.column_selectivity(column.column.name())
                else {
                    // Assume 20% by default
                    return (true, num_rows.with_estimated_selectivity(0.2));
                };

                (
                    can_fw,
                    num_rows.with_estimated_selectivity((*multiplier as f64) * column_selectivity),
                )
            },
        );

        if can_fw { Some(n_rows) } else { None }
    }

    /// Returns true if the passed expression can be directly forwarded to the server
    fn can_forward_exact_physical(
        &self,
        physical_expr: &IndexablePhysicalExpr,
        _: &TableStatistics,
    ) -> Option<IndexSelectivity> {
        let columns = match physical_expr {
            IndexablePhysicalExpr::Eq(c, _, _)
            | IndexablePhysicalExpr::InList(ColumnOrTuple::Column(c), _)
            | IndexablePhysicalExpr::KwSearchLike(c, _) => vec![c.clone()],
            IndexablePhysicalExpr::KwMatch { columns, .. } => {
                columns.iter().map(ColumnWithCast::column).collect()
            }
            IndexablePhysicalExpr::InList(ColumnOrTuple::Tuple(columns), _) => columns.clone(),
            _ => return None,
        };

        if columns.iter().all(|column| {
            // The column must exist in this table and not be encrypted
            let is_same_table = (&column.column.relation).as_ref().is_none_or(|tbl| {
                tbl.table() == self.table_ref.table.as_ref()
                    && tbl
                        .schema()
                        .is_none_or(|schema| schema == self.table_ref.schema.as_ref())
            });

            is_same_table
                && !self
                    .table_metadata
                    .is_column_encrypted(column.column.name())
        }) {
            Some(IndexSelectivity::Absent)
        } else {
            None
        }
    }

    pub fn filter_supported_for_expr(
        &self,
        expr: &Expr,
    ) -> datafusion::common::Result<TableProviderFilterPushDown> {
        let stats = self.parent.get_table_statistics()?;
        let tree = IndexableLogicalExpr::from(expr);

        let (supp, mut unsupported, _) =
            tree.split_into_supported_unsupported(&self.logical_support_options(), stats.as_ref());

        if unsupported.is_none() {
            // Nothing unsupported: the entire filter is supported by the table directly
            return Ok(TableProviderFilterPushDown::Exact);
        };

        if supp.is_some() {
            // There is both a supported and unsupported path, which makes this filter inexact
            // already
            return Ok(TableProviderFilterPushDown::Inexact);
        }

        // Try all indices until at least one reports partial support
        for index in self.table_metadata.indices.iter() {
            let Some(unsupp) = unsupported.take() else {
                break;
            };

            let (supported, unsupp, _) = unsupp
                .split_into_supported_unsupported(&index.logical_support_options(), stats.as_ref());

            unsupported = unsupp;
            if supported.is_some() {
                return Ok(TableProviderFilterPushDown::Inexact);
            }
        }

        Ok(TableProviderFilterPushDown::Unsupported)
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(level = "info", skip_all))]
    pub fn resolve_indices_for_logical_filters(
        &self,
        key_manager: &Arc<LongTermKeyManager>,
        filters: &[Expr],
    ) -> datafusion::common::Result<ResolveIndexResult> {
        let table_forward_config = self.logical_support_options();

        let stats = self.parent.get_table_statistics()?;
        let mut total_index_selectivity = stats.num_rows();
        let (supported, unsupported): (Vec<_>, Vec<_>) = filters
            .iter()
            .map(|expr| IndexableLogicalExpr::from(expr))
            .map(|expr| {
                let (supported, unsupported, selectivity) =
                    expr.split_into_supported_unsupported(&table_forward_config, stats.as_ref());

                // We could try to go multiplicative [multiply the selectivity floats] here but
                // I fear this may lower the number too much and become a lie
                total_index_selectivity = total_index_selectivity.min(&selectivity);

                (supported, unsupported)
            })
            .unzip();

        // A filter is fully supported if it has no unsupported component
        let fully_supported_filters_indices: Vec<_> = unsupported
            .iter()
            .enumerate()
            .filter(|(_, v)| v.is_none())
            .map(|(index, _)| index)
            .collect();

        let mut forwarded_filters = supported
            .into_iter()
            .filter_map(identity)
            .map(ast::Expr::from)
            .collect::<Vec<_>>();

        let mut unsupported = unsupported
            .into_iter()
            .filter_map(identity)
            .reduce(|a, b| a.and(b))
            // ensures the range index can correctly predict its size
            .map(|v| v.optimize_cmp());

        let mut dynamic_todo = vec![];

        for index in self.table_metadata.indices.iter() {
            let Some(unsupp) = unsupported.take() else {
                break;
            };

            let (supported, unsupp, selectivity) = unsupp
                .split_into_supported_unsupported(&index.logical_support_options(), stats.as_ref());

            unsupported = unsupp;

            let Some(supported) = supported else {
                continue;
            };

            // Selectivity of an AND clause is the minimum number of rows
            total_index_selectivity = total_index_selectivity.min(&selectivity);

            let result = Arc::clone(index).logical_query(
                self.table_ref,
                self.row_id,
                key_manager,
                supported,
            )?;
            match result {
                IndexQueryStrategy::Fixed(expr) => {
                    forwarded_filters.push(expr);
                }
                IndexQueryStrategy::Dynamic(dynamic_expr) => {
                    dynamic_todo.push(dynamic_expr);
                }
            }
        }

        Ok(ResolveIndexResult {
            fully_supported_filters: fully_supported_filters_indices,
            forwarded_dynamic: dynamic_todo,
            forwarded_static: forwarded_filters,
            index_selectivity: total_index_selectivity,
        })
    }

    pub fn resolve_indices_for_physical_filters(
        &self,
        key_manager: &Arc<LongTermKeyManager>,
        filters: &[&dyn PhysicalExpr],
    ) -> datafusion::common::Result<ResolveIndexResult> {
        let table_forward_config = self.physical_support_options();

        let stats = self.parent.get_table_statistics()?;
        let (supported, unsupported): (Vec<_>, Vec<_>) = filters
            .iter()
            .map(|expr| IndexablePhysicalExpr::from(*expr))
            .map(|expr| {
                let (supp, unsupp, _) =
                    expr.split_into_supported_unsupported(&table_forward_config, stats.as_ref());
                (supp, unsupp)
            })
            .unzip();

        // A filter is fully supported if it has no unsupported component
        let fully_supported_filters_indices: Vec<_> = unsupported
            .iter()
            .enumerate()
            .filter(|(_, v)| v.is_none())
            .map(|(index, _)| index)
            .collect();

        let mut forwarded_filters = supported
            .into_iter()
            .filter_map(identity)
            .filter_map(|v| ast::Expr::try_from(v).ok())
            .collect::<Vec<_>>();

        let mut unsupported = unsupported
            .into_iter()
            .filter_map(identity)
            .reduce(|a, b| a.and(b));

        let mut dynamic_todo = vec![];

        let mut total_index_selectivity = stats.num_rows();
        for index in self.table_metadata.indices.iter() {
            let Some(unsupp) = unsupported.take() else {
                break;
            };

            let Some(physical_support) = index.physical_support_options() else {
                continue;
            };

            let (supported, unsupp, selectivity) =
                unsupp.split_into_supported_unsupported(&physical_support, stats.as_ref());

            unsupported = unsupp;

            let Some(supported) = supported else {
                continue;
            };

            // Selectivity of an AND clause is the minimum number of rows
            total_index_selectivity = total_index_selectivity.min(&selectivity);

            let result = Arc::clone(index).physical_query(
                self.table_ref,
                self.row_id,
                key_manager,
                supported,
            )?;
            match result {
                IndexQueryStrategy::Fixed(expr) => {
                    forwarded_filters.push(expr);
                }
                IndexQueryStrategy::Dynamic(dynamic_expr) => {
                    dynamic_todo.push(dynamic_expr);
                }
            }
        }

        Ok(ResolveIndexResult {
            fully_supported_filters: fully_supported_filters_indices,
            forwarded_dynamic: dynamic_todo,
            forwarded_static: forwarded_filters,
            index_selectivity: total_index_selectivity,
        })
    }
}
