use crate::metadata::ColumnName;
use common::conversions::column_def_ext::ColumnDefExt;
use datafusion::logical_expr::sqlparser::ast;
use datafusion::logical_expr::sqlparser::ast::TableConstraint::PrimaryKey;
use datafusion::logical_expr::sqlparser::ast::{CreateTable, PrimaryKeyConstraint};

pub(crate) mod catalog_provider;
mod dummy_source;
mod parser;
pub(crate) mod schema_provider;
pub(crate) mod table_provider;

pub fn build_primary_key(statement: &CreateTable) -> Vec<ColumnName> {
    let pk = statement
        .constraints
        .iter()
        .find(|constraint| matches!(constraint, PrimaryKey { .. }));

    match pk {
        Some(PrimaryKey(PrimaryKeyConstraint { columns, .. })) => columns
            .iter()
            .map(|col| {
                let ident = &col.column.expr;

                if let ast::Expr::Identifier(i) = ident {
                    i.value.clone()
                } else {
                    panic!("unhandled ident {ident}");
                }
            })
            .collect(),
        _ => statement
            .columns
            .iter()
            .filter_map(|col| {
                if col.is_primary_key() {
                    Some(col.name.value.clone())
                } else {
                    None
                }
            })
            .collect(),
    }
}
