use datafusion::logical_expr::sqlparser::ast::{
    AlterTable, AssignmentTarget, CheckConstraint, Expr, ForeignKeyConstraint, IndexConstraint,
    ObjectName, ObjectNamePart, Statement, TableConstraint, UniqueConstraint, Update,
};
use datafusion::sql::sqlparser::ast::{
    FullTextOrSpatialConstraint, Ident, PrimaryKeyConstraint, VisitMut, VisitorMut,
};
use std::ops::ControlFlow;

pub struct Normalizer;

fn normalize_object_names(object_name: &mut [ObjectName]) {
    object_name.iter_mut().for_each(normalize_object_name)
}

fn normalize_object_name(object_name: &mut ObjectName) {
    for onp in object_name.0.iter_mut() {
        match onp {
            ObjectNamePart::Identifier(ident) => {
                ident.value = ident.value.to_ascii_lowercase();
            }
            ObjectNamePart::Function(func) => {
                func.name.value = func.name.value.to_ascii_lowercase();
            }
        }
    }
}

fn normalize_identifier(identifier: &mut Ident) {
    identifier.value = identifier.value.to_ascii_lowercase()
}

fn normalize_identifiers(identifier: &mut [Ident]) {
    identifier.iter_mut().for_each(normalize_identifier)
}

impl VisitorMut for Normalizer {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<Self::Break> {
        normalize_object_name(relation);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Identifier(ident) => normalize_identifier(ident),
            Expr::CompoundIdentifier(ident) => normalize_identifiers(ident.as_mut_slice()),
            _ => {}
        }

        ControlFlow::Continue(())
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Update(Update { assignments, .. }) => {
                assignments
                    .iter_mut()
                    .for_each(|assgt| match &mut assgt.target {
                        AssignmentTarget::ColumnName(cn) => normalize_object_name(cn),
                        AssignmentTarget::Tuple(cns) => {
                            cns.iter_mut().for_each(normalize_object_name)
                        }
                    });
            }
            Statement::AlterTable(AlterTable {
                name, operations, ..
            }) => {
                normalize_object_name(name);
                operations.iter_mut().for_each(|op| {
                    // Let's hope this is enough...
                    let _ = op.visit(self);
                });
            }
            Statement::CreateIndex(index) => {
                normalize_object_name(&mut index.table_name);
                normalize_object_names(index.name.as_mut_slice());
                index.columns.iter_mut().for_each(|col| {
                    let _ = col.column.expr.visit(self);
                });
            }
            Statement::Drop { table, names, .. } => {
                normalize_object_names(table.as_mut_slice());
                normalize_object_names(names.as_mut_slice());
            }
            Statement::CreateDatabase { db_name, clone, .. } => {
                normalize_object_name(db_name);
                normalize_object_names(clone.as_mut_slice());
            }
            Statement::CreateTable(tbl) => {
                normalize_object_name(&mut tbl.name);
                normalize_object_names(tbl.clone.as_mut_slice());
                tbl.columns
                    .iter_mut()
                    .for_each(|column| normalize_identifier(&mut column.name));

                tbl.primary_key.iter_mut().for_each(|expr| {
                    let _ = expr.visit(self);
                });

                tbl.constraints
                    .iter_mut()
                    .for_each(|constraint| match constraint {
                        TableConstraint::FulltextOrSpatial(FullTextOrSpatialConstraint {
                            opt_index_name: index_name,
                            columns,
                            ..
                        })
                        | TableConstraint::Index(IndexConstraint {
                            columns,
                            name: index_name,
                            ..
                        })
                        | TableConstraint::PrimaryKey(PrimaryKeyConstraint {
                            index_name,
                            columns,
                            ..
                        })
                        | TableConstraint::Unique(UniqueConstraint {
                            index_name,
                            columns,
                            ..
                        }) => {
                            normalize_identifiers(index_name.as_mut_slice());
                            columns.iter_mut().for_each(|col| {
                                let _ = col.column.expr.visit(self);
                            });
                        }
                        TableConstraint::ForeignKey(ForeignKeyConstraint {
                            index_name,
                            columns,
                            foreign_table,
                            referred_columns,
                            ..
                        }) => {
                            normalize_identifiers(index_name.as_mut_slice());
                            normalize_identifiers(columns.as_mut_slice());
                            normalize_identifiers(referred_columns.as_mut_slice());
                            normalize_object_name(foreign_table);
                        }
                        TableConstraint::Check(CheckConstraint { expr, .. }) => {
                            let _ = expr.visit(self);
                        }
                    });
            }
            _ => {}
        };
        ControlFlow::Continue(())
    }
}
