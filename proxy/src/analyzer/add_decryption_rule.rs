use common::metadata::{MetadataReads, MetadataWrites};
use crypto::encrypted_column_meta::EncryptedColumnMeta;
use crypto::planning::physical::decrypt::DecryptUdf;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, ExprSchema};
use datafusion::config::ConfigOptions;
use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection, ScalarUDF};
use datafusion::optimizer::AnalyzerRule;
use nohash_hasher::IntMap;
use rustc_hash::FxHashSet;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub struct AddDecryptionRule;

impl AddDecryptionRule {
    fn rewrite_plan(
        &self,
        plan: LogicalPlan,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        let encrypted_inputs: IntMap<usize, FxHashSet<String>> = plan
            .expressions()
            .iter()
            .flat_map(|expr| expr.column_refs())
            .filter_map(|column_ref| {
                let (input, col) =
                    plan.inputs()
                        .iter()
                        .enumerate()
                        .find_map(|(index, input)| {
                            input
                                .schema()
                                .field_from_column(column_ref)
                                .ok()
                                .map(|field| (index, field))
                        })?;

                if col.is_encrypted() {
                    Some((input, col.name().clone()))
                } else {
                    None
                }
            })
            .fold(IntMap::default(), |mut acc, (input, col)| {
                acc.entry(input).or_insert(FxHashSet::default()).insert(col);
                acc
            });

        if encrypted_inputs.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let transformed_inputs = plan
            .inputs()
            .into_iter()
            .cloned()
            .enumerate()
            .map(|(index, input)| {
                if let Some(decrypt_columns) = encrypted_inputs.get(&index) {
                    let dec_plan = build_decryption_projection(input, decrypt_columns)?;
                    Ok(dec_plan)
                } else {
                    Ok(input)
                }
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?;

        let transformed_node =
            plan.with_new_exprs(plan.expressions().clone(), transformed_inputs)?;

        Ok(Transformed::yes(transformed_node))
    }
}

impl AnalyzerRule for AddDecryptionRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<LogicalPlan> {
        let output_plan = plan.transform_up_with_subqueries(|p| self.rewrite_plan(p))?;

        if output_plan.transformed {
            output_plan.data.recompute_schema()
        } else {
            Ok(output_plan.data)
        }
    }

    fn name(&self) -> &str {
        "add_decryption_rule"
    }
}

fn build_decryption_projection(
    input: LogicalPlan,
    decrypt_columns: &FxHashSet<String>,
) -> datafusion::common::Result<LogicalPlan> {
    let new_columns: (Vec<_>) = input
        .schema()
        .iter()
        .map(|(table, field)| {
            let base = Expr::Column(Column::new(table.cloned(), field.name().clone()));
            if field.is_encrypted()
                && let Some(table) = table
                && decrypt_columns.contains(field.name())
            {
                let mut new_field = field.deref().clone();
                new_field.clear_encrypted();
                let physical_tbl_name = field.table().unwrap();

                Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::new(ScalarUDF::new_from_impl(DecryptUdf::new(
                        Arc::new(new_field),
                        EncryptedColumnMeta::new(
                            physical_tbl_name.clone(),
                            field.name().clone(),
                            field.data_type().clone(),
                        ),
                    ))),
                    vec![base],
                ))
                .alias_qualified(Some(table.clone()), field.name())
            } else {
                base
            }
            // return pair (qualified_column, optional decryption data)
        })
        .collect();

    Ok(LogicalPlan::Projection(Projection::try_new(
        new_columns,
        Arc::new(input),
    )?))
}
