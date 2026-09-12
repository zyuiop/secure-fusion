use common::metadata::{MetadataReads, MetadataWrites};
use crypto::planning::physical::compute_aad::ComputeAadUdf;
use crypto::planning::physical::decrypt::DecryptUdf;
use crypto::{CipherContext, LongTermKeyManager};
use datafusion::common::tree_node::Transformed;
use datafusion::common::{Column, DataFusionError};
use datafusion::config::ConfigOptions;
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use log::warn;
use mysql_backend::MySqlTableProvider;
use rustc_hash::FxHashSet;
use std::fmt::Debug;
use std::ops::Deref;
use std::sync::Arc;

#[derive(Debug)]
pub struct AddDecryptionRule(pub Arc<LongTermKeyManager>);

impl AddDecryptionRule {
    fn rewrite_plan(
        &self,
        plan: LogicalPlan,
    ) -> datafusion::common::Result<Transformed<LogicalPlan>> {
        let plan = plan.recompute_schema()?;

        // New strategy: just push everything down and let the optimizer move projections as late as possible
        let encrypted_outputs = plan
            .schema()
            .fields()
            .iter()
            .filter_map(|field_ref| {
                if field_ref.is_encrypted() {
                    Some(field_ref.name().clone())
                } else {
                    None
                }
            })
            .collect::<FxHashSet<_>>();

        if encrypted_outputs.is_empty() {
            return Ok(Transformed::no(plan));
        }

        let transformed_plan =
            build_decryption_projection(plan, &encrypted_outputs, self.0.clone())?;
        Ok(Transformed::yes(transformed_plan))
    }
}

impl AnalyzerRule for AddDecryptionRule {
    fn analyze(
        &self,
        plan: LogicalPlan,
        _: &ConfigOptions,
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
    key_manager: Arc<LongTermKeyManager>,
) -> datafusion::common::Result<LogicalPlan> {
    // Is this a scan plan? [temporary - try to find a better way to avoid dependencies by proxy to backend!]
    let table_provider = if let LogicalPlan::TableScan(scan) = &input
        && let Some(source) = scan
            .source
            .as_ref()
            .as_any()
            .downcast_ref::<DefaultTableSource>()
        && let Some(source) = source
            .table_provider
            .as_ref()
            .as_any()
            .downcast_ref::<MySqlTableProvider>()
    {
        source
    } else {
        warn!(
            "Found a possibly encrypted column {decrypt_columns:?} produced by non-scan plan\n{input}"
        );
        return Ok(input);
    };

    let new_columns: Vec<_> = input
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
                let aad =
                    ComputeAadUdf::invoke(new_field.data_type(), table_provider.get_aad_source());

                Ok(DecryptUdf::invoke(
                    Arc::new(new_field),
                    CipherContext::TableColumn {
                        table_context: table_provider.table_reference().clone(),
                        column_name: field.name().clone().into(),
                    },
                    key_manager.clone(),
                    base,
                    aad,
                )
                .alias_qualified(Some(table.clone()), field.name()))
            } else {
                Ok(base)
            }
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;

    Ok(LogicalPlan::Projection(Projection::try_new(
        new_columns,
        Arc::new(input),
    )?))
}
