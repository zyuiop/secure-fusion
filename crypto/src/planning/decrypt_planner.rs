use crate::planning::physical::compute_aad::ComputeAadExpr;
use crate::planning::physical::decrypt::DecryptExpr;
use crate::planning::physical::from_binary::FromBinaryExpr;
use crate::{CipherContext, LongTermKeyManager};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::ResolvedTableReference;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::projection::ProjectionExpr;
use std::sync::Arc;

pub fn project_decrypt<'a>(
    table_ref: &'a ResolvedTableReference,
    schema: &'a Schema,
    decrypt_columns: &'a [Option<(Arc<str>, DataType)>],
    keys_manager: &'a LongTermKeyManager,
    associated_data_columns: Vec<Arc<dyn PhysicalExpr>>,
) -> datafusion::error::Result<Vec<ProjectionExpr>> {
    // Map the output schema!
    let project = schema
        .fields()
        .iter()
        .zip(decrypt_columns.iter())
        .enumerate()
        .map(|(idx, (output_col, decrypt_cfg))| {
            let column_source: Arc<dyn PhysicalExpr> =
                Arc::new(Column::new(output_col.name(), idx));

            let expr = match decrypt_cfg {
                None => column_source,
                Some((column, data_type)) => {
                    let cipher = keys_manager.get_cipher(&CipherContext::TableColumn {
                        table_context: table_ref.clone(),
                        column_name: column.clone(),
                    });

                    let aad = Arc::new(ComputeAadExpr::new(
                        associated_data_columns.clone(),
                        data_type,
                    ));
                    let decrypt = DecryptExpr::new(column_source, aad, cipher);

                    Arc::new(FromBinaryExpr::new(
                        data_type.clone(),
                        output_col.is_nullable(),
                        Arc::new(decrypt),
                    ))
                }
            };

            Ok(ProjectionExpr::new(expr, output_col.name().clone()))
        });

    project.collect()
}
