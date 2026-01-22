use crate::cipher::AssociatedData;
use crate::encrypted_column_meta::EncryptedColumnMeta;
use crate::key_manager::KeyManager;
use crate::planning::physical::decrypt::{DECRYPT_PSEUDOFUNC_NAME, DecryptExpr, DecryptUdf};
use crate::planning::physical::from_binary::FromBinaryExpr;
use crate::{CipherContext, KeyManagerGetter, LongTermKeyManager};
use common::profile;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::plan_datafusion_err;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use std::sync::Arc;

pub fn project_decrypt<'a>(
    schema: &'a Schema,
    decrypt_columns: &'a [Option<EncryptedColumnMeta>],
    keys_manager: &'a LongTermKeyManager,
) -> datafusion::error::Result<impl Iterator<Item = ProjectionExpr> + 'a> {
    // Map the output schema!
    let project = schema
        .fields()
        .iter()
        .zip(decrypt_columns.iter())
        .enumerate()
        .map(|(idx, (output_col, decrypt_cfg))| {
            let base = Column::new(output_col.name(), idx);

            let expr = match decrypt_cfg {
                None => Arc::new(base) as Arc<dyn PhysicalExpr>,
                Some(meta) => {
                    expr_for_column(meta, keys_manager, Arc::new(base), output_col.is_nullable())
                }
            };

            ProjectionExpr::new(expr, output_col.name().clone())
        });

    Ok(project)
}

/// Replaces the __decrypt__ pseudofunctions in an executable plan.
/// Must be run once on a plan before it is executed.
pub fn replace_decryptions_in_plan(
    plan: Arc<dyn ExecutionPlan>,
    context: &dyn KeyManagerGetter,
) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
    let keys_manager = context.get_long_term_keys_manager();
    let transformed = profile!(
        "replace_decryptions",
        plan.transform(|node| replace_decryption_in_node(node, keys_manager.as_ref()))?
    );
    Ok(transformed.data)
}

fn replace_decryption_in_node(
    plan: Arc<dyn ExecutionPlan>,
    keys_manager: &LongTermKeyManager,
) -> datafusion::common::Result<Transformed<Arc<dyn ExecutionPlan>>> {
    let Some(project) = plan.as_any().downcast_ref::<ProjectionExec>() else {
        return Ok(Transformed::no(plan));
    };

    let mut transformed = false;
    let mut expressions = Vec::with_capacity(project.expr().len());
    for projection_expr in project.expr() {
        let ProjectionExpr { expr, alias } = projection_expr;
        let transf = Arc::clone(expr).transform_up(|expr| {
            let Some(udf) = expr.as_any().downcast_ref::<ScalarFunctionExpr>() else {
                return Ok(Transformed::no(expr));
            };

            if udf.name() != DECRYPT_PSEUDOFUNC_NAME {
                return Ok(Transformed::no(expr));
            };

            let child = Arc::clone(udf.children()[0]);
            let decrypt_udf = udf
                .fun()
                .inner()
                .as_any()
                .downcast_ref::<DecryptUdf>()
                .ok_or_else(|| plan_datafusion_err!("invalid decryption function found"))?;

            Ok(Transformed::yes(expr_for_column(
                &decrypt_udf.meta,
                keys_manager,
                child,
                decrypt_udf.output_field.is_nullable(),
            )))
        })?;

        expressions.push(ProjectionExpr {
            expr: transf.data,
            alias: alias.clone(),
        });
        transformed |= transf.transformed;
    }

    if transformed {
        let plan = ProjectionExec::try_new(expressions, Arc::clone(project.input()))?;
        Ok(Transformed::yes(Arc::new(plan)))
    } else {
        Ok(Transformed::no(plan))
    }
}

fn expr_for_column(
    EncryptedColumnMeta {
        table,
        column,
        original_type,
    }: &EncryptedColumnMeta,
    keys_manager: &LongTermKeyManager,
    child: Arc<dyn PhysicalExpr>,
    nullable: bool,
) -> Arc<dyn PhysicalExpr> {
    let cipher = keys_manager.get_cipher(&CipherContext::TableColumn {
        table_name: table,
        column_name: column,
    }); // TODO handle error

    let aad = AssociatedData::column_with_type(table, column, original_type);

    let decrypt = DecryptExpr::new(child, cipher, aad);
    let cast = FromBinaryExpr::new(original_type.clone(), nullable, Arc::new(decrypt));

    Arc::new(cast) as Arc<dyn PhysicalExpr>
}
