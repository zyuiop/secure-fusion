use crate::planning::physical::to_binary::ToBinaryUdf;
use datafusion::arrow::array::{
    Array, AsArray, BinaryArray, BufferBuilder, OffsetBufferBuilder, RecordBatch,
};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::{DataFusionError, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::Expr;
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct ComputeAadUdf {
    signature: Signature,
    inner: ComputeAadImpl,
}

impl ComputeAadUdf {
    pub const COMPUTE_AAD_UDF_NAME: &str = "__internal__aad__";

    pub fn new(target_column_type: &DataType) -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Nullary,
                    TypeSignature::Variadic(vec![DataType::Binary]),
                ],
                Volatility::Immutable,
            ),
            inner: ComputeAadImpl::new(target_column_type),
        }
    }

    pub fn invoke(target_column_type: &DataType, aad_columns: Vec<Expr>) -> Expr {
        ScalarUDF::new_from_impl(Self::new(target_column_type)).call(
            // we choose to call the to_binary cast here to give the chance to DataFusion to share the casts
            aad_columns
                .into_iter()
                .map(|col| ToBinaryUdf::udf().call(vec![col]))
                .collect(),
        )
    }
}

impl ScalarUDFImpl for ComputeAadUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        Self::COMPUTE_AAD_UDF_NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        self.inner.evaluate_aad(args.number_rows, args.args)
    }
}

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
pub struct ComputeAadImpl {
    base_aad: Vec<u8>,
}

impl ComputeAadImpl {
    pub fn new(target_data_type: &DataType) -> Self {
        Self {
            base_aad: target_data_type.to_string().into_bytes(),
        }
    }
}

impl ComputeAadImpl {
    // TODO: this black magic should have a unit test or two...
    pub fn evaluate_aad(
        &self,
        num_rows: usize,
        columns: Vec<ColumnarValue>,
    ) -> datafusion::common::Result<ColumnarValue> {
        if columns.len() > u8::MAX as usize {
            exec_err!("too many values used for AAD computation")?;
        }

        let mut base = Vec::new();
        base.extend_from_slice(self.base_aad.len().to_be_bytes().as_ref()); // value len
        base.extend_from_slice(self.base_aad.as_ref()); // value
        base.push(columns.len() as u8); // number of values (data is always present and does not count)

        // TODO: it may be more efficient to replace AAD(col1, col2, col3) with AAD(concat(col1, col2, col3)),
        // because datafusion could then optimize to compute concat a single time

        // Shortcuts for trivial case
        if columns.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(base))));
        }

        // TODO: maybe we can make it faster for fixed-sized columns by using concat instead
        // TODO: but concat is not necessarily much faster

        let columns = columns
            .into_iter()
            .map(|col|
                // We keep the cast here as we have other callers which may not respect the UDF contract
                col.cast_to(&DataType::Binary, None)
                    .and_then(|col| col.into_array_of_size(num_rows)))
            .collect::<Result<Vec<_>, _>>()?;

        let columns = columns
            .iter()
            .map(|arr| arr.as_binary())
            .collect::<Vec<&BinaryArray>>();

        // Compute output array size
        let per_row_fixed_size: usize = ((columns.len()) * size_of::<usize>()) /* size of value bytes, one per value + data value */
                + base.len(); /* size of the base data array, incl. data type value and size, and value count byte */

        let inner_data_size: usize = columns.iter().map(|col| col.len() as usize).sum();
        let final_array_size = inner_data_size + (per_row_fixed_size * num_rows);

        let mut output_array = BufferBuilder::<u8>::new(final_array_size);
        let mut output_offsets = OffsetBufferBuilder::<i32>::new(num_rows);

        let mut offset: usize = 0;
        for index in 0..num_rows {
            output_array.append_slice(base.as_slice());
            for column in columns.iter() {
                let v = column.value(index);
                output_array.append_slice(v.len().to_be_bytes().as_ref());
                output_array.append_slice(v);
            }
            let len = output_array.len();
            output_offsets.push_length(len - offset);
            offset = len;
        }

        let array = BinaryArray::try_new(output_offsets.finish(), output_array.finish(), None)?;
        Ok(ColumnarValue::Array(Arc::new(array)))
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ComputeAadExpr {
    children: Vec<Arc<dyn PhysicalExpr>>,
    inner: ComputeAadImpl,
}

impl ComputeAadExpr {
    pub fn new(children: Vec<Arc<dyn PhysicalExpr>>, target_column_type: &DataType) -> Self {
        Self {
            children,
            inner: ComputeAadImpl::new(target_column_type),
        }
    }
}

impl Display for ComputeAadExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("ComputeAadExpr(")?;
        for child in self.children() {
            write!(f, "{}, ", child)?;
        }
        f.write_str(")")
    }
}

impl PhysicalExpr for ComputeAadExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let evaluated = self
            .children
            .iter()
            .map(|child| child.evaluate(batch))
            .collect::<Result<Vec<ColumnarValue>, DataFusionError>>()?;
        self.inner.evaluate_aad(batch.num_rows(), evaluated)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        self.children.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Self {
            children,
            inner: self.inner.clone(),
        }))
    }

    fn fmt_sql(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        _f.write_str("compute_aad(")?;
        for child in self.children.iter() {
            child.fmt_sql(_f)?;
            _f.write_str(", ")?;
        }
        _f.write_str(")")
    }

    fn is_volatile_node(&self) -> bool {
        false
    }
}
