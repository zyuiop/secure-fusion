use crate::identifiers::StableIdentifiersGenerator;
use crate::planning::physical::to_binary::ToBinaryExpr;
use common::conversions::column_def_ext::ColumnDefExt;
use common::conversions::datatypes::ArrowDatatypeConverter;
use datafusion::arrow::array::{AsArray, BinaryArray, BinaryBuilder, RecordBatch};
use datafusion::arrow::buffer::{Buffer, OffsetBuffer};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{ScalarValue, plan_err};
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExpr;
use datafusion::sql::sqlparser::ast;
use datafusion::sql::sqlparser::ast::{ColumnDef, Ident};
use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

const DEFAULT_ROWID_COLUMN_NAME: &str = "__sf_rowid";
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowIdColumn {
    column_name: String,
    size: RowIdColumnSize,
    source: RowIdColumnSource,
}

impl RowIdColumn {
    /// Tries to return a row-id column if the table definition already has a column that could fit
    pub fn get_natural_if_any(primary_key: &[&ColumnDef]) -> Option<RowIdColumn> {
        if primary_key.len() != 1
            || primary_key.iter().any(|k| {
                k.is_nullable() || k.is_auto_increment() || k.get_default_value().is_some()
            })
        {
            // We cannot do anything with auto-generated or composite
            return None;
        }

        let column_name = primary_key[0].name.value.clone();
        let datatype = ArrowDatatypeConverter
            .convert_data_type(&primary_key[0].data_type)
            .ok()?;
        // Is the type supported
        Some(match &datatype {
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32 => Self::primary(column_name, RowIdColumnSize::Size4Bytes, datatype),
            DataType::Int64 | DataType::UInt64 => {
                Self::primary(column_name, RowIdColumnSize::Size8Bytes, datatype)
            }
            DataType::FixedSizeBinary(sz) if *sz <= 4 => {
                Self::primary(column_name, RowIdColumnSize::Size4Bytes, datatype)
            }
            DataType::FixedSizeBinary(sz) if *sz <= 8 => {
                Self::primary(column_name, RowIdColumnSize::Size8Bytes, datatype)
            }
            DataType::FixedSizeBinary(sz) if *sz <= 12 => {
                Self::primary(column_name, RowIdColumnSize::Size12Bytes, datatype)
            }
            DataType::FixedSizeBinary(sz) if *sz <= 16 => {
                Self::primary(column_name, RowIdColumnSize::Size16Bytes, datatype)
            }
            _ => return None,
        })
    }

    /// Create a RowId column in a table given the definition of its primary key
    pub fn create_for_table(
        primary_key: &[&ColumnDef],
    ) -> datafusion::common::Result<(Self, Option<ColumnDef>)> {
        if let Some(natural) = Self::get_natural_if_any(primary_key) {
            return Ok((natural, None));
        }

        if primary_key.is_empty()
            || primary_key
                .iter()
                .any(|k| k.is_auto_increment() || k.get_default_value().is_some())
        {
            // We cannot do anything with auto-generated columns
            Ok(Self::random())
        } else {
            Ok(Self::computed(
                primary_key.iter().map(|cd| cd.name.value.clone()).collect(),
            ))
        }
    }
}

impl RowIdColumn {
    /// Returns true if the row ID is not part of the schema expected by the end user
    pub fn is_hidden(&self) -> bool {
        self.column_name == DEFAULT_ROWID_COLUMN_NAME
    }

    pub fn size(&self) -> RowIdColumnSize {
        self.size
    }

    pub fn name(&self) -> &str {
        self.column_name.as_str()
    }

    /// Builds a column definition if we need to create the column
    fn column_def(&self) -> ColumnDef {
        ColumnDef {
            data_type: ast::DataType::Binary(Some(self.size.bytes() as u64)),
            name: Ident::new(&self.column_name),
            options: vec![],
        }
    }

    fn random() -> (Self, Option<ColumnDef>) {
        let me = Self {
            source: RowIdColumnSource::RandomGenerated,
            column_name: DEFAULT_ROWID_COLUMN_NAME.to_string(),
            size: RowIdColumnSize::Size12Bytes,
        };
        let cd = me.column_def();
        (me, Some(cd))
    }

    fn computed(columns: Vec<String>) -> (Self, Option<ColumnDef>) {
        let me = Self {
            source: RowIdColumnSource::DerivedFromPrimaryColumns(columns),
            column_name: DEFAULT_ROWID_COLUMN_NAME.to_string(),
            size: RowIdColumnSize::Size8Bytes,
        };
        let cd = me.column_def();
        (me, Some(cd))
    }

    fn primary(column_name: String, size: RowIdColumnSize, dt: DataType) -> Self {
        Self {
            source: RowIdColumnSource::DirectPrimaryKey(dt),
            column_name,
            size,
        }
    }

    /// Returns an expression that generates the row-id for this column.
    /// This is used when planning the insert.
    pub fn compute_rowid(
        &self,
        ident_generator: Arc<dyn StableIdentifiersGenerator>,
        schema: &Schema,
    ) -> datafusion::common::Result<Option<ProjectionExpr>> {
        let expr = match &self.source {
            RowIdColumnSource::DirectPrimaryKey(_) => return Ok(None),
            RowIdColumnSource::DerivedFromPrimaryColumns(cols) => {
                ComputeRowIdExpr::new(schema, self.size, cols, ident_generator)?
            }
            RowIdColumnSource::RandomGenerated => Arc::new(GenRandomRowId::new(self.size)),
        };

        Ok(Some(ProjectionExpr::new(expr, self.name())))
    }

    pub fn get_rowid_physical(
        &self,
        schema: &Schema,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Column::new_with_schema(
            &self.column_name,
            schema,
        )?))
    }

    pub fn rowid_physical_prefixed(
        &self,
        prefix: &str,
        schema: &Schema,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(Column::new_with_schema(
            format!("{prefix}{}", &self.column_name).as_str(),
            schema,
        )?))
    }

    pub fn rowid_bin_physical(
        &self,
        schema: &Schema,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let base = self.get_rowid_physical(schema)?;
        match &self.source {
            RowIdColumnSource::DirectPrimaryKey(dt) => Ok(Arc::new(
                physical_expr::expressions::CastExpr::new(base, dt.clone(), None),
            )),
            _ => Ok(base),
        }
    }

    pub fn rowid_bin_physical_prefixed(
        &self,
        prefix: &str,
        schema: &Schema,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let base = self.rowid_physical_prefixed(prefix, schema)?;
        match &self.source {
            RowIdColumnSource::DirectPrimaryKey(_) => Ok(Arc::new(ToBinaryExpr::new(base))),
            _ => Ok(base),
        }
    }

    pub fn data_type(&self) -> DataType {
        match &self.source {
            RowIdColumnSource::DirectPrimaryKey(dt) => dt.clone(),
            _ => self.size.array_datatype(),
        }
    }

    pub fn field(&self) -> Field {
        Field::new(&self.column_name, self.data_type(), false)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum RowIdColumnSource {
    /// The indexable column is the primary key of the table, unmodified.
    /// We must keep the original datatype
    DirectPrimaryKey(DataType),

    /// The indexable column is derived from the primary columns of the table
    DerivedFromPrimaryColumns(Vec<String>),

    /// The indexable column is randomly determined at row creation
    RandomGenerated,
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize, PartialEq, Eq, Hash)]
pub enum RowIdColumnSize {
    Size4Bytes,
    Size8Bytes,
    Size12Bytes,
    Size16Bytes,
}

impl RowIdColumnSize {
    pub const fn bytes(&self) -> i32 {
        match self {
            RowIdColumnSize::Size4Bytes => 4,
            RowIdColumnSize::Size8Bytes => 8,
            RowIdColumnSize::Size12Bytes => 12,
            RowIdColumnSize::Size16Bytes => 16,
        }
    }

    pub const fn array_datatype(&self) -> DataType {
        DataType::Binary
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct GenRandomRowId {
    size: RowIdColumnSize,
    signature: Signature,
}

impl GenRandomRowId {
    fn new(size: RowIdColumnSize) -> Self {
        Self {
            size,
            signature: Signature::nullary(Volatility::Volatile),
        }
    }

    fn gen_arr(&self, num_rows: usize) -> datafusion::common::Result<ColumnarValue> {
        if num_rows == 0 {
            let mut vec = vec![0u8; self.size.bytes() as usize];
            rng().fill_bytes(&mut vec);
            Ok(ColumnarValue::Scalar(ScalarValue::Binary(Some(vec))))
        } else {
            let mut output_vec = vec![0u8; num_rows * self.size.bytes() as usize];
            rng().fill_bytes(&mut output_vec);
            let buffer = Buffer::from_vec(output_vec);

            let array = BinaryArray::try_new(
                OffsetBuffer::from_repeated_length(self.size.bytes() as usize, num_rows),
                buffer,
                None,
            )?;
            Ok(ColumnarValue::Array(Arc::new(array)))
        }
    }
}

impl Display for GenRandomRowId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("GenRandomRowId()")
    }
}

impl PhysicalExpr for GenRandomRowId {
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
        self.gen_arr(batch.num_rows())
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        if children.is_empty() {
            Ok(self)
        } else {
            plan_err!("GenRandomRowIdUdf cannot have children")
        }
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ComputeRowIdUdf {
    signature: Signature,
    inner: ComputeRowIdImpl,
}

#[repr(transparent)]
#[derive(Clone)]
struct IdentGeneratorWrapper(Arc<dyn StableIdentifiersGenerator>);

impl PartialEq for IdentGeneratorWrapper {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for IdentGeneratorWrapper {}

impl Hash for IdentGeneratorWrapper {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.0).addr().hash(state);
    }
}

impl Debug for IdentGeneratorWrapper {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("<ident generator>")
    }
}

impl ComputeRowIdUdf {}

impl ScalarUDFImpl for ComputeRowIdUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "compute_row_id"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> {
        self.inner.return_type()
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion::common::Result<ColumnarValue> {
        self.inner.compute_rowid(args.args, args.number_rows)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> datafusion::common::Result<ExprSimplifyResult> {
        // TODO: may be simplified to return the column directly if it already exists in the environment
        Ok(ExprSimplifyResult::Original(args))
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ComputeRowIdExpr {
    children: Vec<Arc<dyn PhysicalExpr>>,
    inner: ComputeRowIdImpl,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct ComputeRowIdImpl {
    size: RowIdColumnSize,
    ident_generator: IdentGeneratorWrapper,
}

impl ComputeRowIdImpl {
    fn new(size: RowIdColumnSize, ident_generator: Arc<dyn StableIdentifiersGenerator>) -> Self {
        Self {
            size,
            ident_generator: IdentGeneratorWrapper(ident_generator),
        }
    }

    fn return_type(&self) -> datafusion::common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn compute_rowid(
        &self,
        arrays: Vec<ColumnarValue>,
        num_rows: usize,
    ) -> datafusion::common::Result<ColumnarValue> {
        let arrays = ColumnarValue::values_to_arrays(arrays.as_slice())?;
        let arrays = arrays
            .iter()
            .map(|array| array.as_binary::<i32>())
            .collect::<Vec<_>>();

        let mut output =
            BinaryBuilder::with_capacity(num_rows, self.size.bytes() as usize * num_rows);
        let size_bytes = self.size.bytes() as usize;

        let args_len = arrays.len() as u8;

        let mut buffer: Vec<u8> = Vec::new();
        let mut ident_buffer: Vec<u8> = vec![0; size_bytes];
        for i in 0..num_rows {
            buffer.push(args_len);

            for arr in arrays.iter() {
                let value = arr.value(i);
                buffer.extend_from_slice(value.len().to_be_bytes().as_slice()); // separator
                buffer.extend_from_slice(value);
            }

            self.ident_generator
                .0
                .get_opaque_stable_identifier_in(&buffer, &mut ident_buffer);
            buffer.clear();

            output.append_value(&ident_buffer);
        }

        Ok(ColumnarValue::Array(Arc::new(output.finish())))
    }
}

impl ComputeRowIdExpr {
    fn new(
        input_schema: &Schema,
        size: RowIdColumnSize,
        columns: &Vec<String>,
        ident_generator: Arc<dyn StableIdentifiersGenerator>,
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        let columns: Vec<_> = columns
            .iter()
            .map(|cn| {
                Ok(Arc::new(ToBinaryExpr::new(Arc::new(Column::new_with_schema(
                    cn,
                    input_schema,
                )?))) as Arc<dyn PhysicalExpr>)
            })
            .collect::<Result<Vec<_>, datafusion::error::DataFusionError>>()?;

        Ok(Arc::new(Self {
            children: columns,
            inner: ComputeRowIdImpl::new(size, ident_generator),
        }))
    }
}

impl Display for ComputeRowIdExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("compute_row_id(")?;
        for child in self.children.iter() {
            child.fmt_sql(f)?;
            f.write_str(", ")?;
        }
        f.write_str(")")
    }
}

impl PhysicalExpr for ComputeRowIdExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> datafusion::common::Result<DataType> {
        self.inner.return_type()
    }

    fn nullable(&self, _: &Schema) -> datafusion::common::Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion::common::Result<ColumnarValue> {
        let number_rows = batch.num_rows();
        let arrays = self
            .children
            .iter()
            .map(|child| child.evaluate(batch))
            .collect::<Result<Vec<_>, _>>()?;
        self.inner.compute_rowid(arrays, number_rows)
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

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self, f)
    }
}
