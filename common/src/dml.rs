use datafusion::arrow::array::{Array, ArrowPrimitiveType, AsArray, PrimitiveArray, RecordBatch};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef, UInt64Type};
use datafusion::common::{DFSchema, DFSchemaRef, exec_err};
use datafusion::error::DataFusionError;
use std::sync::{Arc, LazyLock};

pub const DML_FIELD_NUM_INSERTED_ROWS: &str = "inserted";
pub const DML_FIELD_LAST_INSERT_ID: &str = "last_insert_id";

pub type NumInsertRows = UInt64Type;
pub type LastInsertId = UInt64Type;
type NumInsertRowsPrimitive = <NumInsertRows as ArrowPrimitiveType>::Native;

type LastInsertIdPrimitive = <LastInsertId as ArrowPrimitiveType>::Native;

pub static DML_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(DML_FIELD_NUM_INSERTED_ROWS, NumInsertRows::DATA_TYPE, false),
        Field::new(DML_FIELD_LAST_INSERT_ID, LastInsertId::DATA_TYPE, true),
    ]))
});

pub static DML_SCHEMA_LOGICAL: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    Arc::new(
        DFSchema::from_unqualified_fields(DML_SCHEMA.fields().clone(), Default::default()).unwrap(),
    )
});

#[derive(Copy, Clone, Debug)]
pub struct DmlResult {
    num_rows: NumInsertRowsPrimitive,
    last_insert_id: Option<LastInsertIdPrimitive>,
}

impl DmlResult {
    pub fn empty() -> Self {
        Self::new(0)
    }

    pub fn num_rows(&self) -> NumInsertRowsPrimitive {
        self.num_rows
    }

    pub fn last_insert_id(&self) -> Option<LastInsertIdPrimitive> {
        self.last_insert_id
    }

    pub fn new(num_rows: NumInsertRowsPrimitive) -> Self {
        Self {
            num_rows,
            last_insert_id: None,
        }
    }

    pub fn new_with_insert_id(
        num_rows: NumInsertRowsPrimitive,
        last_insert_id: Option<LastInsertIdPrimitive>,
    ) -> Self {
        Self {
            num_rows,
            last_insert_id,
        }
    }

    pub fn with_last_insert_id(mut self, last_insert_id: LastInsertIdPrimitive) -> Self {
        self.last_insert_id = Some(last_insert_id);
        self
    }

    /// Combines two Dml result, taking the maximum number of inserted rows between the two.
    ///
    /// Takes the first non None last_insert_id, if any
    pub fn combine_max(self, other: Self) -> Self {
        Self {
            num_rows: self.num_rows.max(other.num_rows),
            last_insert_id: self.last_insert_id.or(other.last_insert_id),
        }
    }
}

impl From<DmlResult> for RecordBatch {
    fn from(value: DmlResult) -> Self {
        let num_rows = PrimitiveArray::<NumInsertRows>::from_value(value.num_rows, 1);
        let last_insert_id = match value.last_insert_id {
            None => PrimitiveArray::<LastInsertId>::new_null(1),
            Some(value) => PrimitiveArray::<LastInsertId>::from_value(value, 1),
        };

        RecordBatch::try_new(
            Arc::clone(&DML_SCHEMA),
            vec![Arc::new(num_rows), Arc::new(last_insert_id)],
        )
        .expect("infaillible")
    }
}

impl TryFrom<&'_ RecordBatch> for DmlResult {
    type Error = DataFusionError;

    fn try_from(value: &RecordBatch) -> Result<Self, Self::Error> {
        if !value.schema().contains(&DML_SCHEMA) {
            exec_err!(
                "Invalid schema for DML result! \nGot: {}\nExpected: {}",
                value.schema(),
                DML_SCHEMA.as_ref()
            )?
        }

        if value.num_rows() != 1 {
            exec_err!(
                "Invalid number of rows {} for DML result!",
                value.num_rows()
            )?
        };

        let num_rows = value
            .column_by_name(DML_FIELD_NUM_INSERTED_ROWS)
            .unwrap()
            .as_primitive::<NumInsertRows>();
        let num_rows = num_rows.value(0);

        let last_insert_id = value
            .column_by_name(DML_FIELD_LAST_INSERT_ID)
            .unwrap()
            .as_primitive::<LastInsertId>();
        let last_insert_id = if last_insert_id.is_null(0) {
            None
        } else {
            Some(last_insert_id.value(0))
        };

        Ok(Self {
            last_insert_id,
            num_rows,
        })
    }
}
