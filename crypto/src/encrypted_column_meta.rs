use datafusion::arrow::datatypes::DataType;

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Clone)]
pub struct EncryptedColumnMeta {
    /// The table in which the column is found
    pub table: String,

    /// The name of the column
    pub column: String,

    /// The original type of the column
    pub original_type: DataType,
}

impl EncryptedColumnMeta {
    pub fn new(table: String, column: String, original_type: DataType) -> Self {
        Self {
            table,
            column,
            original_type,
        }
    }
}
