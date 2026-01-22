use hybrid_array::{Array, ArraySize};
use mysql_async::prelude::FromValue;
use mysql_async::{FromValueError, Value};

#[repr(transparent)]
pub struct MySQLByteArray<S: ArraySize>(Array<u8, S>);

impl<S: ArraySize> MySQLByteArray<S> {
    pub fn unwrap(self) -> Array<u8, S> {
        self.0
    }
}

impl<S: ArraySize> TryFrom<Value> for MySQLByteArray<S> {
    type Error = FromValueError;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bytes(b) => Ok(MySQLByteArray(
                Array::try_from(b.as_slice()).map_err(|_| FromValueError(Value::Bytes(b)))?,
            )),
            _ => Err(FromValueError(value)),
        }
    }
}

impl<S: ArraySize> From<MySQLByteArray<S>> for Value {
    fn from(val: MySQLByteArray<S>) -> Self {
        Value::Bytes(val.0.to_vec())
    }
}

impl<S: ArraySize> FromValue for MySQLByteArray<S> {
    type Intermediate = MySQLByteArray<S>;
}
