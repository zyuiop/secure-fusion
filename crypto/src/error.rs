use datafusion::error::DataFusionError;
use hkdf::InvalidLength;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CryptoError {
    CipherError,
    OsError,
    InvalidLength,
}

impl From<aead::Error> for CryptoError {
    fn from(_value: aead::Error) -> Self {
        CryptoError::CipherError
    }
}

impl From<InvalidLength> for CryptoError {
    fn from(_: InvalidLength) -> Self {
        CryptoError::InvalidLength
    }
}

pub type CryptoResult<T> = Result<T, CryptoError>;

impl Display for CryptoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for CryptoError {}

impl From<CryptoError> for DataFusionError {
    fn from(value: CryptoError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}
