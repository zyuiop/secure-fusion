use datafusion::error::DataFusionError;
use rand_core::OsError;
use sha2::digest::crypto_common::getrandom;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum CryptoError {
    CipherError,
    OsError,
}

impl From<aead::Error> for CryptoError {
    fn from(_value: aead::Error) -> Self {
        CryptoError::CipherError
    }
}

impl From<getrandom::Error> for CryptoError {
    fn from(_value: getrandom::Error) -> Self {
        CryptoError::CipherError
    }
}

impl From<OsError> for CryptoError {
    fn from(_value: OsError) -> Self {
        CryptoError::OsError
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
