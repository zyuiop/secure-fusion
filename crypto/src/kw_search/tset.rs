#![allow(unused)]

use bincode::de::{BorrowDecoder, Decoder};
use bincode::error::DecodeError;
use bincode::{BorrowDecode, Decode};
use datafusion::arrow::datatypes::DataType;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

#[derive(bincode::Decode, Clone)]
pub struct TSetMetadataPage {
    pub(crate) _total_size: u64,
    pub(crate) version_number: u64,
    pub(crate) additional_entries: HashMap<String, Vec<TSetEntry>>,
    pub(crate) removed_entries: HashMap<String, HashSet<TSetEntry>>,
    // index_columns: IndexDefinition, // TODO: legacy compatibility here
}

#[derive(bincode::Decode)]
pub struct TSetPage {
    _original_keyword: String,
    pub(crate) entries: Vec<TSetEntry>,
    /// Each page carries a full page count, making sure we can always query *all* pages in 2 queries
    pub(crate) num_pages: u32,
}

/*
pub struct TSetFirstPage {
    original_keyword: String,
    entries: Vec<TSetEntry>,
    num_pages: u32,
}

pub struct TSetPage {
    entries: Vec<TSetEntry>,
}
 */

#[derive(Clone, Eq, PartialEq, Debug, bincode::Decode, Hash)]
pub struct TSetEntry {
    // TODO: when deserializing this, consider that values with size == 8 are likely legacy simple index
    pub document_id: u64, // TODO: DocumentId,
}

#[derive(Clone, Eq, PartialEq, Hash, Ord, PartialOrd, Debug, bincode::Decode)]
pub struct ColumnValue(Vec<u8>);

#[derive(Clone, Eq, PartialEq, Debug, Hash, bincode::Decode)]
pub enum DocumentId {
    LegacySimpleIndex(u64), // TODO: legacy compatibility here
    SimpleIndex(ColumnValue),
    CompositeIndex(Vec<ColumnValue>),
}

#[derive(Clone, Eq, PartialEq, Debug, bincode::Decode)]
pub enum IndexDefinition {
    LegacySimpleIndex,
    SimpleIndex(ColumnDefinition),
    CompositeIndex(Vec<ColumnDefinition>),
}

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct ColumnDefinition(String, DataType);

impl<T> Decode<T> for ColumnDefinition {
    fn decode<D: Decoder<Context = T>>(decoder: &mut D) -> Result<Self, DecodeError> {
        Ok(Self(
            Decode::decode(decoder)?,
            DataType::from_str(&String::decode(decoder)?)
                .map_err(|_| DecodeError::Other("invalid data type"))?,
        ))
    }
}

impl<'a, T> BorrowDecode<'a, T> for ColumnDefinition {
    fn borrow_decode<D: BorrowDecoder<'a, Context = T>>(
        decoder: &mut D,
    ) -> Result<Self, DecodeError> {
        Ok(Self(
            BorrowDecode::borrow_decode(decoder)?,
            DataType::from_str(BorrowDecode::borrow_decode(decoder)?)
                .map_err(|_| DecodeError::Other("invalid data type"))?,
        ))
    }
}
