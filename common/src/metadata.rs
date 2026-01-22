#![allow(clippy::disallowed_types)]

use crate::charset::Charset;
use datafusion::arrow::datatypes::Field;
use std::collections::HashMap;

const KEY_SCHEMA: &str = "proxy_schema";
const KEY_TABLE: &str = "proxy_table";
const KEY_CHARSET: &str = "proxy_charset";
const KEY_COL_LENGTH: &str = "proxy_column_length";
const KEY_ENCRYPTED: &str = "proxy_column_encrypted";

const KEY_DEFAULT_VALUE: &str = "proxy_column_default";
const KEY_RAW_SOURCE_TYPE: &str = "proxy_raw_source_type";
const KEY_RAW_COLLATION: &str = "proxy_raw_collation";

pub trait MetadataReads {
    fn get(&self, key: &'static str) -> Option<&String>;

    fn schema(&self) -> Option<&String> {
        self.get(KEY_SCHEMA)
    }

    fn table(&self) -> Option<&String> {
        self.get(KEY_TABLE)
    }

    fn charset(&self) -> Option<Charset> {
        self.get(KEY_CHARSET).and_then(|value| value.parse().ok())
    }

    fn column_length(&self) -> Option<u32> {
        self.get(KEY_COL_LENGTH)
            .and_then(|value| value.parse().ok())
    }

    fn default_value(&self) -> Option<&String> {
        self.get(KEY_DEFAULT_VALUE)
    }

    fn raw_source_type(&self) -> Option<&String> {
        self.get(KEY_RAW_SOURCE_TYPE)
    }

    fn raw_collation(&self) -> Option<&String> {
        self.get(KEY_RAW_COLLATION)
    }

    fn is_encrypted(&self) -> bool {
        self.get(KEY_ENCRYPTED).is_some_and(|v| v == "1")
    }
}

pub trait MetadataWrites {
    fn set(&mut self, key: &'static str, value: String);
    fn unset(&mut self, key: &'static str);

    fn set_encrypted(&mut self, original_table: &str) {
        self.set(KEY_ENCRYPTED, "1".to_string());
        self.set(KEY_TABLE, original_table.to_string());
    }

    fn clear_encrypted(&mut self) {
        self.unset(KEY_ENCRYPTED);
    }

    fn set_default_value(&mut self, value: String) {
        self.set(KEY_DEFAULT_VALUE, value);
    }

    fn set_raw_source_type(&mut self, value: String) {
        self.set(KEY_RAW_SOURCE_TYPE, value);
    }

    fn set_raw_collation(&mut self, value: String) {
        self.set(KEY_RAW_COLLATION, value);
    }
}

#[repr(transparent)]
pub struct MetadataReader<'a>(pub &'a HashMap<String, String>);

impl<'a> MetadataReads for MetadataReader<'a> {
    fn get(&self, key: &'static str) -> Option<&String> {
        self.0.get(key)
    }
}

#[repr(transparent)]
pub struct MetadataWriter<'a>(pub &'a mut HashMap<String, String>);

impl<'a> MetadataWrites for MetadataWriter<'a> {
    fn set(&mut self, key: &'static str, value: String) {
        let _ = self.0.insert(key.to_string(), value);
    }
    fn unset(&mut self, key: &'static str) {
        let _ = self.0.remove(key);
    }
}

impl MetadataReads for Field {
    fn get(&self, key: &'static str) -> Option<&String> {
        self.metadata().get(key)
    }
}

impl MetadataWrites for Field {
    fn set(&mut self, key: &'static str, value: String) {
        let _ = self.metadata_mut().insert(key.to_string(), value);
    }

    fn unset(&mut self, key: &'static str) {
        let _ = self.metadata_mut().remove(key);
    }
}
