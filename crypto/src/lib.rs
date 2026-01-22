#![deny(
    unused_must_use,
    unreachable_code,
    unreachable_patterns,
    unused_imports,
    dead_code,
    irrefutable_let_patterns,
    unused_unsafe,
    unused_mut,
    unused_variables
)]
#![warn(unused_lifetimes, redundant_lifetimes)]
#![deny(clippy::perf)]

use crate::cipher::Cipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::key_manager::KeyManager;
use crate::raw_keygen::RawKeyGenerator;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::prelude::SessionConfig;
use std::fmt::Display;
use std::sync::Arc;

mod arrow;
pub mod cipher;
pub mod encrypted_column_meta;
pub mod error;
pub mod identifiers;
pub mod key_manager;
pub mod planning;
mod raw_keygen;

#[repr(transparent)]
#[derive(Debug)]
pub struct LongTermKeyManager(Box<dyn KeyManager>);

impl LongTermKeyManager {
    pub fn new(key_manager: Box<dyn KeyManager>) -> Arc<Self> {
        Arc::new(LongTermKeyManager(key_manager))
    }
}

pub enum IdentifierContext<'a> {
    UnnamedIndexInTable(&'a str),
    NamedIndexInTable {
        table: &'a str,
        index_name: &'a str,
    },
    NamedVersionedIndexInTable {
        table_name: &'a str,
        index_name: &'a str,
        version_number: u64,
    },
    Custom(&'a str),
}

impl<'a> Display for IdentifierContext<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentifierContext::UnnamedIndexInTable(table) => {
                write!(f, "unnamed_index{{table:{table}}}")
            }
            IdentifierContext::NamedIndexInTable { table, index_name } => {
                write!(f, "named_index{{table:{table},index:{index_name}}}")
            }
            IdentifierContext::NamedVersionedIndexInTable {
                table_name,
                index_name,
                version_number,
            } => {
                write!(
                    f,
                    "named_index_versioned{{table:{table_name},index:{index_name},version:{version_number}}}"
                )
            }
            IdentifierContext::Custom(a) => write!(f, "{a}"),
        }
    }
}

pub enum KeyGeneratorContext<'a> {
    #[allow(non_camel_case_types)]
    DEMO_ONLY_FixedIndexKey,
    VersionedIndex {
        table_name: &'a str,
        index_name: &'a str,
        version: u64,
    },
}

pub enum CipherContext<'a> {
    TableColumn {
        table_name: &'a str,
        column_name: &'a str,
    },
    VersionedIndexEntry {
        table_name: &'a str,
        index_name: &'a str,
        version_number: Option<u64>,
    },
}

impl KeyManager for LongTermKeyManager {
    #[inline(always)]
    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher> {
        self.0.get_cipher(context)
    }

    #[inline(always)]
    fn get_identifier_generator(
        &self,
        context: &IdentifierContext,
    ) -> Arc<dyn StableIdentifiersGenerator> {
        self.0.get_identifier_generator(context)
    }

    #[inline(always)]
    fn get_raw_key_generator(&self, context: &KeyGeneratorContext) -> Arc<dyn RawKeyGenerator> {
        self.0.get_raw_key_generator(context)
    }
}

pub trait KeyManagerGetter {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager>;
}

impl KeyManagerGetter for &SessionConfig {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager> {
        self.get_extension().expect("no crypto engine configured!")
    }
}

impl KeyManagerGetter for &SessionState {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager> {
        self.config().get_long_term_keys_manager()
    }
}

impl KeyManagerGetter for Arc<TaskContext> {
    fn get_long_term_keys_manager(&self) -> Arc<LongTermKeyManager> {
        self.session_config().get_long_term_keys_manager()
    }
}
