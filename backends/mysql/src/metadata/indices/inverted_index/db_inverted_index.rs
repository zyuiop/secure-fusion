#![allow(clippy::disallowed_types)]

use crate::GetBackend;
use crate::get_conn::ConnGetter;
use crate::metadata::indices::inverted_index::index_error::{IndexError, IndexResult};
use bincode::config::{Configuration, Fixint, LittleEndian, NoLimit};
use bincode::{Decode, Encode, decode_from_slice};
use common::dml::DML_SCHEMA;
use common::profile;
use crypto::cipher::Cipher;
use crypto::identifiers::StableIdentifiersGenerator;
use crypto::{CipherContext, IdentifierContext, KeyManagerGetter, LongTermKeyManager};
use datafusion::arrow::array::{AsArray, RecordBatch};
use datafusion::arrow::datatypes::GenericBinaryType;
use datafusion::arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::sqlparser::ast;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::sql::ResolvedTableReference;
use futures_util::StreamExt;
use futures_util::stream::once;
use log::{error, info, trace};
use mysql_async::prelude::{FromRow, Queryable};
use mysql_async::{Conn, Transaction, TxOpts};
use mysql_common::Value;
use mysql_common::params::Params;
use rand::prelude::SliceRandom;
use rand::rng;
use rustc_hash::{FxHashMap, FxHashSet};
use std::any::Any;
use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Formatter;
use std::sync::Arc;
use tokio::sync::Mutex;
use zerocopy::IntoBytes;

pub type RawIndexTermRef = Arc<RawIndexTerm>;
pub type RawIndexTerm = Vec<u8>;

const MAX_TERM_SIZE: usize = 64;

#[inline(always)]
fn resize_term(term: &mut RawIndexTermRef) {
    if term.len() > MAX_TERM_SIZE {
        *term = Arc::new((&term[..MAX_TERM_SIZE]).to_vec())
    }
}

pub type IndexedDocumentId<const N: usize> = [u8; N];

pub(super) trait IndexedDocumentExtensions {
    fn as_number_value(&self) -> Option<ast::Value>;
    fn as_u64(&self) -> Option<u64>;
}

impl<const N: usize> IndexedDocumentExtensions for IndexedDocumentId<N> {
    fn as_number_value(&self) -> Option<ast::Value> {
        self.as_u64()
            .map(|v| ast::Value::Number(v.to_string(), false))
    }

    fn as_u64(&self) -> Option<u64> {
        if N <= 8 {
            let mut bytes = [0u8; 8];
            bytes[0..N].copy_from_slice(self);
            let num = u64::from_le_bytes(bytes);
            Some(num)
        } else {
            None
        }
    }
}

mod metadata_page {
    // Separated for encapsulation reasons

    use crate::metadata::indices::inverted_index::db_inverted_index::{BINCODE_CFG, RawIndexTerm};
    use crate::metadata::indices::inverted_index::index_error::IndexResult;
    use bincode::{Decode, Encode, decode_from_slice};
    use crypto::cipher::Cipher;
    use log::info;
    use rustc_hash::FxHashMap;
    use std::mem;

    #[derive(Debug, Clone, Decode, Encode)]
    pub(super) struct MetadataPage {
        num_pages: FxHashMap<RawIndexTerm, usize>,
        default_num_pages: usize, // when a term is absent from the array, how many pages to assume
        key_size: usize,          // To check against compile time constants?
    }

    const MAX_METADATA_PAGE_SIZE: usize = 1 << 20;

    impl MetadataPage {
        pub fn new(key_size: usize) -> MetadataPage {
            Self {
                num_pages: Default::default(),
                default_num_pages: 0,
                key_size,
            }
        }

        fn get_nonce(cipher: &dyn Cipher, version: u64) -> Vec<u8> {
            let nonce_size = cipher.nonce_size();
            let mut nonce_buffer = vec![0u8; nonce_size];
            nonce_buffer[0..8].copy_from_slice(version.to_le_bytes().as_ref());
            nonce_buffer
        }

        pub fn decrypt(
            cipher: &dyn Cipher,
            version: u64,
            buffer: &mut Vec<u8>,
        ) -> IndexResult<Self> {
            cipher.decrypt_with_nonce_detached(
                buffer,
                &Self::get_nonce(cipher, version),
                b"metadata_page",
            )?;

            // Parse the data in the page
            let (meta, _) = decode_from_slice(&buffer, BINCODE_CFG)?;
            Ok(meta)
        }

        fn binary_encode(&self, buffer: &mut Vec<u8>) -> IndexResult<()> {
            buffer.clear();
            bincode::encode_into_std_write(self, buffer, BINCODE_CFG)?;
            Ok(())
        }

        fn shrink_to_fit_and_encode(&mut self, buff: &mut Vec<u8>) -> IndexResult<()> {
            while {
                self.binary_encode(buff)?;
                buff.len() > MAX_METADATA_PAGE_SIZE
            } {
                self.default_num_pages += 1;
                info!(
                    "ShrinkToFit: metadata page is too big ({} > {MAX_METADATA_PAGE_SIZE}), removing entries with {} page(s)",
                    buff.len(),
                    self.default_num_pages
                );
                let num_pages = mem::take(&mut self.num_pages);
                self.num_pages = num_pages
                    .into_iter()
                    .filter(|(_, v)| *v > self.default_num_pages)
                    .collect();
            }
            Ok(())
        }

        pub fn get_num_pages(&self, tag: &RawIndexTerm) -> usize {
            self.num_pages
                .get(tag)
                .cloned()
                .unwrap_or(self.default_num_pages)
        }

        pub fn insert_entry(&mut self, keyword: RawIndexTerm, num_pages: usize) {
            if num_pages > self.default_num_pages {
                self.num_pages.insert(keyword, num_pages);
            } else {
                // We may still have the keyword, in this case we should remove it
                self.num_pages.remove(&keyword);
            }
        }

        pub fn encrypt(
            mut self,
            cipher: &dyn Cipher,
            version: u64,
            buffer: &mut Vec<u8>,
        ) -> IndexResult<()> {
            self.shrink_to_fit_and_encode(buffer)?;

            self.binary_encode(buffer)?;
            cipher.encrypt_with_fixed_nonce(
                buffer,
                &Self::get_nonce(cipher, version),
                b"metadata_page",
            )?;
            Ok(())
        }
    }
}

use metadata_page::*;

#[derive(Debug, Clone)]
struct VersionedMetadataPage {
    version: u64,
    metadata: MetadataPage,
}

#[derive(Debug, Clone, Decode, Encode)]
struct ModificationsTable<const KeySize: usize> {
    /// A map from term hash to a list of document IDs to add to the normal output
    added_entries: BTreeMap<RawIndexTerm, BTreeSet<IndexedDocumentId<KeySize>>>,

    /// A map from term hash to a list of document IDs to remove from the normal output
    removed_entries: BTreeMap<RawIndexTerm, BTreeSet<IndexedDocumentId<KeySize>>>,
    // removed_documents: BTreeSet<IndexedDocumentId<KeySize>>,
}

impl<const K: usize> ModificationsTable<K> {
    const AAD: &[u8] = b"modifications_page";

    fn get_nonce(cipher: &dyn Cipher, meta_version: u64, modif_version: u32) -> Vec<u8> {
        let nonce_size = cipher.nonce_size();
        let total_version: u128 = ((meta_version as u128) << 32) | (modif_version as u128);

        let total_version = total_version.to_le_bytes();
        let (nonce_bytes, rest) = total_version.split_at(nonce_size);

        // Check that the total version is not spilling over nonce size (fatal error)
        assert!(rest.iter().all(|byte| *byte == 0), "nonce spill!");

        let mut nonce_buffer = vec![0u8; nonce_size];
        nonce_buffer[0..nonce_size].copy_from_slice(nonce_bytes);
        nonce_buffer
    }

    pub fn decrypt(
        cipher: &dyn Cipher,
        meta_version: u64,
        modif_version: u32,
        buffer: &mut Vec<u8>,
    ) -> IndexResult<Self> {
        cipher.decrypt_with_nonce_detached(
            buffer,
            &Self::get_nonce(cipher, meta_version, modif_version),
            &Self::AAD,
        )?;

        // Parse the data in the page
        let (modifications, _) = decode_from_slice(&buffer, BINCODE_CFG)?;
        Ok(modifications)
    }

    pub fn encrypt(
        &self,
        cipher: &dyn Cipher,
        meta_version: u64,
        modif_version: u32,
        buffer: &mut Vec<u8>,
    ) -> IndexResult<()> {
        buffer.clear();

        bincode::encode_into_std_write(self, buffer, BINCODE_CFG)?;
        cipher.encrypt_with_fixed_nonce(
            buffer,
            &Self::get_nonce(cipher, meta_version, modif_version),
            &Self::AAD,
        )?;
        Ok(())
    }

    fn insert_terms<I: IntoIterator<Item = RawIndexTermRef>>(
        &mut self,
        document: IndexedDocumentId<K>,
        terms: I,
    ) {
        for mut term in terms.into_iter() {
            resize_term(&mut term);

            if let Some(set) = self.removed_entries.get_mut(term.as_ref()) {
                set.remove(&document);
            }

            if let Some(existing) = self.added_entries.get_mut(term.as_ref()) {
                existing.insert(document);
            } else {
                // We don't use `entry` to avoid the need to clone the key in most cases
                let mut set = BTreeSet::new();
                set.insert(document);
                self.added_entries.insert(term.as_ref().clone(), set);
            }
        }
    }

    fn remove_terms<I: IntoIterator<Item = RawIndexTermRef>>(
        &mut self,
        document: IndexedDocumentId<K>,
        terms: I,
    ) {
        for mut term in terms.into_iter() {
            resize_term(&mut term);

            if let Some(set) = self.added_entries.get_mut(term.as_ref()) {
                set.remove(&document);
            }

            if let Some(existing) = self.removed_entries.get_mut(term.as_ref()) {
                existing.insert(document);
            } else {
                // We don't use `entry` to avoid the need to clone the key in most cases
                let mut set = BTreeSet::new();
                set.insert(document);
                self.removed_entries.insert(term.as_ref().clone(), set);
            }
        }
    }
}

#[derive(Debug, Clone)]
struct VersionedModificationsTable<const KeySize: usize> {
    modifications_version: u32,
    modifications: ModificationsTable<KeySize>,
}

const PAGE_SIZE: usize = 4096 - 16 /* AEAD tag size */;
const MAX_BLOB_SIZE: usize = (u16::MAX) as usize;
const REBUILD_THRESHOLD: usize = MAX_BLOB_SIZE >> 2;

#[derive(Decode, Encode)]
struct TermPageHeader {
    /// The number of terms present in this page (some pages are not full)
    num_present: u16,
    term_len: u8,
    term: [u8; MAX_TERM_SIZE],
    page_number: [u8; Self::PAGE_NUM_LEN],
}

impl TermPageHeader {
    const PAGE_NUM_LEN: usize = 5 - (MAX_TERM_SIZE % 8);

    fn term(&self) -> &[u8] {
        &self.term[0..(self.term_len as usize)]
    }

    fn page_number(&self) -> usize {
        let mut page_number = [0u8; size_of::<usize>()];
        page_number[0..self.page_number.len()].copy_from_slice(&self.page_number);
        usize::from_le_bytes(page_number)
    }

    fn new(term: RawIndexTermRef, page_num: usize) -> Self {
        let term_len = min(term.len(), MAX_TERM_SIZE);
        let mut term_array = [0u8; MAX_TERM_SIZE];
        term_array[0..term_len].copy_from_slice(&term[0..term_len]);

        let page_number: [u8; Self::PAGE_NUM_LEN] = (page_num.to_le_bytes()[0..Self::PAGE_NUM_LEN])
            .try_into()
            .unwrap();

        Self {
            num_present: 0,
            term_len: term_len as u8,
            term: term_array,
            page_number,
        }
    }
}

const _: () = {
    assert!(size_of::<TermPageHeader>() % 8 == 0);
};

const ENTRIES_DATA_SIZE: usize = PAGE_SIZE - size_of::<TermPageHeader>();

#[derive(Decode, Encode)]
struct TermPage<const KeySize: usize> {
    header: TermPageHeader,
    // We are at offset 8, remaining size is PAGE_SIZE - 8
    entries_data: [u8; ENTRIES_DATA_SIZE],
}

struct TaggedTermPage<const KeySize: usize> {
    tag: IndexTag,
    page: TermPage<KeySize>,
}

impl<const KeySize: usize> TermPage<KeySize> {
    fn items(&self) -> &[IndexedDocumentId<KeySize>] {
        let total_occupied_size = KeySize * (self.header.num_present as usize);
        let (chunks, _) = self.entries_data[0..total_occupied_size].as_chunks::<KeySize>();
        chunks
    }

    fn new(term: RawIndexTermRef, page_num: usize) -> Self {
        Self {
            entries_data: [0u8; ENTRIES_DATA_SIZE],
            header: TermPageHeader::new(term, page_num),
        }
    }

    fn max_entries() -> u16 {
        let entries_data_size = PAGE_SIZE - size_of::<TermPageHeader>();
        let num_entries = entries_data_size / KeySize;

        num_entries
            .try_into()
            .expect("max entries number is too big")
    }

    fn free_space(&self) -> u16 {
        let max_entries = Self::max_entries();
        max_entries
            .checked_sub(self.header.num_present)
            .expect("max entries smaller than present")
    }

    fn from_iterator<I: Iterator<Item = IndexedDocumentId<KeySize>>>(
        term: RawIndexTermRef,
        page_num: usize,
        i: I,
    ) -> Self {
        let mut page = Self::new(term, page_num);
        let mut position = 0;
        for elem in i {
            page.entries_data[position..(position + KeySize)].copy_from_slice(&elem);

            position += KeySize;
            page.header.num_present += 1;

            assert!(position <= page.entries_data.len());
        }
        page
    }

    pub fn decrypt(
        tag: &[u8],
        cipher: &dyn Cipher,
        version: u64,
        buffer: &mut Vec<u8>,
    ) -> IndexResult<Self> {
        cipher.decrypt_with_nonce_detached(buffer, &tag, &version.to_le_bytes())?;
        let (page, _) = decode_from_slice(&buffer, BINCODE_CFG)?;
        Ok(page)
    }

    pub fn encrypt(
        &self,
        tag: &[u8],
        cipher: &dyn Cipher,
        version: u64,
        buffer: &mut Vec<u8>,
    ) -> IndexResult<()> {
        buffer.clear();

        bincode::encode_into_std_write(self, buffer, BINCODE_CFG)?;
        cipher.encrypt_with_fixed_nonce(buffer, tag, &version.to_le_bytes())?;
        Ok(())
    }
}

const TAG_SIZE: usize = 12;
const TAG_PREFIX_SIZE: usize = 4;

type IndexTag = [u8; TAG_SIZE];

#[derive(FromRow)]
struct IndexTableRow {
    tag: IndexTag,
    version: u64,
    page: Vec<u8>,
}

const BINCODE_CFG: Configuration<LittleEndian, Fixint, NoLimit> = bincode::config::standard()
    .with_fixed_int_encoding()
    .with_little_endian()
    .with_no_limit();

impl IndexTableRow {
    fn decrypt_as_metadata(mut self, cipher: &dyn Cipher) -> IndexResult<VersionedMetadataPage> {
        let metadata = MetadataPage::decrypt(cipher, self.version, &mut self.page)?;
        Ok(VersionedMetadataPage {
            metadata,
            version: self.version,
        })
    }

    fn decrypt_as_modifications<const KeySize: usize>(
        mut self,
        metadata_version: u64,
        cipher: &dyn Cipher,
    ) -> IndexResult<VersionedModificationsTable<KeySize>> {
        let modifications_version = self
            .version
            .try_into()
            .expect("changes version must fit in an u32");
        let modifications = ModificationsTable::<KeySize>::decrypt(
            cipher,
            metadata_version,
            modifications_version,
            &mut self.page,
        )?;
        Ok(VersionedModificationsTable {
            modifications,
            modifications_version,
        })
    }
}

#[derive(Debug)]
pub struct RawInvertedIndex<const KeySize: usize> {
    indexed_table: ResolvedTableReference,
    index_name: Arc<str>,
    index_table_name: String,

    cached_state: Mutex<CachedIndexState<KeySize>>,
}

#[derive(Debug)]
pub struct CachedIndexState<const KeySize: usize> {
    metadata: Option<VersionedMetadataPage>,
    changes: Option<VersionedModificationsTable<KeySize>>,
}

impl<const N: usize> CachedIndexState<N> {
    async fn load_metadata(
        &mut self,
        parent: &RawInvertedIndex<N>,
        conn: &mut Conn,
        crypto: Arc<LongTermKeyManager>,
    ) -> IndexResult<()> {
        // 1. Retrieve the metadata and modifications pages
        let statement = conn
            .prep(format!(
                "SELECT * FROM {} WHERE (tag = ? AND version > ?) OR (tag = ? AND version != ?)",
                &parent.index_table_name
            ))
            .await?;

        let mut metadata_pages = conn
            .exec::<IndexTableRow, _, _>(
                statement,
                Params::Positional(vec![
                    Value::Bytes(METADATA_TAG.as_bytes().to_vec()),
                    Value::UInt(self.metadata.as_ref().map(|meta| meta.version).unwrap_or(0)),
                    Value::Bytes(MODIFICATIONS_TAG.to_vec()),
                    Value::UInt(
                        self.changes
                            .as_ref()
                            .map(|meta| meta.modifications_version)
                            .unwrap_or(0) as u64,
                    ),
                ]),
            )
            .await?;

        let metadata_cipher = crypto.get_metadata_cipher(parent);

        // TODO: determine if we should issue a request for changes again
        if metadata_pages.is_empty() {
            if self.metadata.is_none() || self.changes.is_none() {
                return Err(IndexError::UnknownError(
                    "No metadata pages for index!".to_string(),
                ));
            }
            // Nothing to do here, metadata is up to date
        } else if metadata_pages.len() == 1 {
            let page = metadata_pages.pop().unwrap();
            if page.tag == METADATA_TAG {
                let metadata = page.decrypt_as_metadata(metadata_cipher.as_ref())?;

                self.metadata = Some(metadata);

                if self.changes.is_some() {
                    // Very rare case: the cached changes is set (likely for old meta) and was not
                    // returned in the query (so the version of the change set is the same)
                    // We must issue a new query to get it
                    self.changes = None;
                    Box::pin(self.load_metadata(parent, conn, crypto)).await?;
                }
            } else if page.tag == MODIFICATIONS_TAG {
                let cached_metadata = self.metadata.as_ref().ok_or_else(|| {
                    IndexError::UnknownError("No metadata pages for index!".to_string())
                })?;

                let modifications_cipher =
                    crypto.get_version_cipher(parent, cached_metadata.version);
                let changes = page.decrypt_as_modifications(
                    cached_metadata.version,
                    modifications_cipher.as_ref(),
                )?;

                self.changes = Some(changes);
            } else {
                return Err(IndexError::UnknownError(
                    "Received metadata page with invalid tag!".to_string(),
                ));
            }
        } else if metadata_pages.len() == 2 {
            let (metadata, changes) =
                (metadata_pages.pop().unwrap(), metadata_pages.pop().unwrap());

            let (metadata, changes) =
                if changes.tag == METADATA_TAG && metadata.tag == MODIFICATIONS_TAG {
                    (changes, metadata)
                } else if changes.tag == MODIFICATIONS_TAG && metadata.tag == METADATA_TAG {
                    (metadata, changes)
                } else {
                    return Err(IndexError::UnknownError(
                        "Received metadata page with invalid tag!".to_string(),
                    ));
                };

            let metadata = metadata
                .decrypt_as_metadata(metadata_cipher.as_ref())
                .expect("Failed to decrypt metadata page");

            let modifications_cipher = crypto.get_version_cipher(parent, metadata.version);
            let changes = changes
                .decrypt_as_modifications(metadata.version, modifications_cipher.as_ref())
                .expect("Failed to decrypt changes page");

            self.metadata = Some(metadata);
            self.changes = Some(changes);
        } else {
            return Err(IndexError::UnknownError(format!(
                "Received invalid number of metadata pages ({}, expected 1 or 2)",
                metadata_pages.len()
            )));
        }

        Ok(())
    }
}

// We may want to accept a list of queries? this way we only query the index once

#[derive(Clone, Debug)]
pub enum RawIndexQuery {
    // A AND B
    Intersect(Box<RawIndexQuery>, Box<RawIndexQuery>),
    // A AND NOT(B)
    Difference(Box<RawIndexQuery>, Box<RawIndexQuery>),
    // A OR B
    Union(Box<RawIndexQuery>, Box<RawIndexQuery>),
    // Term
    Term(RawIndexTermRef),
}

impl RawIndexQuery {
    fn collect_terms(&self, out: &mut FxHashSet<RawIndexTermRef>) {
        match self {
            RawIndexQuery::Union(a, b)
            | RawIndexQuery::Intersect(a, b)
            | RawIndexQuery::Difference(a, b) => {
                a.collect_terms(out);
                b.collect_terms(out);
            }
            RawIndexQuery::Term(t) => {
                out.insert(t.clone());
            }
        }
    }

    fn check_max_lengths(&mut self) {
        match self {
            RawIndexQuery::Intersect(a, b)
            | RawIndexQuery::Difference(a, b)
            | RawIndexQuery::Union(a, b) => {
                a.check_max_lengths();
                b.check_max_lengths();
            }
            RawIndexQuery::Term(term) => {
                resize_term(term);
                if term.len() > MAX_TERM_SIZE {
                    *term = Arc::new((&term[..MAX_TERM_SIZE]).to_vec())
                };
            }
        }
    }

    fn evaluate<const N: usize>(
        &self,
        response: &FxHashMap<RawIndexTermRef, Vec<IndexedDocumentId<N>>>,
        modifications: &ModificationsTable<N>,
        // TODO: add and remove sets
    ) -> FxHashSet<IndexedDocumentId<N>> {
        match self {
            RawIndexQuery::Term(t) => {
                let add = modifications.added_entries.get(t.as_ref());
                let remove = modifications.removed_entries.get(t.as_ref());

                // TODO add and remove!
                let stream = response.get(t).cloned().unwrap_or_default().into_iter();

                let stream = if let Some(add) = add {
                    Box::new(stream.chain(add.iter().cloned()))
                        as Box<dyn Iterator<Item = IndexedDocumentId<N>>>
                } else {
                    Box::new(stream)
                };

                let stream = if let Some(remove) = remove {
                    Box::new(stream.filter(|elem| !remove.contains(elem)))
                        as Box<dyn Iterator<Item = IndexedDocumentId<N>>>
                } else {
                    stream
                };

                stream.collect()
            }
            RawIndexQuery::Union(a, b) => {
                let mut set = a.evaluate(response, modifications);
                set.extend(b.evaluate(response, modifications));
                set
            }
            RawIndexQuery::Difference(a, b) => {
                let mut a = a.evaluate(response, modifications);
                b.evaluate(response, modifications).iter().for_each(|item| {
                    a.remove(item);
                });
                a
            }
            RawIndexQuery::Intersect(a, b) => a
                .evaluate(response, modifications)
                .intersection(&b.evaluate(response, modifications))
                .cloned()
                .collect(),
        }
    }
}

const METADATA_TAG: [u8; 12] = *b"metadata\0\0\0\0";
const MODIFICATIONS_TAG: [u8; 12] = *b"modification";

trait InvertedIndexCipherExt {
    fn get_metadata_cipher<T: InvertedIndexGetter>(&self, index: &T) -> Arc<dyn Cipher>;

    fn get_version_cipher<T: InvertedIndexGetter>(
        &self,
        index: &T,
        version: u64,
    ) -> Arc<dyn Cipher>;

    fn get_version_tag_derivator<T: InvertedIndexGetter>(
        &self,
        index: &T,
        version: u64,
    ) -> Arc<dyn StableIdentifiersGenerator>;
}

pub(super) trait InvertedIndexGetter {
    fn indexed_table(&self) -> &ResolvedTableReference;
    fn index_name(&self) -> &Arc<str>;
}

impl<const T: usize> InvertedIndexGetter for RawInvertedIndex<T> {
    fn indexed_table(&self) -> &ResolvedTableReference {
        &self.indexed_table
    }

    fn index_name(&self) -> &Arc<str> {
        &self.index_name
    }
}

impl<const T: usize> InvertedIndexGetter for DbNextVersionTask<T> {
    fn indexed_table(&self) -> &ResolvedTableReference {
        &self.indexed_table
    }

    fn index_name(&self) -> &Arc<str> {
        &self.index_name
    }
}

impl InvertedIndexCipherExt for Arc<LongTermKeyManager> {
    fn get_metadata_cipher<T: InvertedIndexGetter>(&self, index: &T) -> Arc<dyn Cipher> {
        self.get_cipher(&CipherContext::VersionedIndexEntry {
            table_context: index.indexed_table().clone(),
            index_name: index.index_name().clone(),
            version_number: None,
        })
    }

    fn get_version_cipher<T: InvertedIndexGetter>(
        &self,
        index: &T,
        version: u64,
    ) -> Arc<dyn Cipher> {
        self.get_cipher(&CipherContext::VersionedIndexEntry {
            table_context: index.indexed_table().clone(),
            index_name: index.index_name().clone(),
            version_number: Some(version),
        })
    }

    fn get_version_tag_derivator<T: InvertedIndexGetter>(
        &self,
        index: &T,
        version: u64,
    ) -> Arc<dyn StableIdentifiersGenerator> {
        self.get_identifier_generator(&IdentifierContext::NamedVersionedIndexInTable {
            table_context: index.indexed_table().clone(),
            index_name: index.index_name().clone(),
            version_number: version,
        })
    }
}

#[inline]
fn index_table_name(table_name: &str, index_name: &str) -> String {
    format!("__idx_{}_{}", table_name, index_name)
}

impl<const IdSize: usize> RawInvertedIndex<IdSize> {
    pub fn new(indexed_table: ResolvedTableReference, index_name: String) -> Self {
        let index_table_name = index_table_name(&indexed_table.table, &index_name);
        Self {
            indexed_table,
            index_name: index_name.into(),
            index_table_name,

            cached_state: Mutex::new(CachedIndexState {
                changes: None,
                metadata: None,
            }),
        }
    }

    #[allow(unused)]
    pub async fn delete(
        &self,
        context: Arc<TaskContext>,
        document: IndexedDocumentId<IdSize>,
        terms: &[RawIndexTermRef],
    ) -> IndexResult<()> {
        self.update_changeset(context, |modif| {
            modif.remove_terms(document, terms.into_iter().cloned());
        })
        .await
    }

    pub async fn insert_many<I>(&self, context: Arc<TaskContext>, documents: I) -> IndexResult<()>
    where
        I: Iterator<Item = (IndexedDocumentId<IdSize>, FxHashSet<RawIndexTermRef>)>,
    {
        self.update_changeset(context, |modif| {
            for (doc_id, terms) in documents {
                modif.insert_terms(doc_id, terms);
            }
        })
        .await
    }

    /// Updates many documents at once.
    ///
    /// Input format:
    ///
    /// (id, added terms, removed terms)
    pub async fn update_many<I>(&self, context: Arc<TaskContext>, documents: I) -> IndexResult<()>
    where
        I: Iterator<
            Item = (
                IndexedDocumentId<IdSize>,
                (FxHashSet<RawIndexTermRef>, FxHashSet<RawIndexTermRef>),
            ),
        >,
    {
        self.update_changeset(context, |modif| {
            for (doc_id, (added_terms, removed_terms)) in documents {
                modif.insert_terms(doc_id, added_terms);
                modif.remove_terms(doc_id, removed_terms);
            }
        })
        .await
    }

    async fn update_changeset<F: FnOnce(&mut ModificationsTable<IdSize>)>(
        &self,
        context: Arc<TaskContext>,
        modifier: F,
    ) -> IndexResult<()> {
        let conn = context.get_conn();
        let mut conn = conn.try_lock().expect("conn already locked, fix me");
        let crypto = context.get_long_term_keys_manager();

        let mut cache = self.cached_state.lock().await;

        cache.load_metadata(self, &mut conn, crypto.clone()).await?;
        let metadata = cache
            .metadata
            .as_ref()
            .expect("metadata was not loaded properly");
        let cipher = crypto.get_version_cipher(self, metadata.version);

        // TODO: transaction semantics

        let mut modifications_current: Vec<IndexTableRow> = conn
            .exec(
                format!(
                    "SELECT * FROM {} WHERE tag = ? FOR UPDATE",
                    &self.index_table_name
                ),
                vec![Value::Bytes(MODIFICATIONS_TAG.as_bytes().to_vec())],
            )
            .await?;

        let modifications_table = modifications_current
            .remove(0)
            .decrypt_as_modifications(metadata.version, cipher.as_ref())?;

        let VersionedModificationsTable {
            modifications_version,
            mut modifications,
            ..
        } = modifications_table;

        modifier(&mut modifications);

        let mut new_modifications_page = Vec::new();
        modifications.encrypt(
            cipher.as_ref(),
            metadata.version,
            modifications_version + 1,
            &mut new_modifications_page,
        )?;

        let mod_size = new_modifications_page.len();

        conn.exec_drop(
            format!(
                "UPDATE {} SET version = ?, page = ? WHERE tag = ?",
                &self.index_table_name,
            ),
            vec![
                Value::UInt((modifications_version + 1) as u64),
                Value::Bytes(new_modifications_page),
                Value::Bytes(MODIFICATIONS_TAG.as_bytes().to_vec()),
            ],
        )
        .await?;

        drop(conn);
        drop(crypto);

        if modifications_version > 16_000_000 || mod_size >= REBUILD_THRESHOLD {
            // Version 16Mio OR more than 16 pages of data equivalent
            self.next_version(metadata.version, context);
        }

        Ok(())
    }

    pub async fn query(
        &self,
        context: Arc<TaskContext>,
        mut queries: Vec<RawIndexQuery>,
    ) -> IndexResult<Vec<FxHashSet<IndexedDocumentId<IdSize>>>> {
        trace!("Index query {queries:?}");

        let conn = context.get_conn();
        let mut conn = conn.try_lock().expect("conn already locked, fix me");
        let crypto = context.get_long_term_keys_manager();

        let mut terms = FxHashSet::default();
        for q in queries.iter_mut() {
            q.check_max_lengths();
            q.collect_terms(&mut terms);
        }

        // TODO: we're forgetting the changes map
        let results = self
            .query_terms(&mut conn, terms.into_iter(), crypto)
            .await?;

        let cache = self.cached_state.lock().await;
        let modifications = cache.changes.as_ref().unwrap();

        Ok(queries
            .into_iter()
            .map(|q| q.evaluate(&results, &modifications.modifications))
            .collect())
    }

    async fn query_terms<I: Iterator<Item = RawIndexTermRef>>(
        &self,
        conn: &mut Conn,
        terms: I,
        crypto: Arc<LongTermKeyManager>,
    ) -> IndexResult<FxHashMap<RawIndexTermRef, Vec<IndexedDocumentId<IdSize>>>> {
        // Ensure metadata is available
        let mut cache = self.cached_state.lock().await;
        profile!(
            "load_metadata",
            cache.load_metadata(self, conn, crypto.clone()).await?
        );
        let metadata = cache
            .metadata
            .as_ref()
            // Should never be reached as query_metadata raises an error if it cannot set this field
            .expect("Metadata was not properly cached");

        let key_derivator = crypto.get_version_tag_derivator(self, metadata.version);

        let mut tag_to_kw = FxHashMap::default();
        let mut output = FxHashMap::default();

        // For each term, retrieve the number of pages and build the index terms
        for kw in terms {
            // Get the number of pages
            let num_pages = metadata.metadata.get_num_pages(kw.as_ref());

            if num_pages == 0 {
                // TODO short circuit, this keyword does not exist, so the result set is empty
                output.insert(kw, vec![]);
            } else {
                tag_to_kw.reserve(num_pages as usize);

                for page in 0..num_pages {
                    tag_to_kw.insert(key_derivator.page_tag(kw.as_ref(), page), kw.clone());
                }

                // This version may be more performant as it reduces allocations, but it's less clear
                /* let buf_len = kw.len();
                let mut key_buf = Vec::with_capacity(buf_len + size_of::<usize>());
                key_buf.extend_from_slice(kw);

                for page in 0..num_pages {
                    // Add page number to slice
                    key_buf.truncate(buf_len);
                    key_buf.extend_from_slice(&page.to_le_bytes());

                    // Derive tag from slice and insert in map
                    let mut tag: IndexTag = [0u8; TAG_SIZE];
                    key_derivator.get_opaque_stable_identifier_in(&key_buf, &mut tag);
                    tag_to_kw.insert(tag, kw);
                } */
            }
        }

        let pages_to_query = tag_to_kw.keys().collect();

        let cipher = crypto.get_version_cipher(self, metadata.version);

        // TODO(perf): adding a function here makes the code cleaner at the cost of performance, because we have to build an intermediate Vec<TaggedTermPage> to return from the function. Inlining removes this need.
        let pages = self
            .query_pages(conn, pages_to_query, cipher.as_ref(), metadata.version)
            .await?;

        for page in pages {
            let tag = tag_to_kw
                .get(&page.tag)
                .expect("queried a tag which was not in the map");
            output
                .entry(tag.clone())
                .or_insert_with(Vec::new)
                .extend_from_slice(page.page.items());
        }

        Ok(output)
    }

    /// Queries the requested pages in the database and returns them decrypted
    async fn query_pages(
        &self,
        conn: &mut Conn,
        pages: FxHashSet<&IndexTag>,
        cipher: &dyn Cipher,
        version: u64,
    ) -> IndexResult<Vec<TaggedTermPage<IdSize>>> {
        if pages.is_empty() {
            return Ok(vec![]);
        }

        let mut tags_placeholders = "?,".repeat(pages.len());
        tags_placeholders.truncate(tags_placeholders.len() - 1);
        let query = format!(
            "SELECT tag, page FROM {} WHERE tag_msb IN ({tags_placeholders}) AND version = {version}",
            &self.index_table_name
        );
        let mut params = pages
            .iter()
            .map(|item| Value::Bytes(item[0..TAG_PREFIX_SIZE].to_vec()))
            .collect::<Vec<_>>();

        params.shuffle(&mut rng());

        let mut query_result = conn.exec_iter(query, params).await?;

        let mut output = Vec::new();
        while let Some(mut row) = query_result.next().await? {
            let tag: IndexTag = row.take(0).unwrap();

            if !pages.contains(&tag) {
                continue;
            }

            let mut page: Vec<u8> = row.take(1).unwrap();
            let page = TermPage::<IdSize>::decrypt(&tag, cipher, version, &mut page)?;
            output.push(TaggedTermPage { tag, page })
        }
        Ok(output)
    }

    fn next_version(&self, current_version: u64, context: Arc<TaskContext>) {
        // We need to spawn a new thread!
        let mut task_context = DbNextVersionTask::<IdSize> {
            current_version,
            index_table_name: self.index_table_name.clone(),
            indexed_table: self.indexed_table.clone(),
            index_name: self.index_name.clone(),
        };
        let context_clone = context.clone();

        tokio::runtime::Handle::current().spawn(async move {
            task_context
                .do_next_version(context_clone)
                .await
                .expect("couldn't complete next version task");
        });

        // Alternatively, spawn a thread and create a runtime
        /* let _ = thread::spawn(move || {
            let mut rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("could not build tokio thread-local runtime");

            rt.spawn(...)
        });*/
    }
}

struct DbNextVersionTask<const IdSize: usize> {
    index_table_name: String,
    indexed_table: ResolvedTableReference,
    index_name: Arc<str>,
    current_version: u64,
}

impl<const IdSize: usize> DbNextVersionTask<IdSize> {
    async fn load_metadata(
        &self,
        trx: &mut Transaction<'_>,
        crypto: Arc<LongTermKeyManager>,
    ) -> IndexResult<Option<VersionedMetadataPage>> {
        info!("load_meta start");

        let mut retrieved_metadata: Vec<IndexTableRow> = trx
            .exec(
                format!(
                    "SELECT * FROM {} WHERE tag = ? FOR UPDATE",
                    &self.index_table_name
                ),
                vec![Value::Bytes(METADATA_TAG.as_bytes().to_vec())],
            )
            .await?;

        info!("load_meta ok");

        if retrieved_metadata.is_empty() {
            // Metadata lock timed out? (not sure it can happen)
            error!(
                "next_version[{}]: metadata lock timed out?",
                self.index_table_name
            );
            return Ok(None);
        }

        let head = retrieved_metadata.remove(0);
        if head.version != self.current_version {
            // DB returned a metadata with a newer version, implying another "next_version" on another node completed
            error!(
                "next_version[{}]: another next version task may have completed?",
                self.index_table_name
            );
            return Ok(None);
        }

        let metadata_cipher = crypto.get_metadata_cipher(self);
        let current_metadata = head.decrypt_as_metadata(metadata_cipher.as_ref())?;

        Ok(Some(current_metadata))
    }

    async fn load_modifications(
        &self,
        trx: &mut Transaction<'_>,
        crypto: Arc<LongTermKeyManager>,
    ) -> IndexResult<VersionedModificationsTable<IdSize>> {
        let mut retrieved_rows: Vec<IndexTableRow> = trx
            .exec(
                format!("SELECT * FROM {} WHERE tag = ?", &self.index_table_name),
                vec![Value::Bytes(MODIFICATIONS_TAG.as_bytes().to_vec())],
            )
            .await?;

        if retrieved_rows.is_empty() {
            return Err(IndexError::UnknownError(
                "no modifications row in table".to_string(),
            ));
        }
        if retrieved_rows.len() > 1 {
            return Err(IndexError::UnknownError(
                "more than one modifications row in table".to_string(),
            ));
        }

        let head = retrieved_rows.remove(0);
        let cipher = crypto.get_version_cipher(self, self.current_version);
        let current_modif_table =
            head.decrypt_as_modifications(self.current_version, cipher.as_ref())?;

        Ok(current_modif_table)
    }

    async fn do_next_version(&mut self, context: Arc<TaskContext>) -> IndexResult<()> {
        info!(
            "next_version: Start next_version task for index table {}",
            self.index_table_name
        );

        let crypto = context.get_long_term_keys_manager();
        let mut conn = context
            .get_backend()
            .new_connection(context.clone())
            .await
            .expect("could not connect to DB");

        info!(
            "next_version: connection obtained for index table {}",
            self.index_table_name
        );

        let mut trx = conn.start_transaction(TxOpts::new()).await?;
        info!("tx get");

        let Some(current_metadata) = self.load_metadata(&mut trx, crypto.clone()).await? else {
            error!(
                "next_version: failed loading metadata for index table {}",
                self.index_table_name
            );
            return Ok(());
        };

        let new_derivator = crypto.get_version_tag_derivator(self, current_metadata.version + 1);

        // Derive new tags for *all pages*
        // TODO: we may need to write to disk if the dataset is too big

        let changes = self.load_modifications(&mut trx, crypto.clone()).await?;

        info!(
            "next_version: Loading all data for index table {}",
            self.index_table_name
        );
        let mut all_pages_iterator = trx
            .exec_iter(
                format!(
                    "SELECT * FROM {} WHERE version = ? AND tag NOT IN (?, ?)",
                    &self.index_table_name
                ),
                vec![
                    Value::UInt(self.current_version),
                    Value::Bytes(METADATA_TAG.as_bytes().to_vec()),
                    Value::Bytes(MODIFICATIONS_TAG.as_bytes().to_vec()),
                ],
            )
            .await?;

        let old_cipher = crypto.get_version_cipher(self, current_metadata.version);
        let new_cipher = crypto.get_version_cipher(self, current_metadata.version + 1);
        let metadata_cipher = crypto.get_metadata_cipher(self);

        let mut new_pages = Vec::<IndexTableRow>::new();

        let mut partial_pages =
            BTreeMap::<RawIndexTermRef, BTreeSet<IndexedDocumentId<IdSize>>>::new();
        let mut last_full_page = BTreeMap::<RawIndexTermRef, usize>::new();

        info!(
            "next_version: iterating over all pages for index table {}",
            self.index_table_name
        );
        while let Some(page) = all_pages_iterator.next().await? {
            let mut page = IndexTableRow::from_row(page);
            let decrypted = TermPage::<IdSize>::decrypt(
                &page.tag,
                old_cipher.as_ref(),
                current_metadata.version,
                &mut page.page,
            )?;
            let keyword = RawIndexTermRef::new(decrypted.header.term().to_vec());
            let page_index = decrypted.header.page_number();

            if changes
                .modifications
                .removed_entries
                .contains_key(keyword.as_ref())
            {
                // All pages must be pushed to "partial", because we want to be sure the output is compacted
                partial_pages
                    .entry(keyword.clone())
                    .or_default()
                    .extend(decrypted.items().iter());

                continue;
            }

            if changes
                .modifications
                .added_entries
                .contains_key(keyword.as_ref())
            {
                // If we're here, then there is no deletion
                // Only one partial page should be pushed

                // TODO: deduplicate the added_entries set? (ensure we're not adding an item twice)
                if decrypted.free_space() > 0 {
                    // There should only me one page like this
                    if partial_pages.contains_key(keyword.as_ref()) {
                        log::warn!(
                            "Multiple partial pages found for keyword {}",
                            hex::encode(keyword.as_ref())
                        );
                    }

                    partial_pages
                        .entry(keyword.clone())
                        .or_default()
                        .extend(decrypted.items().iter());

                    continue; // Don't insert the page
                } else {
                    let last_full = last_full_page.entry(keyword.clone()).or_default();
                    *last_full = max(*last_full, page_index);
                }
            }

            // Re-encrypt and push
            page.tag = new_derivator.page_tag(keyword.as_ref(), page_index);
            page.version = current_metadata.version + 1;
            decrypted.encrypt(&page.tag, new_cipher.as_ref(), page.version, &mut page.page)?;

            new_pages.push(page);
        }

        info!(
            "next_version: Incorporating changes for table {}",
            self.index_table_name
        );

        // Incorporate changes (deletions)
        for (word, remove) in changes.modifications.removed_entries.iter() {
            let word = RawIndexTermRef::new(word.clone());
            if let Some(terms) = partial_pages.get_mut(&word) {
                for term in remove {
                    // TODO: see if there is a more efficient way to do this?
                    terms.remove(term);
                }
            }
        }

        // Incorporate changes (additions)
        for (word, append) in changes.modifications.added_entries.iter() {
            partial_pages
                .entry(RawIndexTermRef::new(word.clone()))
                .or_default()
                .extend(append);
        }

        info!(
            "next_version: Creating new pages for table {}",
            self.index_table_name
        );

        // Create the new pages
        let mut new_metadata = current_metadata.metadata.clone();

        let entries_per_page = TermPage::<IdSize>::max_entries();
        for (keyword, entries) in partial_pages.into_iter() {
            let mut page_number = last_full_page
                .get(&keyword)
                .map(|v| v + 1)
                .unwrap_or_default();
            let mut iterator = entries.into_iter().peekable();

            while iterator.peek().is_some() {
                let chunk = iterator.by_ref().take(entries_per_page as usize);
                let page = TermPage::<IdSize>::from_iterator(keyword.clone(), page_number, chunk);

                let tag = new_derivator.page_tag(keyword.as_ref(), page_number);
                let mut data = Vec::with_capacity(size_of_val(&page) + new_cipher.nonce_size());
                page.encrypt(
                    &tag,
                    new_cipher.as_ref(),
                    current_metadata.version + 1,
                    &mut data,
                )?;

                new_pages.push(IndexTableRow {
                    version: current_metadata.version + 1,
                    tag,
                    page: data,
                });

                page_number += 1;
            }

            // Insert number of pages in metadata
            new_metadata.insert_entry(Arc::unwrap_or_clone(keyword), page_number);
        }

        info!(
            "next_version: Shuffling pages for table {}",
            self.index_table_name
        );

        // Insert pages in the database, shuffling before (we may need to use a better rng here)
        new_pages.shuffle(&mut rng());

        let statement = trx
            .prep(format!(
                "INSERT INTO {} (tag, tag_msb, version, page) VALUES (?, ?, {}, ?)",
                &self.index_table_name,
                current_metadata.version + 1
            ))
            .await?;

        info!(
            "next_version: Inserting pages for table {}",
            self.index_table_name
        );
        trx.exec_batch(
            &statement,
            new_pages.into_iter().map(|page| {
                vec![
                    Value::Bytes(page.tag.to_vec()),
                    Value::Bytes((&page.tag[..TAG_PREFIX_SIZE]).to_vec()),
                    Value::Bytes(page.page),
                ]
            }),
        )
        .await?;

        info!(
            "next_version: Preparing modifications page for table {}",
            self.index_table_name
        );

        // Create modifications page
        let mut new_modifications: Vec<IndexTableRow> = trx
            .exec(
                format!(
                    "SELECT * FROM {} WHERE tag = ? FOR UPDATE",
                    &self.index_table_name
                ),
                vec![Value::Bytes(MODIFICATIONS_TAG.as_bytes().to_vec())],
            )
            .await?;

        let mut new_modifications: ModificationsTable<IdSize> = new_modifications
            .remove(0)
            .decrypt_as_modifications(current_metadata.version, old_cipher.as_ref())?
            .modifications;

        changes.modifications.added_entries.keys().for_each(|e| {
            new_modifications.added_entries.remove(e);
        });
        changes.modifications.removed_entries.keys().for_each(|e| {
            new_modifications.removed_entries.remove(e);
        });

        let mut new_modifications_page = Vec::new();
        new_modifications.encrypt(
            new_cipher.as_ref(),
            current_metadata.version + 1,
            1,
            &mut new_modifications_page,
        )?;

        // Create metadata page
        let mut new_metadata_page = Vec::new();
        new_metadata.encrypt(
            metadata_cipher.as_ref(),
            current_metadata.version + 1,
            &mut new_metadata_page,
        )?;

        info!(
            "next_version: Deleting all old entries for table {}",
            self.index_table_name
        );

        // Issue the master delete
        // Deletes everything with previous version number + the modifications
        trx.exec_drop(
            format!(
                "DELETE FROM {} WHERE version = {} OR tag = ?",
                self.index_table_name, current_metadata.version
            ),
            vec![Value::Bytes(MODIFICATIONS_TAG.to_vec())],
        )
        .await?;

        // Inserts new metadata + changes
        trx.exec_drop(
            &statement,
            vec![
                Value::Bytes(METADATA_TAG.to_vec()),
                Value::Bytes((&METADATA_TAG[..TAG_PREFIX_SIZE]).to_vec()),
                Value::Bytes(new_metadata_page),
            ],
        )
        .await?;

        trx.exec_drop(
            format!(
                "INSERT INTO {} (tag, tag_msb, version, page) VALUES (?, ?, 1, ?)",
                &self.index_table_name
            ),
            vec![
                Value::Bytes(MODIFICATIONS_TAG.to_vec()),
                Value::Bytes((&MODIFICATIONS_TAG[..TAG_PREFIX_SIZE]).to_vec()),
                Value::Bytes(new_modifications_page),
            ],
        )
        .await?;

        trx.close(statement).await?;
        info!(
            "next_version: Committing transaction for table {}",
            self.index_table_name
        );
        trx.commit().await?;
        info!("next_version: done for table {}", self.index_table_name);

        Ok(())
    }
}

#[derive(Debug)]
struct DbInitialProvisioningTask<const IdSize: usize> {
    index_table_name: String,
    indexed_table: ResolvedTableReference,
    index_name: Arc<str>,
}

#[derive(Debug)]
pub struct CreateDbInvertedIndexPlan<const IdSize: usize> {
    task: Arc<DbInitialProvisioningTask<IdSize>>,
    initial_data_source: Option<Arc<dyn ExecutionPlan>>,
    plan_properties: Arc<PlanProperties>,
}

impl<const N: usize> InvertedIndexGetter for DbInitialProvisioningTask<N> {
    fn indexed_table(&self) -> &ResolvedTableReference {
        &self.indexed_table
    }

    fn index_name(&self) -> &Arc<str> {
        &self.index_name
    }
}

impl<const IdSize: usize> CreateDbInvertedIndexPlan<IdSize> {
    pub const COLUMN_NAME_ID: &str = "row_id";
    pub const COLUMN_NAME_TERMS: &str = "terms";

    pub fn new_empty(indexed_table: ResolvedTableReference, index_name: Arc<str>) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&DML_SCHEMA)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );

        Self {
            task: Arc::new(DbInitialProvisioningTask::new(indexed_table, index_name)),
            initial_data_source: None,
            plan_properties: Arc::new(plan_properties),
        }
    }

    pub fn new_from_data(
        indexed_table: ResolvedTableReference,
        index_name: Arc<str>,
        initial_data_source: Arc<dyn ExecutionPlan>,
    ) -> datafusion::common::Result<Self> {
        let base = Self::new_empty(indexed_table, index_name);
        base.with_initial_data_source(Some(initial_data_source))
    }

    pub fn with_initial_data_source(
        &self,
        initial_data: Option<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Self> {
        if let Some(initial_data) = initial_data.as_ref() {
            let schema = initial_data.schema();
            if schema.fields.len() < 2 {
                plan_err!("Invalid schema when initializing a new index")?;
            }

            // Verify that the TERMS column is a List[Binary]
            let (_, terms_column) = schema
                .column_with_name(Self::COLUMN_NAME_TERMS)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "Missing column {} when initializing a new index",
                        Self::COLUMN_NAME_TERMS
                    ))
                })?;

            if !matches!(terms_column.data_type(), DataType::List(fr) if matches!(fr.data_type(), DataType::Binary))
            {
                plan_err!(
                    "Invalid column type {:?} for {} when initializing a new index - expected a list of binary values",
                    terms_column.data_type(),
                    Self::COLUMN_NAME_TERMS
                )?;
            }

            // Verify that the ID column is of correct type/size
            let (_, id_column) =
                schema
                    .column_with_name(Self::COLUMN_NAME_ID)
                    .ok_or_else(|| {
                        DataFusionError::Plan(format!(
                            "Missing column {} when initializing a new index",
                            Self::COLUMN_NAME_ID
                        ))
                    })?;

            if id_column.data_type() != &DataType::Binary {
                plan_err!(
                    "Invalid column type {:?} for {} when initializing a new index - expected {}",
                    id_column.data_type(),
                    Self::COLUMN_NAME_ID,
                    DataType::Binary
                )?;
            }
        }

        Ok(Self {
            initial_data_source: initial_data,
            task: self.task.clone(),
            plan_properties: self.plan_properties.clone(),
        })
    }
}

impl<const IdSize: usize> DbInitialProvisioningTask<IdSize> {
    fn new(indexed_table: ResolvedTableReference, index_name: Arc<str>) -> Self {
        let index_table_name = index_table_name(&indexed_table.table, &index_name);
        Self {
            index_table_name,
            index_name,
            indexed_table,
        }
    }

    async fn do_create_index_table(&self, conn: &mut Conn) -> datafusion::error::Result<()> {
        let q = format!(
            "CREATE TABLE {} (\
            tag BINARY({TAG_SIZE}) NOT NULL, \
            tag_msb BINARY({TAG_PREFIX_SIZE}) NOT NULL, \
            version INTEGER NOT NULL, \
            page MEDIUMBLOB NOT NULL, \
            PRIMARY KEY (version, tag), \
            INDEX (version, tag_msb), \
            INDEX (tag))",
            self.index_table_name
        );

        #[cfg(feature = "log-outgoing-queries")]
        info!("Query: {q}");

        // TODO: check if index should include version
        conn.query_drop(q).await.map_err(|e| {
            DataFusionError::Execution(format!("could not create index table: {e:?}"))
        })?;

        info!("Created index table {}", self.index_table_name);

        Ok(())
    }

    async fn do_initial_provisioning(
        &self,
        context: Arc<TaskContext>,
        documents_for_term: BTreeMap<RawIndexTermRef, BTreeSet<IndexedDocumentId<IdSize>>>,
    ) -> IndexResult<()> {
        let conn = context.get_conn();
        let mut conn = conn.try_lock().expect(
            "caller error: connection must be released before calling do_initial_provisioning",
        );

        let crypto = context.get_long_term_keys_manager();
        let derivator = crypto.get_version_tag_derivator(self, 1);
        let cipher = crypto.get_version_cipher(self, 1);
        let metadata_cipher = crypto.get_metadata_cipher(self);
        // Create the pages
        let mut new_metadata = MetadataPage::new(IdSize);

        let entries_per_page = TermPage::<IdSize>::max_entries();
        let mut new_pages = Vec::new();
        for (keyword, entries) in documents_for_term.into_iter() {
            let mut page_number = 0;
            let mut iterator = entries.into_iter().peekable();

            while iterator.peek().is_some() {
                let chunk = iterator.by_ref().take(entries_per_page as usize);
                let page = TermPage::<IdSize>::from_iterator(keyword.clone(), page_number, chunk);

                let tag = derivator.page_tag(keyword.as_ref(), page_number);
                let mut data = Vec::with_capacity(size_of_val(&page) + cipher.nonce_size());
                page.encrypt(&tag, cipher.as_ref(), 1, &mut data)?;

                new_pages.push(IndexTableRow {
                    version: 1,
                    tag,
                    page: data,
                });

                page_number += 1;
            }

            // Insert number of pages in metadata
            new_metadata.insert_entry(Arc::unwrap_or_clone(keyword), page_number);
        }

        // Insert pages in the database, shuffling before (we may need to use a better rng here)
        new_pages.shuffle(&mut rng());

        // Create modifications page
        let new_modifications: ModificationsTable<IdSize> = ModificationsTable::<IdSize> {
            removed_entries: Default::default(),
            added_entries: Default::default(),
        };
        let mut new_modifications_page = Vec::new();
        new_modifications.encrypt(cipher.as_ref(), 1, 1, &mut new_modifications_page)?;

        // Create metadata page
        let mut new_metadata_page = Vec::new();
        new_metadata.encrypt(metadata_cipher.as_ref(), 1, &mut new_metadata_page)?;

        new_pages.push(IndexTableRow {
            page: new_metadata_page,
            version: 1,
            tag: METADATA_TAG.clone(),
        });
        new_pages.push(IndexTableRow {
            page: new_modifications_page,
            version: 1,
            tag: MODIFICATIONS_TAG.clone(),
        });

        let statement = conn
            .prep(format!(
                "INSERT INTO {} (tag, tag_msb, version, page) VALUES (?, ?, 1, ?)",
                &self.index_table_name
            ))
            .await?;

        conn.exec_batch(
            &statement,
            new_pages.into_iter().map(|page| {
                vec![
                    Value::Bytes(page.tag.to_vec()),
                    Value::Bytes((&page.tag[..TAG_PREFIX_SIZE]).to_vec()),
                    Value::Bytes(page.page),
                ]
            }),
        )
        .await?;

        conn.close(statement).await?;
        Ok(())
    }

    #[allow(unused)]
    async fn do_initial_provisioning_from_iterator<
        J: Iterator<Item = RawIndexTermRef>,
        I: Iterator<Item = (IndexedDocumentId<IdSize>, J)>,
    >(
        &self,
        context: Arc<TaskContext>,
        iterator: I,
    ) -> IndexResult<()> {
        let mut documents_for_term =
            BTreeMap::<RawIndexTermRef, BTreeSet<IndexedDocumentId<IdSize>>>::new();
        for (document, terms) in iterator {
            for mut term in terms {
                resize_term(&mut term);
                documents_for_term
                    .entry(term.clone())
                    .or_default()
                    .insert(document);
            }
        }

        self.do_initial_provisioning(context, documents_for_term)
            .await
    }

    async fn do_initial_provisioning_from_results(
        &self,
        context: Arc<TaskContext>,
        mut results: SendableRecordBatchStream,
    ) -> IndexResult<()> {
        let mut documents_for_term =
            BTreeMap::<RawIndexTermRef, BTreeSet<IndexedDocumentId<IdSize>>>::new();

        while let Some(batch) = results.next().await {
            let batch = batch?;

            let id_col = batch
                .column_by_name(CreateDbInvertedIndexPlan::<IdSize>::COLUMN_NAME_ID)
                .expect("missing column that should be in schema");
            let terms_col = batch
                .column_by_name(CreateDbInvertedIndexPlan::<IdSize>::COLUMN_NAME_TERMS)
                .expect("missing column that should be in schema");

            let id_col = id_col.as_bytes::<GenericBinaryType<i32>>();
            let terms_col = terms_col.as_list::<i32>();

            let iterator = id_col
                .iter()
                .zip(terms_col.iter())
                .filter_map(|(id, terms)| Some((id?, terms?)));

            for (id, terms) in iterator {
                let Some(id) = IndexedDocumentId::<IdSize>::try_from(id).ok() else {
                    continue;
                };
                let Some(terms) = terms.as_bytes_opt::<GenericBinaryType<i32>>() else {
                    continue;
                };

                for term in terms {
                    let Some(term) = term else { continue };
                    let mut term = Arc::new(term.to_vec());
                    resize_term(&mut term);

                    documents_for_term.entry(term).or_default().insert(id);
                }
            }
        }

        // Hopefully, the end of the stream triggers release of the conn mutex...

        self.do_initial_provisioning(context, documents_for_term)
            .await
    }
}

impl<const IdSize: usize> DisplayAs for CreateDbInvertedIndexPlan<IdSize> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "CreateDbInvertedIndex")
    }
}

impl<const IdSize: usize> ExecutionPlan for CreateDbInvertedIndexPlan<IdSize> {
    fn name(&self) -> &str {
        "CreateDbInvertedIndex"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        if let Some(v) = self.initial_data_source.as_ref() {
            vec![v]
        } else {
            vec![]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let result = if children.is_empty() {
            self.with_initial_data_source(None)?
        } else if children.len() == 1 {
            self.with_initial_data_source(Some(children.remove(0)))?
        } else {
            plan_err!(
                "DbInitialProvisioningPlan can take at most one child (a select plan for all rows in the table)"
            )?
        };

        Ok(Arc::new(result))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let task = self.task.clone();
        let context = context.clone();
        let initial_data_source = self.initial_data_source.clone();

        let result = async move {
            // TODO: transaction semantics!

            // 1. create table
            task.do_create_index_table(
                &mut context
                    .get_conn()
                    .try_lock()
                    .expect("conn is still locked by another part of this plan?"),
            )
            .await?;

            // 2. pull underlying plan and insert rows
            if let Some(initial_data_source) = initial_data_source {
                let rows_stream = initial_data_source.execute(0, context.clone())?;
                task.do_initial_provisioning_from_results(context, rows_stream)
                    .await?;
            }

            Ok(RecordBatch::new_empty(SchemaRef::new(Schema::empty())))
        };

        let result = once(result);
        let schema = self.schema();
        let result = RecordBatchStreamAdapter::new(schema, result);

        Ok(Box::pin(result))
    }
}

trait TagDerivator {
    fn page_tag(&self, kw: &[u8], page: usize) -> IndexTag;
}

impl TagDerivator for Arc<dyn StableIdentifiersGenerator> {
    fn page_tag(&self, kw: &[u8], page: usize) -> IndexTag {
        let mut key_buf = Vec::with_capacity(kw.len() + size_of::<usize>());
        key_buf.extend_from_slice(kw);
        key_buf.extend_from_slice(&page.to_le_bytes());

        let mut tag: IndexTag = [0u8; TAG_SIZE];
        self.get_opaque_stable_identifier_in(&key_buf, &mut tag);
        tag
    }
}
