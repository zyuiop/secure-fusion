use crate::cipher::Cipher as CipherTrait;
use crate::key_manager::KeyManager;
use crate::kw_search::tset::{TSetEntry, TSetMetadataPage, TSetPage};
use crate::kw_search::{STag, TSetQuery};
use crate::{KeyGeneratorContext, KeyManagerGetter};
use aead::consts::{U16, U32};
use aead::{Key, KeyInit};
use async_trait::async_trait;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes;
use bytes::BufMut;
use chacha20poly1305::XChaCha20Poly1305;
use common::profile;
use datafusion::common::DataFusionError;
use datafusion::execution::TaskContext;
use hkdf::Hkdf;
use hybrid_array::{Array, ArraySize};
use log::warn;
use sha2::Sha256;
use std::collections::{HashMap, HashSet};
use std::ops::Deref;
use std::sync::Arc;

type Kdf = Hkdf<Sha256>;
type Cipher = XChaCha20Poly1305;

struct TSetQueryContext {
    context: Arc<TaskContext>,
    page_encryption: Cipher,
    stag_derivation: Kdf,

    version_number: Option<u64>,
    tset_ident: Option<String>,
}

impl TSetQueryContext {
    fn new(ctx: Arc<TaskContext>, tset_name: &str) -> Self {
        let key_gen = ctx
            .get_long_term_keys_manager()
            .get_raw_key_generator(&KeyGeneratorContext::DEMO_ONLY_FixedIndexKey);

        let mut stag_derivation_key = Array::<u8, U32>::default();
        key_gen
            .generate_key("stag_key".as_bytes(), &mut stag_derivation_key)
            .expect("failed to derive stag key");

        let mut page_encryption_key = Key::<Cipher>::default();
        key_gen
            .generate_key("encryption_key".as_bytes(), &mut page_encryption_key)
            .expect("failed to derive index encryption key");

        Self {
            context: ctx,
            page_encryption: Cipher::new(&page_encryption_key),
            stag_derivation: Kdf::new(
                Some(tset_name.as_bytes()), // TODO: remove and use from_ikm (this is legacy compat only...)
                &stag_derivation_key,
            ),
            tset_ident: None,
            version_number: None,
        }
    }

    fn set_version(&mut self, version: u64) {
        let target: Array<u8, U16> = self.derive_value(&version.to_be_bytes());
        let index_identifier = BASE64_STANDARD.encode(target);

        self.version_number.replace(version);
        self.tset_ident.replace(index_identifier);
    }

    fn derive_value<T: ArraySize>(&self, value: &[u8]) -> Array<u8, T> {
        let mut output = Array::default();
        self.stag_derivation.expand(value, &mut output).unwrap();
        output
    }

    fn metadata_key(&self) -> STag {
        self.derive_value("metadata_page".as_bytes())
    }

    fn stag_for_kw(&self, kw: &str, page: u64) -> STag {
        let mut out = Vec::with_capacity(8 + 8 + kw.len());
        out.put_u64(self.version_number.expect("no version_number set!"));
        out.put_u64(page);
        out.put_slice(kw.as_bytes());
        self.derive_value(&out)
    }
}

#[async_trait]
pub trait TSetWrapper {
    async fn query(
        &self,
        context: Arc<TaskContext>,
        tset: &str,
        keywords: HashSet<String>,
    ) -> datafusion::common::Result<HashMap<String, Vec<TSetEntry>>>;
}

#[async_trait]
trait TSetWrapperInternals {
    async fn query_decrypt_pages<'a, I: Iterator<Item = &'a STag> + Send + Sync>(
        &self,
        context: &TSetQueryContext,
        tags: I,
    ) -> datafusion::common::Result<HashMap<STag, Vec<u8>>>;

    async fn get_metadata_page(
        &self,
        context: &TSetQueryContext,
    ) -> datafusion::common::Result<Option<TSetMetadataPage>>;

    async fn collect_tsets(
        &self,
        context: &TSetQueryContext,
        queries: HashMap<String, u32>,
        start_index: Option<HashMap<String, u32>>,
    ) -> datafusion::common::Result<HashMap<String, Vec<TSetEntry>>> {
        let mut queried_tags = std::collections::HashMap::new();

        for (kw, num_pages) in &queries {
            // Generate the tags
            let start_index = if let Some(map) = &start_index {
                map.get(kw).copied().unwrap_or(0)
            } else {
                0
            };
            for i in start_index..*num_pages {
                let query_params = context.stag_for_kw(kw, i as u64);
                queried_tags.insert(query_params, kw.clone());
            }
        }

        // Query the pages
        let result = self
            .query_decrypt_pages(context, queried_tags.keys())
            .await?;

        let mut output = std::collections::HashMap::new();
        let mut additional_query_numpg = std::collections::HashMap::new();
        let mut additional_query_start_index = std::collections::HashMap::new();

        for (tag, page) in result {
            // Find corresponding keyword
            let (page, _): (TSetPage, _) =
                bincode::decode_from_slice(&page, bincode::config::legacy())
                    .map_err(|err| DataFusionError::External(Box::new(err)))?;

            let kw = queried_tags.get(&tag).cloned().ok_or(
                DataFusionError::Execution("invalid tag in response".to_string())
                    .context("collecting tsets"),
            )?;

            if start_index.is_none() {
                // We may need to query more, verify we have everything!
                if queries.get(&kw).filter(|v| **v < page.num_pages).is_some() {
                    additional_query_numpg.insert(kw.clone(), page.num_pages);
                    additional_query_start_index
                        .insert(kw.clone(), queries.get(&kw).copied().unwrap());
                }
            }

            // Append entries
            let vec = output
                .entry(kw.clone())
                .or_insert_with(|| Vec::with_capacity(page.entries.len()));
            vec.extend_from_slice(&page.entries);
        }

        if start_index.is_none() && !additional_query_numpg.is_empty() {
            // If we did not query enough pages, we issue a new query with the missing pages by calling this function again
            let self_output = self
                .collect_tsets(
                    context,
                    additional_query_numpg,
                    Some(additional_query_start_index),
                )
                .await?;

            // We then merge the results
            for (kw, mut self_vec) in self_output {
                let vec = output.entry(kw.clone()).or_insert_with(Vec::new);
                vec.append(&mut self_vec);
            }
        }

        Ok(output)
    }
}

#[async_trait]
impl<T: TSetQuery> TSetWrapperInternals for T {
    async fn query_decrypt_pages<'a, I: Iterator<Item = &'a STag> + Send + Sync>(
        &self,
        ctx: &TSetQueryContext,
        tags: I,
    ) -> datafusion::common::Result<HashMap<STag, Vec<u8>>> {
        let mut pages = self
            .query_pages(ctx.context.clone(), ctx.tset_ident.as_ref().unwrap(), tags)
            .await?;

        for (stag, page) in pages.iter_mut() {
            warn!(
                "Decrypt page {} with stag {}",
                hex::encode(&page),
                hex::encode(stag)
            );
            ctx.page_encryption
                .decrypt_with_nonce_in_place(page, stag)?
        }

        Ok(pages)
    }

    async fn get_metadata_page(
        &self,
        ctx: &TSetQueryContext,
    ) -> datafusion::common::Result<Option<TSetMetadataPage>> {
        let query_stag = ctx.metadata_key();
        let pages = [query_stag];
        let mut pages = self
            .query_pages(ctx.context.clone(), "metadata", pages.iter())
            .await?;

        let Some(mut page) = pages.remove(&query_stag) else {
            return Ok(None);
        };

        // Decrypt page in place
        warn!(
            "Decrypt page {} with stag {}",
            hex::encode(&page),
            hex::encode(query_stag)
        );
        ctx.page_encryption
            .decrypt_with_nonce_in_place(&mut page, &query_stag)?;

        // No metadata page: index is empty
        // TODO(zerocopy): replace with yoke!
        let (page, _): (TSetMetadataPage, _) =
            bincode::decode_from_slice(&page, bincode::config::legacy())
                .map_err(|err| DataFusionError::External(Box::new(err)))?;

        Ok(Some(page))
    }
}

#[async_trait]
impl<T: TSetQuery> TSetWrapper for T {
    async fn query(
        &self,
        context: Arc<TaskContext>,
        tset: &str,
        keywords: HashSet<String>,
    ) -> datafusion::common::Result<HashMap<String, Vec<TSetEntry>>> {
        let mut context = TSetQueryContext::new(context, tset);

        let metadata = profile!(
            "tset_wrapper::get_metadata_page",
            self.get_metadata_page(&context).await?
        );

        // No metadata ==> empty index
        let Some(metadata) = metadata else {
            return Ok(keywords
                .iter()
                .map(|kw| (kw.to_string(), Vec::new()))
                .collect());
        };

        // Update version number
        context.set_version(metadata.version_number);

        let num_pages = 32; // TODO: do heuristics here!
        let pages_map = keywords
            .iter()
            .map(|v| (v.to_string(), num_pages))
            .collect();

        let mut tsets = profile!(
            "tset_wrapper::collect_tsets",
            self.collect_tsets(&context, pages_map, None).await?
        );

        // 3. Filter/Add with the local maps
        Ok(keywords
            .iter()
            .map(|v| {
                let mut matched_entries: Vec<TSetEntry> = tsets.remove(v).unwrap_or(vec![]);

                if let Some(additional) = metadata.additional_entries.get(v) {
                    matched_entries.extend_from_slice(additional.deref());
                }
                if let Some(skip) = metadata.removed_entries.get(v) {
                    matched_entries.retain(|v| !skip.contains(v));
                }

                (v.to_string(), matched_entries)
            })
            .collect())
    }
}
