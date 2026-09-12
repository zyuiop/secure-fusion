use crate::cipher::Cipher;
use crate::cipher::aws_lc::AwsLcCipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::{CipherContext, IdentifierContext, PrincipalKeyManager};
use aws_lc_rs::aead::Algorithm;
use aws_lc_rs::hkdf;
use aws_lc_rs::hkdf::{HKDF_SHA256, KeyType, Prk};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone)]
pub struct AwsLcHkdfSha256KeyManager {
    principal_id: u128,
    algo: &'static Algorithm,
    kdf: Prk,
}

impl Debug for AwsLcHkdfSha256KeyManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "AwsLcHmacSha256KeyManager({:x})", self.principal_id,)
    }
}

impl AwsLcHkdfSha256KeyManager {
    pub fn from_key_and_principal_id(
        principal_id: u128,
        master_key: &[u8],
        algo: &'static Algorithm,
    ) -> Self {
        let kdf = hkdf::Salt::new(hkdf::HKDF_SHA256, &[]).extract(master_key);
        Self {
            principal_id,
            algo,
            kdf,
        }
    }

    pub fn from_hex_key_and_principal_id(
        principal_id: u128,
        hex_key: &str,
        algo: &'static Algorithm,
    ) -> Self {
        let decoded = hex::decode(hex_key).unwrap();
        Self::from_key_and_principal_id(principal_id, decoded.as_slice(), algo)
    }
}

impl PrincipalKeyManager for AwsLcHkdfSha256KeyManager {
    fn get_principal_id(&self) -> u128 {
        self.principal_id
    }

    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher> {
        let mut key = [0u8; 32];

        let info = match context {
            CipherContext::TableColumn {
                table_context,
                column_name,
            } => {
                format!("tbl:{table_context}.{column_name}")
            }
            CipherContext::VersionedIndexEntry {
                table_context,
                index_name,
                version_number,
            } => {
                format!(
                    "versioned_index:{table_context}.{index_name}:{}",
                    version_number.unwrap_or_default()
                )
            }
        };

        let info = info.as_bytes();
        let info = &[info];
        self.kdf
            .expand(info, self.algo)
            .unwrap()
            .fill(&mut key[0..self.algo.key_len()])
            .unwrap();

        Arc::new(AwsLcCipher::new(self.algo, &key[0..self.algo.key_len()]))
    }

    fn get_identifier_generator(
        &self,
        context: &IdentifierContext,
    ) -> Arc<dyn StableIdentifiersGenerator> {
        let mut kdf_key = [0u8; 32];

        self.kdf
            .expand(&[context.to_string().as_bytes()], HKDF_SHA256)
            .unwrap()
            .fill(kdf_key.as_mut_slice())
            .unwrap();

        Arc::new(Prk::new_less_safe(HKDF_SHA256, kdf_key.as_slice()))
    }
}

struct DangerousFixedLen(usize);
impl KeyType for DangerousFixedLen {
    fn len(&self) -> usize {
        self.0
    }
}

impl StableIdentifiersGenerator for Prk {
    fn get_opaque_stable_identifier_in(&self, data: &[u8], output: &mut [u8]) {
        self.expand(&[data], DangerousFixedLen(output.len()))
            .unwrap()
            .fill(output)
            .unwrap()
    }
}
