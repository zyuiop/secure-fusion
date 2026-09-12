use crate::cipher::Cipher;
use crate::cipher::aws_lc::AwsLcCipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::{CipherContext, IdentifierContext, PrincipalKeyManager};
use aead::consts::U32;
use aws_lc_rs::aead::Algorithm;
use hkdf::Hkdf;
use hybrid_array::Array;
use sha2::Sha256;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

#[derive(Clone)]
pub struct CompatAwsLcHkdfSha256KeyManager {
    principal_id: u128,
    master_key: Array<u8, U32>,
    algo: &'static Algorithm,
}

impl Debug for CompatAwsLcHkdfSha256KeyManager {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CompatAwsLcHkdfSha256KeyManager({:x})",
            self.principal_id,
        )
    }
}

impl CompatAwsLcHkdfSha256KeyManager {
    pub fn from_key_and_principal_id(
        principal_id: u128,
        master_key: Array<u8, U32>,
        algo: &'static Algorithm,
    ) -> Self {
        Self {
            principal_id,
            master_key,
            algo,
        }
    }

    pub fn from_hex_key_and_principal_id(
        principal_id: u128,
        hex_key: &str,
        algo: &'static Algorithm,
    ) -> Self {
        let decoded = hex::decode(hex_key).unwrap();
        Self::from_key_and_principal_id(
            principal_id,
            decoded
                .as_slice()
                .try_into()
                .expect("decoded key is not correct length"),
            algo,
        )
    }
}

impl PrincipalKeyManager for CompatAwsLcHkdfSha256KeyManager {
    fn get_principal_id(&self) -> u128 {
        self.principal_id
    }

    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher> {
        let mut key = [0u8; 32];

        match context {
            CipherContext::TableColumn {
                table_context,
                column_name,
            } => {
                Hkdf::<Sha256>::new(
                    Some(format!("tbl:{table_context}.{column_name}").as_bytes()),
                    &self.master_key,
                )
                .expand(column_name.as_bytes(), &mut key)
                .unwrap();
            }
            CipherContext::VersionedIndexEntry {
                table_context,
                index_name,
                version_number,
            } => {
                let data = version_number.unwrap_or(0).to_le_bytes();

                Hkdf::<Sha256>::new(
                    Some(format!("versioned_index({table_context},{index_name})").as_bytes()),
                    &self.master_key,
                )
                .expand(&data, &mut key)
                .unwrap();
            }
        }

        Arc::new(AwsLcCipher::new(self.algo, &key[0..self.algo.key_len()]))
    }

    fn get_identifier_generator(
        &self,
        context: &IdentifierContext,
    ) -> Arc<dyn StableIdentifiersGenerator> {
        Arc::new(Hkdf::<Sha256>::new(
            Some(format!("ident_generator:{context}").as_bytes()),
            &self.master_key,
        ))
    }
}
