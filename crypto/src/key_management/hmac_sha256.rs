use crate::cipher::Cipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::{CipherContext, IdentifierContext, PrincipalKeyManager};
use aead::consts::U32;
use aead::{Key, KeyInit, KeySizeUser};
use hkdf::Hkdf;
use hybrid_array::Array;
use sha2::Sha256;
use std::any::type_name;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

#[derive(Clone)]
pub struct LegacyKeyManager<C: Cipher> {
    principal_id: u128,
    master_key: Array<u8, U32>,
    _phantom: PhantomData<C>,
}

impl<C: Cipher> Debug for LegacyKeyManager<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MasterKeyHmacSha256KeyManager({:x}, {})",
            self.principal_id,
            type_name::<C>()
        )
    }
}

impl<C: Cipher> LegacyKeyManager<C> {
    pub fn from_key_and_principal_id(principal_id: u128, master_key: Array<u8, U32>) -> Self {
        Self {
            principal_id,
            master_key,
            _phantom: PhantomData,
        }
    }

    pub fn from_hex_key_and_principal_id(principal_id: u128, hex_key: &str) -> Self {
        let decoded = hex::decode(hex_key).unwrap();
        Self::from_key_and_principal_id(
            principal_id,
            decoded
                .as_slice()
                .try_into()
                .expect("decoded key is not correct length"),
        )
    }
}

impl<C: Cipher + KeySizeUser + KeyInit + 'static> PrincipalKeyManager for LegacyKeyManager<C> {
    fn get_principal_id(&self) -> u128 {
        self.principal_id
    }

    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher> {
        let mut target: Key<C> = Default::default();

        match context {
            CipherContext::TableColumn {
                table_context,
                column_name,
            } => {
                Hkdf::<Sha256>::new(
                    Some(format!("tbl:{table_context}.{column_name}").as_bytes()),
                    &self.master_key,
                )
                .expand(column_name.as_bytes(), &mut target)
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
                .expand(&data, &mut target)
                .unwrap();
            }
        }

        let cipher = C::new(&target);
        Arc::new(cipher)
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
