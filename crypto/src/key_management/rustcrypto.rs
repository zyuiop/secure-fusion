use crate::cipher::Cipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::{CipherContext, IdentifierContext, PrincipalKeyManager};
use aead::{Key, KeyInit, KeySizeUser};
use hkdf::Hkdf;
use sha2::Sha256;
use std::any::type_name;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use zerocopy::IntoBytes;

#[derive(Clone)]
pub struct RustCryptoHkdfSha256KeyManager<C: Cipher> {
    principal_id: u128,
    kdf: Hkdf<Sha256>,
    _phantom: PhantomData<C>,
}

impl<C: Cipher> Debug for RustCryptoHkdfSha256KeyManager<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MasterKeyHmacSha256KeyManager({:x}, {})",
            self.principal_id,
            type_name::<C>()
        )
    }
}

impl<C: Cipher> RustCryptoHkdfSha256KeyManager<C> {
    pub fn from_key_and_principal_id(principal_id: u128, master_key: &[u8]) -> Self {
        Self {
            principal_id,
            kdf: Hkdf::<Sha256>::new(None, master_key),
            _phantom: PhantomData,
        }
    }

    pub fn from_hex_key_and_principal_id(principal_id: u128, hex_key: &str) -> Self {
        let decoded = hex::decode(hex_key).unwrap();
        Self::from_key_and_principal_id(principal_id, decoded.as_bytes())
    }
}

impl<C: Cipher + KeySizeUser + KeyInit + 'static> PrincipalKeyManager
    for RustCryptoHkdfSha256KeyManager<C>
{
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
                self.kdf
                    .expand(
                        format!("tbl:{table_context}.{column_name}").as_bytes(),
                        &mut target,
                    )
                    .unwrap();
            }
            CipherContext::VersionedIndexEntry {
                table_context,
                index_name,
                version_number,
            } => {
                self.kdf
                    .expand(
                        format!(
                            "versioned_index:{table_context}.{index_name}:{}",
                            version_number.unwrap_or_default()
                        )
                        .as_bytes(),
                        &mut target,
                    )
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
        let mut kdf_key = [0u8; 32];

        self.kdf
            .expand(
                format!("ident_generator:{context}").as_bytes(),
                &mut kdf_key,
            )
            .unwrap();

        Arc::new(Hkdf::<Sha256>::from_prk(&kdf_key).unwrap())
    }
}
