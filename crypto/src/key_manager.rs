use crate::cipher::Cipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::raw_keygen::RawKeyGenerator;
use crate::{CipherContext, IdentifierContext, KeyGeneratorContext};
use aead::{Key, KeyInit, KeySizeUser};
use hkdf::Hkdf;
use hybrid_array::{Array, sizes::U32};
use log::warn;
use rand::prelude::StdRng;
use rand_core::{OsRng, RngCore, SeedableRng, TryRngCore};
use sha2::Sha256;
use std::any::type_name;
use std::array::TryFromSliceError;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

pub trait KeyManager: Debug + Send + Sync {
    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher>;

    /// Returns a generator of stable identifiers tied to a specific context.
    ///
    /// The passed context makes the identifier tied to a specific context, for example a table.
    fn get_identifier_generator(
        &self,
        context: &IdentifierContext,
    ) -> Arc<dyn StableIdentifiersGenerator>;

    fn get_raw_key_generator(&self, context: &KeyGeneratorContext) -> Arc<dyn RawKeyGenerator>;
}

pub struct MasterKeyHmacSha256KeyManager<C: Cipher> {
    master_key: Array<u8, U32>,
    _phantom: PhantomData<C>,
}

impl<C: Cipher> Debug for MasterKeyHmacSha256KeyManager<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MasterKeyHmacSha256KeyManager({})", type_name::<C>())
    }
}

impl<C: Cipher> From<Array<u8, U32>> for MasterKeyHmacSha256KeyManager<C> {
    fn from(master_key: Array<u8, U32>) -> Self {
        MasterKeyHmacSha256KeyManager {
            master_key,
            _phantom: PhantomData,
        }
    }
}

impl<C: Cipher> TryFrom<&[u8]> for MasterKeyHmacSha256KeyManager<C> {
    type Error = TryFromSliceError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self::from_key(value.try_into()?))
    }
}

impl<C: Cipher> MasterKeyHmacSha256KeyManager<C> {
    pub fn from_key(master_key: Array<u8, U32>) -> Self {
        MasterKeyHmacSha256KeyManager {
            master_key,
            _phantom: PhantomData,
        }
    }

    pub fn from_hex_key(hex_key: &str) -> Self {
        let decoded = hex::decode(hex_key).unwrap();
        MasterKeyHmacSha256KeyManager::try_from(decoded.as_slice()).unwrap()
    }

    pub fn with_random_key() -> Self {
        let mut key = Array::<u8, U32>::default();
        OsRng.try_fill_bytes(&mut key).unwrap();
        Self::from_key(key)
    }
}

impl<C: Cipher + KeySizeUser + KeyInit + 'static> KeyManager for MasterKeyHmacSha256KeyManager<C> {
    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher> {
        let mut target: Key<C> = Default::default();

        match context {
            CipherContext::TableColumn {
                table_name,
                column_name,
            } => {
                Hkdf::<Sha256>::new(
                    Some(format!("tbl_encryption:{}", table_name).as_bytes()),
                    &self.master_key,
                )
                .expand(column_name.as_bytes(), &mut target)
                .unwrap();
            }
            &CipherContext::VersionedIndexEntry {
                index_name,
                table_name,
                version_number,
            } => {
                let data = version_number.unwrap_or(0).to_le_bytes();

                Hkdf::<Sha256>::new(
                    Some(format!("versioned_index({},{})", table_name, index_name).as_bytes()),
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

    fn get_raw_key_generator(&self, context: &KeyGeneratorContext) -> Arc<dyn RawKeyGenerator> {
        match context {
            KeyGeneratorContext::DEMO_ONLY_FixedIndexKey => {
                warn!("A deterministic key generator has been used when processing a query!");
                let mut deterministic_index_key = Array::<u8, U32>::default();
                let mut rng = StdRng::seed_from_u64(0x42);
                rng.fill_bytes(&mut deterministic_index_key);

                Arc::new(Hkdf::<Sha256>::from_prk(&deterministic_index_key).unwrap())
            }
            KeyGeneratorContext::VersionedIndex {
                table_name,
                index_name,
                version,
            } => {
                let ikm = format!(
                    "versioned_index{{table:{table_name},index:{index_name},version:{version}}}"
                );
                Arc::new(Hkdf::<Sha256>::new(Some(ikm.as_bytes()), &self.master_key))
            }
        }
    }
}
