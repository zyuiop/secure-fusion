pub mod context;
#[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
pub mod hmac_sha256;
#[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
pub mod rustcrypto;

#[cfg(feature = "crypto_aws_lc")]
pub mod aws_lc;
#[cfg(feature = "crypto_aws_lc")]
pub mod aws_lc_compat;

use crate::cipher::Cipher;
use crate::identifiers::StableIdentifiersGenerator;
use crate::{CipherContext, IdentifierContext};
use std::fmt::Debug;
use std::sync::Arc;

/// Manages the keys for a given principal
pub trait PrincipalKeyManager: Debug + Send + Sync {
    /// An integer uniquely identifying the principal to which this key manager is tied
    fn get_principal_id(&self) -> u128;

    /// Gets a wrapped cipher tied to a specific context, which is used to derive the key for that
    /// cipher.
    fn get_cipher(&self, context: &CipherContext) -> Arc<dyn Cipher>;

    /// Returns a generator of stable identifiers tied to a specific context.
    ///
    /// The passed context makes the identifier tied to a specific context, for example a table.
    fn get_identifier_generator(
        &self,
        context: &IdentifierContext,
    ) -> Arc<dyn StableIdentifiersGenerator>;
}
