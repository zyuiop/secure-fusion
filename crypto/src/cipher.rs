#[cfg(feature = "crypto_aws_lc")]
pub mod aws_lc;
#[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
pub mod rustcrypto;

use crate::error::CryptoResult;

trait CipherAssociatedData {
    const NONCE_SIZE: usize;
    const TAG_SIZE: usize;
}

pub trait CipherSizes {
    fn nonce_size(&self) -> usize;

    fn tag_size(&self) -> usize;
}

impl<T: CipherAssociatedData> CipherSizes for T {
    #[inline(always)]
    fn nonce_size(&self) -> usize {
        <Self as CipherAssociatedData>::NONCE_SIZE
    }

    #[inline(always)]
    fn tag_size(&self) -> usize {
        <Self as CipherAssociatedData>::TAG_SIZE
    }
}

pub trait Cipher: Send + Sync + CipherSizes {
    /// Decrypts a plaintext to an output slice, returning the written bytes count.
    ///
    /// ## Panics
    ///
    /// If output.len() < plaintext size
    fn decrypt_with_nonce_to_slice(
        &self,
        output: &mut [u8],
        ciphertext_with_nonce: &[u8],
        aad: &[u8],
    ) -> CryptoResult<usize>;

    fn decrypt_with_nonce_detached(
        &self,
        ciphertext: &mut Vec<u8>,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()>;

    fn encrypt_with_random_nonce(&self, plaintext: &[u8], aad: &[u8]) -> CryptoResult<Vec<u8>>;

    fn encrypt_with_fixed_nonce(
        &self,
        plaintext: &mut Vec<u8>,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()>;
}
