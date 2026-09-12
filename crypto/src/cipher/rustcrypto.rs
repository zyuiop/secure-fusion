use crate::cipher::{Cipher, CipherAssociatedData};
use crate::error::{CryptoError, CryptoResult};
use aead::common::getrandom::SysRng;
use aead::inout::InOutBuf;
use aead::{Aead, AeadInOut, Nonce, Payload, Tag, TagPosition};
use rand::TryRng;

impl<T: AeadInOut + Send + Sync> CipherAssociatedData for T {
    const NONCE_SIZE: usize = size_of::<Nonce<T>>();
    const TAG_SIZE: usize = size_of::<Tag<T>>();
}

/// Blanket Implementation for any aead crate implementation
impl<T: AeadInOut + Send + Sync> Cipher for T {
    fn decrypt_with_nonce_to_slice(
        &self,
        output: &mut [u8],
        ciphertext_with_nonce: &[u8],
        aad: &[u8],
    ) -> CryptoResult<usize> {
        if ciphertext_with_nonce.len() < Self::NONCE_SIZE {
            // Value does not seem to be a valid ciphertext
            return Err(CryptoError::CipherError);
        }

        let (ciphertext, nonce) =
            ciphertext_with_nonce.split_at(ciphertext_with_nonce.len() - Self::NONCE_SIZE);
        let nonce = Nonce::<T>::try_from(nonce).expect("unreachable: nonce size mismatch");

        let (tag, msg) = match Self::TAG_POSITION {
            TagPosition::Prefix => {
                let (tag, msg) = ciphertext.split_at(Self::TAG_SIZE);
                (tag, msg)
            }
            TagPosition::Postfix => {
                let (msg, tag) = ciphertext.split_at(ciphertext.len() - Self::TAG_SIZE);
                (tag, msg)
            }
        };

        let tag = Tag::<Self>::try_from(tag).expect("tag length mismatch");
        self.decrypt_inout_detached(
            &nonce,
            aad,
            InOutBuf::new(msg, &mut output[..msg.len()]).expect("invalid output buffer size!"),
            &tag,
        )?;
        Ok(msg.len())
    }

    fn decrypt_with_nonce_detached(
        &self,
        ciphertext: &mut Vec<u8>,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce = Nonce::<T>::try_from(nonce).expect("nonce size mismatch");
        self.decrypt_in_place(&nonce, aad.as_ref(), ciphertext)?;
        Ok(())
    }

    fn encrypt_with_random_nonce(&self, plaintext: &[u8], aad: &[u8]) -> CryptoResult<Vec<u8>> {
        let mut nonce = Nonce::<Self>::default();
        SysRng
            .try_fill_bytes(nonce.as_mut_slice())
            .map_err(|_| CryptoError::CipherError)?;

        let mut encrypted = self.encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad,
            },
        )?;
        encrypted.extend_from_slice(&nonce);
        Ok(encrypted)
    }

    fn encrypt_with_fixed_nonce(
        &self,
        plaintext: &mut Vec<u8>,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce = Nonce::<T>::try_from(nonce).expect("nonce size mismatch");
        self.encrypt_in_place(&nonce, aad.as_ref(), plaintext)?;
        Ok(())
    }
}
