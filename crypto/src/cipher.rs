use crate::error::{CryptoError, CryptoResult};
use aead::inout::InOutBuf;
use aead::{Aead, AeadInOut, Buffer, Nonce, Payload, Tag, TagPosition};
use datafusion::arrow::datatypes::DataType;
use log::warn;
use sha2::digest::crypto_common::getrandom;
use std::fmt::Debug;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct AssociatedData(Vec<u8>);

impl AssociatedData {
    pub fn column_with_type(
        table_name: &str,
        column: &str,
        data_type: &DataType,
    ) -> AssociatedData {
        AssociatedData(
            format!("col_with_type{{tbl:{table_name},col:{column},dt:{data_type}}}").into_bytes(),
        )
    }

    pub fn omitted() -> AssociatedData {
        AssociatedData("omitted".to_string().into_bytes())
    }
}

impl AsRef<[u8]> for AssociatedData {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

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
    fn decrypt_with_nonce(
        &self,
        ciphertext_with_nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<Vec<u8>>;

    /// Decrypts a plaintext to an output slice, returning the written bytes count.
    ///
    /// ## Panics
    ///
    /// If output.len() < plaintext size
    fn decrypt_with_nonce_to_slice(
        &self,
        output: &mut [u8],
        ciphertext_with_nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<usize>;

    fn decrypt_with_nonce_detached(
        &self,
        ciphertext: &mut dyn Buffer,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()>;

    fn decrypt_with_nonce_in_place(
        &self,
        ciphertext_with_nonce: &mut Vec<u8>,
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()>;

    fn encrypt_with_random_nonce(
        &self,
        plaintext: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<Vec<u8>>;

    fn encrypt_with_fixed_nonce(
        &self,
        plaintext: &mut dyn Buffer,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()>;
}

impl<T: AeadInOut + Send + Sync> CipherAssociatedData for T {
    const NONCE_SIZE: usize = size_of::<Nonce<T>>();
    const TAG_SIZE: usize = size_of::<Tag<T>>();
}

impl<T: AeadInOut + Send + Sync> Cipher for T {
    fn decrypt_with_nonce(
        &self,
        ciphertext_with_nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<Vec<u8>> {
        let nonce_size = size_of::<Nonce<T>>();

        if ciphertext_with_nonce.len() < nonce_size {
            // Value does not seem to be a valid ciphertext
            return Err(CryptoError::CipherError);
        }

        let (ciphertext, nonce) =
            ciphertext_with_nonce.split_at(ciphertext_with_nonce.len() - nonce_size);
        let nonce = Nonce::<T>::try_from(nonce).expect("unreachable: nonce size mismatch");
        let decrypted_value = self.decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad: aad.as_ref(),
            },
        )?;

        Ok(decrypted_value)
    }

    fn decrypt_with_nonce_to_slice(
        &self,
        output: &mut [u8],
        ciphertext_with_nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
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
            aad.as_ref(),
            InOutBuf::new(msg, &mut output[..msg.len()]).expect("invalid output buffer size!"),
            &tag,
        )?;
        Ok(msg.len())
    }

    fn decrypt_with_nonce_detached(
        &self,
        ciphertext: &mut dyn Buffer,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce = Nonce::<T>::try_from(nonce).expect("nonce size mismatch");
        self.decrypt_in_place(&nonce, aad.as_ref(), ciphertext)?;
        Ok(())
    }

    fn decrypt_with_nonce_in_place(
        &self,
        ciphertext_with_nonce: &mut Vec<u8>,
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce_size = size_of::<Nonce<T>>();

        if ciphertext_with_nonce.len() < nonce_size {
            warn!("ciphertext too short");
            // Value does not seem to be a valid ciphertext
            return Err(CryptoError::CipherError);
        }

        let nonce = ciphertext_with_nonce.split_off(ciphertext_with_nonce.len() - nonce_size);
        let nonce =
            Nonce::<T>::try_from(nonce.as_slice()).expect("unreachable: nonce size mismatch");

        warn!(
            "decrypting in place: nonce={}, aad={}, ct={}",
            hex::encode(&nonce),
            hex::encode(aad.as_ref()),
            hex::encode(&ciphertext_with_nonce)
        );

        self.decrypt_in_place(&nonce, aad.as_ref(), ciphertext_with_nonce)?;

        Ok(())
    }

    fn encrypt_with_random_nonce(
        &self,
        plaintext: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<Vec<u8>> {
        let mut nonce = Nonce::<Self>::default();
        getrandom::fill(nonce.as_mut_slice())?;

        let mut encrypted = self.encrypt(
            &nonce,
            Payload {
                msg: plaintext,
                aad: aad.as_ref(),
            },
        )?;
        encrypted.extend_from_slice(&nonce);
        Ok(encrypted)
    }

    fn encrypt_with_fixed_nonce(
        &self,
        plaintext: &mut dyn Buffer,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce = Nonce::<T>::try_from(nonce).expect("nonce size mismatch");
        self.encrypt_in_place(&nonce, aad.as_ref(), plaintext)?;
        Ok(())
    }
}
/*

pub struct Chacha20Cipher(ChaCha20Poly1305);

impl KeySizeUser for Chacha20Cipher {
    type KeySize = <ChaCha20Poly1305 as KeySizeUser>::KeySize;
}

impl KeyInit for Chacha20Cipher {
    fn new(key: &Key<Self>) -> Self {
        Self(ChaCha20Poly1305::new(key))
    }
}

impl Cipher for Chacha20Cipher {
    fn nonce_size(&self) -> usize {
        size_of::<Nonce<ChaCha20Poly1305>>()
    }

    fn decrypt_with_nonce(&self, ciphertext_with_nonce: &[u8], aad: Option<&[u8]>) -> CryptoResult<Vec<u8>> {
        let (nonce, ciphertext) = extract_nonce::<ChaCha20Poly1305>(ciphertext_with_nonce);
        let decrypted_value = self.0.decrypt(&nonce, Payload {
            msg: ciphertext,
            aad: aad.unwrap_or(&[])
        })?;

        Ok(decrypted_value)
    }

    fn encrypt_with_random_nonce(&self, plaintext: &[u8], aad: Option<&[u8]>) -> CryptoResult<Vec<u8>> {
        todo!()
    }
}*/
