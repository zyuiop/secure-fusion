use crate::cipher::{Cipher, CipherSizes};
use crate::error::{CryptoError, CryptoResult};
use aws_lc_rs::aead::{Aad, Algorithm, LessSafeKey, NONCE_LEN, UnboundKey};
use rand::TryRng;
use rand::rngs::SysRng;

pub(crate) struct AwsLcCipher {
    key: LessSafeKey,
}

impl AwsLcCipher {
    pub fn new(algo: &'static Algorithm, key: &[u8]) -> Self {
        let key = LessSafeKey::new(
            UnboundKey::new(algo, key).expect("failed to instantiate crypto backend"),
        );
        Self { key }
    }
}

impl CipherSizes for AwsLcCipher {
    fn nonce_size(&self) -> usize {
        NONCE_LEN
    }

    fn tag_size(&self) -> usize {
        self.key.algorithm().tag_len()
    }
}

impl Cipher for AwsLcCipher {
    fn decrypt_with_nonce_to_slice(
        &self,
        output: &mut [u8],
        ciphertext_with_nonce: &[u8],
        aad: &[u8],
    ) -> CryptoResult<usize> {
        if ciphertext_with_nonce.len() < NONCE_LEN {
            // Value does not seem to be a valid ciphertext
            return Err(CryptoError::CipherError);
        }

        let (ciphertext, nonce) =
            ciphertext_with_nonce.split_at(ciphertext_with_nonce.len() - NONCE_LEN);

        let nonce: [u8; NONCE_LEN] = nonce.try_into().unwrap();
        let nonce = aws_lc_rs::aead::Nonce::from(&nonce);

        let mut output_vec = Vec::from(ciphertext);

        let result = self
            .key
            .open_in_place(nonce, Aad::from(aad), &mut output_vec)
            .map_err(|_| CryptoError::CipherError)?;

        (&mut output[0..result.len()]).copy_from_slice(result);

        Ok(result.len())
    }

    fn decrypt_with_nonce_detached(
        &self,
        ciphertext: &mut Vec<u8>,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce: [u8; NONCE_LEN] = nonce.try_into().unwrap();
        let nonce = aws_lc_rs::aead::Nonce::from(&nonce);

        self.key
            .open_in_place(nonce, Aad::from(aad), ciphertext)
            .map_err(|_| CryptoError::CipherError)?;
        Ok(())
    }

    fn encrypt_with_random_nonce(&self, plaintext: &[u8], aad: &[u8]) -> CryptoResult<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        SysRng
            .try_fill_bytes(nonce.as_mut_slice())
            .map_err(|_| CryptoError::CipherError)?;

        let aws_nonce = aws_lc_rs::aead::Nonce::from(&nonce);
        let mut buffer = Vec::from(plaintext);
        self.key
            .seal_in_place_append_tag(aws_nonce, Aad::from(aad.as_ref()), &mut buffer)
            .map_err(|_| CryptoError::CipherError)?;
        buffer.extend_from_slice(nonce.as_ref());
        Ok(buffer)
    }

    fn encrypt_with_fixed_nonce(
        &self,
        plaintext: &mut Vec<u8>,
        nonce: &[u8],
        aad: &dyn AsRef<[u8]>,
    ) -> CryptoResult<()> {
        let nonce: [u8; NONCE_LEN] = nonce.try_into().expect("nonce size mismatch");
        let nonce = aws_lc_rs::aead::Nonce::from(&nonce);

        self.key
            .seal_in_place_append_tag(nonce, Aad::from(aad.as_ref()), plaintext)
            .map_err(|_| CryptoError::CipherError)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::aws_lc_compat::CompatAwsLcHkdfSha256KeyManager;
    use crate::cipher::Cipher;
    use crate::hmac_sha256::LegacyKeyManager;
    use crate::{CipherContext, PrincipalKeyManager};
    use aws_lc_rs::aead::CHACHA20_POLY1305;
    use chacha20poly1305::ChaCha20Poly1305;
    use datafusion::common::ResolvedTableReference;
    use std::sync::Arc;

    const MESSAGE: &[u8] = b"HELLO, WORLD!";
    const AAD: &[u8] = b"BONJOUR";

    fn get_rustcrypto() -> Arc<dyn Cipher> {
        LegacyKeyManager::<ChaCha20Poly1305>::from_hex_key_and_principal_id(
            0,
            "b6fd00728958b706fde7f9d5fde9637a5feadab81688b480ae66dcc3393f1284",
        )
        .get_cipher(&CipherContext::TableColumn {
            table_context: ResolvedTableReference {
                schema: String::from("def").into(),
                table: String::from("bench").into(),
                catalog: String::from("bench").into(),
            },
            column_name: "COUCOU".to_string().into(),
        })
    }

    fn get_aws_cipher() -> Arc<dyn Cipher> {
        CompatAwsLcHkdfSha256KeyManager::from_hex_key_and_principal_id(
            0,
            "b6fd00728958b706fde7f9d5fde9637a5feadab81688b480ae66dcc3393f1284",
            &CHACHA20_POLY1305,
        )
        .get_cipher(&CipherContext::TableColumn {
            table_context: ResolvedTableReference {
                schema: String::from("def").into(),
                table: String::from("bench").into(),
                catalog: String::from("bench").into(),
            },
            column_name: "COUCOU".to_string().into(),
        })
    }

    #[test]
    fn aws_can_decrypt_rustcrypto() {
        let rustcrypto = get_rustcrypto()
            .encrypt_with_random_nonce(MESSAGE, &[])
            .unwrap();

        let mut message_copy = vec![0u8; 1024];
        let decrypted = get_aws_cipher()
            .decrypt_with_nonce_to_slice(&mut message_copy, rustcrypto.as_slice(), &[])
            .unwrap();

        assert_eq!(&message_copy[0..decrypted], MESSAGE);
    }

    #[test]
    fn aws_can_decrypt_rustcrypto_with_aad() {
        let rustcrypto = get_rustcrypto()
            .encrypt_with_random_nonce(MESSAGE, AAD)
            .unwrap();

        let mut message_copy = vec![0u8; 1024];
        let decrypted = get_aws_cipher()
            .decrypt_with_nonce_to_slice(&mut message_copy, rustcrypto.as_slice(), AAD)
            .unwrap();

        assert_eq!(&message_copy[0..decrypted], MESSAGE);
    }
}
