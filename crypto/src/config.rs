#[cfg(feature = "crypto_aws_lc")]
use crate::aws_lc::AwsLcHkdfSha256KeyManager;
use log::info;
use std::sync::Arc;

#[cfg(feature = "crypto_aws_lc")]
use crate::aws_lc_compat::CompatAwsLcHkdfSha256KeyManager;

use crate::LongTermKeyManager;

#[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
use crate::key_management::rustcrypto::RustCryptoHkdfSha256KeyManager;

#[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
use crate::key_management::hmac_sha256::LegacyKeyManager;

#[cfg(feature = "crypto_aws_lc")]
mod aws {
    use aws_lc_rs::aead::{AES_128_GCM, AES_256_GCM, Algorithm, CHACHA20_POLY1305};

    #[derive(serde::Deserialize, serde::Serialize, Debug)]
    pub enum AwsCryptoAlg {
        Aes128Gcm,
        Aes256Gcm,
        ChaCha20Poly1305,
    }

    impl AwsCryptoAlg {
        pub(super) fn get_alg(&self) -> &'static Algorithm {
            match self {
                AwsCryptoAlg::Aes128Gcm => &AES_128_GCM,
                AwsCryptoAlg::Aes256Gcm => &AES_256_GCM,
                AwsCryptoAlg::ChaCha20Poly1305 => &CHACHA20_POLY1305,
            }
        }
    }
}

#[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub enum RustCryptoAlg {
    #[cfg(feature = "rustcrypto_aes")]
    Aes128Gcm,
    #[cfg(feature = "rustcrypto_aes")]
    Aes256Gcm,
    #[cfg(feature = "rustcrypto_chacha")]
    ChaCha20Poly1305,
    #[cfg(feature = "rustcrypto_chacha")]
    ChaCha8Poly1305,
}

#[derive(serde::Deserialize, serde::Serialize, Debug)]
pub enum CryptoManager {
    #[cfg(feature = "crypto_aws_lc")]
    LegacyAws(aws::AwsCryptoAlg),
    #[cfg(feature = "crypto_aws_lc")]
    Aws(aws::AwsCryptoAlg),

    #[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
    Legacy(RustCryptoAlg),
    #[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
    RustCrypto(RustCryptoAlg),
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct CryptoConfig {
    secret_key: String,
    backend: CryptoManager,
}

impl CryptoConfig {
    pub fn init_key_manager(self) -> Arc<LongTermKeyManager> {
        info!("Using key manager {:?}", self.backend);

        let km: LongTermKeyManager = match &self.backend {
            #[cfg(feature = "crypto_aws_lc")]
            CryptoManager::LegacyAws(aws) => Box::new(
                CompatAwsLcHkdfSha256KeyManager::from_hex_key_and_principal_id(
                    0,
                    &self.secret_key,
                    aws.get_alg(),
                ),
            ),

            #[cfg(feature = "crypto_aws_lc")]
            CryptoManager::Aws(aws) => {
                Box::new(AwsLcHkdfSha256KeyManager::from_hex_key_and_principal_id(
                    0,
                    &self.secret_key,
                    aws.get_alg(),
                ))
            }

            #[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
            CryptoManager::Legacy(rc) => match rc {
                #[cfg(feature = "rustcrypto_aes")]
                RustCryptoAlg::Aes128Gcm => Box::new(
                    LegacyKeyManager::<aes_gcm::Aes128Gcm>::from_hex_key_and_principal_id(
                        0,
                        &self.secret_key,
                    ),
                ),
                #[cfg(feature = "rustcrypto_aes")]
                RustCryptoAlg::Aes256Gcm => Box::new(
                    LegacyKeyManager::<aes_gcm::Aes256Gcm>::from_hex_key_and_principal_id(
                        0,
                        &self.secret_key,
                    ),
                ),
                #[cfg(feature = "rustcrypto_chacha")]
                RustCryptoAlg::ChaCha20Poly1305 => Box::new(LegacyKeyManager::<
                    chacha20poly1305::ChaCha20Poly1305,
                >::from_hex_key_and_principal_id(
                    0, &self.secret_key
                )),
                #[cfg(feature = "rustcrypto_chacha")]
                RustCryptoAlg::ChaCha8Poly1305 => Box::new(LegacyKeyManager::<
                    chacha20poly1305::ChaCha8Poly1305,
                >::from_hex_key_and_principal_id(
                    0, &self.secret_key
                )),
            },

            #[cfg(any(feature = "rustcrypto_aes", feature = "rustcrypto_chacha"))]
            CryptoManager::RustCrypto(rc) => match rc {
                RustCryptoAlg::Aes128Gcm => Box::new(RustCryptoHkdfSha256KeyManager::<
                    aes_gcm::Aes128Gcm,
                >::from_hex_key_and_principal_id(
                    0, &self.secret_key
                )),
                RustCryptoAlg::Aes256Gcm => Box::new(RustCryptoHkdfSha256KeyManager::<
                    aes_gcm::Aes256Gcm,
                >::from_hex_key_and_principal_id(
                    0, &self.secret_key
                )),
                #[cfg(feature = "rustcrypto_chacha")]
                RustCryptoAlg::ChaCha20Poly1305 => Box::new(RustCryptoHkdfSha256KeyManager::<
                    chacha20poly1305::ChaCha20Poly1305,
                >::from_hex_key_and_principal_id(
                    0, &self.secret_key
                )),
                #[cfg(feature = "rustcrypto_chacha")]
                RustCryptoAlg::ChaCha8Poly1305 => Box::new(RustCryptoHkdfSha256KeyManager::<
                    chacha20poly1305::ChaCha8Poly1305,
                >::from_hex_key_and_principal_id(
                    0, &self.secret_key
                )),
            },
        };

        Arc::new(km)
    }
}

impl Default for CryptoConfig {
    fn default() -> CryptoConfig {
        Self {
            #[cfg(feature = "crypto_aws_lc")]
            backend: CryptoManager::LegacyAws(aws::AwsCryptoAlg::ChaCha20Poly1305),
            #[cfg(not(feature = "crypto_aws_lc"))]
            backend: CryptoManager::Legacy(RustCryptoAlg::ChaCha20Poly1305),

            secret_key: "b6fd00728958b706fde7f9d5fde9637a5feadab81688b480ae66dcc3393f1284"
                .to_string(),
        }
    }
}
