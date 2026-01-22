use hkdf::Hkdf;
use hkdf::hmac::EagerHash;
use sha2::digest::OutputSizeUser;

pub trait RawKeyGenerator: Send + Sync {
    fn generate_key<'r>(&self, provided_data: &[u8], output: &'r mut [u8]) -> Option<&'r mut [u8]>;
}

impl<T: OutputSizeUser + EagerHash + Send + Sync> RawKeyGenerator for Hkdf<T>
where
    <T as EagerHash>::Core: Send + Sync,
{
    fn generate_key<'r>(&self, provided_data: &[u8], output: &'r mut [u8]) -> Option<&'r mut [u8]> {
        self.expand(provided_data, output).ok().map(|_| output)
    }
}
