use hkdf::Hkdf;
use hkdf::hmac::EagerHash;
use sha2::digest::OutputSizeUser;

pub trait StableIdentifiersGenerator: Send + Sync {
    /// Returns an opaque (i.e. can't be computed without a key) but stable (i.e. is always the same
    /// given the same inputs) identifier, derived from a private key.
    fn get_opaque_stable_identifier(&self, data: &[u8], length_bits: usize) -> Vec<u8> {
        let len_bytes = length_bits.div_ceil(8);
        let mut output = vec![0u8; len_bytes];

        self.get_opaque_stable_identifier_in(data, &mut output);

        // Change last byte
        let last_byte_bits = length_bits % 8;
        if last_byte_bits > 0 {
            output[0] >>= (8 - last_byte_bits) as u32;
        }

        output
    }

    fn get_opaque_stable_identifier_in(&self, data: &[u8], output: &mut [u8]);

    fn get_opaque_stable_identifier_hex(&self, data: &[u8], length_bits: usize) -> String {
        hex::encode(
            self.get_opaque_stable_identifier(data, length_bits)
                .as_slice(),
        )
    }
}

impl<T: OutputSizeUser + EagerHash + Send + Sync> StableIdentifiersGenerator for Hkdf<T>
where
    <T as EagerHash>::Core: Send + Sync,
{
    fn get_opaque_stable_identifier_in(&self, data: &[u8], output: &mut [u8]) {
        self.expand(data, output).unwrap();
    }
}
