pub mod block;
pub mod broadcast; // should be pub/sub but no plan to implement topic

pub fn generate_signing_key(rng: impl rand::Rng + rand::CryptoRng) -> ed25519_dalek::SigningKey {
    struct W<T>(T);
    impl<T: rand::Rng> rand_stable::RngCore for W<T> {
        fn fill_bytes(&mut self, dest: &mut [u8]) {
            <T as rand::RngCore>::fill_bytes(&mut self.0, dest)
        }

        fn try_fill_bytes(&mut self, dest: &mut [u8]) -> Result<(), rand_stable::Error> {
            self.fill_bytes(dest);
            Ok(())
        }

        fn next_u32(&mut self) -> u32 {
            <T as rand::RngCore>::next_u32(&mut self.0)
        }

        fn next_u64(&mut self) -> u64 {
            <T as rand::RngCore>::next_u64(&mut self.0)
        }
    }
    impl<T: rand::CryptoRng> rand_stable::CryptoRng for W<T> {}
    ed25519_dalek::SigningKey::generate(&mut W(rng))
}

pub fn thread_rng() -> rand_stable::rngs::ThreadRng {
    rand_stable::thread_rng()
}

pub type PeerId = [u8; 32];
