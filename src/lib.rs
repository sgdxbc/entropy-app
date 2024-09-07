pub mod app;
pub mod block;
pub mod broadcast; // should be pub/sub but no plan to implement topic
pub mod server;
pub mod store;

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

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    use merkle::Hasher as _;
    merkle::algorithms::Sha256::hash(bytes)
}

pub static CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(reqwest::Client::new);

pub use primitive_types::H256 as NodeId;

#[derive(Debug)]
pub struct Node {
    // pub id: PeerId,
    pub verifying_key: ed25519_dalek::VerifyingKey,
    pub addr: std::net::SocketAddr,
}

pub type NodeBook = std::collections::HashMap<NodeId, Node>;

pub fn generate_peers(
    addrs: Vec<std::net::SocketAddr>,
    mut rng: impl rand::Rng + rand::CryptoRng,
) -> (
    NodeBook,
    std::collections::HashMap<NodeId, ed25519_dalek::SigningKey>,
) {
    let mut peers = NodeBook::new();
    let mut signing_keys = std::collections::HashMap::new();
    for addr in addrs {
        let key = generate_signing_key(&mut rng);
        let peer = Node {
            verifying_key: key.verifying_key(),
            addr,
        };
        let id = NodeId(sha256(peer.verifying_key.as_bytes()));
        let replaced = peers.insert(id, peer);
        assert!(replaced.is_none(), "peer id collision");
        signing_keys.insert(id, key);
    }
    (peers, signing_keys)
}

impl Node {
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }
}
