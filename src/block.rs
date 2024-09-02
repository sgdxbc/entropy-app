use std::{collections::BTreeMap, iter::repeat_with};

use bytes::Bytes;
use ed25519_dalek::{ed25519::signature::Signer as _, Signature, SigningKey};
use merkle::{algorithms::Sha256, Hasher, MerkleTree};
use rand::Rng;

pub struct Block {
    chunks: Vec<Bytes>,
    merkle: MerkleTree<Sha256>,
    signature: Signature,
}

pub struct Packet {
    chunks: BTreeMap<usize, Bytes>,
}

pub struct PacketProof {
    //
}

pub struct Parameters {
    pub chunk_size: usize,
    pub k: usize,
}

impl Block {
    pub fn generate(mut rng: impl Rng, parameters: &Parameters, key: &SigningKey) -> Self {
        let chunks = repeat_with(|| {
            let mut buf = vec![0; parameters.chunk_size];
            rng.fill_bytes(&mut buf);
            Bytes::from(buf)
        })
        .take(parameters.k)
        .collect::<Vec<_>>();
        Self::new(chunks, key)
    }

    pub fn new(chunks: Vec<Bytes>, key: &SigningKey) -> Self {
        let merkle = MerkleTree::from_leaves(
            &chunks
                .iter()
                .map(|chunk| Sha256::hash(chunk))
                .collect::<Vec<_>>(),
        );
        // not sure why types not check with this...
        // let signature = key.sign(&merkle.root().expect("root exists"));
        let root_hash: [_; 32] = merkle.root().expect("root exists");
        let signature = key.sign(&root_hash);
        Self {
            chunks,
            merkle,
            signature,
        }
    }

    pub fn encode(&self, indexes: Vec<usize>) -> Packet {
        let mut chunks = BTreeMap::new();
        for index in indexes {
            chunks.insert(index, self.chunks[index].clone());
        }
        Packet { chunks }
    }
}
