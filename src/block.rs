use std::{any::type_name, collections::BTreeMap, fmt::Debug, iter::repeat_with};

use bytes::Bytes;
use derive_more::{Deref, DerefMut};
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use merkle::{algorithms::Sha256, Hasher, MerkleTree};
use rand::{seq::IteratorRandom, Rng};

pub mod distr;

// if later contribute to rs_merkle crate, remember to add `Clone` and `Debug`
//impl for MerkleProof
#[derive(Deref, DerefMut)]
pub struct MerkleProof(merkle::MerkleProof<Sha256>);

impl Debug for MerkleProof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(type_name::<Self>()).finish_non_exhaustive()
    }
}

impl Clone for MerkleProof {
    fn clone(&self) -> Self {
        Self(merkle::MerkleProof::new(self.proof_hashes().to_vec()))
    }
}

impl MerkleProof {
    fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        Ok(Self(merkle::MerkleProof::from_bytes(bytes)?))
    }
}

type Chunk = (Bytes, MerkleProof);
type MerkleHash = <Sha256 as Hasher>::Hash;
type RootCertificate = (MerkleHash, Signature);

#[derive(Debug)]
pub struct Block {
    pub chunks: Vec<Chunk>,
    // merkle: MerkleTree<Sha256>,
    root_certificate: RootCertificate,
}

#[derive(Debug, Clone)]
pub struct Packet {
    chunks: BTreeMap<usize, Chunk>,
    root_certificate: RootCertificate,
}

#[derive(Debug)]
pub struct Parameters {
    pub chunk_size: usize,
    pub k: usize,
}

impl Block {
    pub fn generate(mut rng: impl Rng, key: &SigningKey, parameters: &Parameters) -> Self {
        let chunks = repeat_with(|| {
            let mut buf = vec![0; parameters.chunk_size];
            rng.fill_bytes(&mut buf);
            Bytes::from(buf)
        })
        .take(parameters.k)
        .collect::<Vec<_>>();
        Self::new(chunks, key)
    }

    pub fn new(unchecked_chunks: Vec<Bytes>, key: &SigningKey) -> Self {
        let merkle = MerkleTree::from_leaves(
            &unchecked_chunks
                .iter()
                .map(|chunk| Sha256::hash(chunk))
                .collect::<Vec<_>>(),
        );
        let chunks = unchecked_chunks
            .into_iter()
            .enumerate()
            .map(|(index, chunk)| (chunk, MerkleProof(merkle.proof(&[index]))))
            .collect();
        // may be possible to save a SHA512 digesting by a SHA512 merkle tree and treat root hash as
        // prehashed input for signing
        // but a whole SHA512 tree is probably more expensive
        // not sure why types not check with this...
        // let signature = key.sign(&merkle.root().expect("root exists"));
        let root_hash: MerkleHash = merkle.root().expect("root exists");
        let signature = key.sign(&root_hash);
        Self {
            chunks,
            // merkle,
            root_certificate: (root_hash, signature),
        }
    }

    pub fn encode(&self, indexes: Vec<usize>) -> anyhow::Result<Packet> {
        let mut chunks = BTreeMap::new();
        for index in indexes {
            let replaced = chunks.insert(index, self.chunks[index].clone());
            anyhow::ensure!(replaced.is_none(), "indexes duplicate")
        }
        Ok(Packet {
            chunks,
            root_certificate: self.root_certificate,
        })
    }

    pub fn generate_with_degree(&self, degree: usize, mut rng: impl Rng) -> anyhow::Result<Packet> {
        anyhow::ensure!(degree > 0);
        anyhow::ensure!(degree <= self.chunks.len());
        self.encode((0..self.chunks.len()).choose_multiple(&mut rng, degree))
    }
}

impl Packet {
    pub fn to_bytes(&self) -> Bytes {
        let mut scratch = Vec::new();
        scratch.extend(self.root_certificate.0);
        scratch.extend(self.root_certificate.1.to_bytes());
        for (index, (buf, proof)) in &self.chunks {
            scratch.extend(index.to_le_bytes());
            let proof_bytes = proof.to_bytes();
            scratch.extend(proof_bytes.len().to_le_bytes());
            scratch.extend(proof_bytes);
            scratch.extend(buf)
        }
        scratch.into()
    }

    pub fn from_bytes(mut bytes: Bytes, parameters: &Parameters) -> anyhow::Result<Self> {
        anyhow::ensure!(bytes.len() >= size_of::<MerkleHash>());
        let mut root_hash = [0; size_of::<MerkleHash>()];
        root_hash.copy_from_slice(&bytes.split_to(Signature::BYTE_SIZE));

        anyhow::ensure!(bytes.len() >= Signature::BYTE_SIZE);
        let mut signature_bytes = [0; Signature::BYTE_SIZE];
        signature_bytes.copy_from_slice(&bytes.split_to(Signature::BYTE_SIZE));
        let signature = Signature::from_bytes(&signature_bytes);

        let mut chunks = BTreeMap::new();
        while !bytes.is_empty() {
            anyhow::ensure!(bytes.len() >= size_of::<usize>());
            let mut index_bytes = [0; size_of::<usize>()];
            index_bytes.copy_from_slice(&bytes.split_to(size_of::<usize>()));
            let index = usize::from_le_bytes(index_bytes);

            anyhow::ensure!(bytes.len() >= size_of::<usize>());
            let mut proof_bytes_len_bytes = [0; size_of::<usize>()];
            proof_bytes_len_bytes.copy_from_slice(&bytes.split_to(size_of::<usize>()));
            let proof_bytes_len = usize::from_le_bytes(proof_bytes_len_bytes);

            anyhow::ensure!(bytes.len() >= proof_bytes_len);
            let proof_bytes = bytes.split_to(proof_bytes_len);
            let proof = MerkleProof::from_bytes(&proof_bytes)?;

            anyhow::ensure!(bytes.len() >= parameters.chunk_size);
            let buf = bytes.split_to(parameters.chunk_size);

            let replaced = chunks.insert(index, (buf, proof));
            anyhow::ensure!(replaced.is_none(), "indexes duplicate")
        }
        Ok(Self {
            chunks,
            root_certificate: (root_hash, signature),
        })
    }

    pub fn verify(&self, key: &VerifyingKey, parameters: &Parameters) -> anyhow::Result<()> {
        let (root_hash, signature) = &self.root_certificate;
        key.verify(root_hash, signature)?;
        for (index, (buf, proof)) in &self.chunks {
            let verified = proof.verify(*root_hash, &[*index], &[Sha256::hash(buf)], parameters.k);
            anyhow::ensure!(verified)
        }
        Ok(())
    }

    pub fn can_recover(&self, parameters: &Parameters) -> bool {
        // probably impossible to get more than k right?
        self.chunks.len() == parameters.k
    }

    pub fn recover(self, parameters: &Parameters) -> anyhow::Result<Block> {
        anyhow::ensure!(self.can_recover(parameters));
        let block = Block {
            // self.chunks is a BTreeMap that contains key [0, k)
            // the iteration will be in ascending order
            chunks: self.chunks.into_values().collect(),
            root_certificate: self.root_certificate,
        };
        Ok(block)
    }

    pub fn merge(&mut self, other: Self) {
        // TODO check for same root certificate?
        self.chunks.extend(other.chunks)
    }
}

#[cfg(test)]
mod tests {
    use rand::thread_rng;

    use super::*;

    #[test]
    fn can_verify_valid() {
        let p = Parameters {
            chunk_size: 64,
            k: 1024,
        };
        let mut rng = thread_rng();
        let key = crate::generate_signing_key(&mut rng);
        let block = Block::generate(&mut rng, &key, &p);

        for _ in 0..10 {
            let d = (0..p.k).choose(&mut rng).unwrap();
            let packet = block.generate_with_degree(d, &mut rng).unwrap();
            packet.verify(&key.verifying_key(), &p).unwrap()
        }
    }

    #[test]
    fn cannot_verify_invalid() {
        let p = Parameters {
            chunk_size: 64,
            k: 1024,
        };
        let mut rng = thread_rng();
        let key = crate::generate_signing_key(&mut rng);
        let block = Block::generate(&mut rng, &key, &p);
        let packet = block.generate_with_degree(1, &mut rng).unwrap();

        let mut malformed = packet.clone();
        let (index, chunk) = malformed.chunks.pop_first().unwrap();
        malformed.chunks.insert(index + 1 % p.k, chunk);
        let result = malformed.verify(&key.verifying_key(), &p);
        assert!(result.is_err());

        let mut malformed = packet.clone();
        let entry = &mut malformed.chunks.first_entry().unwrap();
        let (buf, _) = entry.get_mut();
        let mut malformed_buf = buf.to_vec();
        malformed_buf[0] = (malformed_buf[0] == 0) as _;
        *buf = malformed_buf.into();
        let result = malformed.verify(&key.verifying_key(), &p);
        assert!(result.is_err());
    }
}
