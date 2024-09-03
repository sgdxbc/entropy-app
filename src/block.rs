use std::{collections::BTreeMap, iter::repeat_with};

use bytes::Bytes;
use ed25519_dalek::{Signature, Signer as _, SigningKey, Verifier as _, VerifyingKey};
use merkle::{algorithms::Sha256, Hasher, MerkleProof, MerkleTree};
use rand::{seq::IteratorRandom, Rng};

type Chunk = (Bytes, MerkleProof<Sha256>);
type MerkleHash = <Sha256 as Hasher>::Hash;
type RootCertificate = (MerkleHash, Signature);

pub struct Block {
    chunks: Vec<Chunk>,
    // merkle: MerkleTree<Sha256>,
    root_certificate: RootCertificate,
}

pub struct Packet {
    chunks: BTreeMap<usize, Chunk>,
    root_certificate: RootCertificate,
}

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
            .map(|(index, chunk)| (chunk, merkle.proof(&[index])))
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
            let (buf, proof) = &self.chunks[index];
            // if later contribute to rs_merkle crate, remember to add `Clone` impl for MerkleProof
            let replaced = chunks.insert(
                index,
                (buf.clone(), MerkleProof::new(proof.proof_hashes().to_vec())),
            );
            anyhow::ensure!(replaced.is_none(), "indexes duplicate")
        }
        Ok(Packet {
            chunks,
            root_certificate: self.root_certificate,
        })
    }

    pub fn generate_with_degree(&self, degree: usize, mut rng: impl Rng) -> anyhow::Result<Packet> {
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
}
