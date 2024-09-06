use std::{collections::BTreeMap, ffi::OsStr, path::PathBuf};

use bytes::Bytes;
use ed25519_dalek::Signature;
use tokio::fs::{create_dir, create_dir_all, read, read_dir, remove_dir_all, write};

use crate::block::{MerkleHash, MerkleProof, Packet};

pub struct Store {
    path: PathBuf,
}

impl Store {
    pub async fn new(path: PathBuf) -> anyhow::Result<Self> {
        create_dir(&path).await?;
        Ok(Self { path })
    }

    pub async fn save(&self, packet: Packet) -> anyhow::Result<()> {
        let packet_dir = self.packet_dir(packet.block_id());
        create_dir_all(&packet_dir).await?;
        write(packet_dir.join("signature"), packet.signature().to_bytes()).await?;
        for (index, (buf, proof)) in &packet.chunks {
            let chunk_dir = packet_dir.join(index.to_string());
            create_dir(&chunk_dir).await?;
            write(chunk_dir.join("chunk"), buf).await?;
            write(chunk_dir.join("proof"), proof.to_bytes()).await?
        }
        Ok(())
    }

    pub async fn load(&self, block_id: MerkleHash) -> anyhow::Result<Option<Packet>> {
        let packet_dir = self.packet_dir(block_id);
        if !packet_dir.is_dir() {
            return Ok(None);
        }
        let signature = Signature::from_bytes(
            &read(packet_dir.join("signature"))
                .await?
                .try_into()
                .map_err(|_| anyhow::format_err!("invalid signature bytes length"))?,
        );
        let mut chunks = BTreeMap::new();
        let mut read_dir = read_dir(packet_dir).await?;
        while let Some(entry) = read_dir.next_entry().await? {
            if !entry.file_type().await?.is_dir() {
                continue;
            }
            let buf = Bytes::from(read(entry.path().join("chunk")).await?);
            let proof = MerkleProof::from_bytes(&read(entry.path().join("proof")).await?)?;
            chunks.insert(
                entry
                    .path()
                    .file_name()
                    .and_then(OsStr::to_str)
                    .ok_or(anyhow::format_err!("invalid chunk directory name"))?
                    .parse::<usize>()?,
                (buf, proof),
            );
        }
        Ok(Some(Packet {
            chunks,
            root_certificate: (block_id, signature),
        }))
    }

    pub async fn remove(&self, block_id: MerkleHash) -> anyhow::Result<()> {
        remove_dir_all(self.packet_dir(block_id)).await?;
        Ok(())
    }

    fn packet_dir(&self, block_id: primitive_types::H256) -> PathBuf {
        self.path.join(format!("{:?}", block_id))
    }
}
