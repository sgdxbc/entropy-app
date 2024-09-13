use std::{collections::BTreeMap, ffi::OsStr, path::PathBuf};

use bytes::Bytes;
use ed25519_dalek::Signature;
use tokio::fs::{create_dir, create_dir_all, read, read_dir, remove_dir_all, write, ReadDir};

use crate::block::{MerkleHash, MerkleProof, Packet};

pub struct Store {
    pub path: PathBuf,
}

impl Store {
    pub fn new(path: PathBuf) -> Self {
        // not create dir here, because not remove dir on drop (which is because of the lack of
        // async drop)
        Self { path }
    }

    pub async fn save(&self, packet: Packet) -> anyhow::Result<()> {
        let packet_dir = self.packet_dir(packet.block_id());
        create_dir_all(&packet_dir).await?;
        write(packet_dir.join("signature"), packet.signature().to_bytes()).await?;
        for (index, (buf, proof)) in &packet.chunks {
            let chunk_dir = packet_dir.join(index.to_string());
            // TODO better handling, probably just do not send
            if chunk_dir.is_dir() {
                continue;
            }
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
            let index = entry
                .path()
                .file_name()
                .and_then(OsStr::to_str)
                .ok_or(anyhow::format_err!("invalid chunk directory name"))?
                .parse::<usize>()?;
            chunks.insert(index, (buf, proof));
        }
        Ok(Some(Packet {
            chunks,
            root_certificate: (block_id, signature),
        }))
    }

    pub async fn load_singletons(&self, block_id: MerkleHash) -> anyhow::Result<Option<LoadIter>> {
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
        let read_dir = read_dir(packet_dir).await?;
        Ok(Some(LoadIter {
            block_id,
            read_dir,
            signature,
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

#[derive(Debug)]
pub struct LoadIter {
    block_id: MerkleHash,
    read_dir: ReadDir,
    signature: Signature,
}

impl LoadIter {
    pub async fn next(&mut self) -> anyhow::Result<Option<(usize, Packet)>> {
        let Some(entry) = self.read_dir.next_entry().await? else {
            return Ok(None);
        };
        if !entry.file_type().await?.is_dir() {
            return Box::pin(self.next()).await;
        }
        let buf = Bytes::from(read(entry.path().join("chunk")).await?);
        let proof = MerkleProof::from_bytes(&read(entry.path().join("proof")).await?)?;
        let index = entry
            .path()
            .file_name()
            .and_then(OsStr::to_str)
            .ok_or(anyhow::format_err!("invalid chunk directory name"))?
            .parse::<usize>()?;
        Ok(Some((
            index,
            Packet {
                chunks: [(index, (buf, proof))].into_iter().collect(),
                root_certificate: (self.block_id, self.signature),
            },
        )))
    }
}
