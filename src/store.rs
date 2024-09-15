use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs::File,
    io::{Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use ed25519_dalek::Signature;
use tokio::{
    fs::{read, read_dir, remove_file},
    task::spawn_blocking,
};
use zip::{
    result::ZipError, write::SimpleFileOptions, CompressionMethod::Stored, ZipArchive, ZipWriter,
};

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
        // create_dir_all(&packet_dir).await?;
        // write(packet_dir.join("signature"), packet.signature().to_bytes()).await?;
        // for (index, (buf, proof)) in &packet.chunks {
        //     let chunk_dir = packet_dir.join(index.to_string());
        //     // TODO better handling, probably just do not send
        //     if chunk_dir.is_dir() {
        //         continue;
        //     }
        //     create_dir(&chunk_dir).await?;
        //     write(chunk_dir.join("chunk"), buf).await?;
        //     write(chunk_dir.join("proof"), proof.to_bytes()).await?
        // }
        spawn_blocking(move || {
            let options = SimpleFileOptions::default()
                .compression_method(Stored)
                .unix_permissions(0o755);
            let append = packet_dir.is_file();

            let mut zip = if append {
                let file = File::options().read(true).write(true).open(packet_dir)?;
                ZipWriter::new_append(file)?
            } else {
                ZipWriter::new(File::create(packet_dir)?)
            };
            match zip.start_file("signature", options) {
                Ok(()) => {
                    zip.write_all(&packet.signature().to_bytes())?;
                }
                Err(ZipError::InvalidArchive("Duplicate filename")) => {}
                Err(err) => Err(err)?,
            }
            for (index, (buf, proof)) in &packet.chunks {
                let chunk_dir = index.to_string();
                match zip.add_directory(&chunk_dir, options) {
                    Ok(()) => {}
                    Err(ZipError::InvalidArchive("Duplicate filename")) => continue,
                    Err(err) => Err(err)?,
                }
                zip.start_file(format!("{chunk_dir}/chunk"), options)?;
                zip.write_all(buf)?;
                zip.start_file(format!("{chunk_dir}/proof"), options)?;
                zip.write_all(&proof.to_bytes())?
            }
            zip.finish()?;
            anyhow::Ok(())
        })
        .await??;
        Ok(())
    }

    // TODO update
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
        // if !packet_dir.is_dir() {
        //     return Ok(None);
        // }
        // let signature = Signature::from_bytes(
        //     &read(packet_dir.join("signature"))
        //         .await?
        //         .try_into()
        //         .map_err(|_| anyhow::format_err!("invalid signature bytes length"))?,
        // );
        // let read_dir = read_dir(packet_dir).await?;
        // Ok(Some(LoadIter {
        //     block_id,
        //     read_dir,
        //     signature,
        // }))
        if !packet_dir.is_file() {
            return Ok(None);
        }
        spawn_blocking(move || {
            let mut zip = ZipArchive::new(File::open(packet_dir)?)?;
            let mut signature = Vec::new();
            zip.by_name("signature")?.read_to_end(&mut signature)?;
            let signature = Signature::from_bytes(
                &signature
                    .try_into()
                    .map_err(|_| anyhow::format_err!("invalid signature bytes length"))?,
            );
            anyhow::Ok(Some(LoadIter {
                block_id,
                signature,
                zip: Arc::new(Mutex::new((zip, 0))),
            }))
        })
        .await?
    }

    pub async fn remove(&self, block_id: MerkleHash) -> anyhow::Result<()> {
        // remove_dir_all(self.packet_dir(block_id)).await?;
        remove_file(self.packet_dir(block_id)).await?;
        Ok(())
    }

    fn packet_dir(&self, block_id: primitive_types::H256) -> PathBuf {
        self.path.join(format!("{:?}", block_id))
    }
}

#[derive(Debug)]
pub struct LoadIter {
    block_id: MerkleHash,
    signature: Signature,

    // read_dir: ReadDir,
    zip: Arc<Mutex<(ZipArchive<File>, usize)>>,
}

impl LoadIter {
    pub async fn next(&mut self) -> anyhow::Result<Option<(usize, Packet)>> {
        // let Some(entry) = self.read_dir.next_entry().await? else {
        //     return Ok(None);
        // };
        // if !entry.file_type().await?.is_dir() {
        //     return Box::pin(self.next()).await;
        // }
        // let buf = Bytes::from(read(entry.path().join("chunk")).await?);
        // let proof = MerkleProof::from_bytes(&read(entry.path().join("proof")).await?)?;
        // let index = entry
        //     .path()
        //     .file_name()
        //     .and_then(OsStr::to_str)
        //     .ok_or(anyhow::format_err!("invalid chunk directory name"))?
        //     .parse::<usize>()?;

        let zip = self.zip.clone();
        let Some((index, buf, proof)) = spawn_blocking(move || {
            let (zip, index) = &mut *zip.lock().expect("can lock");
            let mut chunk_dir;
            let zip_len = zip.len();
            loop {
                if *index == zip_len {
                    return Ok(None);
                }
                chunk_dir = zip.by_index(*index)?;
                *index += 1;
                if chunk_dir.is_dir() {
                    break;
                }
                drop(chunk_dir)
            }
            let index = chunk_dir
                .name()
                .strip_suffix('/')
                .ok_or(anyhow::format_err!("not a directory"))?
                .parse::<usize>()?;
            drop(chunk_dir);
            let mut buf = Vec::new();
            zip.by_name(&format!("{index}/chunk"))?
                .read_to_end(&mut buf)?;
            let buf = Bytes::from(buf);
            let mut proof = Vec::new();
            zip.by_name(&format!("{index}/proof"))?
                .read_to_end(&mut proof)?;
            let proof = MerkleProof::from_bytes(&proof)?;
            anyhow::Ok(Some((index, buf, proof)))
        })
        .await??
        else {
            return Ok(None);
        };

        Ok(Some((
            index,
            Packet {
                chunks: [(index, (buf, proof))].into_iter().collect(),
                root_certificate: (self.block_id, self.signature),
            },
        )))
    }
}
