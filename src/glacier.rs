use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher as _,
    iter::repeat_with,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse as _, Response},
    routing::{get, post},
    Json, Router,
};
use bincode::Options as _;
use bytes::Bytes;
use ed25519_dalek::{SigningKey, VerifyingKey, PUBLIC_KEY_LENGTH};
use primitive_types::H256;
use rand::{thread_rng, RngCore as _};
use ring::BytesTtl;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    block::{distr::PacketDistr, Block, MerkleHash, Packet, Parameters},
    store::Store,
    NodeBook, NodeId, CLIENT,
};

pub mod ring;

pub struct Context {
    config: ContextConfig,
    store: Store,
    ring_messages: UnboundedReceiver<Bytes>,
}

#[derive(Debug)]
pub struct ContextConfig {
    pub nodes: NodeBook,
    pub local_id: NodeId,
    pub parameters: Parameters,
}

#[derive(Debug, Serialize, Deserialize)]
enum Message {
    Put(PutMessage),
    Get(GetMessage),
}

#[derive(Debug, Serialize, Deserialize)]
struct PutMessage {
    block_id: MerkleHash,
    node_id: NodeId,
}

#[derive(Debug, Serialize, Deserialize)]
struct GetMessage {
    block_id: MerkleHash,
    node_id: NodeId,
}

impl Context {
    pub fn new(
        config: ContextConfig,
        store: Store,
        ring_messages: UnboundedReceiver<Bytes>,
    ) -> Self {
        Self {
            config,
            store,
            ring_messages,
        }
    }

    pub async fn session(self) -> anyhow::Result<()> {
        Self::recv_session(
            self.config.nodes,
            self.config.local_id,
            self.store,
            self.config.parameters,
            self.ring_messages,
        )
        .await
    }

    async fn recv_session(
        nodes: NodeBook,
        local_id: NodeId,
        store: Store,
        parameters: Parameters,
        mut ring_messages: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        'recv: while let Some(bytes) = ring_messages.recv().await {
            let Ok(message) = bincode::options().deserialize::<Message>(&bytes) else {
                eprintln!("malformed broadcast message");
                continue;
            };
            match message {
                Message::Put(message) => {
                    let response = CLIENT
                        .post(format!(
                            "{}/glacier/encode/{:?}/{:?}",
                            nodes[&message.node_id].endpoint(),
                            message.block_id,
                            local_id,
                        ))
                        .send()
                        .await?;
                    if response.status() == StatusCode::NOT_FOUND {
                        eprintln!("put service unavailable");
                        continue 'recv;
                    }
                    let packet = Packet::from_bytes(
                        response.error_for_status()?.bytes().await?,
                        &parameters,
                    )?;
                    // if packet
                    //     .verify(&nodes[&message.node_id].verifying_key, &parameters)
                    if packet.block_id() != message.block_id
                        || packet.verify_merkle_proof(&parameters).is_err()
                    {
                        eprintln!("verify failed");
                        // do not request more packets from a faulty sender
                        // TODO discard received packets? probably no need since those are
                        // verified i.e. are "good" storage
                        continue 'recv;
                    }
                    store.save(packet).await?;

                    let response = CLIENT
                        .post(format!(
                            "{}/glacier/persist/{:?}/{:?}",
                            nodes[&message.node_id].endpoint(),
                            message.block_id,
                            local_id
                        ))
                        .send()
                        .await?;
                    if response.status() != StatusCode::NOT_FOUND {
                        response.error_for_status()?;
                    }
                }
                Message::Get(message) => {
                    // let Some(packet) = store.load(message.block_id).await? else {
                    //     //
                    //     continue;
                    // };
                    // let response = CLIENT
                    //     .post(format!(
                    //         "{}/entropy/upload/{:?}",
                    //         nodes[&message.node_id].endpoint(),
                    //         message.block_id
                    //     ))
                    //     .body(packet.to_bytes())
                    //     .send()
                    //     .await?;
                    // if response.status() != StatusCode::SERVICE_UNAVAILABLE {
                    //     response.error_for_status()?;
                    // }

                    let Some(mut packets) = store.load_singletons(message.block_id).await? else {
                        //
                        continue;
                    };
                    while let Some((index, packet)) = packets.next().await? {
                        let response = CLIENT
                            .post(format!(
                                "{}/glacier/upload/{:?}/{index}",
                                nodes[&message.node_id].endpoint(),
                                message.block_id
                            ))
                            .body(packet.to_bytes())
                            .send()
                            .await;
                        // let response = match response {
                        //     Ok(response) => response,
                        //     Err(err) => {
                        //         // TODO
                        //         eprintln!("{err}");
                        //         break;
                        //     }
                        // };
                        let response = response?;
                        if response.status() == StatusCode::NOT_FOUND {
                            break;
                        }
                        response.error_for_status()?;
                    }

                    store.remove(message.block_id).await?
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerState {
    put: Arc<Mutex<HashMap<MerkleHash, PutState>>>,
    get: Arc<Mutex<HashMap<MerkleHash, GetState>>>,
    ring: UnboundedSender<(H256, BytesTtl)>,
    config: Arc<ServiceConfig>,
    packet_distr: Arc<PacketDistr>,
}

pub struct ServiceConfig {
    pub local_id: NodeId,
    pub key: SigningKey,
    pub parameters: Parameters,
    pub f: usize,
    pub n: usize,
}

struct PutState {
    // the major of data is `Vec<Bytes>`, which is supposed to be reference counted already
    // but there're still other things, also cloning a several hundred K vec is not cheap anyway
    block: Arc<Block>,
    persist_nodes: HashSet<NodeId>,
    start: Instant,
    end: Option<Instant>,
}

struct GetState {
    #[allow(unused)]
    verifying_key: VerifyingKey,
    scratch: Option<Packet>,
    start: Instant,
    end: Option<Instant>,
    checksum: u64,
}

async fn benchmark_put(State(state): State<ServerState>) -> Response {
    let chunks = repeat_with(|| {
        let mut buf = vec![0; state.config.parameters.chunk_size];
        thread_rng().fill_bytes(&mut buf);
        Bytes::from(buf)
    })
    .take(state.config.parameters.k)
    .collect::<Vec<_>>();
    let checksum = FxBuildHasher.hash_one(&chunks);
    let start = Instant::now();
    let block = Block::new(chunks, &state.config.key);
    let put = PutState {
        block: block.into(),
        start,
        persist_nodes: Default::default(),
        end: None,
    };
    let block_id = put.block.id();
    state.put.lock().expect("can lock").insert(block_id, put);
    let put = PutMessage {
        block_id,
        node_id: state.config.local_id,
    };
    let bytes = Bytes::from(
        bincode::options()
            .serialize(&Message::Put(put))
            .expect("can serialize"),
    );
    // the broadcast here will bypass loopback
    // TODO a decent implementation should store packets locally as well
    state
        .ring
        .send((block_id, (bytes, state.config.n as _)))
        .expect("can send");
    Json((
        format!("{block_id:?}"),
        checksum,
        state.config.key.verifying_key().to_bytes(),
    ))
    .into_response()
}

async fn poll_put(State(state): State<ServerState>, Path(block_id): Path<String>) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let put = state.put.lock().expect("can lock");
    let Some(put) = put.get(&block_id) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let latency = put.end.map(|end| end - put.start);
    Json(latency).into_response()
}

async fn encode(
    State(state): State<ServerState>,
    Path((block_id, node_id)): Path<(String, String)>,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let Ok(_node_id) = node_id.parse::<NodeId>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let block;
    {
        let mut put = state.put.lock().expect("can lock");
        let Some(put) = put.get_mut(&block_id) else {
            return StatusCode::NOT_FOUND.into_response();
        };
        block = put.block.clone();
    }
    let packet = block
        .generate_packet(&state.packet_distr, thread_rng())
        .expect("can generate packet");
    packet.to_bytes().into_response()
}

async fn ack_persistence(
    State(state): State<ServerState>,
    Path((block_id, node_id)): Path<(String, String)>,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let Ok(node_id) = node_id.parse::<NodeId>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let state_put = &mut state.put.lock().expect("can lock");
    let Some(put) = state_put.get_mut(&block_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    put.persist_nodes.insert(node_id);
    if put.persist_nodes.len() >= state.config.f + state.config.parameters.k {
        put.end.get_or_insert_with(Instant::now);
    }

    // a basic garbage collection
    // it is not very useful in write throughput evaluation since that should scale up to all puts
    // are concurrent
    // the concurrency = 64 case still need to be performed with at least 64GB memory
    // if put.num_persist_packet.len() >= state.config.num_node - 1 {
    //     state_put.remove(&block_id);
    // }
    // and i just realize it clears benchmark results as well
    StatusCode::OK.into_response()
}

async fn benchmark_get(
    State(state): State<ServerState>,
    Json((block_id, verifying_key)): Json<(String, [u8; PUBLIC_KEY_LENGTH])>,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let Ok(verifying_key) = VerifyingKey::from_bytes(&verifying_key) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let get = GetState {
        verifying_key,
        start: Instant::now(),
        scratch: None,
        end: None,
        checksum: 0,
    };
    let replaced = state.get.lock().expect("can lock").insert(block_id, get);
    if replaced.is_some() {
        return StatusCode::IM_A_TEAPOT.into_response();
    }
    let get = GetMessage {
        block_id,
        node_id: state.config.local_id,
    };
    let bytes = Bytes::from(
        bincode::options()
            .serialize(&Message::Get(get))
            .expect("can serialize"),
    );
    state
        .ring
        .send((block_id, (bytes, state.config.n as _)))
        .expect("can send");
    StatusCode::OK.into_response()
}

async fn poll_get(State(state): State<ServerState>, Path(block_id): Path<String>) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let mut get = state.get.lock().expect("can lock");
    let Some(get) = get.get_mut(&block_id) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    Json(if let Some(end) = get.end {
        Some((end - get.start, get.checksum))
    } else {
        None
    })
    .into_response()
}

async fn upload(
    State(state): State<ServerState>,
    Path(block_id): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let mut get = state.get.lock().expect("can lock");
    let Some(get) = get.get_mut(&block_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if get.end.is_some() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(packet) = Packet::from_bytes(body, &state.config.parameters) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    // if packet
    //     .verify(&get.verifying_key, &state.config.parameters)
    if packet.block_id() != block_id
        || packet
            .verify_merkle_proof(&state.config.parameters)
            .is_err()
    {
        return StatusCode::IM_A_TEAPOT.into_response();
    }
    let packet = if let Some(mut scratch) = get.scratch.take() {
        scratch.merge(packet);
        scratch
    } else {
        packet
    };
    if !packet.can_recover(&state.config.parameters) {
        get.scratch = Some(packet);
    } else {
        let block = packet
            .recover(&state.config.parameters)
            .expect("can recover");
        let replaced = get.end.replace(Instant::now());
        assert!(replaced.is_none());
        get.checksum = FxBuildHasher.hash_one(
            block
                .chunks
                .into_iter()
                .map(|(buf, _)| buf)
                .collect::<Vec<_>>(),
        )
    }
    StatusCode::OK.into_response()
}

async fn upload_index(
    State(state): State<ServerState>,
    Path((block_id, index)): Path<(String, usize)>,
    body: axum::body::Bytes,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let mut get = state.get.lock().expect("can lock");
    let Some(get) = get.get_mut(&block_id) else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if get.end.is_some() {
        // println!("late upload for recover {index}");
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Some(scratch) = &get.scratch {
        if scratch.chunks.contains_key(&index) {
            // consider a more suitable semantic status code
            return StatusCode::OK.into_response();
        }
    }
    let Ok(packet) = Packet::from_bytes(body, &state.config.parameters) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    // if packet
    //     .verify(&get.verifying_key, &state.config.parameters)
    if packet.block_id() != block_id
        || packet
            .verify_merkle_proof(&state.config.parameters)
            .is_err()
    {
        return StatusCode::IM_A_TEAPOT.into_response();
    }
    let packet = if let Some(mut scratch) = get.scratch.take() {
        scratch.merge(packet);
        scratch
    } else {
        packet
    };
    if !packet.can_recover(&state.config.parameters) {
        get.scratch = Some(packet);
    } else {
        let block = packet
            .recover(&state.config.parameters)
            .expect("can recover");
        // println!("recover done");
        let replaced = get.end.replace(Instant::now());
        assert!(replaced.is_none());
        get.checksum = FxBuildHasher.hash_one(
            block
                .chunks
                .into_iter()
                .map(|(buf, _)| buf)
                .collect::<Vec<_>>(),
        )
    }
    StatusCode::OK.into_response()
}

pub fn make_service(config: ServiceConfig, ring: UnboundedSender<(H256, BytesTtl)>) -> Router {
    Router::new()
        .route("/put", post(benchmark_put))
        .route("/put/:block_id", get(poll_put))
        .route("/encode/:block_id/:node_id", post(encode))
        .route("/persist/:block_id/:node_id", post(ack_persistence))
        .route("/get", post(benchmark_get))
        .route("/get/:block_id", get(poll_get))
        .route("/upload/:block_id", post(upload))
        .route("/upload/:block_id/:index", post(upload_index))
        .with_state(ServerState {
            ring,
            packet_distr: PacketDistr::new(config.parameters.k).into(),
            config: config.into(),
            put: Default::default(),
            get: Default::default(),
        })
}
