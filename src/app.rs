use std::{
    cmp::Reverse,
    collections::HashMap,
    convert::identity,
    iter::repeat_with,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use bincode::Options;
use bytes::Bytes;
use ed25519_dalek::SigningKey;
use rand::{thread_rng, RngCore as _};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::{
    block::{distr::PacketDistr, Block, MerkleHash, Packet, Parameters},
    store::Store,
    NodeBook, NodeId, CLIENT,
};

pub struct Context {
    local_id: NodeId,
    nodes: NodeBook,
    parameters: Parameters,
    store: Store,
    broadcast_messages: UnboundedReceiver<Bytes>,
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
    pub async fn session(self) -> anyhow::Result<()> {
        Self::recv_session(
            &self.nodes,
            self.local_id,
            self.store,
            self.parameters,
            self.broadcast_messages,
        )
        .await
    }

    async fn recv_session(
        nodes: &NodeBook,
        local_id: NodeId,
        store: Store,
        parameters: Parameters,
        mut broadcast_messages: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        'recv: while let Some(bytes) = broadcast_messages.recv().await {
            let Ok(message) = bincode::options().deserialize::<Message>(&bytes) else {
                eprintln!("malformed broadcast message");
                continue;
            };
            match message {
                Message::Put(message) => {
                    // TODO deduplicate?
                    for _ in 0..10 {
                        let response = CLIENT
                            .post(format!(
                                "{}/entropy/encode/{:?}",
                                nodes[&message.node_id].endpoint(),
                                message.block_id
                            ))
                            .send()
                            .await?;
                        if response.status() == StatusCode::SERVICE_UNAVAILABLE {
                            eprintln!("put service unavailable");
                            continue 'recv;
                        }
                        let packet = Packet::from_bytes(
                            response.error_for_status()?.bytes().await?,
                            &parameters,
                        )?;
                        if packet
                            .verify(&nodes[&message.node_id].verifying_key, &parameters)
                            .is_err()
                        {
                            eprintln!("verify failed");
                            // do not request more packets from a faulty sender
                            // TODO discard received packets? probably no need since those are
                            // verified i.e. are "good" storage
                            continue 'recv;
                        }
                        store.save(packet).await?
                    }
                    let response = CLIENT
                        .post(format!(
                            "{}/entropy/persist/{:?}/{:?}",
                            nodes[&message.node_id].endpoint(),
                            message.block_id,
                            local_id
                        ))
                        .send()
                        .await?;
                    if response.status() != StatusCode::SERVICE_UNAVAILABLE {
                        response.error_for_status()?;
                    }
                }
                Message::Get(message) => {
                    let Some(packet) = store.load(message.block_id).await? else {
                        //
                        continue;
                    };
                    let response = CLIENT
                        .post(format!(
                            "{}/entropy/upload/{:?}",
                            nodes[&message.node_id].endpoint(),
                            message.block_id
                        ))
                        .body(packet.to_bytes())
                        .send()
                        .await?;
                    if response.status() != StatusCode::SERVICE_UNAVAILABLE {
                        response.error_for_status()?;
                    }
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerState {
    put: Arc<Mutex<HashMap<MerkleHash, PutState>>>,
    broadcast: UnboundedSender<Bytes>,
    packet_distr: Arc<PacketDistr>,
    parameters: Arc<Parameters>,
    local_id: NodeId,
    key: Arc<SigningKey>,
    num_node: usize,
}

struct PutState {
    // the major of data is `Vec<Bytes>`, which is supposed to be reference counted already
    // but there're still other things, also cloning a several hundred K vec is not cheap anyway
    block: Arc<Block>,
    num_node_packet: HashMap<NodeId, usize>,
    num_persist_packet: Vec<usize>,
    start: Instant,
    end: Option<Instant>,
}

async fn benchmark_put(State(state): State<ServerState>) -> Response {
    let chunks = repeat_with(|| {
        let mut buf = vec![0; state.parameters.chunk_size];
        thread_rng().fill_bytes(&mut buf);
        Bytes::from(buf)
    })
    .take(state.parameters.k)
    .collect::<Vec<_>>();
    let start = Instant::now();
    let block = Block::new(chunks, &state.key);
    let put = PutState {
        block: block.into(),
        start,
        num_node_packet: Default::default(),
        num_persist_packet: Default::default(),
        end: None,
    };
    let block_id = put.block.id();
    state.put.lock().expect("can lock").insert(block_id, put);
    let put = PutMessage {
        block_id,
        node_id: state.local_id,
    };
    let bytes = Bytes::from(
        bincode::options()
            .serialize(&Message::Put(put))
            .expect("can serialize"),
    );
    // the broadcast here will bypass loopback
    // TODO a decent implementation should store packets locally as well
    state.broadcast.send(bytes).expect("can send");
    format!("{block_id:?}").into_response()
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
    let Ok(node_id) = node_id.parse::<NodeId>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let block;
    {
        let mut put = state.put.lock().expect("can lock");
        let Some(put) = put.get_mut(&block_id) else {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        };
        *put.num_node_packet.entry(node_id).or_default() += 1;
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
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let count = put.num_node_packet.remove(&node_id).unwrap_or_default();
    let pos = put
        .num_persist_packet
        .binary_search_by_key(&Reverse(count), |other_count| Reverse(*other_count))
        .unwrap_or_else(identity);
    put.num_persist_packet.insert(pos, count);
    if put.num_persist_packet.len() >= state.num_node * 2 / 3
        && put
            .num_persist_packet
            .iter()
            .skip(state.num_node / 3)
            .take(state.num_node / 3)
            .sum::<usize>()
            >= state.parameters.k
    {
        put.end.get_or_insert_with(Instant::now);
    }
    // a basic garbage collection
    // it is not very useful in write throughput evaluation since that should scale up to all puts
    // are concurrent
    // the concurrency = 64 case still need to be performed with at least 64GB memory
    if put.num_persist_packet.len() >= state.num_node - 1 {
        state_put.remove(&block_id);
    }
    StatusCode::OK.into_response()
}

pub fn make_service(
    local_id: NodeId,
    key: SigningKey,
    parameters: Parameters,
    num_node: usize,
    broadcast: UnboundedSender<Bytes>,
) -> Router {
    Router::new()
        .route("/put", post(benchmark_put))
        .route("/put/:block_id", get(poll_put))
        .route("/encode/:block_id", post(encode))
        .route("/persist/:block_id/:node_id", post(ack_persistence))
        .with_state(ServerState {
            put: Default::default(),
            broadcast,
            packet_distr: PacketDistr::new(parameters.k).into(),
            parameters: parameters.into(),
            local_id,
            key: key.into(),
            num_node,
        })
}
