use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use bincode::Options;
use bytes::Bytes;
use futures::channel::mpsc::UnboundedSender;
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::{
    block::{distr::PacketDistr, Block, MerkleHash, Packet, Parameters},
    store::Store,
    NodeBook, NodeId, CLIENT,
};

pub struct Context {
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
    rng: Arc<Mutex<StdRng>>,
}

struct PutState {
    block: Block,
    persistent_nodes: HashSet<NodeId>,
    start: Instant,
}

async fn benchmark_put(State(state): State<ServerState>) {}

async fn encode(State(state): State<ServerState>, Path(block_id): Path<String>) -> Response {
    let block_id = block_id.parse::<MerkleHash>().expect("can parse");
    let put = &mut state.put.lock().expect("can lock");
    let Some(put) = put.get_mut(&block_id) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let packet = put
        .block
        .generate_packet(
            &state.packet_distr,
            &mut *state.rng.lock().expect("can lock"),
        )
        .expect("can generate packet");
    packet.to_bytes().into_response()
}

async fn ack_persistence(
    State(state): State<ServerState>,
    Path((block_id, node_id)): Path<(String, String)>,
) -> Response {
    let block_id = block_id.parse::<MerkleHash>().expect("can parse");
    let put = &mut state.put.lock().expect("can lock");
    let Some(put) = put.get_mut(&block_id) else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    let node_id = node_id.parse::<NodeId>().expect("can parse");
    put.persistent_nodes.insert(node_id);
    if put.persistent_nodes.len() > 42 {
        // TODO
    }
    StatusCode::OK.into_response()
}

pub fn make_service(k: usize, broadcast: UnboundedSender<Bytes>) -> Router {
    Router::new()
        .route("/put", post(benchmark_put))
        .route("/encode/:block_id", post(encode))
        .route("/persist/:block_id/:node_id", post(ack_persistence))
        .with_state(ServerState {
            put: Default::default(),
            broadcast,
            packet_distr: PacketDistr::new(k).into(),
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(117418))),
        })
}
