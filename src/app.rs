use std::{
    collections::HashSet,
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
use rand::{rngs::StdRng, SeedableRng};

use crate::{
    block::{distr::PacketDistr, Block, MerkleHash},
    NodeId,
};

#[derive(Clone)]
pub struct ServerState {
    put: Arc<Mutex<Option<PutState>>>,
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
    let Some(put) = &mut *state.put.lock().expect("can lock") else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if put.block.id() != block_id.parse::<MerkleHash>().expect("can parse") {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
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
    let Some(put) = &mut *state.put.lock().expect("can lock") else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    if put.block.id() != block_id.parse::<MerkleHash>().expect("can parse") {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    let id = node_id.parse::<NodeId>().expect("can parse");
    put.persistent_nodes.insert(id);
    if put.persistent_nodes.len() > 0 {
        // TODO
    }
    StatusCode::OK.into_response()
}

pub fn make_service(k: usize) -> Router {
    Router::new()
        .route("/put", post(benchmark_put))
        .route("/encode/:block_id", post(encode))
        .route("/persist/:block_id/:node_id", post(ack_persistence))
        .with_state(ServerState {
            put: Default::default(),
            packet_distr: PacketDistr::new(k).into(),
            rng: Arc::new(Mutex::new(StdRng::seed_from_u64(117418))),
        })
}
