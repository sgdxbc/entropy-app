use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher as _,
    sync::{Arc, Mutex},
    time::Instant,
};

use axum::{
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    response::{IntoResponse as _, Response},
    routing::{get, post},
    Json, Router,
};
use ed25519_dalek::PUBLIC_KEY_LENGTH;
use primitive_types::H256;
use rand::{thread_rng, RngCore as _};
use rustc_hash::FxBuildHasher;

use crate::{
    block::{MerkleHash, Parameters},
    sha256, NodeId,
};

#[derive(Clone)]
pub struct ServerState {
    put: Arc<Mutex<HashMap<H256, PutState>>>,
    get: Arc<Mutex<HashMap<H256, GetState>>>,
    config: Arc<ServiceConfig>,
}

pub struct ServiceConfig {
    pub local_id: NodeId,
    pub f: usize,
    pub parameters: Parameters,
}

struct PutState {
    persist_nodes: HashSet<NodeId>,
    start: Instant,
    end: Option<Instant>,
}

struct GetState {
    start: Instant,
    end: Option<Instant>,
    checksum: u64,
}

async fn benchmark_put(State(state): State<ServerState>) -> Response {
    let mut block = vec![0; state.config.parameters.chunk_size * state.config.parameters.k];
    thread_rng().fill_bytes(&mut block);
    let checksum = FxBuildHasher.hash_one(&block);
    let start = Instant::now();
    let put = PutState {
        start,
        persist_nodes: Default::default(),
        end: None,
    };
    let block_id = H256(sha256(&block));
    state.put.lock().expect("can lock").insert(block_id, put);

    // TODO

    Json((format!("{block_id:?}"), checksum, [0; PUBLIC_KEY_LENGTH])).into_response()
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
    Json((block_id, _verifying_key)): Json<(String, [u8; PUBLIC_KEY_LENGTH])>,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
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

pub fn make_service(config: ServiceConfig) -> Router {
    Router::new()
        .route("/put", post(benchmark_put))
        .route("/put/:block_id", get(poll_put))
        .route("/persist/:block_id/:node_id", post(ack_persistence))
        .route("/get", post(benchmark_get))
        .route("/get/:block_id", get(poll_get))
        .layer(DefaultBodyLimit::max(64 << 20))
        .with_state(ServerState {
            config: config.into(),
            put: Default::default(),
            get: Default::default(),
        })
}
