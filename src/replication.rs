use std::{
    collections::{HashMap, HashSet},
    hash::BuildHasher as _,
    mem::take,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse as _, Response},
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use ed25519_dalek::PUBLIC_KEY_LENGTH;
use primitive_types::H256;
use rand::{thread_rng, RngCore as _};
use rustc_hash::FxBuildHasher;
use serde::Deserialize;
use tokio::{
    fs::{read, write},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};

use crate::{
    block::{MerkleHash, Parameters},
    sha256, NodeBook, NodeId, CLIENT,
};

type BlockMessage = (NodeId, H256, Bytes, bool);

pub struct Context {
    config: ContextConfig,
    blocks: UnboundedReceiver<BlockMessage>,
    nodes: Arc<NodeBook>,
}

pub struct ContextConfig {
    pub regional_primaries: Vec<String>,
    pub regional_mesh: Vec<String>,
}

impl Context {
    pub fn new(
        config: ContextConfig,
        blocks: UnboundedReceiver<BlockMessage>,
        nodes: Arc<NodeBook>,
    ) -> Self {
        Self {
            config,
            blocks,
            nodes,
        }
    }

    pub async fn session(mut self) -> anyhow::Result<()> {
        let mut client_sessions = JoinSet::new();
        loop {
            let Some((node_id, block_id, block, push_remote)) = (tokio::select! {
                recv = self.blocks.recv() => recv,
                Some(result) = client_sessions.join_next() => {
                    result??;
                    continue;
                }
            }) else {
                break;
            };
            if push_remote {
                for url in &self.config.regional_primaries {
                    let url = url.clone();
                    let block = block.clone();
                    client_sessions.spawn(async move {
                        CLIENT
                            .post(format!("{url}/replication/remote/{block_id:?}"))
                            .query(&[("node_id", format!("{node_id:?}"))])
                            .body(block)
                            .send()
                            .await?
                            .error_for_status()?;
                        anyhow::Ok(())
                    });
                }
            } else {
                // TODO corner case: only one instance in region
                anyhow::ensure!(!self.config.regional_mesh.is_empty());
            }
            for url in &self.config.regional_mesh {
                if *url == self.nodes[&node_id].url() {
                    continue;
                }
                let url = url.clone();
                let block = block.clone();
                client_sessions.spawn(async move {
                    CLIENT
                        .post(format!("{url}/replication/local/{block_id:?}"))
                        .query(&[("node_id", format!("{node_id:?}"))])
                        .body(block)
                        .send()
                        .await?
                        .error_for_status()?;
                    anyhow::Ok(())
                });
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct ServerState {
    config: Arc<ServiceConfig>,
    put: Arc<Mutex<HashMap<H256, PutState>>>,
    get: Arc<Mutex<HashMap<H256, GetState>>>,
    storing_id: Arc<Mutex<H256>>,
    block_sender: UnboundedSender<BlockMessage>,
}

pub struct ServiceConfig {
    pub local_id: NodeId,
    pub f: usize,
    pub parameters: Parameters,
    pub store_path: PathBuf,
    pub nodes: Arc<NodeBook>,
}

struct PutState {
    persist_nodes: HashSet<NodeId>,
    start: Instant,
    end: Option<Instant>,
}

struct GetState {
    latency: Duration,
    checksum: u64,
    redirect: bool,
    block: Vec<u8>,
}

async fn benchmark_put(
    State(state): State<ServerState>,
    redirect_block: axum::body::Bytes,
) -> Response {
    if state.config.parameters.k != 1 {
        return StatusCode::IM_A_TEAPOT.into_response();
    }
    let block = if redirect_block.is_empty() {
        let mut block = vec![0; state.config.parameters.chunk_size * state.config.parameters.k];
        thread_rng().fill_bytes(&mut block);
        Bytes::from(block)
    } else {
        redirect_block
    };
    let checksum = FxBuildHasher.hash_one(&block);
    let start = Instant::now();
    let put = PutState {
        start,
        persist_nodes: Default::default(),
        end: None,
    };
    let block_id = H256(sha256(&block));
    state.put.lock().expect("can lock").insert(block_id, put);
    state
        .block_sender
        .send((state.config.local_id, block_id, block.clone(), true))
        .expect("can send");
    let node_id = state.config.local_id;
    save(state, block, block_id, node_id)
        .await
        .expect("can save");
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

#[derive(Deserialize)]
struct QueryData {
    node_id: String,
}

async fn remote_push(
    State(state): State<ServerState>,
    Path(block_id): Path<String>,
    query: Query<QueryData>,
    body: axum::body::Bytes,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let Ok(node_id) = query.node_id.parse::<NodeId>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    state
        .block_sender
        .send((node_id, block_id, body.clone(), false))
        .expect("can send");
    save(state, body, block_id, node_id)
        .await
        .expect("can save");
    StatusCode::OK.into_response()
}

async fn local_push(
    State(state): State<ServerState>,
    Path(block_id): Path<String>,
    query: Query<QueryData>,
    body: axum::body::Bytes,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let Ok(node_id) = query.node_id.parse::<NodeId>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    save(state, body, block_id, node_id)
        .await
        .expect("can save");
    StatusCode::OK.into_response()
}

async fn save(
    state: ServerState,
    block: Bytes,
    block_id: H256,
    node_id: H256,
) -> anyhow::Result<()> {
    write(state.config.store_path.join("block"), &block).await?;
    *state.storing_id.lock().expect("can lock") = block_id;
    CLIENT
        .post(format!(
            "{}/replication/persist/{block_id:?}/{:?}",
            state.config.nodes[&node_id].url(),
            state.config.local_id
        ))
        .send()
        .await?
        .error_for_status()?;
    Ok(())
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

    StatusCode::OK.into_response()
}

mod benchmark_get {
    use serde::Deserialize;

    #[derive(Deserialize)]
    pub struct QueryData {
        pub redirect: Option<bool>,
    }
}

async fn benchmark_get(
    State(state): State<ServerState>,
    query: Query<benchmark_get::QueryData>,
    Json((block_id, _verifying_key)): Json<(String, [u8; PUBLIC_KEY_LENGTH])>,
) -> Response {
    let Ok(block_id) = block_id.parse::<MerkleHash>() else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    if block_id != *state.storing_id.lock().expect("can lock") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let start = Instant::now();
    let Ok(block) = read(state.config.store_path.join("block")).await else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    let checksum = FxBuildHasher.hash_one(&block);
    state.get.lock().expect("can lock").insert(
        block_id,
        GetState {
            latency: start.elapsed(),
            checksum,
            redirect: query.redirect.unwrap_or(false),
            block,
        },
    );
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
    if !get.redirect {
        Json(Some((get.latency, get.checksum))).into_response()
    } else {
        take(&mut get.block).into_response()
    }
}

pub fn make_service(config: ServiceConfig, block_sender: UnboundedSender<BlockMessage>) -> Router {
    Router::new()
        .route("/put", post(benchmark_put))
        .route("/put/:block_id", get(poll_put))
        .route("/remote/:block_id", post(remote_push))
        .route("/local/:block_id", post(local_push))
        .route("/persist/:block_id/:node_id", post(ack_persistence))
        .route("/get", post(benchmark_get))
        .route("/get/:block_id", get(poll_get))
        .layer(DefaultBodyLimit::max(2 << 30))
        .with_state(ServerState {
            config: config.into(),
            put: Default::default(),
            get: Default::default(),
            storing_id: Default::default(),
            block_sender,
        })
}
