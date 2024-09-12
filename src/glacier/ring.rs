use std::{collections::HashMap, iter::repeat_with, ops::Bound, sync::Arc};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use bincode::Options;
use bytes::Bytes;
use futures::future::select_all;
use primitive_types::H256;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{NodeBook, NodeId, CLIENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    source: NodeId,
    seq: u32,
    payload: Bytes,
}

pub struct ContextConfig {
    pub local_id: NodeId,
    pub mesh: Vec<String>,
    pub nodes: Arc<NodeBook>,
}

pub struct Context {
    config: ContextConfig,
    upcall: UnboundedSender<Bytes>,
    messages: UnboundedReceiver<BytesTtl>,
    invokes: UnboundedReceiver<(H256, BytesTtl)>,
}

pub type BytesTtl = (Bytes, u32);

impl Context {
    pub async fn session(self) -> anyhow::Result<()> {
        let (forward_senders, forward_receivers) = repeat_with(unbounded_channel)
            .take(self.config.mesh.len())
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let recv_session = Self::recv_session(
            self.config.local_id,
            self.messages,
            self.upcall,
            forward_senders,
        );
        let forward_sessions =
            self.config
                .mesh
                .iter()
                .zip(forward_receivers)
                .map(|(endpoint, forward_receiver)| {
                    Box::pin(Self::forward_session(endpoint, forward_receiver))
                });
        let forward_sessions = select_all(forward_sessions);
        let invoke_session =
            Self::invoke_session(self.config.local_id, self.config.nodes, self.invokes);
        tokio::select! {
            // is it true that both of the below sessions lead to graceful shutdown?
            result = recv_session => return result,
            result = invoke_session => return result,
            (result, _ , _) = forward_sessions => result?
        }
        anyhow::bail!("unreachable")
    }

    async fn recv_session(
        local_id: NodeId,
        mut messages: UnboundedReceiver<BytesTtl>,
        upcall: UnboundedSender<Bytes>,
        forward_senders: Vec<UnboundedSender<BytesTtl>>,
    ) -> anyhow::Result<()> {
        let mut high_seqs = HashMap::<_, u32>::new();
        while let Some((buf, mut ttl)) = messages.recv().await {
            let Ok(message) = bincode::options().deserialize::<Message>(&buf) else {
                eprintln!("malformed broadcast");
                continue;
            };
            if message.source == local_id {
                continue;
            }
            let prev_high = high_seqs.entry(message.source).or_default();
            if *prev_high >= message.seq {
                continue;
            }
            if *prev_high < message.seq - 1 {
                eprintln!(
                    "missing seq {:?} from {:?}",
                    *prev_high + 1..message.seq,
                    message.source
                );
            }

            *prev_high = message.seq;
            upcall.send(message.payload.clone())?;
            for sender in &forward_senders {
                if ttl == 0 {
                    break;
                }
                ttl -= 1;
                sender.send((buf.clone(), ttl))?;
            }
        }
        Ok(())
    }

    async fn forward_session(
        endpoint: &str,
        mut forward_receiver: UnboundedReceiver<BytesTtl>,
    ) -> anyhow::Result<()> {
        while let Some((buf, ttl)) = forward_receiver.recv().await {
            // println!("forward to {endpoint}");
            if let Err(err) = async {
                CLIENT
                    // not a good practice to encode arbitrary parameters (instead of index-like
                    // ones) into path
                    .post(format!("{endpoint}/ring/{ttl}"))
                    .body(buf.clone())
                    .send()
                    .await?
                    .error_for_status()?;
                anyhow::Ok(())
            }
            .await
            {
                eprintln!("{err}")
            }
        }
        Ok(())
    }

    async fn invoke_session(
        local_id: NodeId,
        nodes: Arc<NodeBook>,
        mut invokes: UnboundedReceiver<(H256, BytesTtl)>,
    ) -> anyhow::Result<()> {
        let mut seq = 0;
        while let Some((target, (payload, ttl))) = invokes.recv().await {
            seq += 1;
            let message = Message {
                source: local_id,
                seq,
                payload,
            };
            let buf = Bytes::from(bincode::options().serialize(&message)?);

            let (_, node) = nodes.range(target..).next().unwrap_or(
                nodes
                    .first_key_value()
                    .ok_or(anyhow::format_err!("empty nodes"))?,
            );
            CLIENT
                .post(format!("{}/ring/{ttl}", node.endpoint()))
                .body(buf)
                .send()
                .await?
                .error_for_status()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ServerState {
    message_sender: UnboundedSender<BytesTtl>,
}

async fn handle(
    State(state): State<ServerState>,
    Path(ttl): Path<u32>,
    body: axum::body::Bytes,
) -> Response {
    let Ok(()) = state.message_sender.send((body, ttl)) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    StatusCode::OK.into_response()
}

pub struct Api {
    pub invoke_sender: UnboundedSender<(H256, BytesTtl)>,
    pub upcall: UnboundedReceiver<Bytes>,
}

pub fn make_service(config: ContextConfig) -> (Router, Context, Api) {
    let (message_sender, message_receiver) = unbounded_channel();
    let (invoke_sender, invoke_receiver) = unbounded_channel();
    let (upcall_sender, upcall_receiver) = unbounded_channel();
    let router = Router::new()
        .route("/", post(handle))
        .with_state(ServerState { message_sender });
    let context = Context {
        config,
        upcall: upcall_sender,
        messages: message_receiver,
        invokes: invoke_receiver,
    };
    let api = Api {
        invoke_sender,
        upcall: upcall_receiver,
    };
    (router, context, api)
}

impl ContextConfig {
    pub fn construct_ring(nodes: Arc<NodeBook>, mesh_degree: usize) -> anyhow::Result<Vec<Self>> {
        anyhow::ensure!(mesh_degree >= 1);
        let n = nodes.len();
        anyhow::ensure!(n > mesh_degree);
        let keys = nodes.keys().copied().collect::<Vec<_>>();
        let configs = keys
            .iter()
            .map(|&id| {
                let mesh = nodes
                    .range((Bound::Excluded(id), Bound::Unbounded))
                    .chain(nodes.iter())
                    .take(mesh_degree)
                    .map(|(_, node)| node.endpoint())
                    .collect();
                (
                    id,
                    Self {
                        local_id: id,
                        mesh,
                        nodes: nodes.clone(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        Ok(configs.into_values().collect())
    }
}
