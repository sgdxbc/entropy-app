use std::{collections::HashMap, iter::repeat_with};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use bincode::Options;
use bytes::Bytes;
use futures::future::select_all;
use rand::{seq::IndexedRandom, Rng};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{PeerBook, PeerId, CLIENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    source: PeerId,
    seq: u32,
    payload: Bytes,
}

pub struct ContextConfig {
    pub local_id: PeerId,
    pub mesh: Vec<String>,
}

pub struct Context {
    config: ContextConfig,
    upcall: UnboundedSender<Bytes>,
    messages: UnboundedReceiver<Bytes>,
    invokes: UnboundedReceiver<Bytes>,
}

impl Context {
    pub async fn session(self) -> anyhow::Result<()> {
        let (forward_senders, forward_receivers) = repeat_with(unbounded_channel)
            .take(self.config.mesh.len())
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let recv_session = Self::recv_session(
            self.config.local_id,
            self.messages,
            self.upcall,
            forward_senders.clone(),
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
            Self::invoke_session(self.config.local_id, self.invokes, forward_senders);
        tokio::select! {
            // is it true that both of the below sessions lead to graceful shutdown?
            result = recv_session => return result,
            result = invoke_session => return result,
            (result, _ , _) = forward_sessions => result?
        }
        anyhow::bail!("unreachable")
    }

    async fn recv_session(
        local_id: PeerId,
        mut messages: UnboundedReceiver<Bytes>,
        upcall: UnboundedSender<Bytes>,
        forward_senders: Vec<UnboundedSender<Bytes>>,
    ) -> anyhow::Result<()> {
        // only forward messages with unseen high sequence numbers
        // based on the assumption of underlying point to point channels to be sequential, the only
        // case for out of order receiving is through a longer path (latency wise). this includes
        // the loopback case we are trying for, and the trivial case where there are indeed more
        // than one path that connect A and B, say A -> B and A -> C -> B
        //
        // if A send #1 and #2, B may receive #2 through A -> B, then receive #1 through A -> C -> B
        // at a later point. in this case #1 must have already been received through A -> B, so B
        // should not forward #1 (again)
        //
        // if this reasoning is correct, the messages the actually get forwarded are expected to
        // have consecutive sequence numbers. not very sure so just produce a warning for now. hope
        // it to be so
        let mut high_seqs = HashMap::<_, u32>::new();
        while let Some(buf) = messages.recv().await {
            let Ok(message) = bincode::options().deserialize::<Message>(&buf) else {
                println!("malformed broadcast");
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
                // TODO warning
            }

            *prev_high = message.seq;
            upcall.send(message.payload.clone())?;
            for sender in &forward_senders {
                sender.send(buf.clone())?;
            }
        }
        Ok(())
    }

    async fn forward_session(
        endpoint: &str,
        mut forward_receiver: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        while let Some(buf) = forward_receiver.recv().await {
            CLIENT
                .post(format!("{endpoint}/broadcast"))
                .body(buf.clone())
                .send()
                .await?
                .error_for_status()?;
        }
        Ok(())
    }

    async fn invoke_session(
        local_id: PeerId,
        mut invokes: UnboundedReceiver<Bytes>,
        forward_senders: Vec<UnboundedSender<Bytes>>,
    ) -> anyhow::Result<()> {
        let mut seq = 0;
        while let Some(payload) = invokes.recv().await {
            seq += 1;
            let message = Message {
                source: local_id,
                seq,
                payload,
            };
            let buf = Bytes::from(bincode::options().serialize(&message)?);
            for sender in &forward_senders {
                sender.send(buf.clone())?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct ServerState {
    message_sender: UnboundedSender<Bytes>,
}

async fn handle(State(state): State<ServerState>, body: axum::body::Bytes) -> Response {
    let Ok(()) = state.message_sender.send(body) else {
        return StatusCode::IM_A_TEAPOT.into_response();
    };
    StatusCode::OK.into_response()
}

pub struct Api {
    pub invoke_sender: UnboundedSender<Bytes>,
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
    pub fn generate_network(
        peers: &PeerBook,
        mesh_degree: usize,
        mut rng: impl Rng,
    ) -> anyhow::Result<Vec<Self>> {
        anyhow::ensure!(peers.len() > mesh_degree);
        let keys = peers.keys().copied().collect::<Vec<_>>();
        let configs = keys
            .iter()
            .map(|id| {
                let mesh_ids = repeat_with(|| keys.choose(&mut rng).expect("peer book not empty"))
                    .filter(|mesh_id| *mesh_id != id)
                    .take(mesh_degree);
                Self {
                    local_id: *id,
                    mesh: mesh_ids.map(|id| peers[id].endpoint()).collect(),
                }
            })
            .collect();
        // TODO verify connectivity
        Ok(configs)
    }
}
