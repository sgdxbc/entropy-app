use std::{
    collections::{HashMap, HashSet},
    future::pending,
    iter::{repeat, repeat_with},
};

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use bincode::Options;
use bytes::Bytes;
use futures::{future::select_all, FutureExt};
use rand::{seq::SliceRandom, Rng};
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
}

pub struct Context {
    config: ContextConfig,
    upcall: UnboundedSender<Bytes>,
    messages: UnboundedReceiver<Bytes>,
    invokes: UnboundedReceiver<Bytes>,
}

impl Context {
    pub async fn session(self) -> anyhow::Result<()> {
        let mesh_len = self.config.mesh.len();
        let (forward_senders, forward_receivers) = repeat_with(unbounded_channel)
            .take(mesh_len)
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
        let forward_sessions = if mesh_len != 0 {
            select_all(forward_sessions).left_future()
        } else {
            pending().right_future()
        };
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
        local_id: NodeId,
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
            // println!("forward to {endpoint}");
            if let Err(err) = async {
                CLIENT
                    .post(format!("{endpoint}/broadcast"))
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
        nodes: &NodeBook,
        mesh_degree: usize,
        mut rng: impl Rng,
    ) -> anyhow::Result<Vec<Self>> {
        fn suitable(
            edges: &HashSet<(NodeId, NodeId)>,
            potential_edges: &HashMap<NodeId, usize>,
        ) -> bool {
            if potential_edges.is_empty() {
                return true;
            }
            for &s1 in potential_edges.keys() {
                for &s2 in potential_edges.keys() {
                    // # Two iterators on the same dictionary are guaranteed
                    // # to visit it in the same order if there are no
                    // # intervening modifications.
                    if s1 == s2 {
                        break;
                    }
                    if !edges.contains(&(s1.min(s2), s1.max(s2))) {
                        return true;
                    }
                }
            }
            false
        }

        fn try_creation(
            keys: &[NodeId],
            d: usize,
            mut rng: impl Rng,
        ) -> Option<HashSet<(NodeId, NodeId)>> {
            let mut edges = HashSet::new();
            let mut stubs = keys
                .iter()
                .flat_map(|&id| repeat(id).take(d))
                .collect::<Vec<_>>();
            while !stubs.is_empty() {
                let mut potential_edges = HashMap::<_, usize>::new();
                stubs.shuffle(&mut rng);
                for chunk in stubs.chunks_exact(2) {
                    let &[s1, s2] = chunk else { unreachable!() };
                    if s1 == s2 {
                        continue;
                    }
                    let inserted = edges.insert((s1.min(s2), s1.max(s2)));
                    if !inserted {
                        *potential_edges.entry(s1).or_default() += 1;
                        *potential_edges.entry(s2).or_default() += 1;
                    }
                }
                if !suitable(&edges, &potential_edges) {
                    return None;
                }
                stubs = potential_edges
                    .into_iter()
                    .flat_map(|(id, potential)| repeat(id).take(potential))
                    .collect()
            }
            Some(edges)
        }

        let keys = nodes.keys().copied().collect::<Vec<_>>();
        let mut configs = keys
            .iter()
            .map(|&id| {
                (
                    id,
                    Self {
                        local_id: id,
                        mesh: Default::default(),
                    },
                )
            })
            .collect::<HashMap<_, _>>();
        if mesh_degree == 0 {
            return Ok(configs.into_values().collect());
        }

        anyhow::ensure!(mesh_degree >= 3);
        let n = nodes.len();
        anyhow::ensure!(n > mesh_degree);
        anyhow::ensure!(n * mesh_degree % 2 == 0);
        let edges = loop {
            if let Some(edges) = try_creation(&keys, mesh_degree, &mut rng) {
                break edges;
            }
        };

        // assert_eq!(edges.len(), mesh_degree * n / 2);
        for (id1, id2) in edges {
            configs
                .get_mut(&id1)
                .unwrap()
                .mesh
                .push(nodes[&id2].endpoint());
            configs
                .get_mut(&id2)
                .unwrap()
                .mesh
                .push(nodes[&id1].endpoint());
        }
        Ok(configs.into_values().collect())
    }
}
