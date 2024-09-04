use std::{collections::HashMap, iter::repeat_with};

use bincode::Options;
use bytes::Bytes;
use futures::future::select_all;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::PeerId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    source: PeerId,
    seq: u32,
    payload: Bytes,
}

pub struct State {
    local_id: PeerId,
    mesh: Vec<String>,
    client: reqwest::Client,
    upcall: UnboundedSender<Bytes>,
}

impl State {
    pub async fn session(
        &self,
        messages: UnboundedReceiver<Message>,
        invokes: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        let (forward_senders, forward_receivers) = repeat_with(unbounded_channel)
            .take(self.mesh.len())
            .unzip::<_, _, Vec<_>, Vec<_>>();
        let recv_session = self.recv_session(messages, forward_senders.clone());
        let forward_sessions =
            self.mesh
                .iter()
                .zip(forward_receivers)
                .map(|(endpoint, forward_receiver)| {
                    Box::pin(self.forward_session(endpoint, forward_receiver))
                });
        let forward_sessions = select_all(forward_sessions);
        let invoke_session = self.invoke_session(invokes, forward_senders);
        tokio::select! {
            // is it true that both of the below sessions lead to graceful shutdown?
            result = recv_session => return result,
            result = invoke_session => return result,
            (result, _ , _) = forward_sessions => result?
        }
        anyhow::bail!("unreachable")
    }

    async fn recv_session(
        &self,
        mut messages: UnboundedReceiver<Message>,
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
        while let Some(message) = messages.recv().await {
            let prev_high = high_seqs.entry(message.source).or_default();
            if *prev_high >= message.seq {
                continue;
            }
            if *prev_high < message.seq - 1 {
                // TODO warning
            }

            *prev_high = message.seq;
            self.upcall.send(message.payload.clone())?;
            // is there any simple way to reuse the serialized message received by this node?
            let buf = Bytes::from(bincode::options().serialize(&message)?);
            for sender in &forward_senders {
                sender.send(buf.clone())?;
            }
        }
        Ok(())
    }

    async fn forward_session(
        &self,
        endpoint: &str,
        mut forward_receiver: UnboundedReceiver<Bytes>,
    ) -> anyhow::Result<()> {
        while let Some(buf) = forward_receiver.recv().await {
            self.client
                .post(format!("{endpoint}/broadcast"))
                .body(buf.clone())
                .send()
                .await?
                .error_for_status()?;
        }
        Ok(())
    }

    async fn invoke_session(
        &self,
        mut invokes: UnboundedReceiver<Bytes>,
        forward_senders: Vec<UnboundedSender<Bytes>>,
    ) -> anyhow::Result<()> {
        let mut seq = 0;
        while let Some(payload) = invokes.recv().await {
            seq += 1;
            let message = Message {
                source: self.local_id,
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
