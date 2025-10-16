use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
};

use anyhow::Context;
use gossip_glomers::{Body, Event, Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BroadcastPayload {
    Broadcast(Broadcast),
    BroadcastOk,
    Read,
    ReadOk(ReadOk),
    Topology(Topology),
    TopologyOk,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Broadcast {
    pub message: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadOk {
    pub messages: Vec<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Topology {
    pub topology: HashMap<String, Vec<String>>,
}

struct BroadcastNode {
    id: String,
    msg_id: usize,
    messages: HashSet<usize>,
    neighbours: Vec<String>,
}

impl Node<BroadcastPayload> for BroadcastNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(BroadcastNode {
            id: init.node_id,
            msg_id: 0,
            messages: HashSet::new(),
            neighbours: Vec::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<BroadcastPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("Did not receive expected message");
        };

        match &input.body.payload {
            BroadcastPayload::Broadcast(broadcast) => {
                self.messages.insert(broadcast.message);

                for neighbour in &self.neighbours {
                    if neighbour == &input.src {
                        continue;
                    }

                    Message {
                        src: self.id.clone(),
                        dst: neighbour.clone(),
                        body: Body {
                            id: None,
                            in_reply_to: None,
                            payload: BroadcastPayload::Broadcast(Broadcast {
                                message: broadcast.message,
                            }),
                        },
                    }
                    .send(output)
                    .context("Sending broadcast to neighbour")?;
                }

                let mut reply = input.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastPayload::BroadcastOk;
                reply.send(output).context("Sending broadcast ok reply")?;
            }
            BroadcastPayload::BroadcastOk => {}
            BroadcastPayload::Read => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastPayload::ReadOk(ReadOk {
                    messages: self.messages.clone().into_iter().collect(),
                });
                reply.send(output).context("Sending read ok reply")?;
            }
            BroadcastPayload::ReadOk(_) => {}
            BroadcastPayload::Topology(topology) => {
                self.neighbours = topology
                    .topology
                    .get(&self.id)
                    .cloned()
                    .unwrap_or(Vec::new());
                let mut reply = input.into_reply(Some(&mut self.msg_id));
                reply.body.payload = BroadcastPayload::TopologyOk;
                reply.send(output).context("Sending topology ok reply")?;
            }
            BroadcastPayload::TopologyOk => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastNode, BroadcastPayload>()
}
