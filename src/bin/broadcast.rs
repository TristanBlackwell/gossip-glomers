use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    sync::mpsc::Sender,
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
    Gossip(Gossip),
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Gossip {
    pub messages: Vec<usize>,
}

struct BroadcastNode {
    id: String,
    msg_id: usize,
    messages: HashSet<usize>,
    neighbours: Vec<String>,
}

impl Node<BroadcastPayload> for BroadcastNode {
    fn from_init(init: Init, tx: Sender<Event<BroadcastPayload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        std::thread::spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_millis(300));

                if tx.send(Event::Interval).is_err() {
                    break;
                }
            }
        });

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
        match input {
            Event::Message(input) => match &input.body.payload {
                BroadcastPayload::Broadcast(broadcast) => {
                    self.messages.insert(broadcast.message);

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
                BroadcastPayload::Gossip(gossip) => {
                    self.messages.extend(gossip.messages.clone());
                }
            },
            Event::Interval => {
                for neighbour in &self.neighbours {
                    Message {
                        src: self.id.clone(),
                        dst: neighbour.clone(),
                        body: Body {
                            id: None,
                            in_reply_to: None,
                            payload: BroadcastPayload::Gossip(Gossip {
                                messages: self.messages.clone().into_iter().collect(),
                            }),
                        },
                    }
                    .send(output)
                    .context("Sending gossip to neighbour")?;
                }
            }
            Event::EOF => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<BroadcastNode, BroadcastPayload>()
}
