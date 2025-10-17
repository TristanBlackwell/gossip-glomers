use std::{
    collections::{HashMap, HashSet},
    io::StdoutLock,
    sync::mpsc::Sender,
};

use anyhow::Context;
use gossip_glomers::{Body, Event, Init, Message, Node, main_loop};
use rand::seq::IndexedRandom;
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
    /// ID of the last message this node sent
    msg_id: usize,
    /// Messages that have been broadcast or gossiped to this node
    messages: HashSet<usize>,
    /// The messages that other nodes have gossiped to this node.
    seen: HashMap<String, HashSet<usize>>,
    /// Other known nodes in the topology.
    nodes: HashSet<String>,
}

impl Node<BroadcastPayload> for BroadcastNode {
    fn from_init(init: Init, tx: Sender<Event<BroadcastPayload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        // Fire an `interval` event every 300 millseconds
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
            seen: HashMap::new(),
            nodes: HashSet::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<BroadcastPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                let mut reply = input.into_reply(Some(&mut self.msg_id));

                match reply.body.payload {
                    BroadcastPayload::Broadcast(broadcast) => {
                        self.messages.insert(broadcast.message);

                        reply.body.payload = BroadcastPayload::BroadcastOk;
                        reply.send(output).context("Sending broadcast ok reply")?;
                    }
                    BroadcastPayload::BroadcastOk => {}
                    BroadcastPayload::Read => {
                        reply.body.payload = BroadcastPayload::ReadOk(ReadOk {
                            messages: self.messages.clone().into_iter().collect(),
                        });
                        reply.send(output).context("Sending read ok reply")?;
                    }
                    BroadcastPayload::ReadOk(_) => {}
                    BroadcastPayload::Topology(topology) => {
                        for (node, neighbours) in &topology.topology {
                            self.nodes.insert(node.clone());
                            self.seen.insert(node.to_string(), HashSet::new());
                            for n in neighbours {
                                self.nodes.insert(n.clone());
                                self.seen.insert(n.to_string(), HashSet::new());
                            }
                        }

                        reply.body.payload = BroadcastPayload::TopologyOk;
                        reply.send(output).context("Sending topology ok reply")?;
                    }
                    BroadcastPayload::TopologyOk => {}
                    BroadcastPayload::Gossip(gossip) => {
                        if let Some(known_node) = self.seen.get_mut(&reply.dst) {
                            known_node.extend(gossip.messages.clone());
                        } else {
                            self.seen
                                .insert(reply.dst, HashSet::from_iter(gossip.messages.clone()));
                        }

                        // Add to this nodes messages any that have not been seen.
                        self.messages.extend(gossip.messages.clone());
                    }
                }
            }
            Event::Interval => {
                let mut rng = rand::rng();
                let mut nodes: Vec<_> = self
                    .nodes
                    .iter()
                    .filter(|n| *n != &self.id)
                    .cloned()
                    .collect();
                // Select 3 nodes at random to gossip to.
                nodes = nodes
                    .choose_multiple(&mut rng, 3.min(nodes.len()))
                    .cloned()
                    .collect::<Vec<String>>();

                for node in &nodes {
                    // Messages we've seen from the neighbour.
                    let known = self.seen.get(node).cloned().unwrap_or(HashSet::new());
                    // Messages we have that the neighbour does not (that we are aware of)
                    let not_seen: Vec<usize> = self.messages.difference(&known).cloned().collect();

                    Message {
                        src: self.id.clone(),
                        dst: node.clone(),
                        body: Body {
                            id: None,
                            in_reply_to: None,
                            payload: BroadcastPayload::Gossip(Gossip { messages: not_seen }),
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
