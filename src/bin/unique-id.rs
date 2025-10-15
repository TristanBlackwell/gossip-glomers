use std::io::StdoutLock;

use anyhow::Context;
use gossip_glomers::{Event, Init, Node, main_loop};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum UniqueIdPayload {
    Generate,
    GenerateOk(GenerateOk),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GenerateOk {
    pub id: String,
}

struct UniqueIdNode {
    id: String,
    msg_id: usize,
}

impl Node<UniqueIdPayload> for UniqueIdNode {
    fn from_init(init: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(UniqueIdNode {
            id: init.node_id,
            msg_id: 0,
        })
    }

    fn step(
        &mut self,
        input: Event<UniqueIdPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("Did not receive expected message");
        };

        let mut reply = input.into_reply(Some(&mut self.msg_id));

        match reply.body.payload {
            UniqueIdPayload::Generate => {
                reply.body.payload = UniqueIdPayload::GenerateOk(GenerateOk {
                    id: format!("{}-{}", self.id, self.msg_id),
                });
                reply.send(output).context("Sending generate ok reply")?;
            }
            UniqueIdPayload::GenerateOk(_) => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<UniqueIdNode, UniqueIdPayload>()
}
