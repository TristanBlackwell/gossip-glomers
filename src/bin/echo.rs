use std::io::StdoutLock;

use anyhow::Context;
use gossip_glomers::{Event, Init, Node, main_loop};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EchoPayload {
    Echo(Echo),
    EchoOk(Echo),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Echo {
    pub echo: String,
}

struct EchoNode {
    id: usize,
}

impl Node<EchoPayload> for EchoNode {
    fn from_init(_: Init) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(EchoNode { id: 1 })
    }

    fn step(&mut self, input: Event<EchoPayload>, output: &mut StdoutLock) -> anyhow::Result<()> {
        let Event::Message(input) = input else {
            panic!("Did not receive expected message");
        };

        let mut reply = input.into_reply(Some(&mut self.id));

        match reply.body.payload {
            EchoPayload::Echo(echo) => {
                reply.body.payload = EchoPayload::EchoOk(echo);
                reply.send(output).context("Sending echo ok reply")?;
            }
            EchoPayload::EchoOk(_) => {}
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<EchoNode, EchoPayload>()
}
