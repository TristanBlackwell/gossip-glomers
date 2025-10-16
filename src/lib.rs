use anyhow::Context;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::{BufRead, StdoutLock, Write},
    sync::mpsc::Sender,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Message<Payload> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

impl<Payload> Message<Payload> {
    /// Converts an incoming message into a reply message with
    /// a new ID.
    pub fn into_reply(self, msg_id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: msg_id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
                }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }
    }

    /// Sends the Message to the provided output as JSON.
    pub fn send(&self, output: &mut impl Write) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        serde_json::to_writer(&mut *output, self).context("Cannot serialise reply message")?;
        output
            .write_all(b"\n")
            .context("Cannot write trailing newline")?;
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum InitPayload {
    Init(Init),
    InitOk,
    Error(Error),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Init {
    pub node_id: String,
    pub node_ids: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Error {
    code: usize,
    text: String,
}

#[derive(Debug, Clone)]
pub enum Event<Payload> {
    Message(Message<Payload>),
    Interval,
    EOF,
}

pub trait Node<Payload> {
    /// Initialises a node based of an init message.
    fn from_init(init: Init, tx: Sender<Event<Payload>>) -> anyhow::Result<Self>
    where
        Self: Sized;
    /// Consume and act upon a message for the node.
    fn step(&mut self, input: Event<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<N, P>() -> anyhow::Result<()>
where
    N: Node<P>,
    P: DeserializeOwned + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();

    // stdin/stdout are shared global handles. We take a lock to ensure
    // exclusive access when reading or writing.
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();
    let mut stdout = std::io::stdout().lock();

    /* First message should be initialisation giving us our node ID and list of
        other known nodes.

        This read will block until the message is received.
    */
    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin
            .next()
            .expect("Init message not received")
            .context("Failed to read init message from input")?,
    )
    .context("Init message could not be deserialised as JSON")?;

    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("First message should be init");
    };

    let mut node: N = Node::from_init(init, tx.clone()).context("Node initialisation failed")?;

    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        },
    };

    // Indicate init has been received
    reply
        .send(&mut stdout)
        .context("Cannot send response to init message")?;

    // Our main handler no longer requires the lock on reading stdin.
    drop(stdin);

    let jh = std::thread::spawn(move || {
        let stdin = std::io::stdin().lock();

        for line in stdin.lines() {
            let line = line.context("Could not read Maelstrom input message")?;
            let input: Message<P> = serde_json::from_str(&line)
                .context("Maelstrom input could not be deserialised as JSON")?;

            if tx.send(Event::Message(input)).is_err() {
                return Ok::<_, anyhow::Error>(());
            }
        }

        let _ = tx.send(Event::EOF);
        Ok(())
    });

    for input in rx {
        node.step(input, &mut stdout)
            .context("Failed to execute node step function")?;
    }

    jh.join()
        .expect("stdin thread panicked")
        .context("stdin thread error")?;

    Ok(())
}
