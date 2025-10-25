use std::{collections::HashMap, io::StdoutLock, sync::mpsc::Sender};

use anyhow::Context;
use gossip_glomers::{Body, Event, Init, Message, Node, main_loop};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum GCounterPayload {
    Add(Add),
    AddOk,
    Read,
    ReadOk(ReadOk),
    CasOk,
    Error(Error),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Add {
    pub delta: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ReadOk {
    pub value: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Error {
    code: usize,
    text: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum SeqKvPayload {
    Read(SeqKvRead),
    ReadOk(SeqKvReadOk),
    Write(SeqKvWrite),
    WriteOk,
    Cas(SeqKvCas),
    CasOk,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SeqKvRead {
    pub key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SeqKvReadOk {
    pub value: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SeqKvWrite {
    pub key: String,
    pub value: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SeqKvCas {
    pub key: String,
    /// Current value of the key.
    pub from: usize,
    /// New value of the key.
    pub to: usize,
    /// Whether the key should be created if it does not already exist (upsert).
    #[serde(default, rename = "create_if_not_exists")]
    put: bool,
}

#[derive(Debug, Clone)]
enum OperationType {
    Read,
    Write(OperationWrite),
    Cas,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OperationWrite {
    pub value: usize,
}

struct PendingOperation {
    src: String,
    op_type: OperationType,
}

struct GCounterNode {
    id: String,
    msg_id: usize,
    pending_ops: HashMap<usize, PendingOperation>,
}

impl Node<GCounterPayload> for GCounterNode {
    fn from_init(init: Init, _: Sender<Event<GCounterPayload>>) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        Ok(GCounterNode {
            id: init.node_id,
            msg_id: 0,
            pending_ops: HashMap::new(),
        })
    }

    fn step(
        &mut self,
        input: Event<GCounterPayload>,
        output: &mut StdoutLock,
    ) -> anyhow::Result<()> {
        match input {
            Event::Message(input) => {
                match input.body.payload {
                    GCounterPayload::Add(add) => {
                        // Send a read request to the KV with our write operation. Once
                        // KV returns the read value we can perform a CAS.
                        self.send_seq_kv_request(
                            output,
                            SeqKvPayload::Read(SeqKvRead {
                                key: "counter".to_string(),
                            }),
                            input.body.id.expect("Read request with no id"),
                            input.src,
                            OperationType::Write(OperationWrite { value: add.delta }),
                        )?;
                    }
                    GCounterPayload::AddOk => {}
                    GCounterPayload::Read => {
                        // Send our read request to KV, the ok handler will return the value.
                        self.send_seq_kv_request(
                            output,
                            SeqKvPayload::Read(SeqKvRead {
                                key: "counter".to_string(),
                            }),
                            input.body.id.expect("Read request with no id"),
                            input.src,
                            OperationType::Read,
                        )?;
                    }
                    GCounterPayload::ReadOk(read) => {
                        if input.src != "seq-kv" {
                            panic!(
                                "Received unsupported read ok operation '{}'. Only 'seq-kv' is implemented.",
                                input.src,
                            );
                        }

                        let in_reply_to = &input
                            .body
                            .in_reply_to
                            .expect("seq kv read response with no 'in_reply_to'");

                        let Some(pending_op) = self.pending_ops.remove(in_reply_to) else {
                            panic!(
                                "Could not find pending operation for replying to '{}'",
                                in_reply_to
                            );
                        };

                        match pending_op.op_type {
                            OperationType::Read => {
                                // Was a read operation so return the value to the original src.
                                self.msg_id += 1;
                                Message {
                                    src: self.id.clone(),
                                    dst: pending_op.src,
                                    body: Body {
                                        id: Some(self.msg_id),
                                        in_reply_to: input.body.in_reply_to,
                                        payload: GCounterPayload::ReadOk(ReadOk {
                                            value: read.value,
                                        }),
                                    },
                                }
                                .send(output)
                                .context("Sending read ok")?;
                            }
                            OperationType::Write(write) => {
                                // Was a write so with the latest value attempt the CAS operation.
                                self.send_seq_kv_request(
                                    output,
                                    SeqKvPayload::Cas(SeqKvCas {
                                        key: "counter".to_string(),
                                        from: read.value,
                                        to: read.value + write.value,
                                        // We successfully read the key so wouldn't expect an upsert behaviour here.
                                        put: false,
                                    }),
                                    input.body.id.expect("Read request with no id"),
                                    input.src,
                                    OperationType::Cas,
                                )?;
                            }
                            _op => panic!("Unexpected operation from read ok - {:?}", _op),
                        }
                    }
                    GCounterPayload::CasOk => {
                        if let Some(pending_op) = self.pending_ops.remove(
                            &input
                                .body
                                .in_reply_to
                                .expect("seq kv cas ok response with no reply to"),
                        ) {
                            match pending_op.op_type {
                                OperationType::Cas => {
                                    self.msg_id += 1;
                                    Message {
                                        src: self.id.clone(),
                                        dst: pending_op.src,
                                        body: Body {
                                            id: Some(self.msg_id),
                                            in_reply_to: input.body.in_reply_to,
                                            payload: GCounterPayload::AddOk,
                                        },
                                    }
                                    .send(output)
                                    .context("Sending add ok")?;
                                }
                                _op => panic!("Unexpected operation from read ok - {:?}", _op),
                            }
                        }
                    }
                    GCounterPayload::Error(error) => {
                        if let Some(pending_op) = self.pending_ops.remove(
                            &input
                                .body
                                .in_reply_to
                                .expect("seq kv cas error response with no reply to"),
                        ) {
                            match pending_op.op_type {
                                OperationType::Read => {
                                    // Attempt to read and key does not exist. We can return 0 since
                                    // this is equivalent to no key (at least in this use case).
                                    self.msg_id += 1;
                                    Message {
                                        src: self.id.clone(),
                                        dst: pending_op.src,
                                        body: Body {
                                            id: Some(self.msg_id),
                                            in_reply_to: input.body.in_reply_to,
                                            payload: GCounterPayload::ReadOk(ReadOk { value: 0 }),
                                        },
                                    }
                                    .send(output)
                                    .context("Sending seq kv read")?;
                                }
                                OperationType::Write(write) => {
                                    // Attempted to read they key (before our CAS operation as we need the current value)
                                    // and key does not exist. We can bypass this and send the CAS operation now since we
                                    // now know this is 0 and will insert.
                                    self.send_seq_kv_request(
                                        output,
                                        SeqKvPayload::Cas(SeqKvCas {
                                            key: "counter".to_string(),
                                            from: 0,
                                            to: write.value,
                                            put: true,
                                        }),
                                        input.body.in_reply_to.expect("Read request with no id"),
                                        input.src,
                                        OperationType::Cas,
                                    )?;
                                }
                                _op => {
                                    panic!("error response from maelstrom - {:?}:{:?}", error, _op)
                                }
                            }
                        }
                    }
                }
            }
            Event::Interval => {}
            Event::EOF => {}
        }

        Ok(())
    }
}

impl GCounterNode {
    fn send_seq_kv_request(
        &mut self,
        output: &mut StdoutLock,
        payload: SeqKvPayload,
        in_reply_to: usize,
        src: String,
        op_type: OperationType,
    ) -> anyhow::Result<()> {
        self.msg_id += 1;
        Message {
            src: self.id.clone(),
            dst: String::from("seq-kv"),
            body: Body {
                id: Some(self.msg_id),
                in_reply_to: Some(in_reply_to),
                payload,
            },
        }
        .send(output)
        .context("Sending seq kv read")?;
        self.pending_ops
            .insert(self.msg_id, PendingOperation { src, op_type });
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<GCounterNode, GCounterPayload>()
}
