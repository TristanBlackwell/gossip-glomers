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
                        self.send_seq_kv_read_request(
                            output,
                            input.body.id.expect("Read request with no id"),
                            input.src,
                            OperationType::Write(OperationWrite { value: add.delta }),
                        )?;
                        // self.msg_id += 1;
                        // Message {
                        //     src: self.id,
                        //     dst: String::from("seq-kv"),
                        //     body: Body {
                        //         id: Some(self.msg_id),
                        //         in_reply_to: None,
                        //         payload: SeqKvPayload::Write(SeqKvWrite {
                        //             key: format!("counter-{}", self.id),
                        //             value: add.delta,
                        //         }),
                        //     },
                        // };

                        // let mut reply = input.into_reply(Some(&mut self.msg_id));

                        // reply.body.payload = GCounterPayload::AddOk;
                        // reply.send(output).context("Sending add ok reply")?;
                    }
                    GCounterPayload::AddOk => {}
                    GCounterPayload::Read => {
                        self.send_seq_kv_read_request(
                            output,
                            input.body.id.expect("Read request with no id"),
                            input.src,
                            OperationType::Read,
                        )?;
                    }
                    GCounterPayload::ReadOk(read) => {
                        if input.src != "seq-kv" {
                            panic!("Received read ok from {}", input.src);
                        }

                        if let Some(pending_op) = self.pending_ops.remove(
                            &input
                                .body
                                .in_reply_to
                                .expect("seq kv read response with no reply to"),
                        ) {
                            match pending_op.op_type {
                                OperationType::Read => {
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
                                    .context("Sending seq kv read")?;
                                }
                                OperationType::Write(write) => {
                                    self.send_seq_kv_cas_request(
                                        output,
                                        input.body.id.expect("Read request with no id"),
                                        input.src,
                                        SeqKvCas {
                                            key: "counter".to_string(),
                                            from: read.value,
                                            to: read.value + write.value,
                                            put: true,
                                        },
                                    )?;
                                }
                                _op => panic!("Unexpected operation from read ok - {:?}", _op),
                            }
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
                        panic!("Received an error from maelstrom: {:?}", error);
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
    fn send_seq_kv_read_request(
        &mut self,
        output: &mut StdoutLock,
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
                payload: SeqKvPayload::Read(SeqKvRead {
                    key: "counter".to_string(),
                }),
            },
        }
        .send(output)
        .context("Sending seq kv read")?;
        self.pending_ops
            .insert(self.msg_id, PendingOperation { src, op_type });
        Ok(())
    }

    fn send_seq_kv_cas_request(
        &mut self,
        output: &mut StdoutLock,
        in_reply_to: usize,
        src: String,
        cas: SeqKvCas,
    ) -> anyhow::Result<()> {
        self.msg_id += 1;
        Message {
            src: self.id.clone(),
            dst: String::from("seq-kv"),
            body: Body {
                id: Some(self.msg_id),
                in_reply_to: Some(in_reply_to),
                payload: SeqKvPayload::Cas(cas),
            },
        }
        .send(output)
        .context("Sending seq kv cas")?;
        self.pending_ops.insert(
            self.msg_id,
            PendingOperation {
                src,
                op_type: OperationType::Cas,
            },
        );
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    main_loop::<GCounterNode, GCounterPayload>()
}
