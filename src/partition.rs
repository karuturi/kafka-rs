use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

pub enum PartitionCommand {
    Append {
        records: Bytes,
        resp_tx: oneshot::Sender<u64>, // returns base offset
    },
    Fetch {
        offset: u64,
        max_bytes: u32,
        resp_tx: oneshot::Sender<Bytes>,
    },
}

pub struct PartitionActor {
    receiver: mpsc::Receiver<PartitionCommand>,
    current_offset: u64,
}

impl PartitionActor {
    pub fn new(receiver: mpsc::Receiver<PartitionCommand>) -> Self {
        Self {
            receiver,
            current_offset: 0,
        }
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.receiver.recv().await {
            match cmd {
                PartitionCommand::Append { records, resp_tx } => {
                    // Minimal implementation to pass initial test
                    let offset = self.current_offset;
                    self.current_offset += 1; // Simplification for now
                    let _ = resp_tx.send(offset);
                }
                PartitionCommand::Fetch { offset: _, max_bytes: _, resp_tx: _ } => {
                    // Not implemented yet
                }
            }
        }
    }
}
