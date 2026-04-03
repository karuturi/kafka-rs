use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use crate::storage::local::LogAppender;
use anyhow::Result;

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
    appender: LogAppender,
    current_offset: u64,
}

impl PartitionActor {
    pub async fn new(receiver: mpsc::Receiver<PartitionCommand>, log_path: std::path::PathBuf) -> Result<Self> {
        let appender = LogAppender::new(log_path).await?;
        Ok(Self {
            receiver,
            appender,
            current_offset: 0,
        })
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.receiver.recv().await {
            match cmd {
                PartitionCommand::Append { records, resp_tx } => {
                    match self.appender.append(records).await {
                        Ok(physical_offset) => {
                            let logical_offset = self.current_offset;
                            self.current_offset += 1;
                            let _ = resp_tx.send(logical_offset);
                        }
                        Err(e) => {
                            eprintln!("Failed to append to log: {:?}", e);
                        }
                    }
                }
                PartitionCommand::Fetch { offset: _, max_bytes: _, resp_tx: _ } => {
                    // Not implemented yet
                }
            }
        }
    }
}
