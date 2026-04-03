use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use crate::storage::local::LogAppender;
use anyhow::Result;
use std::path::PathBuf;

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
    base_path: PathBuf,
    max_segment_size: u64,
}

impl PartitionActor {
    pub async fn new(receiver: mpsc::Receiver<PartitionCommand>, base_path: PathBuf) -> Result<Self> {
        let active_segment_path = base_path.join(format!("{:020}.log", 0));
        let appender = LogAppender::new(active_segment_path).await?;
        
        Ok(Self {
            receiver,
            appender,
            current_offset: 0,
            base_path,
            max_segment_size: 10 * 1024 * 1024, // 10MB default
        })
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.receiver.recv().await {
            match cmd {
                PartitionCommand::Append { records, resp_tx } => {
                    let logical_offset = self.current_offset;
                    
                    // Segment rotation check
                    if self.appender.size() >= self.max_segment_size {
                        let new_segment_path = self.base_path.join(format!("{:020}.log", logical_offset));
                        if let Ok(new_appender) = LogAppender::new(new_segment_path).await {
                            self.appender = new_appender;
                        }
                    }

                    match self.appender.append(records, logical_offset).await {
                        Ok(_) => {
                            self.current_offset += 1;
                            let _ = resp_tx.send(logical_offset);
                        }
                        Err(e) => {
                            eprintln!("Failed to append to log: {:?}", e);
                        }
                    }
                }
                PartitionCommand::Fetch { offset, max_bytes, resp_tx } => {
                    // For now, assume physical_position == logical_offset * 1024 as a placeholder
                    // In a real implementation, we'd use the SparseIndex to find the position
                    let physical_position = offset * 1024; // MOCK for Task 5
                    match self.appender.read(physical_position, max_bytes).await {
                        Ok(records) => {
                            let _ = resp_tx.send(records);
                        }
                        Err(e) => {
                            eprintln!("Failed to read from log: {:?}", e);
                        }
                    }
                }
            }
        }
    }
}
