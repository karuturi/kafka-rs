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
    segment_offsets: Vec<u64>,
}

impl PartitionActor {
    pub async fn new(receiver: mpsc::Receiver<PartitionCommand>, base_path: PathBuf) -> Result<Self> {
        if !base_path.exists() {
            tokio::fs::create_dir_all(&base_path).await?;
        }

        let mut segment_offsets = Vec::new();
        let mut dir = tokio::fs::read_dir(&base_path).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("log") {
                if let Some(name) = path.file_stem().and_then(|s| s.to_str()) {
                    if let Ok(offset) = name.parse::<u64>() {
                        segment_offsets.push(offset);
                    }
                }
            }
        }
        segment_offsets.sort();

        if segment_offsets.is_empty() {
            segment_offsets.push(0);
        }

        let last_base_offset = *segment_offsets.last().unwrap();
        let active_segment_path = base_path.join(format!("{:020}.log", last_base_offset));
        let appender = LogAppender::new(active_segment_path).await?;
        
        let last_offset = appender.find_last_offset().await.unwrap_or(last_base_offset);
        let current_offset = if last_offset == 0 && appender.size() == 0 {
            0
        } else {
            last_offset + 1
        };
        
        Ok(Self {
            receiver,
            appender,
            current_offset,
            base_path,
            max_segment_size: 10 * 1024 * 1024, // 10MB default
            segment_offsets,
        })
    }

    pub fn set_max_segment_size(&mut self, size: u64) {
        self.max_segment_size = size;
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
                            self.segment_offsets.push(logical_offset);
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
                    // Find the correct segment
                    let mut segment_base_offset = 0;
                    for &base in self.segment_offsets.iter().rev() {
                        if offset >= base {
                            segment_base_offset = base;
                            break;
                        }
                    }

                    let segment_path = self.base_path.join(format!("{:020}.log", segment_base_offset));
                    if let Ok(appender) = LogAppender::new(segment_path).await {
                        match appender.find_position(offset).await {
                            Ok(physical_position) => {
                                match appender.read(physical_position, max_bytes).await {
                                    Ok(records) => {
                                        let _ = resp_tx.send(records);
                                    }
                                    Err(e) => {
                                        eprintln!("Failed to read from log: {:?}", e);
                                        let _ = resp_tx.send(Bytes::new());
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("Failed to find position in index: {:?}", e);
                                let _ = resp_tx.send(Bytes::new());
                            }
                        }
                    } else {
                        let _ = resp_tx.send(Bytes::new());
                    }
                }
            }
        }
    }
}
