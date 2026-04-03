use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use anyhow::Result;
use bytes::Bytes;
use crate::storage::index::SparseIndex;

pub struct LogAppender {
    path: PathBuf,
    index: SparseIndex,
    current_size: u64,
    last_indexed_size: u64,
    index_interval_bytes: u64,
}

impl LogAppender {
    pub async fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        
        let mut index_path = path.clone();
        index_path.set_extension("index");
        let index = SparseIndex::new(index_path);

        let current_size = match tokio::fs::metadata(&path).await {
            Ok(m) => m.len(),
            Err(_) => 0,
        };

        Ok(Self { 
            path, 
            index,
            current_size,
            last_indexed_size: 0,
            index_interval_bytes: 4096, // 4KB interval
        })
    }

    pub async fn append(&mut self, records: Bytes, logical_offset: u64) -> Result<u64> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        
        let physical_position = self.current_size;
        
        // Sparse Indexing logic
        if self.current_size == 0 || self.current_size - self.last_indexed_size >= self.index_interval_bytes {
            // In a real Kafka segment, relative_offset is (logical_offset - base_offset)
            // For now we use the full logical_offset as a simplification
            self.index.add_entry(logical_offset as u32, physical_position as u32).await?;
            self.last_indexed_size = self.current_size;
        }

        file.write_all(&records).await?;
        file.flush().await?;
        
        self.current_size += records.len() as u64;
        
        Ok(physical_position)
    }

    pub fn size(&self) -> u64 {
        self.current_size
    }
}
