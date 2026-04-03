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
            index_interval_bytes: 0, // Index every message for Phase 1 accuracy
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

        file.write_u64(logical_offset).await?;
        file.write_u32(records.len() as u32).await?;
        file.write_all(&records).await?;
        file.flush().await?;
        
        let appended_len = 8 + 4 + records.len() as u64;
        self.current_size += appended_len;
        
        Ok(physical_position)
    }

    pub async fn read(&self, physical_position: u64, max_bytes: u32) -> Result<Bytes> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut file = File::open(&self.path).await?;
        
        let file_len = file.metadata().await?.len();
        if physical_position + 12 >= file_len {
            return Ok(Bytes::new());
        }

        file.seek(std::io::SeekFrom::Start(physical_position)).await?;
        
        let mut total_records_buf = Vec::new();
        let mut current_pos = physical_position;

        while current_pos + 12 <= file_len && total_records_buf.len() < max_bytes as usize {
            let _offset = file.read_u64().await?;
            let len = file.read_u32().await?;
            
            if current_pos + 12 + len as u64 > file_len {
                break;
            }

            let mut record_buf = vec![0u8; len as usize];
            file.read_exact(&mut record_buf).await?;
            total_records_buf.extend_from_slice(&record_buf);
            
            current_pos += 12 + len as u64;
            
            // If we've already exceeded max_bytes, we stop
            if total_records_buf.len() >= max_bytes as usize {
                break;
            }
        }
        
        Ok(Bytes::from(total_records_buf))
    }

    pub async fn find_position(&self, logical_offset: u64) -> Result<u64> {
        // For now, we use the logical_offset as the relative_offset in the index
        self.index.find_position(logical_offset as u32).await
    }

    pub fn size(&self) -> u64 {
        self.current_size
    }

    pub async fn find_last_offset(&self) -> Result<u64> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        if !self.path.exists() {
            return Ok(0);
        }

        let mut file = File::open(&self.path).await?;
        let len = file.metadata().await?.len();
        if len < 12 {
            return Ok(0);
        }

        // We can use the index to find the starting position of the last indexed record
        let last_indexed_pos = self.index.find_last_pos().await?;
        file.seek(std::io::SeekFrom::Start(last_indexed_pos)).await?;
        
        let mut last_offset = 0u64;
        while let Ok(offset) = file.read_u64().await {
            last_offset = offset;
            let record_len = file.read_u32().await?;
            file.seek(std::io::SeekFrom::Current(record_len as i64)).await?;
        }
        
        Ok(last_offset)
    }
}
