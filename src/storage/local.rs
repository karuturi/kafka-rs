use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;
use anyhow::Result;
use bytes::Bytes;

pub struct LogAppender {
    path: PathBuf,
}

impl LogAppender {
    pub async fn new(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        Ok(Self { path })
    }

    pub async fn append(&mut self, records: Bytes) -> Result<u64> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        
        let offset = file.metadata().await?.len();
        file.write_all(&records).await?;
        file.flush().await?;
        
        Ok(offset)
    }
}
