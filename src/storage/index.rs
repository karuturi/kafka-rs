use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use anyhow::Result;
use std::path::PathBuf;

pub struct SparseIndex {
    path: PathBuf,
}

impl SparseIndex {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub async fn add_entry(&mut self, relative_offset: u32, physical_position: u32) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        
        file.write_u32(relative_offset).await?;
        file.write_u32(physical_position).await?;
        file.flush().await?;
        
        Ok(())
    }
}
