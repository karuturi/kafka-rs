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

    pub async fn find_position(&self, target_offset: u32) -> Result<u64> {
        use tokio::io::AsyncReadExt;
        if !self.path.exists() {
            return Ok(0);
        }

        let mut file = tokio::fs::File::open(&self.path).await?;
        let mut last_position = 0u64;
        
        let mut buf = [0u8; 8];
        while file.read_exact(&mut buf).await.is_ok() {
            let offset = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            let position = u32::from_be_bytes([buf[4], buf[5], buf[6], buf[7]]);
            
            if offset > target_offset {
                break;
            }
            last_position = position as u64;
        }
        
        Ok(last_position)
    }
}
