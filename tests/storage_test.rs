use kafka_rust::storage::local::LogAppender;
use kafka_rust::storage::index::SparseIndex;
use bytes::Bytes;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;
use bytes::Buf;

#[tokio::test]
async fn test_log_appender_writes_to_disk() {
    let test_dir = PathBuf::from("target/test_storage/test_log_appender");
    let log_path = test_dir.join("00000000000000000000.log");
    let index_path = test_dir.join("00000000000000000000.index");
    
    // Clean up from previous runs
    let _ = tokio::fs::remove_dir_all(&test_dir).await;
    
    let mut appender = LogAppender::new(log_path.clone()).await.unwrap();
    
    let records1 = Bytes::from("first-record");
    let offset1 = appender.append(records1.clone(), 0).await.unwrap();
    assert_eq!(offset1, 0);
    
    let records2 = Bytes::from("second");
    let offset2 = appender.append(records2.clone(), 1).await.unwrap();
    assert_eq!(offset2, records1.len() as u64);

    // Verify index was created (first append always indexes)
    assert!(tokio::fs::metadata(&index_path).await.is_ok());
}

#[tokio::test]
async fn test_log_appender_read_with_index() {
    let test_dir = PathBuf::from("target/test_storage/test_log_appender_read");
    let log_path = test_dir.join("00000000000000000000.log");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;
    
    let mut appender = LogAppender::new(log_path.clone()).await.unwrap();
    
    // We'll append several records to trigger indexing (interval is 4KB)
    // For testing, we might want to reduce the interval or just write enough data.
    // Let's check what the interval is in LogAppender::new.
    
    let record = Bytes::from(vec![0u8; 1000]);
    for i in 0..6 {
        appender.append(record.clone(), i).await.unwrap();
    }
    
    // Total size is 6000 bytes. With 4096 interval, we should have 2 index entries (at 0 and at 5000 because 5000-0 >= 4096).
    
    // Find position for offset 5
    let pos = appender.find_position(5).await.unwrap();
    assert!(pos >= 5000);
    
    let read_records = appender.read(pos, 1000).await.unwrap();
    assert_eq!(read_records.len(), 1000);
}

#[tokio::test]
async fn test_sparse_index_writes_to_disk() {
    let test_dir = PathBuf::from("target/test_storage/test_sparse_index");
    let index_path = test_dir.join("00000000000000000000.index");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;
    tokio::fs::create_dir_all(&test_dir).await.unwrap();

    let mut index = SparseIndex::new(index_path.clone());
    index.add_entry(10, 1024).await.unwrap();
    index.add_entry(20, 2048).await.unwrap();

    let mut file = tokio::fs::File::open(&index_path).await.unwrap();
    let mut buf = vec![];
    file.read_to_end(&mut buf).await.unwrap();

    assert_eq!(buf.len(), 16); // 2 entries * 8 bytes (two u32s)
    
    let mut reader = &buf[..];
    assert_eq!(reader.get_u32(), 10);
    assert_eq!(reader.get_u32(), 1024);
    assert_eq!(reader.get_u32(), 20);
    assert_eq!(reader.get_u32(), 2048);
}

#[tokio::test]
async fn test_sparse_index_find_position() {
    let test_dir = PathBuf::from("target/test_storage/test_sparse_index_lookup");
    let index_path = test_dir.join("00000000000000000000.index");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;
    tokio::fs::create_dir_all(&test_dir).await.unwrap();

    let mut index = SparseIndex::new(index_path.clone());
    index.add_entry(0, 0).await.unwrap();
    index.add_entry(10, 1024).await.unwrap();
    index.add_entry(20, 2048).await.unwrap();

    // Exact match
    assert_eq!(index.find_position(10).await.unwrap(), 1024);
    
    // Nearest lower bound
    assert_eq!(index.find_position(15).await.unwrap(), 1024);
    assert_eq!(index.find_position(25).await.unwrap(), 2048);
    
    // First entry
    assert_eq!(index.find_position(5).await.unwrap(), 0);
}
