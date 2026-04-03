use kafka_rust::storage::local::LogAppender;
use bytes::Bytes;
use std::path::PathBuf;

#[tokio::test]
async fn test_log_appender_writes_to_disk() {
    let test_dir = PathBuf::from("target/test_storage/test_log_appender");
    let log_path = test_dir.join("00000000000000000000.log");
    
    // Clean up from previous runs
    let _ = tokio::fs::remove_dir_all(&test_dir).await;
    
    let mut appender = LogAppender::new(log_path.clone()).await.unwrap();
    
    let records1 = Bytes::from("first-record");
    let offset1 = appender.append(records1.clone()).await.unwrap();
    assert_eq!(offset1, 0);
    
    let records2 = Bytes::from("second");
    let offset2 = appender.append(records2.clone()).await.unwrap();
    assert_eq!(offset2, records1.len() as u64);
    
    // Verify file content
    let content = tokio::fs::read(&log_path).await.unwrap();
    let mut expected = vec![];
    expected.extend_from_slice(&records1);
    expected.extend_from_slice(&records2);
    assert_eq!(content, expected);
}
