use kafka_rust::partition::{PartitionActor, PartitionCommand};
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;
use std::path::PathBuf;

#[tokio::test]
async fn test_partition_actor_append() {
    let test_dir = PathBuf::from("target/test_storage/test_partition_actor");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;

    let (tx, rx) = mpsc::channel(10);
    let actor = PartitionActor::new(rx, test_dir.clone()).await.unwrap();
    
    tokio::spawn(async move {
        actor.run().await;
    });

    let (resp_tx, resp_rx) = oneshot::channel();
    let records = Bytes::from("test-records");
    
    tx.send(PartitionCommand::Append { records: records.clone(), resp_tx }).await.unwrap();
    
    let offset = resp_rx.await.unwrap();
    assert_eq!(offset, 0);
    
    // Verify file exists (active segment is 0.log)
    let log_path = test_dir.join(format!("{:020}.log", 0));
    let content = tokio::fs::read(&log_path).await.unwrap();
    assert_eq!(content, records);
}

#[tokio::test]
async fn test_partition_actor_fetch() {
    let test_dir = PathBuf::from("target/test_storage/test_partition_actor_fetch");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;

    let (tx, rx) = mpsc::channel(10);
    let actor = PartitionActor::new(rx, test_dir.clone()).await.unwrap();
    
    tokio::spawn(async move {
        actor.run().await;
    });

    // Append some data
    let (append_tx, append_rx) = oneshot::channel();
    let records = Bytes::from("hello kafka");
    tx.send(PartitionCommand::Append { records: records.clone(), resp_tx: append_tx }).await.unwrap();
    append_rx.await.unwrap();

    // Fetch it back
    let (fetch_tx, fetch_rx) = oneshot::channel();
    // Use 0 as offset (mock maps 0 -> 0)
    tx.send(PartitionCommand::Fetch { offset: 0, max_bytes: 1024, resp_tx: fetch_tx }).await.unwrap();
    
    let fetched_records = fetch_rx.await.unwrap();
    assert_eq!(fetched_records, records);
}

#[tokio::test]
async fn test_partition_actor_rotation_and_fetch() {
    let test_dir = PathBuf::from("target/test_storage/test_partition_actor_rotation");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;

    let (tx, rx) = mpsc::channel(10);
    // Set a very small max segment size for testing (e.g. 100 bytes)
    let mut actor = PartitionActor::new(rx, test_dir.clone()).await.unwrap();
    actor.set_max_segment_size(100);
    
    tokio::spawn(async move {
        actor.run().await;
    });

    // Append first record (will trigger rotation if it's > 100 bytes)
    let records1 = Bytes::from(vec![0u8; 120]);
    let (append_tx1, append_rx1) = oneshot::channel();
    tx.send(PartitionCommand::Append { records: records1.clone(), resp_tx: append_tx1 }).await.unwrap();
    let offset1 = append_rx1.await.unwrap();
    assert_eq!(offset1, 0);

    // Append second record (should be in a new segment)
    let records2 = Bytes::from("second-segment");
    let (append_tx2, append_rx2) = oneshot::channel();
    tx.send(PartitionCommand::Append { records: records2.clone(), resp_tx: append_tx2 }).await.unwrap();
    let offset2 = append_rx2.await.unwrap();
    assert_eq!(offset2, 1);

    // Verify two log files exist
    let log0 = test_dir.join(format!("{:020}.log", 0));
    let log1 = test_dir.join(format!("{:020}.log", 1));
    assert!(tokio::fs::metadata(&log0).await.is_ok());
    assert!(tokio::fs::metadata(&log1).await.is_ok());

    // Fetch from first segment
    let (fetch_tx1, fetch_rx1) = oneshot::channel();
    tx.send(PartitionCommand::Fetch { offset: 0, max_bytes: 1024, resp_tx: fetch_tx1 }).await.unwrap();
    let fetched1 = fetch_rx1.await.unwrap();
    assert_eq!(fetched1.len(), 120);

    // Fetch from second segment
    let (fetch_tx2, fetch_rx2) = oneshot::channel();
    tx.send(PartitionCommand::Fetch { offset: 1, max_bytes: 1024, resp_tx: fetch_tx2 }).await.unwrap();
    let fetched2 = fetch_rx2.await.unwrap();
    assert_eq!(fetched2, records2);
}
