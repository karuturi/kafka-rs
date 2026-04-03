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
