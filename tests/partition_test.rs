use kafka_rust::partition::{PartitionActor, PartitionCommand};
use tokio::sync::{mpsc, oneshot};
use bytes::Bytes;

#[tokio::test]
async fn test_partition_actor_append() {
    let (tx, rx) = mpsc::channel(10);
    let actor = PartitionActor::new(rx);
    
    tokio::spawn(async move {
        actor.run().await;
    });

    let (resp_tx, resp_rx) = oneshot::channel();
    let records = Bytes::from("test-records");
    
    tx.send(PartitionCommand::Append { records, resp_tx }).await.unwrap();
    
    let offset = resp_rx.await.unwrap();
    assert_eq!(offset, 0);
    
    let (resp_tx2, resp_rx2) = oneshot::channel();
    tx.send(PartitionCommand::Append { records: Bytes::from("more"), resp_tx: resp_tx2 }).await.unwrap();
    let offset2 = resp_rx2.await.unwrap();
    assert_eq!(offset2, 1);
}
