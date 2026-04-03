use tokio::sync::mpsc;
use std::collections::HashMap;
use crate::partition::PartitionCommand;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct BrokerRegistry {
    // Topic Name -> Partition ID -> Sender
    partitions: Arc<Mutex<HashMap<String, HashMap<i32, mpsc::Sender<PartitionCommand>>>>>,
}

impl BrokerRegistry {
    pub fn new() -> Self {
        Self {
            partitions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn register_partition(&self, topic: String, partition_id: i32, tx: mpsc::Sender<PartitionCommand>) {
        let mut partitions = self.partitions.lock().await;
        partitions.entry(topic).or_insert_with(HashMap::new).insert(partition_id, tx);
    }

    pub async fn get_partition_tx(&self, topic: &str, partition_id: i32) -> Option<mpsc::Sender<PartitionCommand>> {
        let partitions = self.partitions.lock().await;
        partitions.get(topic)?.get(&partition_id).cloned()
    }

    pub async fn get_all_topics(&self) -> Vec<String> {
        let partitions = self.partitions.lock().await;
        let mut topics: Vec<String> = partitions.keys().cloned().collect();
        topics.sort();
        topics
    }
}
