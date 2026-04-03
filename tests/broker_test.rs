use kafka_rust::broker::BrokerRegistry;
use tokio::sync::mpsc;

#[tokio::test]
async fn test_broker_registry_registration() {
    let registry = BrokerRegistry::new();
    let (tx, _rx) = mpsc::channel(1);
    
    registry.register_partition("test-topic".to_string(), 0, tx.clone()).await;
    
    let retrieved_tx = registry.get_partition_tx("test-topic", 0).await;
    assert!(retrieved_tx.is_some());
    
    let non_existent = registry.get_partition_tx("other", 0).await;
    assert!(non_existent.is_none());
}
