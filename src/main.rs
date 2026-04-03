use kafka_rust::protocol;
use kafka_rust::broker::BrokerRegistry;
use kafka_rust::partition::{PartitionActor, PartitionCommand};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use bytes::BytesMut;
use std::sync::Arc;
use tokio::sync::mpsc;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let registry = Arc::new(BrokerRegistry::new());
    
    // Setup a default test topic partition
    let (tx, rx) = mpsc::channel(100);
    let storage_path = PathBuf::from("storage/test-0");
    let actor = PartitionActor::new(rx, storage_path).await?;
    tokio::spawn(async move {
        actor.run().await;
    });
    registry.register_partition("test".to_string(), 0, tx).await;

    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    println!("Kafka-RS listening on 127.0.0.1:9092");

    loop {
        let (socket, _) = listener.accept().await?;
        let registry_clone = registry.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket, registry_clone).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(mut socket: tokio::net::TcpStream, registry: Arc<BrokerRegistry>) -> Result<()> {
    loop {
        // Read length
        let mut len_buf = [0u8; 4];
        if let Err(_) = socket.read_exact(&mut len_buf).await {
            return Ok(()); // Connection closed
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        
        // Read body
        let mut body_buf = vec![0u8; len];
        socket.read_exact(&mut body_buf).await?;
        let mut body_mut = BytesMut::from(&body_buf[..]);
        
        // Decode header
        let header = protocol::decode_request_header(&mut body_mut)?;
        println!("Received request: API={}, Version={}, CID={}", 
            header.request_api_key, header.request_api_version, header.correlation_id);
        
        match header.request_api_key {
            18 => { // ApiVersions
                let _request = protocol::decode_apiversions_request(&mut body_mut)?;
                let res_buf = protocol::encode_apiversions_response(header.correlation_id)?;
                
                let res_len = res_buf.len() as u32;
                socket.write_all(&res_len.to_be_bytes()).await?;
                socket.write_all(&res_buf).await?;
            }
            3 => { // Metadata
                let request = protocol::decode_metadata_request(&mut body_mut)?;
                let mut topic_names: Vec<String> = request.topics.unwrap_or_default().into_iter()
                    .filter_map(|t| t.name.map(|n| n.to_string()))
                    .collect();
                
                if topic_names.is_empty() {
                    topic_names = registry.get_all_topics().await;
                }
                
                let res_buf = protocol::encode_metadata_response(header.correlation_id, topic_names)?;
                let res_len = res_buf.len() as u32;
                socket.write_all(&res_len.to_be_bytes()).await?;
                socket.write_all(&res_buf).await?;
            }
            0 => { // Produce
                let request = protocol::decode_produce_request(&mut body_mut)?;
                if let Some(topic) = request.topic_data.first() {
                    if let Some(partition) = topic.partition_data.first() {
                        let topic_name = topic.name.to_string();
                        let partition_index = partition.index;
                        
                        let tx = if let Some(tx) = registry.get_partition_tx(&topic_name, partition_index).await {
                            Some(tx)
                        } else {
                            // Auto-create partition
                            let (tx, rx) = mpsc::channel(100);
                            let storage_path = PathBuf::from(format!("storage/{}-{}", topic_name, partition_index));
                            match PartitionActor::new(rx, storage_path).await {
                                Ok(actor) => {
                                    tokio::spawn(async move {
                                        let _ = actor.run().await;
                                    });
                                    registry.register_partition(topic_name.clone(), partition_index, tx.clone()).await;
                                    Some(tx)
                                }
                                Err(e) => {
                                    eprintln!("Failed to create partition actor: {:?}", e);
                                    None
                                }
                            }
                        };

                        if let Some(tx) = tx {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            tx.send(PartitionCommand::Append { 
                                records: partition.records.clone().unwrap_or_default(), 
                                resp_tx 
                            }).await?;
                            
                            let base_offset = resp_rx.await?;
                            let res_buf = protocol::encode_produce_response(
                                header.correlation_id, topic_name, partition_index, base_offset)?;
                            
                            let res_len = res_buf.len() as u32;
                            socket.write_all(&res_len.to_be_bytes()).await?;
                            socket.write_all(&res_buf).await?;
                        } else {
                            // Still send a response but with error (offset -1 for now as a simple error signal)
                            let res_buf = protocol::encode_produce_response(
                                header.correlation_id, topic_name, partition_index, u64::MAX)?;
                            let res_len = res_buf.len() as u32;
                            socket.write_all(&res_len.to_be_bytes()).await?;
                            socket.write_all(&res_buf).await?;
                        }
                    }
                }
            }
            1 => { // Fetch
                let request = protocol::decode_fetch_request(&mut body_mut)?;
                // Simple implementation: handle first topic/partition requested
                if let Some(topic) = request.topics.first() {
                    if let Some(partition) = topic.partitions.first() {
                        let topic_name = topic.topic.to_string();
                        let partition_index = partition.partition.into();
                        
                        if let Some(tx) = registry.get_partition_tx(&topic_name, partition_index).await {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            tx.send(PartitionCommand::Fetch { 
                                offset: partition.fetch_offset as u64, 
                                max_bytes: partition.partition_max_bytes as u32, 
                                resp_tx 
                            }).await?;
                            
                            let (records, high_watermark) = resp_rx.await?;
                            let res_buf = protocol::encode_fetch_response(
                                header.correlation_id, topic_name, partition_index, records, high_watermark)?;
                            
                            let res_len = res_buf.len() as u32;
                            socket.write_all(&res_len.to_be_bytes()).await?;
                            socket.write_all(&res_buf).await?;
                        }
                    }
                }
            }
            _ => {
                eprintln!("Unsupported API key: {}", header.request_api_key);
                break;
            }
        }
    }
    Ok(())
}
