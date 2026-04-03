use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kafka_protocol::messages::{ApiVersionsRequest, RequestHeader, ResponseHeader, ProduceRequest, FetchRequest, FetchResponse};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use bytes::{BytesMut, Bytes};
use std::sync::Arc;
use tokio::sync::mpsc;
use std::path::PathBuf;
use kafka_rust::broker::BrokerRegistry;
use kafka_rust::partition::{PartitionActor, PartitionCommand};

// Mock the handle_connection from main.rs for testing
async fn handle_test_connection(mut socket: tokio::net::TcpStream, registry: Arc<BrokerRegistry>) -> anyhow::Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        if let Err(_) = socket.read_exact(&mut len_buf).await {
            return Ok(());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        
        let mut body_buf = vec![0u8; len];
        socket.read_exact(&mut body_buf).await?;
        let mut body_mut = BytesMut::from(&body_buf[..]);
        
        let header = kafka_rust::protocol::decode_request_header(&mut body_mut)?;
        
        match header.request_api_key {
            18 => { // ApiVersions
                let _request = kafka_rust::protocol::decode_apiversions_request(&mut body_mut)?;
                let res_buf = kafka_rust::protocol::encode_apiversions_response(header.correlation_id)?;
                let res_len = res_buf.len() as u32;
                socket.write_all(&res_len.to_be_bytes()).await?;
                socket.write_all(&res_buf).await?;
            }
            3 => { // Metadata
                let request = kafka_rust::protocol::decode_metadata_request(&mut body_mut)?;
                let topic_names = request.topics.unwrap_or_default().into_iter()
                    .filter_map(|t| t.name.map(|n| n.to_string()))
                    .collect();
                let res_buf = kafka_rust::protocol::encode_metadata_response(header.correlation_id, topic_names)?;
                let res_len = res_buf.len() as u32;
                socket.write_all(&res_len.to_be_bytes()).await?;
                socket.write_all(&res_buf).await?;
            }
            0 => { // Produce
                let request = kafka_rust::protocol::decode_produce_request(&mut body_mut)?;
                if let Some(topic) = request.topic_data.first() {
                    if let Some(partition) = topic.partition_data.first() {
                        let topic_name = topic.name.to_string();
                        let partition_index = partition.index;
                        if let Some(tx) = registry.get_partition_tx(&topic_name, partition_index).await {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            tx.send(PartitionCommand::Append { records: partition.records.clone().unwrap_or_default(), resp_tx }).await?;
                            let base_offset = resp_rx.await?;
                            let res_buf = kafka_rust::protocol::encode_produce_response(header.correlation_id, topic_name, partition_index, base_offset)?;
                            let res_len = res_buf.len() as u32;
                            socket.write_all(&res_len.to_be_bytes()).await?;
                            socket.write_all(&res_buf).await?;
                        }
                    }
                }
            }
            1 => { // Fetch
                let request = kafka_rust::protocol::decode_fetch_request(&mut body_mut)?;
                if let Some(topic) = request.topics.first() {
                    if let Some(partition) = topic.partitions.first() {
                        let topic_name = topic.topic.to_string();
                        let partition_index = partition.partition.into();
                        if let Some(tx) = registry.get_partition_tx(&topic_name, partition_index).await {
                            let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                            tx.send(PartitionCommand::Fetch { offset: partition.fetch_offset as u64, max_bytes: partition.partition_max_bytes as u32, resp_tx }).await?;
                            let records = resp_rx.await?;
                            let res_buf = kafka_rust::protocol::encode_fetch_response(header.correlation_id, topic_name, partition_index, records)?;
                            let res_len = res_buf.len() as u32;
                            socket.write_all(&res_len.to_be_bytes()).await?;
                            socket.write_all(&res_buf).await?;
                        }
                    }
                }
            }
            _ => break,
        }
    }
    Ok(())
}

#[tokio::test]
async fn test_server_full_roundtrip() {
    let registry = Arc::new(BrokerRegistry::new());
    let test_dir = PathBuf::from("target/test_storage/test_server_roundtrip");
    let _ = tokio::fs::remove_dir_all(&test_dir).await;
    
    // Setup partition
    let (tx, rx) = mpsc::channel(100);
    let actor = PartitionActor::new(rx, test_dir).await.unwrap();
    tokio::spawn(async move {
        actor.run().await;
    });
    registry.register_partition("test".to_string(), 0, tx).await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    
    let registry_clone = registry.clone();
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        handle_test_connection(socket, registry_clone).await.unwrap();
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    
    // 1. ApiVersions
    {
        let mut header = RequestHeader::default();
        header.request_api_key = 18;
        header.request_api_version = 3;
        header.correlation_id = 1;
        let request = ApiVersionsRequest::default();
        let mut req_buf = BytesMut::new();
        header.encode(&mut req_buf, 2).unwrap();
        request.encode(&mut req_buf, 3).unwrap();
        let req_len = req_buf.len() as u32;
        stream.write_all(&req_len.to_be_bytes()).await.unwrap();
        stream.write_all(&req_buf).await.unwrap();

        let mut res_len_buf = [0u8; 4];
        stream.read_exact(&mut res_len_buf).await.unwrap();
        let res_len = u32::from_be_bytes(res_len_buf) as usize;
        let mut res_body_buf = vec![0u8; res_len];
        stream.read_exact(&mut res_body_buf).await.unwrap();
        let mut res_body_mut = BytesMut::from(&res_body_buf[..]);
        let res_header = ResponseHeader::decode(&mut res_body_mut, 0).unwrap();
        assert_eq!(res_header.correlation_id, 1);
    }

    // 2. Produce
    {
        let mut header = RequestHeader::default();
        header.request_api_key = 0; // Produce
        header.request_api_version = 9;
        header.correlation_id = 2;
        
        let mut request = ProduceRequest::default();
        let mut topic = kafka_protocol::messages::produce_request::TopicProduceData::default();
        topic.name = StrBytes::from("test").into();
        let mut partition = kafka_protocol::messages::produce_request::PartitionProduceData::default();
        partition.index = 0;
        partition.records = Some(Bytes::from("hello kafka"));
        topic.partition_data.push(partition);
        request.topic_data.push(topic);

        let mut req_buf = BytesMut::new();
        header.encode(&mut req_buf, 2).unwrap();
        request.encode(&mut req_buf, 9).unwrap();
        let req_len = req_buf.len() as u32;
        stream.write_all(&req_len.to_be_bytes()).await.unwrap();
        stream.write_all(&req_buf).await.unwrap();

        let mut res_len_buf = [0u8; 4];
        stream.read_exact(&mut res_len_buf).await.unwrap();
        let res_len = u32::from_be_bytes(res_len_buf) as usize;
        let mut res_body_buf = vec![0u8; res_len];
        stream.read_exact(&mut res_body_buf).await.unwrap();
        let mut res_body_mut = BytesMut::from(&res_body_buf[..]);
        let res_header = ResponseHeader::decode(&mut res_body_mut, 0).unwrap();
        assert_eq!(res_header.correlation_id, 2);
    }

    // 3. Fetch
    {
        let mut header = RequestHeader::default();
        header.request_api_key = 1; // Fetch
        header.request_api_version = 11;
        header.correlation_id = 3;
        
        let mut request = FetchRequest::default();
        let mut topic = kafka_protocol::messages::fetch_request::FetchTopic::default();
        topic.topic = StrBytes::from("test").into();
        let mut partition = kafka_protocol::messages::fetch_request::FetchPartition::default();
        partition.partition = 0;
        partition.fetch_offset = 0;
        partition.partition_max_bytes = 1024;
        topic.partitions.push(partition);
        request.topics.push(topic);

        let mut req_buf = BytesMut::new();
        header.encode(&mut req_buf, 2).unwrap();
        request.encode(&mut req_buf, 11).unwrap();
        let req_len = req_buf.len() as u32;
        stream.write_all(&req_len.to_be_bytes()).await.unwrap();
        stream.write_all(&req_buf).await.unwrap();

        let mut res_len_buf = [0u8; 4];
        stream.read_exact(&mut res_len_buf).await.unwrap();
        let res_len = u32::from_be_bytes(res_len_buf) as usize;
        let mut res_body_buf = vec![0u8; res_len];
        stream.read_exact(&mut res_body_buf).await.unwrap();
        let mut res_body_mut = BytesMut::from(&res_body_buf[..]);
        let _res_header = ResponseHeader::decode(&mut res_body_mut, 0).unwrap();
        let res = FetchResponse::decode(&mut res_body_mut, 11).unwrap();
        
        let topic_res = res.responses.first().unwrap();
        let part_res = topic_res.partitions.first().unwrap();
        assert_eq!(part_res.records.as_ref().unwrap(), &Bytes::from("hello kafka"));
    }
}
