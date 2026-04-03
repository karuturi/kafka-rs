use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse, MetadataRequest, MetadataResponse, RequestHeader, ResponseHeader};
use kafka_protocol::messages::metadata_response::{MetadataResponseBroker, MetadataResponseTopic, MetadataResponsePartition};
use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
use bytes::{BytesMut};
use anyhow::Result;

pub fn decode_request_header(buf: &mut BytesMut) -> Result<RequestHeader> {
    // RequestHeader v2 is commonly used in Kafka 3.0
    RequestHeader::decode(buf, 2).map_err(|e| anyhow::anyhow!("Failed to decode request header: {:?}", e))
}

pub fn decode_apiversions_request(buf: &mut BytesMut) -> Result<ApiVersionsRequest> {
    ApiVersionsRequest::decode(buf, 3).map_err(|e| anyhow::anyhow!("Failed to decode ApiVersionsRequest: {:?}", e))
}

pub fn encode_apiversions_response(correlation_id: i32) -> Result<BytesMut> {
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;

    let mut response = ApiVersionsResponse::default();
    response.error_code = 0;
    
    let mut res_buf = BytesMut::new();
    header.encode(&mut res_buf, 0).map_err(|e| anyhow::anyhow!("Failed to encode response header: {:?}", e))?;
    response.encode(&mut res_buf, 3).map_err(|e| anyhow::anyhow!("Failed to encode ApiVersionsResponse: {:?}", e))?;
    
    Ok(res_buf)
}

pub fn decode_metadata_request(buf: &mut BytesMut) -> Result<MetadataRequest> {
    MetadataRequest::decode(buf, 9).map_err(|e| anyhow::anyhow!("Failed to decode MetadataRequest: {:?}", e))
}

pub fn encode_metadata_response(correlation_id: i32, topics: Vec<String>) -> Result<BytesMut> {
    let mut header = ResponseHeader::default();
    header.correlation_id = correlation_id;

    let mut response = MetadataResponse::default();
    
    // Minimal broker info (node 1)
    let mut broker = MetadataResponseBroker::default();
    broker.node_id = 1.into();
    broker.host = "127.0.0.1".into();
    broker.port = 9092;
    response.brokers.insert(1 as usize, broker);

    for topic_name in topics {
        let mut topic = MetadataResponseTopic::default();
        topic.name = Some(StrBytes::from(topic_name).into());
        
        let mut partition = MetadataResponsePartition::default();
        partition.partition_index = 0.into();
        partition.leader_id = 1.into();
        partition.replica_nodes = vec![1.into()];
        partition.isr_nodes = vec![1.into()];
        
        topic.partitions.push(partition);
        response.topics.push(topic);
    }

    let mut res_buf = BytesMut::new();
    header.encode(&mut res_buf, 0).map_err(|e| anyhow::anyhow!("Failed to encode response header: {:?}", e))?;
    response.encode(&mut res_buf, 9).map_err(|e| anyhow::anyhow!("Failed to encode MetadataResponse: {:?}", e))?;
    
    Ok(res_buf)
}
