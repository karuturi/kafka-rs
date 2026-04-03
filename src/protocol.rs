use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable};
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
    
    // In a real implementation, we'd populate api_keys here
    // For now, minimal to pass the handshake

    let mut res_buf = BytesMut::new();
    header.encode(&mut res_buf, 0).map_err(|e| anyhow::anyhow!("Failed to encode response header: {:?}", e))?;
    response.encode(&mut res_buf, 3).map_err(|e| anyhow::anyhow!("Failed to encode ApiVersionsResponse: {:?}", e))?;
    
    Ok(res_buf)
}
