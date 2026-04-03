use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kafka_protocol::messages::{FetchRequest, RequestHeader, ResponseHeader, FetchResponse, fetch_request::{FetchTopic, FetchPartition}};
use kafka_protocol::protocol::{Encodable, Decodable, StrBytes};
use bytes::{BytesMut};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("Connected to 127.0.0.1:9092");

    let mut header = RequestHeader::default();
    header.request_api_key = 1; // Fetch
    header.request_api_version = 11;
    header.correlation_id = 1;
    header.client_id = Some(StrBytes::from("example-consumer").into());

    let mut request = FetchRequest::default();
    let mut topic = FetchTopic::default();
    topic.topic = StrBytes::from("test").into();
    
    let mut partition = FetchPartition::default();
    partition.partition = 0;
    partition.fetch_offset = 0;
    partition.partition_max_bytes = 1024;
    
    topic.partitions.push(partition);
    request.topics.push(topic);

    let mut req_buf = BytesMut::new();
    header.encode(&mut req_buf, 2)?;
    request.encode(&mut req_buf, 11)?;

    let req_len = req_buf.len() as u32;
    stream.write_all(&req_len.to_be_bytes()).await?;
    stream.write_all(&req_buf).await?;
    println!("Sent FetchRequest for 'test:0' from offset 0");

    let mut res_len_buf = [0u8; 4];
    stream.read_exact(&mut res_len_buf).await?;
    let res_len = u32::from_be_bytes(res_len_buf) as usize;
    
    let mut res_body_buf = vec![0u8; res_len];
    stream.read_exact(&mut res_body_buf).await?;
    let mut res_body_mut = BytesMut::from(&res_body_buf[..]);
    
    let _res_header = ResponseHeader::decode(&mut res_body_mut, 0)?;
    let response = FetchResponse::decode(&mut res_body_mut, 11)?;
    println!("Received FetchResponse");

    if let Some(topic_res) = response.responses.first() {
        if let Some(part_res) = topic_res.partitions.first() {
            if let Some(records) = &part_res.records {
                println!("Message data: {:?}", String::from_utf8_lossy(records));
            } else {
                println!("No records found.");
            }
        }
    }
    
    Ok(())
}
