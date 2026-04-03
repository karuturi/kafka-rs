use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kafka_protocol::messages::{ProduceRequest, RequestHeader, ResponseHeader, produce_request::{TopicProduceData, PartitionProduceData}};
use kafka_protocol::protocol::{Encodable, Decodable, StrBytes};
use bytes::{BytesMut, Bytes};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("Connected to 127.0.0.1:9092");

    let mut header = RequestHeader::default();
    header.request_api_key = 0; // Produce
    header.request_api_version = 9;
    header.correlation_id = 1;
    header.client_id = Some(StrBytes::from("example-producer").into());

    let mut request = ProduceRequest::default();
    let mut topic = TopicProduceData::default();
    topic.name = StrBytes::from("test").into();
    
    let mut partition = PartitionProduceData::default();
    partition.index = 0;
    partition.records = Some(Bytes::from("Hello Kafka-RS from the Rust example!"));
    
    topic.partition_data.push(partition);
    request.topic_data.push(topic);

    let mut req_buf = BytesMut::new();
    header.encode(&mut req_buf, 2)?;
    request.encode(&mut req_buf, 9)?;

    let req_len = req_buf.len() as u32;
    stream.write_all(&req_len.to_be_bytes()).await?;
    stream.write_all(&req_buf).await?;
    println!("Sent ProduceRequest for 'test:0'");

    let mut res_len_buf = [0u8; 4];
    stream.read_exact(&mut res_len_buf).await?;
    let res_len = u32::from_be_bytes(res_len_buf) as usize;
    
    let mut res_body_buf = vec![0u8; res_len];
    stream.read_exact(&mut res_body_buf).await?;
    let mut res_body_mut = BytesMut::from(&res_body_buf[..]);
    
    let _res_header = ResponseHeader::decode(&mut res_body_mut, 0)?;
    println!("Received ProduceResponse");
    
    Ok(())
}
