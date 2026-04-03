use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt, AsyncBufReadExt, BufReader};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse, RequestHeader, ResponseHeader, produce_request::{TopicProduceData, PartitionProduceData}};
use kafka_protocol::protocol::{Encodable, Decodable, StrBytes};
use bytes::{BytesMut, Bytes};
use anyhow::Result;
use std::env;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: echo \"message\" | {} <topic>", args[0]);
        std::process::exit(1);
    }
    let topic_name = &args[1];

    let mut stdin_reader = BufReader::new(tokio::io::stdin());
    let mut message = String::new();
    stdin_reader.read_line(&mut message).await?;
    let message = message.trim();

    if message.is_empty() {
        eprintln!("No message received from stdin");
        return Ok(());
    }

    let mut stream = TcpStream::connect("127.0.0.1:9092").await?;
    println!("Connected to 127.0.0.1:9092");

    let mut header = RequestHeader::default();
    header.request_api_key = 0; // Produce
    header.request_api_version = 9;
    header.correlation_id = 1;
    header.client_id = Some(StrBytes::from("example-producer").into());

    let mut request = ProduceRequest::default();
    let mut topic = TopicProduceData::default();
    topic.name = StrBytes::from(topic_name.clone()).into();
    
    let mut partition = PartitionProduceData::default();
    partition.index = 0;
    partition.records = Some(Bytes::from(message.to_string()));
    
    topic.partition_data.push(partition);
    request.topic_data.push(topic);

    let mut req_buf = BytesMut::new();
    header.encode(&mut req_buf, 2)?;
    request.encode(&mut req_buf, 9)?;

    let req_len = req_buf.len() as u32;
    stream.write_all(&req_len.to_be_bytes()).await?;
    stream.write_all(&req_buf).await?;
    println!("Sent ProduceRequest for '{}:0' with message: '{}'", topic_name, message);

    let mut res_len_buf = [0u8; 4];
    stream.read_exact(&mut res_len_buf).await?;
    let res_len = u32::from_be_bytes(res_len_buf) as usize;
    
    let mut res_body_buf = vec![0u8; res_len];
    stream.read_exact(&mut res_body_buf).await?;
    let mut res_body_mut = BytesMut::from(&res_body_buf[..]);
    
    let _res_header = ResponseHeader::decode(&mut res_body_mut, 0)?;
    let response = ProduceResponse::decode(&mut res_body_mut, 9)?;
    
    if let Some(topic_res) = response.responses.first() {
        if let Some(part_res) = topic_res.partition_responses.first() {
            println!("Received ProduceResponse: Offset={}", part_res.base_offset);
        }
    }
    
    Ok(())
}
