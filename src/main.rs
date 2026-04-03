mod protocol;

use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::Result;
use bytes::BytesMut;

#[tokio::main]
async fn main() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:9092").await?;
    println!("Kafka-RS listening on 127.0.0.1:9092");

    loop {
        let (socket, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(e) = handle_connection(socket).await {
                eprintln!("Connection error: {}", e);
            }
        });
    }
}

async fn handle_connection(mut socket: tokio::net::TcpStream) -> Result<()> {
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
            _ => {
                eprintln!("Unsupported API key: {}", header.request_api_key);
                break;
            }
        }
    }
    Ok(())
}
