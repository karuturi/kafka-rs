use tokio::net::TcpListener;
use anyhow::Result;

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

async fn handle_connection(_socket: tokio::net::TcpStream) -> Result<()> {
    // Phase 1.1: Just log the connection
    Ok(())
}
