use tokio::net::TcpStream;
use std::time::Duration;

#[tokio::test]
async fn test_server_receives_kafka_request_length() {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    
    let server_handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        use tokio::io::AsyncReadExt;
        socket.read_exact(&mut len_buf).await.unwrap();
        u32::from_be_bytes(len_buf)
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    let request_len: u32 = 12;
    stream.write_all(&request_len.to_be_bytes()).await.unwrap();

    let received_len = server_handle.await.unwrap();
    assert_eq!(received_len, request_len);
}
