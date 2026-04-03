use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse, RequestHeader, ResponseHeader};
use kafka_protocol::protocol::{Decodable, Encodable};
use bytes::{BytesMut};

#[tokio::test]
async fn test_server_responds_with_apiversions() {
    use tokio::net::TcpListener;
    
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    
    tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        
        // Read length
        let mut len_buf = [0u8; 4];
        socket.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_be_bytes(len_buf) as usize;
        
        // Read body
        let mut body_buf = vec![0u8; len];
        socket.read_exact(&mut body_buf).await.unwrap();
        let mut body_mut = BytesMut::from(&body_buf[..]);
        
        // Decode header (v2)
        let header = RequestHeader::decode(&mut body_mut, 2).unwrap();
        // Decode request (v3)
        let _request = ApiVersionsRequest::decode(&mut body_mut, 3).unwrap();
        
        // Encode response
        let mut res_header = ResponseHeader::default();
        res_header.correlation_id = header.correlation_id;
        
        let mut response = ApiVersionsResponse::default();
        response.error_code = 0;
        
        let mut res_buf = BytesMut::new();
        res_header.encode(&mut res_buf, 0).unwrap();
        response.encode(&mut res_buf, 3).unwrap();
        
        let res_len = res_buf.len() as u32;
        socket.write_all(&res_len.to_be_bytes()).await.unwrap();
        socket.write_all(&res_buf).await.unwrap();
    });

    let mut stream = TcpStream::connect(addr).await.unwrap();
    
    let mut header = RequestHeader::default();
    header.request_api_key = 18; // ApiVersions
    header.request_api_version = 3;
    header.correlation_id = 1;
    header.client_id = Some("test-client".into());
    
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
    let apiversions_res = ApiVersionsResponse::decode(&mut res_body_mut, 3).unwrap();
    
    assert_eq!(res_header.correlation_id, 1);
    assert_eq!(apiversions_res.error_code, 0);
}
