use {
    crate::{
        S3Config,
        object_store_api::{ObjectStore, ObjectStoreExt as _, PutPayload, path::Path},
    },
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::{TcpListener, TcpStream},
    },
};

pub(crate) async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut chunk = [0; 1024];
        let read = socket.read(&mut chunk).await.unwrap();
        assert2::assert!(read > 0, "client closed before completing HTTP headers");
        request.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(request).unwrap()
}

pub(crate) async fn accept_http_request(listener: TcpListener) -> TcpStream {
    let (mut socket, _) = listener.accept().await.expect("accept HTTP request");
    read_request(&mut socket).await;
    socket
}

/// Bind a local HTTP test endpoint without starting its response task.
pub(crate) async fn http_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    (listener, endpoint)
}

pub(crate) async fn respond(socket: &mut TcpStream, status: &str, body: &str) {
    socket
        .write_all(
            format!(
                "HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
}

pub(crate) fn s3_config(bucket: &str, endpoint: String) -> S3Config {
    S3Config {
        bucket: bucket.into(),
        region: "us-east-1".into(),
        endpoint: Some(endpoint),
        access_key_id: Some("key".into()),
        secret_access_key: Some("secret".into()),
        allow_http: true,
        ..Default::default()
    }
}

/// Serve canned HTTP pages, preserving request and optional `SigV4` checks.
pub(crate) async fn serve_http_pages(
    pages: Vec<(String, &'static str, &'static str)>,
    signed: bool,
) -> (String, tokio::task::JoinHandle<()>) {
    let (listener, endpoint) = http_listener().await;
    let server = tokio::spawn(async move {
        for (expected_request, status, body) in pages {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert2::check!(request.starts_with(&expected_request));
            if signed {
                assert2::check!(
                    request
                        .to_ascii_lowercase()
                        .contains("authorization: aws4-hmac-sha256")
                );
            }
            respond(&mut socket, status, body).await;
        }
    });
    (endpoint, server)
}

pub(crate) async fn round_trip(
    store: &(impl ObjectStore + ?Sized),
    path: &Path,
    bytes: &[u8],
) -> bytes::Bytes {
    store
        .put(path, PutPayload::from(bytes.to_vec()))
        .await
        .unwrap();
    store.get(path).await.unwrap().bytes().await.unwrap()
}
