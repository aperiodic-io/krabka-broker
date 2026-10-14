use {
    super::*,
    assert2::{assert, check},
    krabka_protocol::owned::{
        api_versions_response::ApiVersion,
        metadata_request::MetadataRequest,
        metadata_response::{MetadataResponse, MetadataResponseBroker},
    },
    krabka_units::secs,
    tokio::io::{DuplexStream, duplex},
};

/// A request as the fake leader read it.
#[derive(Debug, PartialEq)]
struct Received {
    api_key: i16,
    version: i16,
    correlation: i32,
    client_id: String,
}

/// The leader's end of a link: reads request frames and writes responses.
struct FakeLeader(DuplexStream);

impl FakeLeader {
    /// Reads one request frame and its header. A header with a tagged-fields
    /// byte is recognised by `flexible`, which the caller knows from the API.
    async fn read(&mut self, flexible: bool) -> Received {
        let length = usize::try_from(self.0.read_i32().await.expect("length")).expect("length");
        let mut frame = vec![0_u8; length];
        self.0.read_exact(&mut frame).await.expect("frame");
        let mut frame = Bytes::from(frame);
        let api_key = frame.get_i16();
        let version = frame.get_i16();
        let correlation = frame.get_i32();
        let client_id_length = usize::try_from(frame.get_i16()).expect("client id");
        let client_id = String::from_utf8(frame.split_to(client_id_length).to_vec()).expect("utf8");
        if flexible {
            check!(frame.get_u8() == 0, "an empty tagged-fields byte");
        }
        Received {
            api_key,
            version,
            correlation,
            client_id,
        }
    }

    /// Writes a response frame for `correlation`: the header, a tagged-fields
    /// byte when `flexible_header`, then `body`.
    async fn answer(&mut self, correlation: i32, flexible_header: bool, body: &[u8]) {
        let mut frame = BytesMut::new();
        frame.put_i32(correlation);
        if flexible_header {
            frame.put_u8(0);
        }
        frame.put_slice(body);
        self.0
            .write_i32(i32::try_from(frame.len()).expect("length"))
            .await
            .expect("length");
        self.0.write_all(&frame).await.expect("frame");
    }

    /// Answers the link's first `ApiVersions` with `api_keys`.
    async fn advertise(&mut self, api_keys: Vec<ApiVersion>) {
        let request = self.read(true).await;
        let response = ApiVersionsResponse {
            api_keys,
            ..ApiVersionsResponse::default()
        };
        self.answer(
            request.correlation,
            false,
            &encoded(&response, request.version),
        )
        .await;
    }
}

fn encoded(value: &impl Encode, version: i16) -> Vec<u8> {
    let mut buf = Vec::new();
    value.encode(&mut buf, version).expect("encode");
    buf
}

fn api(api_key: i16, max_version: i16) -> ApiVersion {
    ApiVersion {
        api_key,
        min_version: 0,
        max_version,
        ..ApiVersion::default()
    }
}

fn settings() -> LinkSettings {
    LinkSettings {
        client_id: "follower-2".to_owned(),
        frame_max: 1024,
        request_timeout: secs(30),
    }
}

/// A link to a fake leader that advertised `api_keys`.
async fn opened(api_keys: Vec<ApiVersion>) -> (LeaderLink<DuplexStream>, FakeLeader) {
    let (follower, leader) = duplex(64 * 1024);
    let mut leader = FakeLeader(leader);
    let (link, ()) = tokio::join!(
        LeaderLink::open(follower, settings()),
        leader.advertise(api_keys)
    );
    (link.expect("open"), leader)
}

#[tokio::test]
async fn an_unsupported_api_versions_version_is_retried_at_the_leaders_highest() {
    let (follower, leader) = duplex(64 * 1024);
    let mut leader = FakeLeader(leader);
    let script = async {
        let first = leader.read(true).await;
        let refusal = ApiVersionsResponse {
            error_code: UNSUPPORTED_VERSION,
            api_keys: vec![api(ApiVersionsRequest::API_KEY, 2)],
            ..ApiVersionsResponse::default()
        };
        leader
            .answer(first.correlation, false, &encoded(&refusal, 0))
            .await;
        let second = leader.read(false).await;
        let accepted = ApiVersionsResponse {
            api_keys: vec![api(ApiVersionsRequest::API_KEY, 2)],
            ..ApiVersionsResponse::default()
        };
        leader
            .answer(second.correlation, false, &encoded(&accepted, 2))
            .await;
        (first, second)
    };

    let (link, (first, second)) = tokio::join!(LeaderLink::open(follower, settings()), script);

    assert!(link.is_ok());
    let client_id = "follower-2".to_owned();
    check!(
        [first, second]
            == [
                Received {
                    api_key: ApiVersionsRequest::API_KEY,
                    version: ApiVersionsRequest::MAX_VERSION,
                    correlation: 0,
                    client_id: client_id.clone(),
                },
                Received {
                    api_key: ApiVersionsRequest::API_KEY,
                    version: 2,
                    correlation: 1,
                    client_id,
                },
            ]
    );
}

#[tokio::test]
async fn send_frames_a_request_and_decodes_a_flexible_response() {
    let (mut link, mut leader) = opened(vec![api(MetadataRequest::API_KEY, 12)]).await;
    let expected = MetadataResponse {
        brokers: vec![MetadataResponseBroker {
            node_id: 1,
            host: "leader".to_owned(),
            port: 9092,
            ..MetadataResponseBroker::default()
        }],
        controller_id: 1,
        ..MetadataResponse::default()
    };
    let script = async {
        let request = leader.read(true).await;
        leader
            .answer(request.correlation, true, &encoded(&expected, 12))
            .await;
        request
    };

    let (response, request) = tokio::join!(link.send(MetadataRequest::default()), script);

    check!(response.expect("response") == expected);
    check!(
        request
            == Received {
                api_key: MetadataRequest::API_KEY,
                version: 12,
                correlation: 1,
                client_id: "follower-2".to_owned(),
            }
    );
}

#[tokio::test]
async fn a_response_for_another_correlation_id_fails_the_request() {
    let (mut link, mut leader) = opened(vec![api(MetadataRequest::API_KEY, 12)]).await;
    let script = async {
        let request = leader.read(true).await;
        let body = encoded(&MetadataResponse::default(), 12);
        leader.answer(request.correlation + 7, true, &body).await;
    };

    let (response, ()) = tokio::join!(link.send(MetadataRequest::default()), script);

    assert!(let Err(ClientError::Io(error)) = response);
    check!(error.kind() == std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn a_response_over_the_frame_limit_fails_the_request() {
    let (mut link, mut leader) = opened(vec![api(MetadataRequest::API_KEY, 12)]).await;
    let script = async {
        let request = leader.read(true).await;
        leader.answer(request.correlation, true, &[0; 2048]).await;
    };

    let (response, ()) = tokio::join!(link.send(MetadataRequest::default()), script);

    assert!(let Err(ClientError::Io(error)) = response);
    check!(error.kind() == std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn a_leader_that_closes_mid_response_disconnects_the_link() {
    let (mut link, mut leader) = opened(vec![api(MetadataRequest::API_KEY, 12)]).await;
    let script = async {
        leader.read(true).await;
        leader.0.write_i32(100).await.expect("length");
        leader.0.write_all(&[0; 10]).await.expect("part of a frame");
        drop(leader);
    };

    let (response, ()) = tokio::join!(link.send(MetadataRequest::default()), script);

    assert!(let Err(ClientError::Disconnected) = response);
}

#[tokio::test(start_paused = true)]
async fn a_leader_that_never_answers_times_the_request_out() {
    let (mut link, mut leader) = opened(vec![api(MetadataRequest::API_KEY, 12)]).await;
    let script = async {
        leader.read(true).await;
        leader
    };

    let (response, _leader) = tokio::join!(link.send(MetadataRequest::default()), script);

    assert!(let Err(ClientError::Timeout(after)) = response);
    check!(after == secs(30));
}
