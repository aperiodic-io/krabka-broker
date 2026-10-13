//! The two controller mutations the quota is measured against: `CreateTopics`
//! (`api_key=19`) and `DeleteTopics` (`api_key=20`), each driven over an
//! authenticated connection.
//!
//! Both drivers return the pair the assertions care about — the response
//! `throttle_time_ms` and the first per-topic error code — so a test can tell a
//! quota rejection apart from an authorization failure.

use {
    crate::{
        CLIENT_ID,
        kafka_wire::{self, PlainAuthenticationSetup},
        support::{
            topics::{creatable_topic, create_topic_request_with_setup},
            wire::HeaderEncoding,
        },
    },
    bytes::BytesMut,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            create_topics_response::CreateTopicsResponse,
            delete_topics_request::DeleteTopicsRequest,
            delete_topics_response::DeleteTopicsResponse,
        },
    },
    std::net::SocketAddr,
};

/// The throttle time and the first topic result's error code of a topic
/// mutation response.
pub(crate) struct TopicMutationResult {
    pub(crate) throttle_time_ms: i32,
    pub(crate) error_code: i16,
}

/// Drive `CreateTopics` (`api_key=19`) over a SASL/PLAIN connection.
/// Returns the throttle time and the first topic result's error code.
pub(crate) async fn drive_create_topics_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
    partitions: i32,
) -> TopicMutationResult {
    const VERSION: i16 = 7; // MAX_VERSION; flexible (>= 5)

    let req = create_topic_request_with_setup()
        .topic(
            creatable_topic()
                .name(topic.to_owned())
                .partitions(partitions)
                .call(),
        )
        .timeout(30_000)
        .call();

    let mut stream = kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (pass.as_bytes()).to_vec(),
    })
    .await
    .expect("SASL authenticate for CreateTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode CreateTopics");
    let resp_bytes = kafka_wire::round_trip()
        .stream(&mut stream)
        .api_key(19)
        .version(VERSION)
        .client_id(CLIENT_ID)
        .header(HeaderEncoding::Flexible)
        .body(&body)
        .call()
        .await
        .expect("CreateTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        CreateTopicsResponse::decode(&mut cur, VERSION).expect("decode CreateTopicsResponse");

    let err_code = resp.topics.first().map_or(-1, |t| t.error_code);
    TopicMutationResult {
        throttle_time_ms: resp.throttle_time_ms,
        error_code: err_code,
    }
}

/// Drive `DeleteTopics` (`api_key=20`) over a SASL/PLAIN connection.
/// Returns the throttle time and the first topic result's error code.
pub(crate) async fn drive_delete_topics_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    topic: &str,
) -> TopicMutationResult {
    // Use version 3 (flexible=4+, topic_names field for versions 0-5).
    // Flexible starts at version 4; use version 4 to get throttle_time_ms (v1+)
    // and flexible encoding, while using topic_names (not the v6+ topics field).
    const VERSION: i16 = 4;

    let req = DeleteTopicsRequest {
        topic_names: vec![topic.to_string()],
        timeout_ms: 30_000,
        ..Default::default()
    };

    let auth_setup = PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: pass.as_bytes().to_vec(),
    };
    let mut stream = kafka_wire::sasl_plain_authenticate(addr, &auth_setup)
        .await
        .expect("SASL authenticate for DeleteTopics");
    let mut body = BytesMut::new();
    req.encode(&mut body, VERSION).expect("encode DeleteTopics");
    let resp_bytes = kafka_wire::round_trip()
        .stream(&mut stream)
        .api_key(20)
        .version(VERSION)
        .client_id(CLIENT_ID)
        .header(HeaderEncoding::Flexible)
        .body(&body)
        .call()
        .await
        .expect("DeleteTopics round-trip");
    let mut cur: &[u8] = &resp_bytes;
    let resp =
        DeleteTopicsResponse::decode(&mut cur, VERSION).expect("decode DeleteTopicsResponse");

    let err_code = resp.responses.first().map_or(-1, |r| r.error_code);
    TopicMutationResult {
        throttle_time_ms: resp.throttle_time_ms,
        error_code: err_code,
    }
}
