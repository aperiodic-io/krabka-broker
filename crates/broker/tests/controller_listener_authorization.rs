//! Per-request authorization on the controller listener (#684).
//!
//! Kafka's `ControllerApis` authorizes every request against the principal of
//! the connection, with the cluster operation that the api needs. A denial is
//! the error response of that api, and the connection stays open. On
//! `PLAINTEXT` the principal is `ANONYMOUS`. On `SASL_PLAINTEXT` it is the
//! SASL principal, and a principal without `ClusterAction` can still run the
//! Admin apis that its own grants allow.
//!
//! Each case talks to a live one-node cluster over a raw controller-listener
//! socket, with a `SimpleAclAuthorizer` whose only super user is the node's
//! own inter-broker principal.

mod support;

use {
    crate::support::{
        acl::cluster_acl_record,
        controller_listener::*,
        quorum::describe_quorum_request as support_describe_quorum_request,
        start_with_bound_listeners,
        tls::{PinnedCertVerifier, fixture_signature_schemes},
        topics::{creatable_topic, create_topic_request},
    },
    assert2::{assert, check},
    krabka_metadata::{MetadataRecord, TopicRecord},
    krabka_protocol::owned::{
        broker_heartbeat_request::{self, BrokerHeartbeatRequest},
        broker_heartbeat_response::BrokerHeartbeatResponse,
        create_topics_request::{self},
        create_topics_response::{CreatableTopicResult, CreateTopicsResponse},
        describe_cluster_request::{self, DescribeClusterRequest},
        describe_cluster_response::DescribeClusterResponse,
        describe_quorum_request::{self},
        describe_quorum_response::DescribeQuorumResponse,
        sasl_authenticate_request::SaslAuthenticateRequest,
        sasl_authenticate_response::SaslAuthenticateResponse,
        sasl_handshake_request::SaslHandshakeRequest,
        sasl_handshake_response::SaslHandshakeResponse,
        vote_request::{self, VoteRequest},
        vote_response::VoteResponse,
    },
    krabka_raft::{
        API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE, KrabkaMetadataFetchRequest,
        KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest, KrabkaSubmitChangeResponse,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    tokio::{
        io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _},
        net::TcpStream,
    },
    tokio_rustls::rustls::{
        ClientConfig,
        pki_types::{CertificateDer, PrivateKeyDer, ServerName, pem::PemObject as _},
    },
};

const CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
const MESSAGE: &str = "Cluster authorization failed.";

/// A PLAIN user and its password.
struct Credential {
    user: &'static str,
    password: &'static str,
}

/// The node's own inter-broker principal, and its only super user.
const NODE: Credential = Credential {
    user: "node",
    password: "node-secret",
};

/// A running broker and the directory that backs it.
struct RunningBroker {
    broker: BrokerHandle,
    dir: tempfile::TempDir,
}

async fn start(protocol: ListenerProtocol, users: &[Credential]) -> RunningBroker {
    start_with(protocol, users, |_| {}).await
}

async fn start_with(
    protocol: ListenerProtocol,
    users: &[Credential],
    adjust: impl FnOnce(&mut BrokerConfig),
) -> RunningBroker {
    let (broker, dir) = start_with_bound_listeners(|config| {
        config.controller_listener_protocol = protocol;
        config.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
        for Credential { user, password } in std::iter::once(&NODE).chain(users) {
            config
                .plain_credentials
                .insert((*user).to_owned(), (*password).to_owned());
        }
        config.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: NODE.user.to_owned(),
            password: NODE.password.to_owned(),
        });
        config.super_users = std::iter::once(NODE.user.to_owned()).collect();
        config.authorizer =
            std::sync::Arc::new(SimpleAclAuthorizer::new(config.super_users.clone()));
        adjust(config);
    })
    .await;
    RunningBroker { broker, dir }
}

async fn allow(broker: &BrokerHandle, principal: &str, operation: AclOperation) {
    broker
        .submit_metadata_record_for_test(
            cluster_acl_record()
                .principal(principal)
                .operation(operation)
                .call(),
        )
        .await
        .expect("seed ACL");
}

fn encode<T: Encode>(message: &T, version: i16) -> Bytes {
    let mut body = BytesMut::new();
    message.encode(&mut body, version).expect("encode");
    body.freeze()
}

/// Sends one request and returns the response body after its header. The
/// request and response headers are flexible when `flexible` is set.
async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    api_key: i16,
    version: i16,
    flexible: bool,
    body: &[u8],
) -> Bytes {
    let mut frame = BytesMut::new();
    frame.put_i16(api_key);
    frame.put_i16(version);
    frame.put_i32(77);
    frame.put_i16(-1);
    if flexible {
        frame.put_u8(0);
    }
    frame.put_slice(body);
    stream
        .write_all(&i32::try_from(frame.len()).expect("length").to_be_bytes())
        .await
        .expect("write length");
    stream.write_all(&frame).await.expect("write request");

    let mut length = [0_u8; 4];
    stream
        .read_exact(&mut length)
        .await
        .expect("a response, and the connection stays open");
    let mut response = vec![0_u8; usize::try_from(i32::from_be_bytes(length)).expect("length")];
    stream.read_exact(&mut response).await.expect("read body");
    let header = if flexible { 5 } else { 4 };
    check!(response[..4] == [0, 0, 0, 77], "correlation id");
    Bytes::copy_from_slice(&response[header..])
}

fn decode<T: Decode<'static>>(bytes: &Bytes, version: i16) -> T {
    let mut cursor: &[u8] = bytes;
    let decoded = T::decode(&mut cursor, version).expect("decode response");
    check!(cursor.is_empty(), "the response consumed its body");
    decoded
}

async fn check_quorum_denied<S: AsyncRead + AsyncWrite + Unpin>(stream: &mut S, version: i16) {
    let refused = exchange(
        stream,
        describe_quorum_request::API_KEY,
        version,
        true,
        &describe_quorum(),
    )
    .await;
    check!(
        decode::<DescribeQuorumResponse>(&refused, version)
            == DescribeQuorumResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some(MESSAGE.to_owned()),
                ..Default::default()
            }
    );
}

async fn sasl_plain(stream: &mut TcpStream, user: &str, password: &str) {
    let handshake = exchange(
        stream,
        17,
        1,
        false,
        &encode(
            &SaslHandshakeRequest {
                mechanism: "PLAIN".to_owned(),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            1,
        ),
    )
    .await;
    check!(decode::<SaslHandshakeResponse>(&handshake, 1).error_code == 0);
    let mut auth_bytes = vec![0];
    auth_bytes.extend_from_slice(user.as_bytes());
    auth_bytes.push(0);
    auth_bytes.extend_from_slice(password.as_bytes());
    let authenticate = exchange(
        stream,
        36,
        2,
        true,
        &encode(
            &SaslAuthenticateRequest {
                auth_bytes: Bytes::from(auth_bytes),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            2,
        ),
    )
    .await;
    assert!(decode::<SaslAuthenticateResponse>(&authenticate, 2).error_code == 0);
}

fn submit_change(topic: &str) -> Bytes {
    let records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: topic.into(),
        topic_id: uuid::Uuid::new_v4(),
        partitions: 1,
        replication_factor: 1,
    })];
    let records =
        <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
            &records,
        )
        .expect("wincode");
    let mut body = Vec::new();
    KrabkaSubmitChangeRequest {
        records: Bytes::from(records),
    }
    .encode_v0(&mut body)
    .expect("encode SubmitChange");
    Bytes::from(body)
}

fn submit_change_response(bytes: &Bytes) -> KrabkaSubmitChangeResponse {
    let mut cursor: &[u8] = bytes;
    KrabkaSubmitChangeResponse::decode_v0(&mut cursor).expect("decode SubmitChange")
}

fn describe_quorum() -> Bytes {
    encode(
        &support_describe_quorum_request("__cluster_metadata".to_owned(), vec![0]),
        describe_quorum_request::MAX_VERSION,
    )
}

/// A `PLAINTEXT` controller listener authorizes every request for
/// `ANONYMOUS`.
///
/// Without a grant, `Vote`, the private `SubmitChange` and `BrokerHeartbeat`
/// get `CLUSTER_AUTHORIZATION_FAILED`, and no record is committed. The
/// connection stays open. After an ACL grants `ClusterAction` to
/// `User:ANONYMOUS`, the next `SubmitChange` on the same connection commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_plaintext_controller_listener_authorizes_every_request_for_anonymous() {
    let RunningBroker { broker, dir: _dir } = start(ListenerProtocol::Plaintext, &[]).await;
    let mut stream = TcpStream::connect(broker.controller_addr())
        .await
        .expect("connect controller listener");

    let vote_version = vote_request::MAX_VERSION;
    let vote = exchange(
        &mut stream,
        vote_request::API_KEY,
        vote_version,
        true,
        &encode(&VoteRequest::default(), vote_version),
    )
    .await;
    check!(
        decode::<VoteResponse>(&vote, vote_version)
            == VoteResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            }
    );

    let refused = exchange(
        &mut stream,
        API_KEY_SUBMIT_CHANGE,
        0,
        true,
        &submit_change("anonymous-denied"),
    )
    .await;
    check!(
        submit_change_response(&refused)
            == KrabkaSubmitChangeResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                leader_hint: -1,
                result: Bytes::new(),
            }
    );
    check!(
        broker
            .controller_image_for_test()
            .topic("anonymous-denied")
            .is_none()
    );

    let heartbeat_version = broker_heartbeat_request::MAX_VERSION;
    let heartbeat = exchange(
        &mut stream,
        broker_heartbeat_request::API_KEY,
        heartbeat_version,
        true,
        &encode(
            &BrokerHeartbeatRequest {
                broker_id: 1,
                broker_epoch: -1,
                ..Default::default()
            },
            heartbeat_version,
        ),
    )
    .await;
    check!(
        decode::<BrokerHeartbeatResponse>(&heartbeat, heartbeat_version).error_code
            == CLUSTER_AUTHORIZATION_FAILED
    );

    allow(&broker, "User:ANONYMOUS", AclOperation::ClusterAction).await;
    let applied = exchange(
        &mut stream,
        API_KEY_SUBMIT_CHANGE,
        0,
        true,
        &submit_change("anonymous-allowed"),
    )
    .await;
    check!(submit_change_response(&applied).error_code == 0);
    check!(
        broker
            .controller_image_for_test()
            .topic("anonymous-allowed")
            .is_some()
    );

    broker.shutdown().await;
}

/// A `SASL_PLAINTEXT` controller listener keeps a connection whose principal
/// lacks `ClusterAction`, and it authorizes each request for that principal.
///
/// - `creator` holds `Create` on the cluster: `CreateTopics` creates the
///   topic. Before #684 the handshake dropped the connection.
/// - `replicator` holds `ClusterAction`: `MetadataFetch` is served, and
///   `DescribeQuorum`, which needs `Describe`, is refused.
/// - `reader` holds `Describe`: `DescribeQuorum` is served, and
///   `DescribeCluster`, which needs `Alter` on a controller, is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sasl_controller_listener_authorizes_each_request_for_its_principal() {
    const CREATOR: Credential = Credential {
        user: "creator",
        password: "creator-secret",
    };
    const REPLICATOR: Credential = Credential {
        user: "replicator",
        password: "replicator-secret",
    };
    const READER: Credential = Credential {
        user: "reader",
        password: "reader-secret",
    };

    let RunningBroker { broker, dir: _dir } = start(ListenerProtocol::SaslPlaintext, &[
        CREATOR, REPLICATOR, READER,
    ])
    .await;
    allow(&broker, "User:creator", AclOperation::Create).await;
    allow(&broker, "User:replicator", AclOperation::ClusterAction).await;
    allow(&broker, "User:reader", AclOperation::Describe).await;

    let mut creator = TcpStream::connect(broker.controller_addr())
        .await
        .expect("connect");
    sasl_plain(&mut creator, CREATOR.user, CREATOR.password).await;
    let create_version = create_topics_request::MAX_VERSION;
    let created = exchange(
        &mut creator,
        create_topics_request::API_KEY,
        create_version,
        true,
        &encode(
            &create_topic_request(
                creatable_topic()
                    .name("created-by-creator".to_owned())
                    .call(),
            ),
            create_version,
        ),
    )
    .await;
    let created = decode::<CreateTopicsResponse>(&created, create_version);
    check!(
        created
            .topics
            .iter()
            .map(|topic: &CreatableTopicResult| (topic.name.as_str(), topic.error_code))
            .collect::<Vec<_>>()
            == vec![("created-by-creator", 0)]
    );

    let mut replicator = TcpStream::connect(broker.controller_addr())
        .await
        .expect("connect");
    sasl_plain(&mut replicator, REPLICATOR.user, REPLICATOR.password).await;
    let mut fetch = Vec::new();
    KrabkaMetadataFetchRequest {
        fetch_offset: 0,
        max_bytes: 1024,
        replica_id: -1,
        replica_directory_id: uuid::Uuid::nil(),
    }
    .encode_v0(&mut fetch);
    let fetched = exchange(&mut replicator, API_KEY_METADATA_FETCH, 0, true, &fetch).await;
    let mut cursor: &[u8] = &fetched;
    check!(
        KrabkaMetadataFetchResponse::decode_v0(&mut cursor)
            .expect("decode MetadataFetch")
            .error_code
            == 0
    );
    let quorum_version = describe_quorum_request::MAX_VERSION;
    check_quorum_denied(&mut replicator, quorum_version).await;

    let mut reader = TcpStream::connect(broker.controller_addr())
        .await
        .expect("connect");
    sasl_plain(&mut reader, READER.user, READER.password).await;
    let served = exchange(
        &mut reader,
        describe_quorum_request::API_KEY,
        quorum_version,
        true,
        &describe_quorum(),
    )
    .await;
    check!(decode::<DescribeQuorumResponse>(&served, quorum_version).error_code == 0);
    let cluster_version = describe_cluster_request::MAX_VERSION;
    let refused = exchange(
        &mut reader,
        describe_cluster_request::API_KEY,
        cluster_version,
        true,
        &encode(
            &DescribeClusterRequest {
                endpoint_type: 2,
                ..Default::default()
            },
            cluster_version,
        ),
    )
    .await;
    check!(
        decode::<DescribeClusterResponse>(&refused, cluster_version)
            == DescribeClusterResponse {
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some(MESSAGE.to_owned()),
                ..Default::default()
            }
    );

    broker.shutdown().await;
}

use crate::support::tls::{
    CLIENT_PRINCIPAL, DEV_CERT, DEV_CLIENT_CA, DEV_CLIENT_CERT, DEV_CLIENT_KEY, DEV_KEY,
};

/// An `SSL` controller listener authorizes each request for the principal of
/// the client certificate.
///
/// Before #684 the controller listener took no principal from a certificate
/// and checked nothing on `SSL`. Now `DescribeQuorum` from the certificate
/// principal is refused until an ACL grants `Describe` to that principal, and
/// then it is served on the same connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ssl_controller_listener_authorizes_each_request_for_the_certificate_principal() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let pem_dir = tempfile::TempDir::new().expect("tempdir");
    let write = |name: &str, contents: &str| {
        let path = pem_dir.path().join(name);
        std::fs::write(&path, contents).expect("write fixture");
        path
    };
    let tls = krabka_security::TlsConfig {
        cert_chain_path: write("server.pem", DEV_CERT),
        private_key_path: write("server.key", DEV_KEY),
        trust_roots_path: None,
        client_ca_path: Some(write("client_ca.pem", DEV_CLIENT_CA)),
        client_auth: krabka_security::ClientAuthMode::Required,
    };
    let RunningBroker { broker, dir: _dir } = start_with(ListenerProtocol::Ssl, &[], |config| {
        config.tls_config = Some(tls);
    })
    .await;

    let server_certificate = CertificateDer::pem_slice_iter(DEV_CERT.as_bytes())
        .next()
        .expect("fixture server certificate")
        .expect("parse server certificate");
    let client_certificates: Vec<CertificateDer<'static>> =
        CertificateDer::pem_slice_iter(DEV_CLIENT_CERT.as_bytes())
            .collect::<Result<_, _>>()
            .expect("parse client certificate");
    let client_key =
        PrivateKeyDer::from_pem_slice(DEV_CLIENT_KEY.as_bytes()).expect("parse client key");
    let client = ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(std::sync::Arc::new(PinnedCertVerifier {
            pinned: server_certificate,
            schemes: fixture_signature_schemes(),
            mismatch: "not the pinned fixture certificate",
        }))
        .with_client_auth_cert(client_certificates, client_key)
        .expect("client certificate");
    let tcp = TcpStream::connect(broker.controller_addr())
        .await
        .expect("connect controller listener");
    let mut stream = tokio_rustls::TlsConnector::from(std::sync::Arc::new(client))
        .connect(
            ServerName::try_from("krabka-dev").expect("server name"),
            tcp,
        )
        .await
        .expect("mTLS handshake");

    let quorum_version = describe_quorum_request::MAX_VERSION;
    check_quorum_denied(&mut stream, quorum_version).await;

    allow(
        &broker,
        &format!("User:{CLIENT_PRINCIPAL}"),
        AclOperation::Describe,
    )
    .await;
    let served = exchange(
        &mut stream,
        describe_quorum_request::API_KEY,
        quorum_version,
        true,
        &describe_quorum(),
    )
    .await;
    check!(decode::<DescribeQuorumResponse>(&served, quorum_version).error_code == 0);

    broker.shutdown().await;
}
