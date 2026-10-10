//! KIP-430: the `authorized_operations` bitfield on `Metadata`,
//! `DescribeCluster`, and `DescribeGroups` responses.
//!
//! Covered:
//!   * `Metadata.cluster_authorized_operations` (request v8-10) and
//!     `MetadataResponseTopic.topic_authorized_operations` (request v8+)
//!     populate only when the corresponding `include_*` flag is set.
//!   * `DescribeCluster.cluster_authorized_operations` follows the
//!     `include_cluster_authorized_operations` request flag.
//!   * `DescribeGroups.DescribedGroup.authorized_operations` follows the
//!     `include_authorized_operations` request flag.
//!   * Bitfield contents show the configured authorizer's decisions:
//!     super-user → full mask; explicit ACL row → only that bit plus any
//!     implications; opt-out → `i32::MIN` "not present" sentinel.
//!
//! These tests run against a plaintext loopback listener through the in-process
//! handler dispatch. There is no SASL framing step. The tests drive
//! `BrokerConfig.authorizer` directly with a `SimpleAclAuthorizer`.

mod support;

use std::{collections::HashSet, sync::Arc};

use assert2::assert;
use bytes::{Buf, BytesMut};
use krabka_broker::{Broker, BrokerConfig, authorizer::SimpleAclAuthorizer};
use krabka_client_core::Client;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_groups_request::DescribeGroupsRequest, metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
    },
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::support::{
    acl::{
        BIT_ALTER, BIT_ALTER_CONFIGS, BIT_CLUSTER_ACTION, BIT_CREATE, BIT_DELETE, BIT_DESCRIBE,
        BIT_DESCRIBE_CONFIGS, BIT_IDEMPOTENT_WRITE, BIT_READ, TOPIC_FULL_MASK,
    },
    client::connect_owned,
    topics::{creatable_topic, create_topic_request, metadata_topic},
};

// Bit positions match Kafka's `AclOperation.code()` (verified by the
// helper's unit tests). Spelled out here so the assertions don't depend
// on importing the crate-private helper module.

const GROUP_FULL_MASK: i32 =
    BIT_READ | BIT_DESCRIBE | BIT_DELETE | BIT_DESCRIBE_CONFIGS | BIT_ALTER_CONFIGS;

const CLUSTER_FULL_MASK: i32 = BIT_CREATE
    | BIT_ALTER
    | BIT_DESCRIBE
    | BIT_CLUSTER_ACTION
    | BIT_ALTER_CONFIGS
    | BIT_DESCRIBE_CONFIGS
    | BIT_IDEMPOTENT_WRITE;

struct Harness {
    handle: krabka_broker::BrokerHandle,
    client: Client,
    _tempdir: tempfile::TempDir,
}

impl Harness {
    async fn shutdown(self) {
        // Drop the client first so it doesn't keep a socket open across
        // broker shutdown.
        drop(self.client);
        self.handle.shutdown().await;
    }
}

/// Boot a single broker with a plaintext loopback listener and an authorizer.
///
/// The [`SimpleAclAuthorizer`] is configured so that the connecting principal,
/// the PLAINTEXT default `User:ANONYMOUS`, is a super-user. That gives the
/// metadata-driving client full access. The test can still seed ACLs and see
/// their effect on the authorized-operations bitfield for a principal name that
/// the authorizer evaluates separately.
///
/// The name goes to the authorizer only, which is all this suite needs to
/// drive metadata over a PLAINTEXT listener.
fn boot_with_super_user(super_user: &str) -> impl std::future::Future<Output = Harness> {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let authorizer_super_users: HashSet<String> = {
        let mut s = HashSet::new();
        s.insert(super_user.to_string());
        s
    };
    cfg.authorizer = Arc::new(SimpleAclAuthorizer::new(authorizer_super_users));
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker start");
        // `DescribeGroups` needs a loaded group coordinator.
        handle.wait_until_group_coordinator_ready().await;
        let client = connect_owned(
            handle.listen_addr().to_string(),
            "krabka-kip-430-test",
            "client build",
        )
        .await;
        Harness {
            handle,
            client,
            _tempdir: tempdir,
        }
    })
}

async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let resp = client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: name.to_string(),
                partitions: crate::support::topics::TopicPartitionCount(partitions),
                ..Default::default()
            },
        )))
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "topic create: {resp:?}");
}

async fn seeded_group() -> Harness {
    let h = boot_with_super_user("ANONYMOUS").await;
    h.handle.group_create_for_test("g1");
    h
}

async fn describe_group(
    client: &Client,
    include_authorized_operations: bool,
) -> krabka_protocol::owned::describe_groups_response::DescribeGroupsResponse {
    let resp = client
        .send(DescribeGroupsRequest {
            groups: vec!["g1".into()],
            include_authorized_operations,
            ..Default::default()
        })
        .await
        .expect("DescribeGroups");
    assert!(resp.groups.len() == 1);
    let g = &resp.groups[0];
    assert!(g.error_code == 0, "DescribeGroups error: {g:?}");
    resp
}

// ── Metadata ────────────────────────────────────────────────────────────────

/// Without `include_topic_authorized_operations`, the per-topic field
/// must stay at the `i32::MIN` "not present" sentinel even when the
/// authorizer would otherwise grant operations on the topic.
#[tokio::test]
async fn metadata_topic_authorized_operations_default_is_not_present() {
    let h = boot_with_super_user("ANONYMOUS").await;
    create_topic(&h.client, "foo", 1).await;

    let resp = h
        .client
        .send(MetadataRequest {
            topics: Some(vec![metadata_topic(
                Some("foo".into()),
                krabka_protocol::primitives::uuid::Uuid::default(),
            )]),
            // include_topic_authorized_operations defaults to false.
            ..Default::default()
        })
        .await
        .expect("Metadata");

    let row = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("foo"))
        .expect("foo row present");
    assert!(
        row.topic_authorized_operations == i32::MIN,
        "opt-out must leave the sentinel: {row:?}"
    );
    h.shutdown().await;
}

/// With `include_topic_authorized_operations = true`, a super-user sees the
/// full topic mask.
///
/// A topic with no matching ACL for a non-super principal would show zero. Both
/// rows appear in the same response. The super-user drives the request, but the
/// broker computes the bitfield against the request's principal.
#[tokio::test]
async fn metadata_topic_authorized_operations_super_user_gets_full_mask() {
    let h = boot_with_super_user("ANONYMOUS").await;
    create_topic(&h.client, "foo", 1).await;

    let resp = h
        .client
        .send(MetadataRequest {
            include_topic_authorized_operations: true,
            ..crate::support::discovery::named_topic_metadata("foo")
        })
        .await
        .expect("Metadata");

    let row = resp
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("foo"))
        .expect("foo row present");
    assert!(
        row.topic_authorized_operations == TOPIC_FULL_MASK,
        "super-user must see the full topic mask, got 0b{:b}",
        row.topic_authorized_operations
    );
    h.shutdown().await;
}

// ── DescribeCluster ─────────────────────────────────────────────────────────

/// `DescribeCluster` without the include flag leaves the
/// `cluster_authorized_operations` sentinel in place.
#[tokio::test]
async fn describe_cluster_authorized_operations_default_is_not_present() {
    let h = boot_with_super_user("ANONYMOUS").await;

    let resp = h
        .client
        .send(DescribeClusterRequest::default())
        .await
        .expect("DescribeCluster");
    assert!(resp.error_code == 0, "DescribeCluster error: {resp:?}");
    assert!(
        resp.cluster_authorized_operations == i32::MIN,
        "opt-out must leave the sentinel"
    );
    h.shutdown().await;
}

/// With the include flag set, the super-user sees the full cluster mask.
#[tokio::test]
async fn describe_cluster_authorized_operations_super_user_gets_full_mask() {
    let h = boot_with_super_user("ANONYMOUS").await;

    let resp = h
        .client
        .send(DescribeClusterRequest {
            include_cluster_authorized_operations: true,
            ..Default::default()
        })
        .await
        .expect("DescribeCluster");
    assert!(resp.error_code == 0, "DescribeCluster error: {resp:?}");
    assert!(
        resp.cluster_authorized_operations == CLUSTER_FULL_MASK,
        "super-user must see the full cluster mask, got 0b{:b}",
        resp.cluster_authorized_operations
    );
    h.shutdown().await;
}

// ── DescribeGroups ──────────────────────────────────────────────────────────

/// Without `include_authorized_operations`, each `DescribedGroup` row
/// keeps the `i32::MIN` sentinel even when the authorizer would grant
/// the full mask.
#[tokio::test]
async fn describe_groups_authorized_operations_default_is_not_present() {
    let h = seeded_group().await;

    let resp = describe_group(&h.client, false).await;
    let g = &resp.groups[0];
    assert!(
        g.authorized_operations == i32::MIN,
        "opt-out must leave the sentinel"
    );
    h.shutdown().await;
}

/// With the include flag set, a super-user driving the request sees the
/// full group mask on a seeded group.
#[tokio::test]
async fn describe_groups_authorized_operations_super_user_gets_full_mask() {
    let h = seeded_group().await;

    let resp = describe_group(&h.client, true).await;
    let g = &resp.groups[0];
    assert!(
        g.authorized_operations == GROUP_FULL_MASK,
        "super-user must see the full group mask, got 0b{:b}",
        g.authorized_operations
    );
    h.shutdown().await;
}

/// The cluster-level field also travels on the v8-10 Metadata response window.
///
/// The request's `include_cluster_authorized_operations` bit is the opt-in. The
/// default, false, must leave the wire-default sentinel even on those versions.
/// The wire codec gates the field on the encoded response version anyway, so
/// this test exercises the populate path on the in-window v9 codec with
/// `MetadataRequest.include_cluster_authorized_operations`.
#[tokio::test]
async fn metadata_cluster_authorized_operations_super_user_gets_full_mask_v9() {
    let h = boot_with_super_user("ANONYMOUS").await;

    // Pin v9 so the cluster-level field is in the on-wire version range
    // (v8-10). The `Client` default would pick a higher Metadata version
    // that drops the field, in which case the value can't be observed
    // round-trip.
    let req = MetadataRequest {
        // Fetch-all so we don't need a topic for the field to be set.
        topics: None,
        include_cluster_authorized_operations: true,
        ..Default::default()
    };
    let version: i16 = 9;

    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("Metadata encode");

    // Build the v2 request header (flexible — Metadata went flexible at
    // v9). One TCP round-trip, plaintext, no SASL.
    let frame = crate::support::wire::request_frame(crate::support::wire::WireFrameSetup {
        api_key: krabka_ids::ApiKey(3),
        version: krabka_ids::ApiVersion(version),
        correlation: crate::support::wire::CorrelationId(7),
        header: crate::support::wire::HeaderEncoding::Flexible,
        client_id: "krabka-kip-430-v9",
        body: &body,
        capacity: Some(crate::support::wire::request_body_capacity(&body)),
        ..Default::default()
    });

    let mut stream = tokio::net::TcpStream::connect(h.handle.listen_addr())
        .await
        .expect("tcp connect");
    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .expect("write len");
    stream.write_all(&frame).await.expect("write frame");
    stream.flush().await.expect("flush");

    let resp_len = stream.read_u32().await.expect("read len");
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.expect("read body");

    // Strip response header v1 (flexible): i32 correlation_id + 1 tagged byte.
    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    let _hdr_tagged = cur.get_u8();
    let resp = MetadataResponse::decode(&mut cur, version).expect("Metadata decode");

    assert!(
        resp.cluster_authorized_operations == CLUSTER_FULL_MASK,
        "super-user must see the full cluster mask on Metadata v9, got 0b{:b}",
        resp.cluster_authorized_operations
    );
    h.shutdown().await;
}
