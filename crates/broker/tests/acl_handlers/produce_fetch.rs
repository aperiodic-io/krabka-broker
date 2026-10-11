//! Produce and Fetch enforcement (T23): a Write ACL on the topic gates a
//! Produce, a Read ACL gates a Fetch, and the decision lands on the
//! per-partition row of the response.
//!
//! Each test boots a fresh single-broker `SASL_PLAINTEXT` cluster with admin
//! as the super-user. Admin (over SASL) drives a `CreateTopics` request to
//! materialise the partition, the test seeds whatever ACL records are needed
//! via the controller-direct test helper, then alice authenticates (a
//! separate connection) and drives a Produce / Fetch. The assertions look at
//! the per-partition `error_code` row of the response and, on the happy
//! path, also check the broker's local log end offset via the existing
//! `BrokerHandle::local_log_end_offset` helper.

use assert2::assert;

use crate::{
    ERR_TOPIC_AUTHORIZATION_FAILED,
    client_api::{drive_fetch_as_plain, drive_produce_as_plain, single_record_produce_request},
    framing::CLIENT_ID,
    kafka_wire::PlainAuthenticationSetup,
    polling::retry_produce_until_allowed,
    sasl_cluster::{start_alice_topic_grant, start_denied_foo},
    support::{
        fetch::{FetchLimits, RequestWaitMillis, named_topic_fetch},
        produce::single_partition_response,
    },
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_denied_without_topic_acl() {
    let (handle, _dir, addr) = start_denied_foo().await;

    // alice has NO Write-on-foo binding → Produce must return 29.
    let resp = drive_produce_as_plain(
        addr,
        single_record_produce_request("foo", 0, b"hello"),
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            ..Default::default()
        },
    )
    .await
    .expect("Produce must round-trip");
    handle.shutdown().await;

    let p = single_partition_response(&resp);
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no Write ACL on foo, expected TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn produce_allowed_with_topic_write_acl() {
    // Commit the literal Allow Write grant through the controller.
    let (handle, _dir, addr) = start_alice_topic_grant(krabka_metadata::AclOperation::Write).await;

    // The ACL submit above is committed via the controller's raft path
    // then applied into the in-memory `MetadataImage` asynchronously, so
    // Produce reads from that image. Retry on Deny for up to 10 s — on
    // CI the commit-then-apply gap is usually a few ms but can spike.
    let resp = retry_produce_until_allowed(addr, "alice", b"wonderland", "foo")
        .await
        .expect("Produce must round-trip");

    let p = single_partition_response(&resp);
    assert!(
        p.error_code == 0,
        "alice has Write ACL on foo, expected error_code=0, got {p:?}"
    );

    // Verify the record actually landed in the local log.
    let leo = handle
        .local_log_end_offset("foo", 0)
        .expect("foo-0 must be hosted on this broker");
    handle.shutdown().await;
    assert!(
        leo >= 1,
        "log_end_offset must advance after a successful Produce, got {leo}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_denied_without_topic_read_acl() {
    let (handle, _dir, addr) = start_denied_foo().await;

    // alice has NO Read-on-foo binding → Fetch must return 29 on the
    // partition row.
    let req = named_topic_fetch("foo", FetchLimits::one_mebibyte(RequestWaitMillis(0)));
    let resp = drive_fetch_as_plain(
        addr,
        req,
        &PlainAuthenticationSetup {
            client_id: CLIENT_ID,
            ..Default::default()
        },
    )
    .await
    .expect("Fetch must round-trip");
    handle.shutdown().await;

    assert!(resp.responses.len() == 1, "one topic in response");
    assert!(
        resp.responses[0].partitions.len() == 1,
        "one partition row in response"
    );
    let p = &resp.responses[0].partitions[0];
    assert!(
        p.error_code == ERR_TOPIC_AUTHORIZATION_FAILED,
        "alice has no Read ACL on foo, expected TOPIC_AUTHORIZATION_FAILED (29), got {p:?}"
    );
}
