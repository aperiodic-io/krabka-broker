//! Authorization test for `AlterPartitionReassignments`.
//!
//! A principal that authenticated over SASL/PLAIN and holds no ACLs must be
//! refused with `CLUSTER_AUTHORIZATION_FAILED` (31) on every partition, because
//! the request needs cluster `Alter` permission.

use std::time::{Duration, Instant};

use assert2::assert;

use crate::{
    plaintext_cluster::wait_partition_exists,
    sasl_wire::{
        create_topic_as_admin, drive_alter_reassignments_sasl_plain,
        start_single_broker_sasl_plaintext_with_users,
    },
    support::acl::TopicAclSetup,
};

/// Test 4: alice, who authenticated over SASL/PLAIN and holds no ACLs, sends
/// `AlterPartitionReassignments` and must receive
/// `CLUSTER_AUTHORIZATION_FAILED (31)` on each partition.
///
/// The test seeds a dummy ACL first, to disable the compat shim. That shim
/// allows everything when `image.acls` is empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_super_user_denied() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_users(
        "admin",
        &[("admin", "admin-secret"), ("alice", "alice-secret")],
    )
    .await;

    // Seed a dummy ACL to disable the compat shim.  The ACL itself is
    // irrelevant — any non-empty `image.acls` flips the shim off and forces
    // the authorizer to evaluate every request.
    handle
        .submit_metadata_record_for_test(crate::support::acl::topic_acl_record(TopicAclSetup {
            topic: "__compat_shim_disable__",
            principal: "User:admin",
            ..Default::default()
        }))
        .await
        .expect("seed dummy ACL");

    // `submit_metadata_record_for_test` blocks until the raft entry is
    // committed and the state machine applies it to the image, so the ACL
    // is already present here; await it explicitly on the image watch channel
    // rather than sleeping.
    handle
        .wait_for_image(|img| img.all_acls().next().is_some())
        .await;

    create_topic_as_admin(
        addr,
        crate::kafka_wire::AutomaticTopicSetup {
            client_id: crate::plaintext_wire::CLIENT_ID,
            topic: crate::support::topics::ConfiguredTopicSetup {
                name: ("foo").into(),
                ..Default::default()
            },
        },
    )
    .await;
    wait_partition_exists(&handle, "foo", 0).await;

    // Retry up to 5s to absorb raft apply latency on slow runners.
    // intentional: bounded RPC-response poll on the alter response error code
    // (CLUSTER_AUTHORIZATION_FAILED=31) — an end-to-end authorizer verdict, not
    // a metadata-image or metric signal, so there is no awaiter to wait on.
    let deadline_auth = Instant::now() + Duration::from_secs(5);
    let resp = loop {
        let r = drive_alter_reassignments_sasl_plain(
            addr,
            "alice",
            "alice-secret",
            vec![("foo", 0, Some(vec![1]))],
        )
        .await;
        // If we see 31, the shim is off and we're done.
        if r.iter()
            .all(|(_, parts)| parts.iter().all(|(_, ec)| *ec == 31))
        {
            break r;
        }
        assert!(
            Instant::now() <= deadline_auth,
            "ACL shim still active or wrong error after 5s; got {r:?}"
        );
        // real-time wait (not a progress poll): retry/backoff cadence between attempts — each attempt opens a fresh TCP connection + full SASL handshake, so the 100ms backoff bounds connection churn while the raft ACL apply propagates.
        tokio::time::sleep(Duration::from_millis(100)).await;
    };

    handle.shutdown().await;

    assert!(
        resp[0].1 == vec![(0, 31)],
        "expected CLUSTER_AUTHORIZATION_FAILED (31) for alice; got {resp:?}"
    );
}
