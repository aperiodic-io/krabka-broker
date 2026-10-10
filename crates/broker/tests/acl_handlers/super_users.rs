//! Principal handling for the `super_users` set: a `CreateAcls` request from
//! any principal in the set succeeds, while a principal outside it still
//! gets `CLUSTER_AUTHORIZATION_FAILED` on every binding.

use assert2::assert;
use krabka_broker::Broker;
use krabka_protocol::owned::create_acls_request::CreateAclsRequest;

use crate::{
    OPERATION_READ, OPERATION_WRITE,
    acl_admin::{drive_create_acls_as_plain, topic_allow_creation},
    sasl_cluster::sasl_plain_broker_config_multi_super,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multi_super_user_both_can_provision() {
    let log_dir = tempfile::tempdir().unwrap();
    // Two super-users: admin + ops-bot. alice has PLAIN creds but is NOT
    // in the super-users set, so her CreateAcls must hit the cluster gate.
    let cfg = sasl_plain_broker_config_multi_super(
        log_dir.path(),
        &[
            ("admin", "admin-secret"),
            ("ops-bot", "ops-secret"),
            ("alice", "wonderland"),
        ],
        &["admin", "ops-bot"],
    );

    let handle = Broker::start(cfg).await.expect("broker must start");
    let addr = handle.listen_addr();

    // admin (super-user #1) must succeed.
    let admin_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("t-admin", "User:bob", OPERATION_READ)],
        ..Default::default()
    };
    let admin_resp = drive_create_acls_as_plain(
        addr,
        admin_req,
        &crate::kafka_wire::PlainAuthenticationSetup::admin(crate::framing::CLIENT_ID),
    )
    .await
    .expect("CreateAcls as admin must round-trip");
    assert!(admin_resp.results.len() == 1);
    assert!(
        admin_resp.results[0].error_code == 0,
        "admin is a super-user, CreateAcls must succeed: {:?}",
        admin_resp.results[0]
    );

    // ops-bot (super-user #2) must also succeed.
    let ops_req = CreateAclsRequest {
        creations: vec![topic_allow_creation("t-ops", "User:carol", OPERATION_WRITE)],
        ..Default::default()
    };
    let ops_resp = drive_create_acls_as_plain(
        addr,
        ops_req,
        &crate::kafka_wire::PlainAuthenticationSetup {
            client_id: crate::framing::CLIENT_ID,
            user: "ops-bot",
            password: (b"ops-secret").to_vec(),
        },
    )
    .await
    .expect("CreateAcls as ops-bot must round-trip");
    assert!(ops_resp.results.len() == 1);
    assert!(
        ops_resp.results[0].error_code == 0,
        "ops-bot is a super-user, CreateAcls must succeed: {:?}",
        ops_resp.results[0]
    );

    // alice (not in super-set, no Cluster Alter ACL) must be denied per
    // binding with CLUSTER_AUTHORIZATION_FAILED (31).
    let alice_req = CreateAclsRequest {
        creations: vec![
            topic_allow_creation("t-x", "User:dave", OPERATION_READ),
            topic_allow_creation("t-y", "User:eve", OPERATION_WRITE),
        ],
        ..Default::default()
    };
    let alice_resp = drive_create_acls_as_plain(
        addr,
        alice_req,
        &crate::kafka_wire::PlainAuthenticationSetup {
            client_id: crate::framing::CLIENT_ID,
            ..Default::default()
        },
    )
    .await
    .expect("CreateAcls request must round-trip even when denied");
    handle.shutdown().await;

    assert!(alice_resp.results.len() == 2);
    for (i, r) in alice_resp.results.iter().enumerate() {
        assert!(
            r.error_code == 31, /* CLUSTER_AUTHORIZATION_FAILED */
            "binding {i} must be denied for alice (not in super_users), got {r:?}"
        );
    }
}
