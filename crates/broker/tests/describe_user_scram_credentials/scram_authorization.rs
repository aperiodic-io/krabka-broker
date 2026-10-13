//! The authorization half: `DescribeUserScramCredentials` needs `Describe` on
//! the cluster resource, which a non-super-user can hold through an ACL alone.
//!
//! Both tests run with an empty `super_users` list, so the ACL is the only
//! thing that separates the allowed caller from the rejected one.

use {
    crate::{
        scram_cluster::{
            alice_test_password, seed_cluster_acl,
            start_single_broker_sasl_plaintext_with_acl_authorizer,
        },
        scram_driver::drive_describe_user_scram_credentials_sasl,
    },
    assert2::assert,
    krabka_metadata::AclOperation,
    uuid::Uuid,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_allows_cluster_describe_acl() {
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_acl_authorizer(&[], &[(
        "alice",
        &alice_test_password(),
    )])
    .await;
    seed_cluster_acl(&handle, "alice", AclOperation::Describe).await;

    let (top_err, per_user) =
        drive_describe_user_scram_credentials_sasl(addr, "alice", &alice_test_password(), None)
            .await;

    handle.shutdown().await;
    assert!(top_err == 0, "Cluster Describe ACL should authorize");
    assert!(per_user.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_rejects_without_cluster_describe_acl() {
    let alice_password = Uuid::new_v4().to_string();
    let (handle, _dir, addr) = start_single_broker_sasl_plaintext_with_acl_authorizer(&[], &[(
        "alice",
        alice_password.as_str(),
    )])
    .await;

    let (top_err, per_user) =
        drive_describe_user_scram_credentials_sasl(addr, "alice", alice_password.as_str(), None)
            .await;

    handle.shutdown().await;
    assert!(
        top_err == 31,
        "missing Cluster Describe ACL should be rejected"
    );
    assert!(per_user.is_empty());
}
