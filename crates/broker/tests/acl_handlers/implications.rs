//! End-to-end operation implications: a Read or a Write ACL on a topic also
//! grants Describe, so Metadata-by-name resolves the topic without a
//! separate Describe seed.

use assert2::assert;

use crate::polling::named_metadata_as_alice;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_read_acl() {
    metadata_implication(krabka_metadata::AclOperation::Read).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn implication_metadata_describes_after_write_acl() {
    metadata_implication(krabka_metadata::AclOperation::Write).await;
}

async fn metadata_implication(operation: krabka_metadata::AclOperation) {
    // Read and Write each imply Describe without a separate Describe ACL.
    let (handle, _dir, addr) = crate::sasl_cluster::start_alice_topic_grant(operation).await;

    // Wait for raft commit-then-apply, then ask Metadata for foo by name.
    // Pre-13b would have returned TOPIC_AUTHORIZATION_FAILED (29).
    let resp = named_metadata_as_alice(addr, "foo").await;
    handle.shutdown().await;

    assert!(resp.topics.len() == 1, "one topic row in response");
    let row = &resp.topics[0];
    assert!(row.name.as_deref() == Some("foo"));
    assert!(
        row.error_code == 0,
        "{operation:?} implies Describe, foo must be visible to alice with error_code=0, got {row:?}"
    );
}
