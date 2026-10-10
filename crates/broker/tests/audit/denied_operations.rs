//! The authorizer decorator's deny path, seen from the client side.
//!
//! The decorator sits in front of every admin operation, so a broker that
//! panics or wedges on a denial takes the audit sink down with it. The case
//! here has a deny-all authorizer refuse a `CreateTopics`, and then reads the
//! audit topic back to show that the broker still serves. Its own doc comment
//! records why it stops short of asserting the `AuthorizationDenied` record.

use krabka_broker::coordinator::AUDIT_TOPIC;

use crate::{
    support,
    support::{
        fetch::single_partition_fetch,
        topics::{creatable_topic, create_topic_request},
    },
};

/// Verifies that the authorizer-decorator path denies an unauthorized
/// operation.
///
/// This test asserts that:
///   1. The broker denies a `CreateTopics` request with
///      `TOPIC_AUTHORIZATION_FAILED` -- a deny-all authorizer denies the
///      cluster-wide `Create` shortcut, then denies the per-topic `Create`
///      fallback too (#698's per-topic fallback), so this is the same error
///      Kafka's `ControllerApis.createTopics` answers for a denied topic row,
///      not the whole-request `CLUSTER_AUTHORIZATION_FAILED` a pre-#698
///      broker answered when the cluster shortcut alone was ever checked.
///   2. The broker stays healthy and does not crash.
///
/// This test does NOT assert that the broker emitted an `AuthorizationDenied`
/// audit record to the audit topic.
///
/// The full end-to-end path, which sends a denied request and then observes
/// the `AuthorizationDenied` record in the audit topic through the same
/// client, is impractical for these reasons:
///   - The test client connects anonymously, with the principal
///     `"ANONYMOUS"`.
///   - `SimpleAclAuthorizer` with no ACLs and no super-users denies every
///     request, including the `Fetch` that reads the audit topic back.
///   - There is no plaintext SASL path that would give the anonymous reader a
///     higher principal without SCRAM credentials.
///
/// The unit test `deny_decision_emits_audit_record` in
/// `crates/broker/src/audit_authorizer.rs` already proves the audit emit on a
/// deny.
#[tokio::test]
async fn denied_operation_returns_topic_authorization_failed() {
    // Start a broker with a deny-all authorizer.
    let p = Box::pin(support::start_with_deny_all_authz()).await;

    // Attempt a create that will be denied.
    let resp = p
        .client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("denied-topic").into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();

    // Verify the broker actually denied the request (error_code
    // TOPIC_AUTHORIZATION_FAILED = 29): the deny-all authorizer denies the
    // cluster Create shortcut, then denies the per-topic Create fallback too.
    let denied = resp
        .topics
        .iter()
        .any(|t| t.error_code == krabka_broker::codes::TOPIC_AUTHORIZATION_FAILED);
    assert2::check!(denied, "expected CreateTopics to be denied; resp: {resp:?}");

    // Verify the broker is still alive by checking the audit topic is reachable.
    let topic_id = support::topic_id_for(&p.client, AUDIT_TOPIC).await;
    let fr = p
        .client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: AUDIT_TOPIC.into(),
                topic_id,
                limits: crate::support::fetch::FetchLimits {
                    wait: crate::support::fetch::RequestWaitMillis(100),
                    minimum: crate::support::fetch::FetchByteLimit(0),
                    maximum: crate::support::fetch::FetchByteLimit(1 << 20),
                },
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    // The broker responded to the Fetch request without crashing.
    let _ = fr;

    p.broker.shutdown().await;
}
