//! The setup a case performs through krabka's own APIs before any JVM tool
//! runs: the topics, the freezes, and the break-glass proposal two operators
//! approve.
//!
//! Every call here travels a krabka-private API that a JVM tool cannot reach,
//! so it is fixture rather than finding: a setup step that fails stops the case
//! with `assert!`. The approval count is the one exception, because the
//! two-person rule is what the case is about, and it is checked the way the
//! suite checks everything else.

use {
    crate::{
        host_broker::{JvmBroker, start_jvm_broker},
        jvm_acceptance::break_glass::{approve, propose},
        support,
        vocabulary::{APPROVER_ONE, APPROVER_TWO, PROPOSER, WIRE_UNCLEAN_ELECT_LEADERS},
    },
    assert2::{assert, check},
    krabka_client_admin::{AdminClient, CreateTopicSpec},
    krabka_client_core::{Client, security::ClientSecurity},
    krabka_protocol::krabka::freeze::SetTopicFreezeRequest,
};

/// A plaintext host-side client for the krabka-private APIs.
pub(super) async fn plain_client(bootstrap: &str) -> Client {
    support::client::connect_owned(bootstrap, "kfc9-jvm-acceptance", "client build").await
}

/// Create every topic a case needs, and fail the case when one does not open.
pub(super) async fn create_topics(
    bootstrap: &str,
    security: Option<ClientSecurity>,
    names: &[&str],
) {
    let mut admin = AdminClient::connect_secured(&[bootstrap.to_owned()], security)
        .await
        .expect("admin connect");
    let specs: Vec<CreateTopicSpec> = names
        .iter()
        .map(|name| CreateTopicSpec {
            name: (*name).to_owned(),
            partitions: 1,
            replicas: 1,
            configs: std::collections::BTreeMap::default(),
            replica_assignments: std::collections::BTreeMap::new(),
        })
        .collect();
    let outcomes = admin
        .create_topics(
            &specs,
            krabka_client_admin::TopicMutationOptions::with_timeout(krabka_units::secs(30)),
        )
        .await
        .expect("create topics");
    for outcome in outcomes {
        let name = outcome.name;
        let error = outcome.error;
        assert!(error.is_none(), "create topic {name}: {error:?}");
    }
}

/// Freeze one scope through the krabka-private `SetTopicFreeze` (api key
/// 1015).
///
/// The request is unsigned, which the broker accepts for a freeze while
/// `freeze.require_signature` is off. A freeze is the safe direction, and
/// KFC-9 keeps it reachable in one command on a cluster with no key material.
pub(super) async fn freeze(client: &Client, scope: &str, pattern_type: i8, reason: &str) {
    let response = client
        .send(SetTopicFreezeRequest {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: reason.to_owned(),
            ..SetTopicFreezeRequest::default()
        })
        .await
        .expect("SetTopicFreeze");
    let code = response.error_code;
    let message = response.error_message;
    assert!(code == 0, "freeze {scope}: code={code} message={message:?}");
}

/// Open a break-glass proposal as `PROPOSER`, and have both approvers sign off.
///
/// The target is the bare topic name rather than `<topic>-<partition>`. KFC-9
/// lets a proposal on a topic cover every partition of it for the actions that
/// name a partition, and an unclean election is one of those, so this also
/// checks that widening on the way through.
pub(super) async fn approved_unclean_election(bootstrap: &str, target: &str) {
    let proposal_id = propose(
        bootstrap,
        PROPOSER,
        WIRE_UNCLEAN_ELECT_LEADERS,
        target,
        "the whole ISR is gone and the site has to come back",
    )
    .await;

    let first = approve(bootstrap, APPROVER_ONE, proposal_id).await;
    check!(
        first.held == 1,
        "one approval is one distinct principal, not {first:?}"
    );
    check!(
        first.held < first.required,
        "one person must not be enough: {first:?}"
    );

    let second = approve(bootstrap, APPROVER_TWO, proposal_id).await;
    check!(
        second.held == second.required,
        "two distinct principals must satisfy the rule: {second:?}"
    );
}

/// Plaintext fixture with all topics installed before the first control-plane mutation.
pub(super) async fn plain_topic_fixture(names: &[&str]) -> (JvmBroker, Client) {
    let broker = start_jvm_broker(|_| {}).await;
    let client = plain_client(&broker.host).await;
    create_topics(&broker.host, None, names).await;
    (broker, client)
}
