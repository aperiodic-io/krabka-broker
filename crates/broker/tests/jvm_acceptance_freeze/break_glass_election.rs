//! `kafka-leader-election --election-type unclean` before and after two
//! operators approve a break-glass proposal for it.
//!
//! This is the one case in the suite that authenticates. An approval by two
//! distinct principals cannot be shown over a listener on which every
//! connection is the same anonymous one, so the broker here speaks
//! `SASL_PLAINTEXT` and each operator dials it with their own credentials.

use {
    crate::{
        control_plane::{approved_unclean_election, create_topics},
        host_broker::{gate_on, sasl_listener, sasl_props, start_jvm_broker},
        jvm_acceptance::write_client_props,
        jvm_tool::jvm_unclean_election,
        support,
        vocabulary::{
            ACTION_UNCLEAN_ELECT_LEADERS, APPROVER_ONE, APPROVER_TWO, ELECT_TOPIC,
            POLICY_VIOLATION_EXCEPTION, PROPOSER, no_proposal_refusal,
        },
    },
    assert2::check,
};

/// `kafka-leader-election --election-type unclean` fails without an approved
/// proposal, and stops failing once two people approve one.
///
/// This is the case that proves an approval is a standing authorization rather
/// than a request field. `kafka-leader-election` sends the stock `ElectLeaders`
/// that KIP-460 defines, with nowhere to put a proposal id, and the operator
/// gets the approval out of band through the krabka-private APIs. The tool is
/// byte-for-byte the same on both runs; only the metadata image differs.
///
/// The success run exits zero rather than electing anything. The partition is
/// healthy on a single node, so an unclean election answers
/// `ELECTION_NOT_NEEDED` (84), which `LeaderElectionCommand` counts as a no-op
/// and not a failure. That is the honest signal available here: the request
/// got past the two-person rule, which is the whole of what the approval
/// changed. The assertions say exactly that and no more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires docker"]
async fn kafka_leader_election_needs_an_approved_proposal_for_an_unclean_election() {
    let users = [PROPOSER, APPROVER_ONE, APPROVER_TWO];
    let broker = start_jvm_broker(|config| {
        sasl_listener(config, &users);
        gate_on(config);
    })
    .await;
    let props = write_client_props(&sasl_props(PROPOSER.0, PROPOSER.1));
    create_topics(
        &broker.host,
        Some(support::sasl_plain_security(PROPOSER.0, PROPOSER.1)),
        &[ELECT_TOPIC],
    )
    .await;

    let target = format!("{ELECT_TOPIC}-0");
    let refusal = no_proposal_refusal(ACTION_UNCLEAN_ELECT_LEADERS, &target);
    let refused = jvm_unclean_election(&broker.container, &props);
    check!(
        !refused.succeeded(),
        "an unclean election with no approval must exit non-zero"
    );
    check!(
        refused.says(POLICY_VIOLATION_EXCEPTION),
        "the election must name the policy violation, got {refused:?}"
    );
    check!(
        refused.says(&refusal),
        "the election must print {refusal:?}, got {refused:?}"
    );

    approved_unclean_election(&broker.host, ELECT_TOPIC).await;

    let allowed = jvm_unclean_election(&broker.container, &props);
    check!(
        allowed.succeeded(),
        "an approved election must exit zero, got {allowed:?}"
    );
    check!(
        !allowed.says(POLICY_VIOLATION_EXCEPTION),
        "an approved election must not be refused, got {allowed:?}"
    );
    check!(
        !allowed.says("break-glass refused"),
        "an approved election must not be refused, got {allowed:?}"
    );
    check!(
        allowed.says(&target),
        "the tool must report on the partition it was asked about, got {allowed:?}"
    );
}
