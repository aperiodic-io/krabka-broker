//! The gate itself: a frozen topic refuses `Produce`, and the topic beside it
//! does not.
//!
//! The literal case is the feature in one test, the prefix case is the
//! namespace scope that an ACL cannot express atomically, and the late-topic
//! case is the disaster-recovery order in which an operator freezes a
//! namespace before a restore writes into it.

use krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED};

use crate::{
    control_plane::freeze_scope,
    support,
    wire::{CONTROL, accepted, controlled_fixture, create_topic, refused},
};

/// A literal freeze stops writes to the topic it names, and to nothing else.
///
/// This is the feature in one case. Both topics take a write before the freeze
/// lands, so the refusal afterwards is the registry entry rather than a produce
/// path that stopped working; and the control topic keeps taking writes after
/// it, so the refusal is scoped rather than global. Delete this case and
/// nothing proves that `POLICY_VIOLATION` ever reaches a producer.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_literal_freeze_refuses_produce_and_the_control_topic_still_accepts() {
    let (p, frozen, control) = Box::pin(controlled_fixture("orders")).await;

    crate::wire::check_produce!(&p.broker, &p.client, "orders", frozen => accepted(1));
    crate::wire::check_produce!(&p.broker, &p.client, CONTROL, control => accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_LITERAL, "orders", "DR cutover").await;

    crate::wire::check_produce!(&p.broker, &p.client, "orders", frozen => refused("literal", "orders", "DR cutover", 1));
    crate::wire::check_produce!(&p.broker, &p.client, CONTROL, control => accepted(2));

    p.broker.shutdown().await;
}

/// A prefixed freeze stops writes to every topic in the namespace, and stops at
/// the namespace boundary.
///
/// The namespace scope is the half an ACL cannot express atomically, and it is
/// the reason this feature exists rather than a deny binding per topic. The
/// case runs two controls on purpose: `CONTROL` shows the produce path works at
/// all, and `tenant-b.orders` shows the prefix walk stops where the scope stops
/// rather than matching every topic once the registry is non-empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_prefix_freeze_refuses_produce_to_every_topic_it_covers() {
    let p = support::start().await;
    let covered = create_topic(&p.broker, &p.client, "tenant-a.orders").await;
    let neighbour = create_topic(&p.broker, &p.client, "tenant-b.orders").await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    crate::wire::check_produce!(&p.broker, &p.client, "tenant-a.orders", covered => accepted(1));

    freeze_scope(&p.client, PATTERN_TYPE_PREFIXED, "tenant-a.", "offboarding").await;

    crate::wire::check_produce!(&p.broker, &p.client, "tenant-a.orders", covered => refused("prefixed", "tenant-a.", "offboarding", 1));
    crate::wire::check_produce!(&p.broker, &p.client, "tenant-b.orders", neighbour => accepted(1));
    crate::wire::check_produce!(&p.broker, &p.client, CONTROL, control => accepted(1));

    p.broker.shutdown().await;
}

/// A topic created after a covering prefix freeze is frozen the moment it
/// exists.
///
/// This is the disaster-recovery case the design is written for: an operator
/// freezes a namespace *before* a restore writes into it, so the resolve has to
/// run against the topic name at produce time and not against a set of names
/// materialised when the freeze landed. A cached set would let the newest topic
/// through, which is exactly the topic the restore is about to fill.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_created_after_a_covering_prefix_freeze_is_frozen_on_arrival() {
    let p = support::start().await;
    let control = create_topic(&p.broker, &p.client, CONTROL).await;

    freeze_scope(&p.client, PATTERN_TYPE_PREFIXED, "tenant-a.", "pre-restore").await;

    let late = create_topic(&p.broker, &p.client, "tenant-a.late").await;
    crate::wire::check_produce!(&p.broker, &p.client, "tenant-a.late", late => refused("prefixed", "tenant-a.", "pre-restore", 0));
    crate::wire::check_produce!(&p.broker, &p.client, CONTROL, control => accepted(1));

    p.broker.shutdown().await;
}
