//! Deterministic, minimized cases of what the chaos runs found.

use {
    crate::{
        check::{Findings, await_converged},
        cluster::ChaosCluster,
        plan::Stop,
        run::{deliver, verify},
        support::cluster_lock,
        topics::targets,
        workload::{History, Outcome, Sent},
    },
    assert2::assert,
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// One record of key and value `key`.
fn record(key: &str) -> Arc<[Sent]> {
    vec![Sent {
        key: key.to_owned(),
        value: format!("value-of-{key}"),
        header: None,
    }]
    .into()
}

/// The leader of a partition crashes and loses the last batch it wrote,
/// which every replica held and which was acknowledged with `acks=all`, and
/// it starts again at once, before the controller fences it. The record
/// must survive: the restarted broker may not lead the partition with the
/// shorter log at the old leader epoch and write new records over the
/// offsets its followers already hold.
///
/// Chaos seeds 140274395289229 and 266145556157555 (60s) found this as two
/// replicas serving different batches at the same offsets.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "product bug: an unclean self-registration keeps the restarted leader at its leader \
            epoch, so a lost log tail takes acknowledged records with it"]
async fn a_leader_restarted_before_it_is_fenced_keeps_what_was_acknowledged() {
    let _g = cluster_lock().lock().await;
    let mut cluster = ChaosCluster::boot().await;
    let target = targets()[0];
    let start = Instant::now();
    let mut history = History::default();
    for key in ["a", "b"] {
        history
            .attempts
            .push(deliver(&cluster, target, record(key), 0, start).await);
    }
    await_converged(&cluster, target, Duration::from_secs(30))
        .await
        .expect("every replica holds a and b");
    let leader = cluster.leader_of(target).expect("a leader");
    let before = (leader, cluster.leader_epoch(target));

    // The cut takes `b`, the last batch, from the leader's log only.
    let damage = cluster.stop(leader, Stop::CrashCutTail(0)).await;
    cluster.restart(leader).await;
    let after = (cluster.leader_of(target), cluster.leader_epoch(target));
    let next = deliver(&cluster, target, record("c"), 0, start).await;
    history.attempts.push(next.clone());
    let mut findings = Findings::default();
    verify(&cluster, &history, target, false, &mut findings).await;
    eprintln!(
        "leader and epoch before the crash {before:?}, after the restart {after:?}; {damage}; \
         c: {}\n{findings}",
        next.outcome
    );
    cluster.shutdown().await;

    assert!(matches!(next.outcome, Outcome::Acked(_)));
    assert!(findings.is_clean(), "{findings}");
}
