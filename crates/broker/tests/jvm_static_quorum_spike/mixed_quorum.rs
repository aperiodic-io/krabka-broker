//! The KIP-595 JVM mixed-quorum acceptance spike: two Krabka controllers and one JVM
//! controller form a static three-voter quorum, and the JVM joins the
//! Krabka-led quorum as a follower and replicates its committed metadata.
//!
//! This is the leader-to-follower direction of the cross-implementation goal,
//! and it needs no KIP-853 dynamic voters.

use std::time::Duration;

use assert2::check;
use uuid::Uuid;

use crate::{
    static_quorum_harness::{
        MixedQuorum, docker_rm, jvm_controller_properties, kafka_cluster_id_string,
        start_jvm_controller,
    },
    support,
};

const CONTAINER: &str = "krabka-kip595-slice5-spike";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker + a published controller port (throwaway spike)"]
async fn static_mixed_jvm_krabka_quorum() {
    support::init_tracing();
    docker_rm(CONTAINER);

    // ── shared cluster id ──────────────────────────────────────────────────
    let cluster_id = Uuid::from_u128(0x4d6b_5533_4f45_5642_4e54_6377_4e54_4a45);
    let cid_str = kafka_cluster_id_string(cluster_id);
    eprintln!("shared cluster_id uuid={cluster_id} kafka_str={cid_str}");

    // ── pre-bind 3 controller ports on the host ────────────────────────────
    let ([p1, p2, p3], [c1, c2], [_dir1, _dir2]) = MixedQuorum::start(cluster_id, None).await;

    eprintln!("both Krabka controllers started (2/3 majority should self-elect)");

    // ── format + start the JVM controller (id 3) ───────────────────────────
    // The JVM's controller.quorum.voters lists addresses reachable FROM the
    // container: the Krabka voters at host.docker.internal, itself on localhost.
    let props = jvm_controller_properties([p1, p2, p3], "");
    let _propdir = start_jvm_controller(CONTAINER, p3, &cid_str, &props);

    eprintln!("JVM controller (id 3) container started");

    // ── observe for ~40s ────────────────────────────────────────────────────
    // Success criterion 1: a single leader emerges across all three voters and
    // the two Krabka nodes agree on it. Success criterion 2: a follower's image
    // reflects the leader's committed records.
    let deadline = std::time::Instant::now() + Duration::from_secs(50);
    let mut elected = false;
    let mut last_l1 = None;
    let mut last_l2 = None;
    let mut tick = 0u32;
    while std::time::Instant::now() < deadline {
        let l1 = c1.controller_leader_id();
        let l2 = c2.controller_leader_id();
        last_l1 = l1;
        last_l2 = l2;
        if l1.is_some() && l1 == l2 {
            elected = true;
        }
        // Krabka-side telemetry every ~2s: leader epoch, HWM, and per-voter
        // matched index (does the JVM voter id=3 show up as fetching?).
        if tick.is_multiple_of(4) {
            let qs = c1.controller_quorum_state_for_test();
            eprintln!(
                "[t={}s] krabka n1 view: leader={:?} epoch={} hwm={} matched={:?}",
                tick / 2,
                qs.current_leader,
                qs.current_term,
                qs.last_applied_index,
                qs.per_voter_matched_index,
            );
        }
        tick += 1;
        // intentional: paces the fixed ~50s observation window; the loop
        // deliberately never breaks so the external JVM container has time to
        // boot, join, and produce the logs/telemetry this test greps below.
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Capture JVM logs regardless of outcome — they ARE the finding.
    let log_text = support::save_jvm_logs(CONTAINER, "/tmp/jvm_spike.log");
    eprintln!("==== JVM controller logs (tail) ====");
    support::print_log_tail(&log_text, 40);

    // Krabka-side observations.
    eprintln!(
        "Krabka leader view: node1={last_l1:?} node2={last_l2:?}  \
         voter_count(n1)={} voter_count(n2)={}",
        c1.voter_count_for_test(),
        c2.voter_count_for_test(),
    );

    // Did the JVM successfully join the quorum cross-impl? Success looks like
    // the JVM transitioning to Follower of the Krabka leader (or, less likely,
    // winning leadership itself). The dominant *failure* signal is the JVM
    // declaring `UNSUPPORTED_VERSION` ("The node does not support VOTE") because
    // Krabka's controller-listener ApiVersions handshake advertises no APIs —
    // so the JVM's NetworkClient refuses to even send Vote/Fetch on the wire.
    let jvm_joined = log_text.contains("Completed transition to FollowerState")
        || log_text.contains("Completed transition to LeaderState");
    let jvm_unsupported_version =
        log_text.contains("does not support VOTE") || log_text.contains("UNSUPPORTED_VERSION");
    let jvm_fatal_fault = log_text.contains("Encountered fatal fault");
    // The done bar: the JVM follower replicated the Krabka leader's committed
    // log and built its FeaturesImage from it — proving cross-impl metadata
    // replication, not just election.
    let jvm_replicated = log_text.contains("finished catching up to the current high water mark")
        && log_text.contains("metadata.version=25");
    eprintln!(
        "JVM cross-impl: joined={jvm_joined} unsupported={jvm_unsupported_version} \
         fatal_fault={jvm_fatal_fault} replicated={jvm_replicated}"
    );

    docker_rm(CONTAINER);
    c1.shutdown().await;
    c2.shutdown().await;

    // The two Krabka voters MUST elect among themselves regardless of the JVM.
    check!(
        elected,
        "Krabka 2/3 majority failed to elect a stable shared leader \
         (n1={last_l1:?} n2={last_l2:?})"
    );

    // The JVM mixed-quorum acceptance bar: the JVM controller joins the Krabka-led
    // static quorum as a follower, never fatal-faults, and replicates the
    // leader's committed metadata (HWM catch-up + a FeaturesImage carrying
    // metadata.version=25).
    check!(
        jvm_joined && !jvm_unsupported_version,
        "JVM did not join cross-impl: joined={jvm_joined}, unsupported={jvm_unsupported_version}"
    );
    check!(
        !jvm_fatal_fault,
        "JVM raft thread fatal-faulted (a wire/record inconsistency); see logs"
    );
    check!(
        jvm_replicated,
        "JVM did not replicate the Krabka leader's committed metadata (no HWM catch-up / \
         metadata.version not loaded); see JVM logs"
    );
}
