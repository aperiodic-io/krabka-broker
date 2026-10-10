//! A bounded Jepsen-style fault schedule over a live diskless WAL quorum.
//!
//! The one case below boots three real brokers, creates an RF=1 diskless
//! topic, pins its sole classic replica onto the broker that also leads the
//! controller, and then takes that broker away. Every acknowledged record must
//! still read back afterwards, from a broker that never held a classic replica
//! of the partition.
//!
//! ## What the schedule proves
//!
//! The faults overlap on purpose, because each one alone has an escape. The
//! object store is blocked before the first produce, so a flush cannot rescue
//! the tail. The acking broker is killed and its canonical log is erased, so a
//! local fsync cannot serve the readback. The controller leader goes with it,
//! so the promotion is a real authority handoff rather than a metadata edit on
//! a live leader. What remains is the WAL quorum on the two survivors, and the
//! readback can only come from there.
//!
//! Two independent readers check the same offsets: a direct Rust partition
//! fetch and the JVM `kafka-console-consumer` in a container. The second one
//! is a byte differential against a client this project does not write.
//!
//! Every fault carries a discriminating witness, so a no-op fails. A blocked
//! PUT is counted on the flusher's own failure metric before and after the
//! crash, the erased log is counted on disk before it is erased, and the two
//! leader handoffs are asserted to name different nodes.
//!
//! ## Why the cluster runs two listeners
//!
//! A diskless quorum in this tree needs an **authenticated** inter-broker
//! listener. A WAL follower's Fetch is authorized against the caller's
//! principal: the leader resolves `broker-<id>` to a node id and serves the
//! shard only to a voter that is who it claims to be. That convention applies
//! only to connections on the inter-broker listener, so an anonymous plaintext
//! cluster never forms a quorum at all.
//!
//! The JVM console consumer runs in a container and has to reach the brokers
//! from there, which means the client-facing endpoint must advertise
//! `host.docker.internal`. Putting SASL on that same endpoint would make the
//! container hold credentials for a test whose subject is durability, not
//! authentication.
//!
//! So each broker binds two data-plane listeners: a `PLAINTEXT` one on all
//! interfaces that advertises `host.docker.internal` and carries every client
//! in the suite, and a `SASL_PLAINTEXT` one on loopback that
//! `inter_broker_listener_name` selects and that carries the raft, replication
//! and WAL follower traffic. `BrokerConfig.listeners` is a list, so this costs
//! one more `ListenerSpec` per broker.
//!
//! ## Layout
//!
//! The binary root carries the module tree, the constants every part shares
//! and the one case. [`cluster`] boots the three brokers and drives their
//! metadata, [`history`] produces the ledger and checks it for
//! linearizability, [`faults`] injects each fault and holds the witness, and
//! [`readback`] reads the acknowledged offsets back with both clients.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `diskless_jepsen/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "diskless_jepsen/cluster.rs"]
mod cluster;
#[path = "diskless_jepsen/faults.rs"]
mod faults;
#[path = "diskless_jepsen/history.rs"]
mod history;
#[path = "diskless_jepsen/readback.rs"]
mod readback;

use assert2::assert;
use tempfile::TempDir;

use crate::faults::FaultWitness;

/// The one diskless topic this suite creates. The name is
/// `[A-Za-z0-9_-]`-only, so it reaches the filesystem unchanged and
/// [`faults`] can build the WAL and log paths by hand.
const TOPIC: &str = "diskless-jepsen";

/// Concurrent producers on the accepting broker. Two is the smallest number
/// that gives the linearizability checker a real choice of orders.
const APPENDERS: u64 = 2;

/// Records each appender sends. The ledger stays small enough that one Fetch
/// and one `--max-messages` run read the whole of it.
const RECORDS_PER_APPENDER: u64 = 4;

/// The diskless WAL quorum this suite runs: three voters, so the loss of one
/// still leaves a strict majority.
const VOTERS: usize = 3;

/// The one password every SASL PLAIN principal in the suite authenticates
/// with. Only the inter-broker listener asks for it; the clients use the
/// plaintext listener.
const PASSWORD: &str = "diskless-jepsen";

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
#[ignore = "requires Docker; Linux-bound (host.docker.internal bridge)"]
async fn three_broker_fault_schedule_preserves_the_acked_ledger() {
    let object_dir = TempDir::new().expect("shared object dir");
    let put_blocker = faults::block_object_puts(object_dir.path());

    let mut cluster = cluster::start_jepsen_cluster(object_dir.path()).await;
    cluster::await_brokers_registered(&cluster).await;
    cluster::create_diskless_topic(&cluster[0].client_bootstrap()).await;
    cluster::await_topic_placement(&cluster).await;

    // Make one exact broker both the accepting partition leader and the
    // controller leader. Killing it therefore exercises the accepting-broker
    // fault and a sequencer/controller-authority handoff in the same bounded
    // schedule, while two of three voters remain live.
    let old_controller = cluster::converged_controller_leader(&cluster).await;
    let victim = cluster::index_of(&cluster, old_controller);
    let all: Vec<usize> = (0..cluster.len()).collect();
    let survivors: Vec<usize> = all.iter().copied().filter(|i| *i != victim).collect();
    cluster::force_partition_owner(&cluster, &all, 0, old_controller).await;
    cluster::await_wal_runtime(&cluster[victim], old_controller).await;
    cluster::assert_sole_classic_owner(&cluster, victim, &survivors, old_controller).await;

    let put_failures_before = cluster[victim]
        .handle()
        .diskless_put_failure_count_for_test();
    let ledger = history::produce_concurrently(&cluster[victim].client_bootstrap()).await;
    history::assert_acked_ledger(&ledger);
    history::assert_linearizable_history(&ledger);
    let durable_end = history::durable_end(&ledger);

    faults::await_put_failure(&cluster[victim], put_failures_before).await;
    faults::assert_puts_still_blocked(object_dir.path(), &put_blocker);

    // The acked prefix has to be on the two non-replica voters' disks before
    // the victim's copy is taken away, or the readback proves nothing.
    cluster[victim]
        .handle()
        .wait_until_local_log_end_offset(TOPIC, 0, durable_end)
        .await;
    for &index in &survivors {
        faults::await_follower_checkpoint(&cluster[index], durable_end).await;
    }

    faults::crash_and_erase(&mut cluster, victim).await;

    let new_controller =
        faults::converged_new_controller(&cluster, &survivors, old_controller).await;
    let promoted = cluster::index_of(&cluster, new_controller);
    cluster::force_partition_owner(&cluster, &survivors, promoted, new_controller).await;
    cluster::await_promoted_leader(&cluster[promoted], new_controller, durable_end).await;

    // The object tier is still unavailable here. A successful readback
    // therefore proves that the acknowledged tail survived on the remaining
    // WAL quorum, rather than being rescued by a completed object flush.
    assert!(put_blocker.is_file());
    readback::assert_rust_readback(&cluster[survivors[0]].client_bootstrap(), &ledger).await;

    // The failed pre-crash attempt belonged to the victim's flusher. Before
    // unblocking the store, require another failure after the handoff, so the
    // replacement leader proves that its own retry loop is live and owns the
    // durable tail.
    let put_failures_after_crash = cluster[promoted]
        .handle()
        .diskless_put_failure_count_for_test();
    faults::report_flush_state(&cluster[promoted], put_failures_after_crash).await;
    let put_failures =
        faults::await_put_failure(&cluster[promoted], put_failures_after_crash).await;

    // Let the failed PUT retry only after the no-acked-loss assertion. A real
    // `.ckwl` object is the recovery witness; removing the blocker alone is
    // not enough.
    faults::unblock_object_puts(object_dir.path(), &put_blocker);
    faults::await_wal_object(object_dir.path(), cluster[promoted].config.broker_id).await;

    readback::assert_jvm_differential(&cluster[survivors[0]].docker_bootstrap(), &ledger).await;

    let witness = FaultWitness {
        put_failures,
        lost_wal_node: Some(old_controller),
        old_controller: Some(old_controller),
        new_controller: Some(new_controller),
        old_partition_leader: Some(old_controller),
        new_partition_leader: Some(new_controller),
        object_retry_succeeded: true,
        rust_ledger_checked: true,
        jvm_differential_checked: true,
    };
    faults::assert_complete_witness(&witness, put_failures_after_crash);
    eprintln!("[diskless_jepsen_witness] {witness:?}");

    cluster::shutdown(cluster).await;
}
