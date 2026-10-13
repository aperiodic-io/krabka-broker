//! The faults themselves, and the witness that says each one really landed.
//!
//! There are three faults, and they are injected in this order.
//!
//! 1. **The object store is unavailable.** `RemoteStorageBackend::Local`
//!    writes under `<dir>/diskless-wal/<broker>/<uuid>.ckwl`, so a plain file
//!    where that directory belongs makes every PUT fail at the filesystem.
//!    The witness is the flusher's own failure counter, sampled before and
//!    after, not the presence of the file.
//! 2. **The acking broker is lost, canonical log and all.** It is killed
//!    without a controlled shutdown, then its `<topic>-<partition>` directory
//!    is erased. The file count is taken before the erase, so an empty
//!    directory cannot pass for a node-loss injection.
//! 3. **The controller authority moves.** The dead broker led the controller
//!    as well, so the survivors have to elect a new one before the partition
//!    can be reassigned. The witness is that the two leaders differ.
//!
//! [`assert_complete_witness`] is the guard on all of it: every field has to
//! be filled in, and the two leader pairs have to name different nodes. A
//! schedule that silently skipped a fault fails there rather than passing.

use {
    crate::{TOPIC, cluster::TestNode},
    assert2::assert,
    krabka_broker::NodeId,
    std::{
        path::{Path, PathBuf},
        time::Duration,
    },
};

/// Name of the follower's durable-offset checkpoint, from
/// `wal::quorum::follower::checkpoint`.
const DURABLE_OFFSET_FILE: &str = "wal-durable-offset.checkpoint";

/// The checkpoint's first line, its version, from
/// `wal::quorum::follower::checkpoint::DURABLE_OFFSET_VERSION`.
const CHECKPOINT_VERSION: &str = "0";

/// The `[start, end]` on the second line of a durable-offset checkpoint, or
/// `None` unless its first line is the version, `0`, and its second is two
/// offsets. A half-written file reads as `None`, and the caller polls again.
fn checkpoint_offsets(raw: &str) -> Option<[i64; 2]> {
    let mut lines = raw.lines();
    if lines.next()? != CHECKPOINT_VERSION {
        return None;
    }
    let mut offsets = lines
        .next()?
        .split_ascii_whitespace()
        .map(str::parse::<i64>);
    let range = [offsets.next()?.ok()?, offsets.next()?.ok()?];
    offsets.next().is_none().then_some(range)
}

/// What each fault left behind. The suite fills this in as it goes and checks
/// it at the end, so a fault that turned into a no-op cannot pass unnoticed.
#[derive(Debug, Default)]
pub(crate) struct FaultWitness {
    pub(crate) put_failures: u64,
    pub(crate) lost_wal_node: Option<NodeId>,
    pub(crate) old_controller: Option<NodeId>,
    pub(crate) new_controller: Option<NodeId>,
    pub(crate) old_partition_leader: Option<NodeId>,
    pub(crate) new_partition_leader: Option<NodeId>,
    pub(crate) object_retry_succeeded: bool,
    pub(crate) rust_ledger_checked: bool,
    pub(crate) jvm_differential_checked: bool,
}

/// Make every diskless object PUT fail, and return the path of the blocker.
///
/// The blocker is an ordinary file at the key prefix the flusher writes under.
/// Creating `<dir>/diskless-wal/<broker>/` then fails with `NotADirectory`,
/// which is a real store error on a real store rather than an injected one in
/// a stub.
pub(crate) fn block_object_puts(object_dir: &Path) -> PathBuf {
    let blocker = object_dir.join("diskless-wal");
    std::fs::write(&blocker, b"force object PUT to fail").expect("install the PUT blocker");
    blocker
}

/// Remove the blocker and create the namespace the flusher needs, so its next
/// retry can succeed.
pub(crate) fn unblock_object_puts(object_dir: &Path, blocker: &Path) {
    std::fs::remove_file(blocker).expect("remove the PUT blocker");
    std::fs::create_dir(object_dir.join("diskless-wal"))
        .expect("create the object namespace after the fault");
}

/// The blocker is still in place and nothing got past it.
pub(crate) fn assert_puts_still_blocked(object_dir: &Path, blocker: &Path) {
    assert!(
        blocker.is_file(),
        "PUT failure witness disappeared before the WAL-loss fault"
    );
    assert!(
        !object_dir.join("diskless-wal").is_dir(),
        "a blocked PUT unexpectedly created its object namespace"
    );
}

/// Wait until `node`'s flusher records a PUT failure later than `before`, and
/// return the new count.
///
/// This reads the flusher's own counter, so it proves the failure reached the
/// real retry loop rather than proving only that the blocker file exists.
pub(crate) async fn await_put_failure(node: &TestNode, before: u64) -> u64 {
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let current = node.handle().diskless_put_failure_count_for_test();
            if current > before {
                return current;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a real diskless object PUT failure was never observed")
}

/// Print the replacement leader's flusher inputs once, so a timeout in the
/// wait that follows is readable without a rerun.
pub(crate) async fn report_flush_state(node: &TestNode, failures: u64) {
    eprintln!(
        "[diskless_jepsen_replacement_flusher] node={} state={:?} failures={failures}",
        node.config.node_id,
        node.handle().diskless_flush_state_for_test(TOPIC, 0).await
    );
}

/// Wait until this broker's WAL voter directory checkpoints exactly
/// `[0, expected_end)` as fsynced.
///
/// The layout mirrors `wal::quorum::shard_dirs` and
/// `wal::quorum::follower::log`:
///
/// ```text
/// <log.dir>/__diskless_wal_quorum/<topic>-<topic-id>-<partition>/voter-<node-id>/
///     wal-durable-offset.checkpoint    "0\n<start> <end>\n": the version line, then the
///                                      range, fsynced after each append
/// ```
///
/// The shard directory carries the topic id, which this suite never resolves,
/// so the scan matches on the topic-name prefix instead of building the name.
///
/// Requiring the exact range rather than `end >= expected_end` is what makes
/// this a durability assertion: the voter says it holds the acknowledged
/// prefix and nothing beyond it.
pub(crate) async fn await_follower_checkpoint(node: &TestNode, expected_end: i64) {
    let root = node.config.log_dir.join("__diskless_wal_quorum");
    let voter = format!("voter-{}", node.config.node_id.0);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(entries) = std::fs::read_dir(&root) {
                for entry in entries.flatten() {
                    let checkpoint = entry.path().join(&voter).join(DURABLE_OFFSET_FILE);
                    if entry.file_name().to_string_lossy().starts_with(TOPIC)
                        && let Ok(value) = std::fs::read_to_string(checkpoint)
                        && checkpoint_offsets(&value) == Some([0, expected_end])
                    {
                        return;
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("a non-replica WAL voter did not checkpoint the acknowledged prefix");
}

/// Kill the acking broker and erase its canonical partition log.
///
/// The file count is read first. An already-empty directory would make the
/// erase a no-op, and the readback that follows would then prove nothing about
/// where the bytes came from.
pub(crate) async fn crash_and_erase(cluster: &mut [TestNode], victim: usize) {
    let log = cluster[victim].partition_dir();
    assert!(
        recursive_file_count(&log) > 0,
        "the victim's canonical log was empty; node-loss injection would be a no-op"
    );
    cluster[victim]
        .handle
        .take()
        .expect("victim live")
        .crash_for_test()
        .await;
    std::fs::remove_dir_all(&log).expect("erase the exact victim canonical log");
    assert!(
        !log.exists(),
        "the victim canonical log still exists after node-loss injection"
    );
}

/// Wait until every survivor elects the same new controller leader, and return
/// it.
pub(crate) async fn converged_new_controller(
    cluster: &[TestNode],
    survivors: &[usize],
    old: NodeId,
) -> NodeId {
    let leader = await_new_controller(&cluster[survivors[0]], old).await;
    for &index in &survivors[1..] {
        let observed = await_new_controller(&cluster[index], old).await;
        assert!(
            observed == leader,
            "survivors did not converge on one controller: {leader} vs {observed}"
        );
    }
    leader
}

async fn await_new_controller(node: &TestNode, old: NodeId) -> NodeId {
    let mut leaders = node.handle().watch_leader_for_test();
    tokio::time::timeout(
        Duration::from_secs(30),
        leaders
            .wait_for(|leader| leader.is_some_and(|leader| leader != old && leader != NodeId(0))),
    )
    .await
    .expect("the controller did not hand off after leader loss")
    .expect("the controller leader watch closed")
    .to_owned()
    .expect("the predicate requires a leader")
}

/// Wait until a real `.ckwl` object appears under `broker_id`'s prefix.
///
/// Removing the blocker is not the recovery witness; a committed object is.
pub(crate) async fn await_wal_object(object_dir: &Path, broker_id: i32) {
    let namespace = object_dir.join("diskless-wal").join(broker_id.to_string());
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            let landed = std::fs::read_dir(&namespace).is_ok_and(|entries| {
                entries.flatten().any(|entry| {
                    entry.path().is_file()
                        && entry
                            .path()
                            .extension()
                            .is_some_and(|extension| extension == "ckwl")
                })
            });
            if landed {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("the diskless WAL object PUT did not recover after the blocker was removed");
}

/// Every fault landed, and the two handoffs moved authority to another node.
pub(crate) fn assert_complete_witness(witness: &FaultWitness, put_failures_before: u64) {
    assert!(witness.put_failures > put_failures_before);
    assert!(witness.lost_wal_node.is_some());
    assert!(witness.old_controller.is_some());
    assert!(witness.new_controller.is_some());
    assert!(witness.old_controller != witness.new_controller);
    assert!(witness.old_partition_leader.is_some());
    assert!(witness.new_partition_leader.is_some());
    assert!(witness.old_partition_leader != witness.new_partition_leader);
    assert!(witness.lost_wal_node == witness.old_partition_leader);
    assert!(witness.object_retry_succeeded);
    assert!(witness.rust_ledger_checked);
    assert!(witness.jvm_differential_checked);
}

fn recursive_file_count(path: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir(path) else {
        return usize::from(path.is_file());
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            if path.is_dir() {
                recursive_file_count(&path)
            } else {
                usize::from(path.is_file())
            }
        })
        .sum()
}
