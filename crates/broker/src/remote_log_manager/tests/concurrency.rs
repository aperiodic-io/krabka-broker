//! One tick, two partitions, one of them stuck: what the copier bound buys.
//!
//! The sweep used to walk the partition snapshot serially, so a partition
//! whose upload hung held every partition behind it for as long as the object
//! store cared to hold the connection -- and, because local retention only
//! evicts what the tier already holds, their disks kept filling while they
//! waited. These cases pin the property that replaces that walk: a copy that
//! is parked is one copier slot, not the whole tick.

use {
    super::*,
    crate::remote_log_manager::test_support as fixtures,
    assert2::check,
    fixtures::rolled_tiered_partition_at,
    krabka_remote_storage::{
        CustomMetadata, LogSegmentData, RemoteLogSegmentMetadata, RemoteStorageError,
    },
    std::{
        sync::{
            Condvar, Mutex,
            atomic::{AtomicBool, AtomicUsize},
        },
        time::Instant,
    },
};

/// How long a parked copy waits for the other partition before it gives up.
///
/// It is a failure deadline, not a delay: on a sweep that reaches both
/// partitions the gate opens in microseconds. It exists so a regression to a
/// serial sweep fails this suite in a second instead of hanging it forever.
const GATE_WAIT: Duration = Duration::from_secs(2);

/// The partition whose copies park.
const BLOCKED_PARTITION: i32 = 0;

/// What a [`TestRsm`] does when a copy arrives.
enum CopyBehaviour {
    /// Park every copy of [`BLOCKED_PARTITION`] until some *other*
    /// partition's copy has finished, and answer every other partition at
    /// once.
    ///
    /// The park is a blocking wait inside `copy_log_segment_data`, which is
    /// exactly where a stalled object store blocks: the copy path hands the
    /// SPI to the blocking pool, so nothing in the sweep can make progress on
    /// that partition until the call returns.
    Gate,
    /// Accept every copy after [`COPY_DWELL`], so copies the sweep starts
    /// together are in the store together.
    Dwell,
}

/// How long a dwelling copy is held. Long enough that two copies the sweep
/// starts together overlap on any machine, short enough that a tick over four
/// partitions is still quick.
const COPY_DWELL: Duration = Duration::from_millis(100);

/// The remote store both cases run against: one implementation, because the
/// three SPI methods a copy pass never calls are the same however a copy
/// behaves.
struct TestRsm {
    behaviour: CopyBehaviour,
    /// Set once a copy for a partition other than [`BLOCKED_PARTITION`] has
    /// finished.
    gate: Mutex<bool>,
    opened: Condvar,
    /// Whether a parked copy ever gave up waiting, which is what a serial
    /// sweep leaves behind: nothing else can run to open the gate.
    gave_up: AtomicBool,
    /// Copies the store completed for partitions that were never parked.
    unblocked_copies: AtomicUsize,
    in_flight: AtomicUsize,
    /// The most copies the store ever held at one time.
    peak_in_flight: AtomicUsize,
}

impl TestRsm {
    fn new(behaviour: CopyBehaviour) -> Self {
        Self {
            behaviour,
            gate: Mutex::new(false),
            opened: Condvar::new(),
            gave_up: AtomicBool::new(false),
            unblocked_copies: AtomicUsize::new(0),
            in_flight: AtomicUsize::new(0),
            peak_in_flight: AtomicUsize::new(0),
        }
    }
}

#[krabka_macros::missing_remote_reads(delete_ok)]

impl RemoteStorageManager for TestRsm {
    fn copy_log_segment_data(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        _data: &LogSegmentData,
    ) -> Result<Option<CustomMetadata>, RemoteStorageError> {
        match self.behaviour {
            CopyBehaviour::Gate => {
                if metadata
                    .remote_log_segment_id()
                    .topic_id_partition
                    .partition
                    == BLOCKED_PARTITION
                {
                    let open = self.gate.lock().expect("gate mutex poisoned");
                    let (_open, wait) = self
                        .opened
                        .wait_timeout_while(open, GATE_WAIT, |open| !*open)
                        .expect("gate mutex poisoned");
                    if wait.timed_out() {
                        self.gave_up.store(true, Ordering::Relaxed);
                        return Err(RemoteStorageError::Io(std::io::Error::other(
                            "no other partition copied while this one was parked",
                        )));
                    }
                    return Ok(None);
                }
                self.unblocked_copies.fetch_add(1, Ordering::Relaxed);
                *self.gate.lock().expect("gate mutex poisoned") = true;
                self.opened.notify_all();
                Ok(None)
            }
            CopyBehaviour::Dwell => {
                let now = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                self.peak_in_flight.fetch_max(now, Ordering::SeqCst);
                std::thread::sleep(COPY_DWELL);
                self.in_flight.fetch_sub(1, Ordering::SeqCst);
                Ok(None)
            }
        }
    }
}

/// The `orders` topic with `partitions` partitions, so a tick has more than
/// one partition to reach.
use fixtures::orders_image as image_with_orders_partitions;

/// `orders-index`, led by this broker, with sealed segments the tier lacks.
fn tiered_partition_at(index: i32, log_dir: &std::path::Path) -> Arc<Partition> {
    rolled_tiered_partition_at(PartitionIndex(index), log_dir, LogConfig {
        segment_size: bytes(256),
        remote_storage_enable: true,
        retention: None,
        retention_size: None,
        ..LogConfig::default()
    })
}

/// The head-of-line block this issue is about. Partition 0's copy parks
/// inside the object store; partition 1's copy has to finish anyway, in the
/// same tick, without waiting for it.
///
/// The gate is what makes that a claim about ordering rather than about
/// wall-clock timing: partition 0 cannot return until partition 1 has copied,
/// so the tick can only end at all if the two ran concurrently. A serial
/// sweep leaves `gave_up` set, because nothing else could run.
#[tokio::test(flavor = "multi_thread")]
async fn a_parked_copy_does_not_hold_the_next_partitions_copy() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let partitions = PartitionRegistry::new();
    partitions.insert(
        "orders".into(),
        PartitionIndex(0),
        tiered_partition_at(0, first_dir.path()),
    );
    partitions.insert(
        "orders".into(),
        PartitionIndex(1),
        tiered_partition_at(1, second_dir.path()),
    );

    let gated = Arc::new(TestRsm::new(CopyBehaviour::Gate));
    let rsm: Arc<dyn RemoteStorageManager> = gated.clone();
    let rlmm = fixtures::in_memory_metadata();
    let controller = fixed_source(image_with_orders_partitions(2));

    let started = Instant::now();
    fixtures::sweep_mutable(&partitions, &controller, &rsm, &rlmm).await;
    let elapsed = started.elapsed();

    check!(
        !gated.gave_up.load(Ordering::Relaxed),
        "a parked copy waited out the whole tick: no other partition was copied while it sat \
         in the store"
    );
    check!(
        elapsed < GATE_WAIT,
        "the tick took {elapsed:?}, so the parked partition held it"
    );
    check!(
        gated.unblocked_copies.load(Ordering::Relaxed) >= 1,
        "the second partition never reached the store"
    );
    // Both partitions finished: the parked one resumes the moment the gate
    // opens, so a concurrent sweep costs it nothing but the wait.
    fixtures::check_partitions_copied(&rlmm, [0, 1]);
}

/// The bound itself. Four partitions and two copier slots: the store may
/// never see more than two copies at once, and it must see two, so the value
/// the config carries is the number the sweep uses and not a hard-coded one.
#[tokio::test(flavor = "multi_thread")]
async fn the_copier_bound_caps_the_copies_in_flight() {
    let dirs: Vec<tempfile::TempDir> = (0..4).map(|_| tempfile::tempdir().unwrap()).collect();
    let partitions = PartitionRegistry::new();
    for (index, dir) in dirs.iter().enumerate() {
        let index = i32::try_from(index).expect("four partitions fit in i32");
        partitions.insert(
            "orders".into(),
            PartitionIndex(index),
            tiered_partition_at(index, dir.path()),
        );
    }

    let counting = Arc::new(TestRsm::new(CopyBehaviour::Dwell));
    let rsm: Arc<dyn RemoteStorageManager> = counting.clone();
    let rlmm = fixtures::in_memory_metadata();
    let controller = fixed_source(image_with_orders_partitions(4));

    tick_all(
        &partitions,
        &controller,
        &tier(ArchiveMode::Mutable, &rsm, &rlmm),
        NodeId(1),
        1,
        SweepConcurrency {
            copier: 2,
            expiration: 2,
        },
    )
    .await;

    let peak = counting.peak_in_flight.load(Ordering::Relaxed);
    check!(
        peak <= 2,
        "the sweep had {peak} copies in flight under a bound of 2"
    );
    check!(
        peak == 2,
        "the sweep used only {peak} of its two copier slots"
    );
    fixtures::check_partitions_copied(&rlmm, 0..4);
}
