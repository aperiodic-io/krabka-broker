//! Exercise real appends against a disk-sync gate, not a timing-only workload.

use {
    crate::{
        Log, LogConfig, LogError, LogIo, Segment,
        log::test_support::{
            append_samples, install_stamps, sample_batch, tiny_segments, verbatim_from,
        },
        name,
        producer_snapshot::{self, ProducerSnapshotEntry},
        test_support::open_log,
    },
    assert2::assert,
    krabka_ids::{Offset, ProducerId},
    krabka_units::bytes,
    std::{
        collections::HashSet,
        fs::File,
        io,
        sync::{Arc, Condvar, Mutex, mpsc},
        time::Duration,
    },
    tempfile::tempdir,
};

fn check_boundary_snapshot(
    log: &Log,
    directory: &std::path::Path,
    expected: &[ProducerSnapshotEntry],
) {
    let copy = tempdir().unwrap();
    std::fs::copy(
        name::producer_snapshot_path(directory, 1),
        name::producer_snapshot_path(copy.path(), 1),
    )
    .unwrap();
    let (_, state) = producer_snapshot::reload(copy.path(), log.producer_reload_range(Offset(1)))
        .unwrap()
        .unwrap();
    assert!(state.into_values().collect::<Vec<_>>() == expected);
}

#[derive(Debug)]
struct GatedIo {
    started: Mutex<Option<mpsc::Sender<std::thread::ThreadId>>>,
    released: Mutex<bool>,
    changed: Condvar,
    fail: bool,
}

impl GatedIo {
    fn new(fail: bool) -> (Arc<Self>, mpsc::Receiver<std::thread::ThreadId>) {
        let (tx, rx) = mpsc::channel();
        (
            Arc::new(Self {
                started: Mutex::new(Some(tx)),
                released: Mutex::new(false),
                changed: Condvar::new(),
                fail,
            }),
            rx,
        )
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}

impl LogIo for GatedIo {
    fn sync_data(&self, file: &File) -> io::Result<()> {
        let started = self.started.lock().unwrap().take();
        if let Some(started) = started {
            let _ = started.send(std::thread::current().id());
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.changed.wait(released).unwrap();
            }
            if self.fail {
                return Err(io::ErrorKind::StorageFull.into());
            }
        }
        file.sync_data()
    }
}

fn first_append(dir: &std::path::Path, strict: bool) -> Log {
    let mut log = Log::open(dir, LogConfig {
        flush_on_append: strict,
        retention: None,
        ..tiny_segments()
    })
    .unwrap();
    let mut first = sample_batch(1);
    first.producer_id = 41;
    first.producer_epoch = 0;
    first.base_sequence = 0;
    log.append(&mut first).unwrap();
    // Disk gates must not occupy the process-wide pool shared by other tests.
    log.rollover_flusher.executor =
        Some(super::executor::Executor::new(2, 256, 64 * 1024 * 1024).unwrap());
    log
}

fn install_gate(
    log: &mut Log,
    fail: bool,
) -> (Arc<GatedIo>, mpsc::Receiver<std::thread::ThreadId>) {
    let (gate, started) = GatedIo::new(fail);
    log.test_set_io(gate.clone());
    (gate, started)
}

fn await_progress<T>(receiver: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvTimeoutError> {
    receiver.recv_timeout(Duration::from_secs(5))
}

fn observe_pending<T>(receiver: &mpsc::Receiver<T>) -> Result<T, mpsc::RecvTimeoutError> {
    receiver.recv_timeout(Duration::from_millis(100))
}

fn spawn_writer<T: Send + 'static>(
    mut log: Log,
    write: impl FnOnce(&mut Log) -> T + Send + 'static,
) -> (std::thread::JoinHandle<Log>, mpsc::Receiver<T>) {
    let (sent, received) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        sent.send(write(&mut log)).unwrap();
        log
    });
    (writer, received)
}

fn partition_with_executor(
    executor: impl FnOnce() -> Arc<super::executor::Executor>,
) -> (tempfile::TempDir, Log) {
    let directory = tempdir().unwrap();
    let mut log = first_append(directory.path(), false);
    log.rollover_flusher.executor = Some(executor());
    (directory, log)
}

fn check_flushed_boundary(log: &Log, directory: &std::path::Path) {
    assert!(log.log_end_offset() == Offset(2));
    assert!(name::producer_snapshot_path(directory, 1).exists());
}

fn begin_gated_rollover(
    log: &mut Log,
) -> (
    Arc<GatedIo>,
    Result<std::thread::ThreadId, mpsc::RecvTimeoutError>,
) {
    let (gate, started) = install_gate(log, false);
    log.append(&mut sample_batch(1)).unwrap();
    let began = await_progress(&started);
    (gate, began)
}

fn join_blocked_writer<T, U>(
    gate: &GatedIo,
    writer: std::thread::JoinHandle<T>,
    began: Result<std::thread::ThreadId, mpsc::RecvTimeoutError>,
    pending: &Result<U, mpsc::RecvTimeoutError>,
) -> T {
    gate.release();
    let log = writer.join().unwrap();
    began.unwrap();
    assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
    log
}

#[test]
fn buffered_rollovers_allow_appends_reads_and_idle_maintenance_during_disk_sync() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let first_state = log.producer_state_snapshot();
    let (gate, started) = install_gate(&mut log, false);
    let (tx, rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        for sequence in [1, 2] {
            let mut batch = sample_batch(1);
            batch.producer_id = 41;
            batch.producer_epoch = 0;
            batch.base_sequence = sequence;
            log.append(&mut batch).unwrap();
        }
        log.tick(std::time::SystemTime::now(), Offset(3)).unwrap();
        assert!(log.read(Offset(0), bytes(4096)).unwrap().batches.len() == 3);
        tx.send(log).unwrap();
    });
    let began = await_progress(&started);
    let returned = rx.recv_timeout(Duration::from_secs(2));
    // Always release before asserting, so the synchronous negative control
    // fails without leaving a worker or Drop waiting forever.
    let unpublished = !name::producer_snapshot_path(dir.path(), 1).exists();
    let untierable = returned
        .as_ref()
        .is_ok_and(|log| log.tierable_segments().is_empty());
    gate.release();
    writer.join().unwrap();
    began.unwrap();
    let mut log = returned.expect("buffered roll must not wait for disk sync");
    assert!(unpublished, "the snapshot must wait for its record flush");
    assert!(untierable, "tiering must wait for the boundary snapshot");
    log.sync().unwrap();
    check_boundary_snapshot(&log, dir.path(), &first_state);
    assert!(
        log.producer_state_entry(ProducerId(41))
            .unwrap()
            .last_sequence
            == 2
    );
    assert!(log.tierable_segments().len() == 2);
    drop(log);
    let recovered = open_log(dir.path());
    assert!(recovered.log_end_offset() == Offset(3));
    assert!(
        recovered
            .producer_state_entry(ProducerId(41))
            .unwrap()
            .last_sequence
            == 2
    );
}

#[test]
fn stamped_appends_wait_for_earlier_rollovers_in_both_append_paths() {
    for verbatim in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let (gate, began) = begin_gated_rollover(&mut log);
        install_stamps(&mut log, 100, 1);
        let (writer, rx) = spawn_writer(log, move |log| {
            if verbatim {
                let (_, batch) = verbatim_from(&sample_batch(1), krabka_ids::LeaderEpoch(1));
                log.append_verbatim(&batch).unwrap();
            } else {
                log.append(&mut sample_batch(1)).unwrap();
            }
            log.stamp_for_offset(Offset(2))
        });
        let pending = observe_pending(&rx);
        let log = join_blocked_writer(&gate, writer, began, &pending);
        assert!(rx.recv().unwrap() == Some(100));
        assert!(name::producer_snapshot_path(dir.path(), 1).exists());
        assert!(log.log_end_offset() == Offset(3));
    }
}

#[test]
fn slow_disk_bounds_the_rollover_queue_without_losing_boundary_state() {
    for (max_jobs, budget) in [
        (256, 3 * producer_snapshot::allocation_size(1).unwrap()),
        (3, 64 * 1024 * 1024),
    ] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        // Bound job counts and bytes independently, including the running
        // snapshot: only two more rolls may be queued.
        log.rollover_flusher.executor =
            Some(super::executor::Executor::new(2, max_jobs, budget).unwrap());
        let (gate, began) = begin_gated_rollover(&mut log);
        let (tx, rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            for offset in 2..=4 {
                log.append(&mut sample_batch(1)).unwrap();
                tx.send(offset).unwrap();
            }
            log
        });
        let queued: Vec<_> = (2..=3).map(|_| await_progress(&rx)).collect();
        let pending = observe_pending(&rx);
        gate.release();
        let mut log = writer.join().unwrap();
        began.unwrap();
        assert!(
            queued.into_iter().collect::<Result<Vec<_>, _>>().unwrap()
                == (2..=3).collect::<Vec<_>>()
        );
        assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(rx.recv().unwrap() == 4);
        log.sync().unwrap();
        assert!(log.log_end_offset() == Offset(5));
        assert!(producer_snapshot::list(dir.path()).unwrap().len() == 4);
        assert!(log.tierable_segments().len() == 4);
    }
}

#[test]
fn strict_rollover_waits_for_disk_before_acknowledging() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), true);
    let (gate, started) = install_gate(&mut log, false);
    let (tx, rx) = mpsc::channel();
    let writer = std::thread::spawn(move || {
        log.append(&mut sample_batch(1)).unwrap();
        tx.send(log).unwrap();
    });
    let began = await_progress(&started);
    let pending = rx.try_recv();
    gate.release();
    writer.join().unwrap();
    began.unwrap();
    assert!(matches!(pending, Err(mpsc::TryRecvError::Empty)));
    let log = rx.recv().unwrap();
    check_flushed_boundary(&log, dir.path());
}

#[test]
fn failed_background_flush_is_reported_by_sync_and_both_append_paths() {
    let dir = tempdir().unwrap();
    let mut log = first_append(dir.path(), false);
    let (gate, started) = install_gate(&mut log, true);
    log.append(&mut sample_batch(1)).unwrap();
    let began = await_progress(&started);
    log.append(&mut sample_batch(1)).unwrap();
    gate.release();
    began.unwrap();
    assert!(matches!(log.sync(), Err(LogError::Io(e)) if e.kind() == io::ErrorKind::StorageFull));
    assert!(!name::producer_snapshot_path(dir.path(), 1).exists());
    assert!(!name::producer_snapshot_path(dir.path(), 2).exists());
    assert!(matches!(
        log.append(&mut sample_batch(1)),
        Err(LogError::Io(_))
    ));
    let (_, batch) = verbatim_from(&sample_batch(1), krabka_ids::LeaderEpoch(1));
    assert!(matches!(log.append_verbatim(&batch), Err(LogError::Io(_))));
}

#[test]
fn reset_and_truncation_wait_and_cannot_recreate_stale_snapshots() {
    for reset in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let (gate, began) = begin_gated_rollover(&mut log);
        let (writer, rx) = spawn_writer(log, move |log| {
            if reset {
                log.reset_to(Offset(0))
            } else {
                log.truncate_to(Offset(0))
            }
        });
        let pending = observe_pending(&rx);
        let log = join_blocked_writer(&gate, writer, began, &pending);
        rx.recv().unwrap().unwrap();
        assert!(log.log_end_offset() == Offset(0));
        assert!(producer_snapshot::list(dir.path()).unwrap().is_empty());
        drop(log);
        let recovered = open_log(dir.path());
        assert!(recovered.producer_state_snapshot().is_empty());
    }
}

#[test]
fn many_partitions_share_a_fixed_worker_pool() {
    let shared = super::executor::Executor::shared().unwrap();
    for _ in 0..24 {
        let mut flusher = super::Flusher::default();
        let permit = flusher.reserve(10).unwrap();
        assert!(Arc::ptr_eq(flusher.executor.as_ref().unwrap(), &shared));
        drop(permit);
    }
    // Isolate disk gates from other tests, using the same executor constructor.
    let executor = super::executor::Executor::new(4, 256, 64 * 1024 * 1024).unwrap();
    let mut partitions = Vec::new();
    for _ in 0..24 {
        let (dir, mut log) = partition_with_executor(|| executor.clone());
        let (gate, started) = install_gate(&mut log, false);
        log.append(&mut sample_batch(1)).unwrap();
        partitions.push((dir, log, gate, started));
    }
    let started: Vec<_> = partitions[..4]
        .iter()
        .map(|(_, _, _, started)| await_progress(started))
        .collect();
    let pending: Vec<_> = partitions[4..]
        .iter()
        .map(|(_, _, _, started)| started.recv_timeout(Duration::from_millis(10)))
        .collect();
    for (_, _, gate, _) in &partitions {
        gate.release();
    }
    let mut threads: HashSet<_> = started.into_iter().map(Result::unwrap).collect();
    assert!(
        pending
            .iter()
            .all(|result| matches!(result, Err(mpsc::RecvTimeoutError::Timeout)))
    );
    for (_, log, _, _) in &mut partitions {
        log.sync().unwrap();
    }
    for (_, _, _, started) in &partitions[4..] {
        threads.insert(started.recv().unwrap());
    }
    assert!(
        threads.len() == 4,
        "all 24 partitions must use the same four native workers"
    );
    for (dir, log, _, _) in &partitions {
        assert!(name::producer_snapshot_path(dir.path(), 1).exists());
        assert!(log.tierable_segments().len() == 1);
    }
}

#[test]
fn snapshot_byte_backpressure_is_shared_between_partitions_and_admits_oversized_jobs_exclusively() {
    for budget in [producer_snapshot::allocation_size(1).unwrap(), 1] {
        let executor = super::executor::Executor::new(2, 256, budget).unwrap();
        let (_first_dir, mut first) = partition_with_executor(|| executor.clone());
        let (second_dir, mut second) = partition_with_executor(|| executor);
        let (gate, started) = install_gate(&mut first, false);
        first.append(&mut sample_batch(1)).unwrap();
        let began = await_progress(&started);
        let (tx, rx) = mpsc::channel();
        let (preparing, prepared) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            producer_snapshot::test_observe_prepare(preparing);
            second.append(&mut sample_batch(1)).unwrap();
            tx.send(()).unwrap();
            second
        });
        let pending = observe_pending(&rx);
        let not_prepared = observe_pending(&prepared);
        let uncaptured = !name::producer_snapshot_path(second_dir.path(), 1).exists();
        gate.release();
        began.unwrap();
        let mut second = writer.join().unwrap();
        assert!(matches!(pending, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(uncaptured);
        assert!(
            matches!(not_prepared, Err(mpsc::RecvTimeoutError::Timeout)),
            "backpressure must apply before allocation"
        );
        prepared.recv().unwrap();
        rx.recv().unwrap();
        first.sync().unwrap();
        second.sync().unwrap();
        assert!(first.tierable_segments().len() == 1);
        assert!(second.tierable_segments().len() == 1);
    }
}

#[test]
fn sealed_segment_flush_survives_each_post_roll_setup_failure() {
    for extension in ["log", "txnindex", "stampindex"] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), false);
        let first_state = log.producer_state_snapshot();
        if extension == "stampindex" {
            install_stamps(&mut log, 100, 1);
        }
        let blocked = dir.path().join(format!("{:020}.{extension}", 1));
        std::fs::create_dir(&blocked).unwrap();
        let result = log.append(&mut sample_batch(1));
        // Flush even when setup failed before there was a new active segment.
        log.rollover_flusher.finish().unwrap();
        assert!(matches!(result, Err(LogError::Io(_))), "{extension}");
        check_boundary_snapshot(&log, dir.path(), &first_state);
        if extension == "stampindex" {
            assert!(log.tierable_segments().len() == 1);
            std::fs::remove_dir(&blocked).unwrap();
            // A retry and another roll must not leave a gap in the archive.
            append_samples(&mut log, 2, 1);
            log.sync().unwrap();
            assert!(log.tierable_segments().len() == 2);
        }
    }
}

#[test]
fn synchronous_rollovers_do_not_clone_flush_handles_and_clone_failure_does_not_seal() {
    for strict in [false, true] {
        let dir = tempdir().unwrap();
        let mut log = first_append(dir.path(), strict);
        Segment::test_fail_flush_handles(true);
        let result = log.append(&mut sample_batch(1));
        Segment::test_fail_flush_handles(false);
        if strict {
            result.unwrap();
            check_flushed_boundary(&log, dir.path());
        } else {
            assert!(matches!(result, Err(LogError::Io(_))));
            assert!(!log.active.as_ref().unwrap().is_sealed());
            log.append(&mut sample_batch(1)).unwrap();
            log.sync().unwrap();
            assert!(log.log_end_offset() == Offset(2));
        }
    }
}

#[test]
fn a_busy_partition_yields_between_flushes_without_reordering_its_boundaries() {
    let executor = super::executor::Executor::new(1, 256, 64 * 1024 * 1024).unwrap();
    let (first_dir, mut first) = partition_with_executor(|| executor.clone());
    let (first_gate, first_started) = install_gate(&mut first, false);
    first.append(&mut sample_batch(1)).unwrap();
    let first_began = await_progress(&first_started);
    first.append(&mut sample_batch(1)).unwrap();
    let (_second_dir, mut second) = partition_with_executor(|| executor);
    let (second_gate, second_started) = install_gate(&mut second, false);
    second.append(&mut sample_batch(1)).unwrap();
    first_gate.release();
    let second_began = await_progress(&second_started);
    let first_boundary = name::producer_snapshot_path(first_dir.path(), 1).exists();
    let later_boundary = name::producer_snapshot_path(first_dir.path(), 2).exists();
    second_gate.release();
    first_began.unwrap();
    second_began.unwrap();
    first.sync().unwrap();
    second.sync().unwrap();
    assert!(first_boundary);
    assert!(
        !later_boundary,
        "the second partition must run before the first partition's backlog"
    );
    assert!(first.tierable_segments().len() == 2);
    assert!(second.tierable_segments().len() == 1);
}
