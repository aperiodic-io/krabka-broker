//! Reading the restored partitions back: every batch the archive held, at the
//! absolute offset it held it at.
//!
//! This is the claim the whole round trip exists to make, and it is checked
//! with a fresh `krabka_log::Log` over the restored directory rather than
//! through any restore-side value.

use assert2::check;
use krabka_ids::Offset;
use krabka_log::{Log, LogConfig, name};
use krabka_restore::restore;

use crate::{args::restore_args, fixture::build_fixture};

/// 2. Read back the restored data: every partition's log holds exactly the
/// batches this fixture archived for it, each at its original absolute
/// offset (part of the whole-`RecordBatch` equality below, since
/// `base_offset` is a field of it), and starts where the archive starts
/// rather than at zero.
#[tokio::test]
async fn restored_partitions_read_back_the_original_batches_at_their_original_offsets() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let args = restore_args(
        fixture.archive_root.path(),
        &log_dir,
        crate::args::RestoreOptions::default(),
    );

    restore(&args).await.expect("restore");

    for partition in fixture.partitions() {
        let dir = name::partition_dir(&log_dir, partition.topic, partition.partition);
        let log = Log::open(&dir, LogConfig::default()).expect("reopen restored partition");
        // Reading from the partition's own first archived offset rather than
        // from zero: `payments-1`'s archive begins above zero, and `Log::read`
        // raises `OffsetTooLow` below the log start rather than clamping.
        check!(
            log.log_start_offset() == Offset(partition.base_offset()),
            "{}-{}",
            partition.topic,
            partition.partition,
        );
        let read = log
            .read(
                Offset(partition.base_offset()),
                LogConfig::default().segment_size,
            )
            .expect("read restored partition");
        check!(
            read.batches == partition.expected_batches(),
            "{}-{}",
            partition.topic,
            partition.partition,
        );
    }
}
