//! Open-time swap recovery, one directory state per interrupted step.

use {
    super::{cleaned_path, swap_orphan_recover, swap_path},
    crate::{
        error::LogError,
        name,
        test_support::{Files, directory_files},
    },
    assert2::check,
    bytes::BytesMut,
    krabka_protocol::records::{Record, RecordBatch},
    std::path::Path,
    tempfile::tempdir,
};

/// The encoded bytes of one batch per `(base, last offset delta)`.
fn batches(spans: &[(i64, i32)]) -> Vec<u8> {
    let mut wire = BytesMut::new();
    for &(base_offset, last_offset_delta) in spans {
        let batch = RecordBatch {
            base_offset,
            last_offset_delta,
            records: (0..=last_offset_delta)
                .map(|offset_delta| Record {
                    offset_delta,
                    value: Some(bytes::Bytes::from(format!("v{base_offset}+{offset_delta}"))),
                    ..Default::default()
                })
                .collect(),
            ..RecordBatch::default()
        };
        batch.encode(&mut wire).unwrap();
    }
    wire.to_vec()
}

fn file(base: i64, ext: &str) -> String { format!("{}.{ext}", name::format_base_offset(base)) }

/// Write `files` into `dir`.
fn populate(dir: &Path, files: &Files) {
    for (file_name, contents) in files {
        std::fs::write(dir.join(file_name), contents).unwrap();
    }
}

fn populated_directory(files: &Files) -> tempfile::TempDir {
    let dir = tempdir().unwrap();
    populate(dir.path(), files);
    dir
}

fn recovered_files(dir: &Path) -> Files {
    swap_orphan_recover(dir).unwrap();
    directory_files(dir)
}

fn check_corrupt_files(files: &Files) {
    let dir = populated_directory(files);
    check!(let Err(LogError::Corrupt(_)) = swap_orphan_recover(dir.path()));
    check!(directory_files(dir.path()) == *files);
}

/// The segments at 0, 10 and 20 as they were before compaction.
fn originals() -> Files {
    let mut files = Files::new();
    for (base, span) in [(0, (0, 9)), (10, (10, 9)), (20, (20, 4))] {
        files.insert(file(base, "log"), batches(&[span]));
        files.insert(file(base, "index"), format!("index {base}").into_bytes());
        files.insert(file(base, "timeindex"), format!("time {base}").into_bytes());
    }
    files.insert(file(0, "txnindex"), b"stale txn 0".to_vec());
    files
}

/// The survivor of compacting 0 and 10: gapped batches ending at offset 19.
fn survivor_log() -> Vec<u8> { batches(&[(3, 0), (12, 7)]) }

/// The directory once the swap of 0 and 10 has finished: the survivor at 0,
/// and the untouched segment at 20.
fn compacted() -> Files {
    let mut files: Files = originals()
        .into_iter()
        .filter(|(file_name, _)| file_name.starts_with(&name::format_base_offset(20)))
        .collect();
    files.insert(file(0, "log"), survivor_log());
    files.insert(file(0, "index"), b"survivor index".to_vec());
    files.insert(file(0, "timeindex"), b"survivor time".to_vec());
    files
}

fn survivor_swaps() -> Files {
    Files::from([
        (file(0, "log.swap"), survivor_log()),
        (file(0, "index.swap"), b"survivor index".to_vec()),
        (file(0, "timeindex.swap"), b"survivor time".to_vec()),
    ])
}

fn originals_with_swaps() -> Files {
    let mut files = originals();
    files.append(&mut survivor_swaps());
    files
}

/// A crash after the sidecars reached `.swap` but before the log did: the
/// swap never committed, so the originals win and every swap file goes.
#[test]
fn an_uncommitted_swap_is_aborted() {
    let dir = tempdir().unwrap();
    let mut files = originals();
    let mut survivor = survivor_swaps();
    let log = survivor.remove(&file(0, "log.swap")).unwrap();
    files.append(&mut survivor);
    files.insert(file(0, "log.cleaned"), log);
    files.insert(file(0, "txnindex.cleaned"), b"survivor txn".to_vec());
    populate(dir.path(), &files);

    check!(recovered_files(dir.path()) == originals());
}

/// A committed swap is completed whichever consumed segments a crash left
/// behind -- including the survivor's own base, which the previous rule read
/// as "the swap never started" and threw the only copy of 10..19 away.
#[test]
fn a_committed_swap_is_completed_whatever_originals_remain() {
    for remaining in [&[0, 10][..], &[0], &[10], &[]] {
        let mut files: Files = originals()
            .into_iter()
            .filter(|(file_name, _)| {
                file_name.starts_with(&name::format_base_offset(20))
                    || remaining
                        .iter()
                        .any(|base| file_name.starts_with(&name::format_base_offset(*base)))
            })
            .collect();
        files.append(&mut survivor_swaps());
        let dir = populated_directory(&files);

        check!(
            recovered_files(dir.path()) == compacted(),
            "remaining {remaining:?}"
        );
    }
}

/// A committed swap's own `.txnindex` replaces the stale one at its base.
#[test]
fn a_committed_swap_promotes_its_transaction_index() {
    let txn_bytes = b"survivor txn".to_vec();
    let mut files = originals_with_swaps();
    files.insert(file(0, "txnindex.swap"), txn_bytes.clone());
    let dir = populated_directory(&files);

    swap_orphan_recover(dir.path()).unwrap();

    let mut expected = compacted();
    expected.insert(file(0, "txnindex"), txn_bytes);
    check!(directory_files(dir.path()) == expected);
}

/// A crash after the log reached its final name: only sidecars remain to
/// promote, and an index with neither form is created empty.
#[test]
fn sidecar_promotion_resumes_after_the_log_rename() {
    let mut files = compacted();
    files.remove(&file(0, "index"));
    files.remove(&file(0, "timeindex"));
    files.insert(file(0, "timeindex.swap"), b"survivor time".to_vec());
    files.insert(file(0, "txnindex.swap"), b"survivor txn".to_vec());
    let dir = populated_directory(&files);

    swap_orphan_recover(dir.path()).unwrap();

    let mut expected = compacted();
    expected.insert(file(0, "index"), Vec::new());
    expected.insert(file(0, "txnindex"), b"survivor txn".to_vec());
    check!(directory_files(dir.path()) == expected);

    check!(
        recovered_files(dir.path()) == expected,
        "recovery is idempotent"
    );
}

/// A `.cleaned` file with no swap is a rewrite that never finished.
#[test]
fn stray_cleaned_files_are_deleted() {
    let mut files = originals();
    files.insert(file(0, "log.cleaned"), survivor_log());
    files.insert(file(0, "index.cleaned"), Vec::new());
    let dir = populated_directory(&files);

    check!(recovered_files(dir.path()) == originals());
}

#[test]
fn a_torn_committed_swap_is_corrupt_and_nothing_is_deleted() {
    let mut files = originals_with_swaps();
    let torn = survivor_log()[..70].to_vec();
    files.insert(file(0, "log.swap"), torn);
    check_corrupt_files(&files);
}

#[test]
fn a_committed_swap_that_overlaps_itself_is_corrupt() {
    let mut files = originals_with_swaps();
    files.insert(file(0, "log.swap"), batches(&[(5, 3), (7, 0)]));
    check_corrupt_files(&files);
}

#[test]
fn rejects_sidecars_without_any_log() {
    let dir = tempdir().unwrap();
    std::fs::write(swap_path(dir.path(), 0, "index"), b"").unwrap();

    check!(let Err(LogError::Corrupt(_)) = swap_orphan_recover(dir.path()));
}

#[test]
fn ignores_malformed_names() {
    let dir = tempdir().unwrap();
    let files = Files::from([
        ("not-a-segment.log.swap".to_owned(), Vec::new()),
        ("+0000000000000000001.log.swap".to_owned(), Vec::new()),
        (
            "00000000000000000001.snapshot.cleaned".to_owned(),
            Vec::new(),
        ),
    ]);
    populate(dir.path(), &files);

    check!(recovered_files(dir.path()) == files);
}

#[test]
fn reports_directory_scan_failure() {
    let dir = tempdir().unwrap();

    check!(let Err(LogError::Io(_)) = swap_orphan_recover(&dir.path().join("missing")));
}

#[test]
fn survivor_paths_are_canonical() {
    let dir = Path::new("/log");
    check!(swap_path(dir, 7, "log") == Path::new("/log/00000000000000000007.log.swap"));
    check!(
        cleaned_path(dir, 7, "txnindex") == Path::new("/log/00000000000000000007.txnindex.cleaned")
    );
}
