//! Unit tests for the offline verifier, driven end to end through
//! [`verify_partition_dir`] over partitions built on disk.

use assert2::check;
use bytes::Bytes;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch, RecordHeader};

use super::*;
use crate::{
    FileEd25519Signer,
    chain::{ChainState, GENESIS_HEAD},
    checkpoint::Checkpoint,
    event::AuditEventClass,
    ids::EpochMs,
    sink::AuditRecord,
    test_support::shared_signer as signer,
};

fn audit_record_to_batch(rec: &AuditRecord, base_offset: i64) -> RecordBatch {
    let mut batch = RecordBatch {
        base_offset,
        last_offset_delta: 0,
        ..RecordBatch::default()
    };
    batch.records.push(record_at_delta(rec, 0));
    batch
}

/// Several records inside one batch, each at its own `offset_delta`.
///
/// [`audit_record_to_batch`] puts one record per batch at delta 0, where a
/// record's offset is its batch's base offset. This is the other shape: the
/// offset is the base plus the delta, and the two differ.
fn audit_records_to_batch(recs: &[AuditRecord], base_offset: i64) -> RecordBatch {
    let last = i32::try_from(recs.len()).expect("record count fits i32") - 1;
    let mut batch = RecordBatch {
        base_offset,
        last_offset_delta: last,
        ..RecordBatch::default()
    };
    for (i, rec) in recs.iter().enumerate() {
        batch.records.push(record_at_delta(
            rec,
            i32::try_from(i).expect("delta fits i32"),
        ));
    }
    batch
}

fn record_at_delta(rec: &AuditRecord, offset_delta: i32) -> Record {
    let headers = rec
        .headers
        .iter()
        .map(|(k, v)| RecordHeader {
            key: k.clone(),
            value: Some(Bytes::from(v.clone())),
        })
        .collect();
    Record {
        offset_delta,
        value: Some(Bytes::from(rec.value.clone())),
        headers,
        ..Default::default()
    }
}

fn lifecycle_record(value: Vec<u8>) -> AuditRecord {
    AuditRecord {
        class: AuditEventClass::ApplicationLifecycle,
        value,
        headers: vec![("event_class".into(), b"application_lifecycle".to_vec())],
    }
}

fn partition() -> (tempfile::TempDir, Log) {
    let directory = tempfile::tempdir().unwrap();
    let log = Log::open(directory.path(), LogConfig::default()).unwrap();
    (directory, log)
}

fn untrusted_report(directory: &std::path::Path) -> VerifyReport {
    verify_partition_dir(directory, &TrustedKeys::default()).unwrap()
}

fn trusted_report(directory: &std::path::Path, public_key: Vec<u8>) -> VerifyReport {
    verify_partition_dir(directory, &TrustedKeys::single("k1".into(), public_key)).unwrap()
}

fn signed_head(chain: &ChainState, signer: &FileEd25519Signer, time: i64) -> Checkpoint {
    Checkpoint::signed(
        signer,
        Seq(chain.next_seq() - 1),
        &chain.head(),
        EpochMs(time),
    )
}

fn reserved_marker_report(count: u64, relabel: bool) -> VerifyReport {
    let (directory, mut log) = partition();
    let mut marker = AuditRecord::records_lost(count, 1);
    if relabel {
        marker.headers[0].1 = b"application_lifecycle".to_vec();
    }
    marker.push_chain_headers(0, &GENESIS_HEAD);
    append_record(&mut log, &marker, 0);
    untrusted_report(directory.path())
}

fn check_summary(report: &VerifyReport, expected: (bool, u64, u64, u64)) {
    check!(
        (
            report.ok,
            report.checkpoints.0,
            report.records.0,
            report.unanchored_records.0
        ) == expected
    );
}

fn append_record(log: &mut Log, record: &AuditRecord, offset: i64) {
    log.append(&mut audit_record_to_batch(record, offset))
        .unwrap();
}

fn chain_record(chain: &mut ChainState, record: &mut AuditRecord) {
    let (seq, previous) = chain.extend(&record.value);
    record.push_chain_headers(seq, &previous);
}

fn check_break(report: &VerifyReport, missing: &str, reason: &str) {
    check!(
        report
            .first_break
            .as_ref()
            .expect(missing)
            .reason
            .contains(reason)
    );
}

fn append_lifecycle_chain(
    log: &mut Log,
    chain: &mut ChainState,
    offset: &mut i64,
    values: std::ops::Range<u8>,
) {
    for i in values {
        let mut record = lifecycle_record(format!("{{\"i\":{i}}}").into_bytes());
        chain_record(chain, &mut record);
        append_record(log, &record, *offset);
        *offset += 1;
    }
}

fn checkpointed_partition(
    directory: &std::path::Path,
    time: i64,
) -> (Vec<u8>, Log, ChainState, i64) {
    let (signer, public_key) = signer();
    let mut log = Log::open(directory, LogConfig::default()).unwrap();
    let mut chain = ChainState::new();
    let mut offset = 0i64;
    append_lifecycle_chain(&mut log, &mut chain, &mut offset, 0..3);
    let checkpoint = signed_head(&chain, signer.as_ref(), time);
    append_record(&mut log, &checkpoint.to_record(), offset);
    (public_key, log, chain, offset)
}

fn append_and_verify(
    log: &mut Log,
    record: &AuditRecord,
    directory: &std::path::Path,
) -> VerifyReport {
    append_record(log, record, 0);
    untrusted_report(directory)
}

/// Build a valid chained and checkpointed partition on disk, and return the
/// public key.
fn build_partition(tmp: &std::path::Path) -> Vec<u8> {
    // checkpoint over the chain head
    let (pubkey, _log, _chain, _offset) = checkpointed_partition(tmp, 123);
    pubkey
}

#[test]
fn valid_partition_verifies_ok() {
    let tmp = tempfile::tempdir().unwrap();
    let pubkey = build_partition(tmp.path());
    let trusted = TrustedKeys::single("k1".into(), pubkey);
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    // build_partition writes 3 records (seq 0..2) + 1 checkpoint (seq_high=2),
    // so all records are covered.
    check!(
        (
            report.ok,
            report.records.0,
            report.checkpoints.0,
            report.first_break.is_none(),
            report.losses.is_empty(),
            report.unanchored_records.0,
        ) == (true, 3, 1, true, true, 0)
    );
}

#[test]
fn records_lost_marker_is_reported_without_breaking_the_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let (signer, public_key) = signer();
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    let mut chain = ChainState::new();
    let mut marker = AuditRecord::records_lost(3, 1);
    chain_record(&mut chain, &mut marker);
    append_record(&mut log, &marker, 0);
    let checkpoint = signed_head(&chain, signer.as_ref(), 123);
    log.append(&mut audit_record_to_batch(&checkpoint.to_record(), 1))
        .unwrap();

    let report = trusted_report(tmp.path(), public_key);
    check!(
        (
            report.ok,
            report.records,
            report.checkpoints,
            report.losses,
            report.unanchored_records,
        ) == (
            true,
            RecordCount(1),
            CheckpointCount(1),
            vec![VerifyLoss {
                offset: 0,
                seq: Seq(0),
                records: RecordCount(3),
            }],
            RecordCount(0),
        )
    );
}

#[test]
fn records_lost_body_cannot_be_hidden_by_changing_its_header() {
    let report = reserved_marker_report(3, true);
    check!(!report.ok);
    check_break(
        &report,
        "body/header mismatch",
        "body/event_class header mismatch",
    );
    check!(report.losses.is_empty());
}

#[test]
fn records_lost_body_without_a_generation_is_rejected() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();
    let mut marker = AuditRecord::records_lost(4, 1);
    marker.value = br#"{"records_lost":4}"#.to_vec();
    chain_record(&mut chain, &mut marker);
    let report = append_and_verify(&mut log, &marker, tmp.path());
    check!((report.ok, report.records, report.losses.is_empty()) == (false, RecordCount(0), true));
    check_break(&report, "generation-less marker", "records-lost");
}

#[test]
fn persisted_loss_generation_must_advance() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();
    for (offset, generation) in [2_u64, 2].into_iter().enumerate() {
        let mut marker = AuditRecord::records_lost(4, generation);
        chain_record(&mut chain, &mut marker);
        log.append(&mut audit_record_to_batch(
            &marker,
            i64::try_from(offset).unwrap(),
        ))
        .unwrap();
    }

    let report = untrusted_report(tmp.path());
    check!(!report.ok);
    check!(report.records == RecordCount(1));
    check!(report.losses.len() == 1);
    check_break(&report, "stale loss generation", "records-lost");
}

#[test]
fn malformed_reserved_loss_body_cannot_hide_behind_another_header() {
    let report = reserved_marker_report(0, true);
    check!(!report.ok);
    check!(report.records == RecordCount(0));
}

#[test]
fn records_lost_marker_does_not_excuse_a_sequence_gap() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();
    let _ = chain.extend(b"missing");
    let mut marker = AuditRecord::records_lost(1, 1);
    chain_record(&mut chain, &mut marker);
    let report = append_and_verify(&mut log, &marker, tmp.path());
    check!(!report.ok);
    check_break(&report, "sequence gap", "seq gap");
    check!(report.losses.is_empty());
}

#[test]
fn records_lost_marker_requires_a_positive_count() {
    let report = reserved_marker_report(0, false);
    check!(!report.ok);
    check_break(&report, "malformed marker", "records-lost");
    check!(report.records == RecordCount(0));
}

#[test]
fn wrong_trusted_key_fails_at_checkpoint() {
    let tmp = tempfile::tempdir().unwrap();
    let _pubkey = build_partition(tmp.path());
    let (_other, other_pub) = signer();
    let trusted = TrustedKeys::single("k1".into(), other_pub);
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check!(!report.ok);
    let b = report.first_break.unwrap();
    check!(b.reason.contains("signature"));
}

#[test]
fn signed_checkpoint_cannot_claim_an_empty_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let (signer, public_key) = signer();
    let checkpoint = Checkpoint::signed(signer.as_ref(), Seq(0), &GENESIS_HEAD, EpochMs(123));
    let mut log = Log::open(tmp.path(), LogConfig::default()).unwrap();
    log.append(&mut audit_record_to_batch(&checkpoint.to_record(), 0))
        .unwrap();

    let report = trusted_report(tmp.path(), public_key);
    check!(!report.ok);
    check_break(&report, "empty checkpoint", "seq_high");
}

// ── Fix 1 tests: unanchored_records field ─────────────────────────────────

/// Partition with a trailing tail of 2 records beyond the last checkpoint.
#[test]
fn unanchored_tail_records_are_counted() {
    let tmp = tempfile::tempdir().unwrap();
    // 3 records + checkpoint (seq_high=2)
    let (pubkey, mut log, mut chain, mut offset) = checkpointed_partition(tmp.path(), 100);
    offset += 1;

    // 2 more records WITHOUT a trailing checkpoint
    append_lifecycle_chain(&mut log, &mut chain, &mut offset, 3..5);

    let trusted = TrustedKeys::single("k1".into(), pubkey);
    let report = verify_partition_dir(tmp.path(), &trusted).unwrap();
    check_summary(&report, (true, 1, 5, 2));
}

/// Chain-only partition with no signing key and no checkpoints. All records
/// are unanchored.
#[test]
fn chain_only_partition_all_records_unanchored() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();

    for (offset, i) in (0..3u8).enumerate() {
        let mut rec = lifecycle_record(format!("{{\"i\":{i}}}").into_bytes());
        chain_record(&mut chain, &mut rec);
        let mut b = audit_record_to_batch(&rec, i64::try_from(offset).unwrap());
        log.append(&mut b).unwrap();
    }

    // No trusted key needed — no checkpoints present
    let report = untrusted_report(tmp.path());
    check_summary(&report, (true, 0, 3, 3));
}

// ── Fix 2 tests: direct tamper-detection (chain-inconsistent fixtures) ────

/// Dropped record creates a seq gap that the verifier detects as a break.
#[test]
fn dropped_record_detected_as_seq_gap() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();

    // Build 3 records into memory first, then write only [0] and [2] (skip [1]).
    let mut records: Vec<AuditRecord> = (0..3u8)
        .map(|i| lifecycle_record(format!("{{\"i\":{i}}}").into_bytes()))
        .collect();

    for rec in &mut records {
        chain_record(&mut chain, rec);
    }

    // Write records[0] (seq=0) then records[2] (seq=2) — skip records[1]
    append_record(&mut log, &records[0], 0);
    append_record(&mut log, &records[2], 1);

    let report = untrusted_report(tmp.path());
    check!(!report.ok, "dropped record must be detected as tamper");
    let reason = &report.first_break.unwrap().reason;
    check!(
        reason.contains("seq"),
        "reason should mention seq gap, got: {reason}"
    );
}

/// The verifier detects a record with the wrong `prev_hash` as a chain break.
#[test]
fn wrong_prev_hash_detected() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();

    // Record 0: correct chain
    let mut rec0 = lifecycle_record(b"{\"i\":0}".to_vec());
    let (seq0, prev0) = chain.extend(&rec0.value);
    rec0.push_chain_headers(seq0, &prev0);
    append_record(&mut log, &rec0, 0);

    // Record 1: stamped with GENESIS_HEAD as prev (wrong — should be head after rec0)
    let mut rec1 = lifecycle_record(b"{\"i\":1}".to_vec());
    // Advance chain to get the correct seq, but use GENESIS_HEAD as wrong prev
    let (seq1, _correct_prev) = chain.extend(&rec1.value);
    rec1.push_chain_headers(seq1, &GENESIS_HEAD); // wrong prev
    append_record(&mut log, &rec1, 1);

    let report = untrusted_report(tmp.path());
    check!(!report.ok, "wrong prev_hash must be detected as tamper");
    let reason = &report.first_break.unwrap().reason;
    check!(
        reason.contains("prev_hash"),
        "reason should mention prev_hash, got: {reason}"
    );
}

/// A break in the second record of a batch is reported at
/// `base_offset + offset_delta`.
///
/// Every other test writes one record per batch at delta 0, where the base
/// alone is the offset and so a sum is indistinguishable from a difference
/// or a product. Here the record sits at base 1, delta 1: the offset is 2,
/// and 1 and 0 are what the other arithmetic would give.
#[test]
fn break_offset_within_a_batch_is_base_plus_delta() {
    let (tmp, mut log) = partition();
    let mut chain = ChainState::new();

    let record = |value: &str, chain: &mut ChainState, tamper: bool| {
        let mut rec = lifecycle_record(value.as_bytes().to_vec());
        let (seq, prev) = chain.extend(&rec.value);
        rec.push_chain_headers(seq, if tamper { &GENESIS_HEAD } else { &prev });
        rec
    };

    // Offset 0, its own batch, chain intact.
    let rec0 = record("{\"i\":0}", &mut chain, false);
    append_record(&mut log, &rec0, 0);

    // Offsets 1 and 2 in one batch. The second carries a wrong prev hash,
    // so the walk breaks on it rather than on the batch's first record.
    let rec1 = record("{\"i\":1}", &mut chain, false);
    let rec2 = record("{\"i\":2}", &mut chain, true);
    log.append(&mut audit_records_to_batch(&[rec1, rec2], 1))
        .unwrap();

    let report = untrusted_report(tmp.path());
    check!(!report.ok, "a wrong prev_hash must break the walk");
    let brk = report.first_break.expect("a break was reported");
    check!(brk.offset == 2, "break offset, got {}", brk.offset);
    check!(
        brk.reason.contains("prev_hash"),
        "reason should name prev_hash, got: {}",
        brk.reason
    );
}

/// A checkpoint signed over the original chain head does not match after
/// records are replaced with different values, because the re-stamped chain
/// head differs.
#[test]
fn stale_checkpoint_chain_head_mismatch_detected() {
    let tmp_orig = tempfile::tempdir().unwrap();
    let tmp_tampered = tempfile::tempdir().unwrap();

    let (s, pubkey) = signer();

    // Build original partition: 2 records + checkpoint
    let mut orig_chain = ChainState::new();
    let mut orig_records: Vec<AuditRecord> = (0..2u8)
        .map(|i| lifecycle_record(format!("{{\"i\":{i}}}").into_bytes()))
        .collect();
    for rec in &mut orig_records {
        chain_record(&mut orig_chain, rec);
    }
    let orig_cp = signed_head(&orig_chain, s.as_ref(), 42);

    // Build tampered partition: same structure but different values → different chain head
    // but reuse the OLD checkpoint (signed over the original head)
    let mut tampered_chain = ChainState::new();
    let mut tampered_records: Vec<AuditRecord> = (0..2u8)
        .map(|i| lifecycle_record(format!("{{\"i\":{},\"tampered\":true}}", i + 10).into_bytes()))
        .collect();
    for rec in &mut tampered_records {
        chain_record(&mut tampered_chain, rec);
    }

    let mut log = Log::open(tmp_tampered.path(), LogConfig::default()).unwrap();
    let mut offset = 0i64;
    for rec in &tampered_records {
        let mut b = audit_record_to_batch(rec, offset);
        log.append(&mut b).unwrap();
        offset += 1;
    }
    // Reuse the OLD checkpoint (signed over original chain head — won't match tampered head)
    append_record(&mut log, &orig_cp.to_record(), offset);

    let _ = tmp_orig; // keep alive

    let trusted = TrustedKeys::single("k1".into(), pubkey);
    let report = verify_partition_dir(tmp_tampered.path(), &trusted).unwrap();
    check!(
        !report.ok,
        "stale checkpoint over wrong chain_head must be detected"
    );
    let reason = &report.first_break.unwrap().reason;
    check!(
        reason.contains("chain_head"),
        "reason should mention chain_head, got: {reason}"
    );
}
