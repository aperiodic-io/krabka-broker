//! Recovery of the hash-chain position from the records still in the spool.
//!
//! After a crash the writer has to continue the chain where the spooled
//! records left off, so this module walks the spool backwards to the last
//! chained record and recomputes the sequence number and the head hash that
//! follow it. Checkpoints carry no chain headers and are skipped.

use {
    super::Spool,
    crate::{
        chain::{chain_hash, from_hex32},
        event::AuditEventClass,
        ids::Seq,
        sink::{AuditError, AuditRecord, HEADER_PREV_HASH, HEADER_SEQ},
    },
    krabka_verified::{ChainStep, chain_step},
};

impl Spool {
    /// The `(next_seq, head)` from the last chained record in the spool.
    ///
    /// A chained record is a record that is not a checkpoint. Returns `None` if
    /// the spool has no chained record.
    #[tracing::instrument(level = "debug", skip_all, fields(count = self.count.0), err)]
    /// # Errors
    /// Returns an error if the spool file cannot be opened or read.
    pub fn resume_point(&self) -> Result<Option<(u64, [u8; 32])>, AuditError> {
        let records = self.read_all()?;
        resume_from_records(&records).map(|resume| resume.map(|(seq, head)| (seq.0, head)))
    }
}

fn poisoned(reason: &str) -> AuditError {
    AuditError::Poisoned(format!("audit spool resume: {reason}"))
}

fn resume_from_records(records: &[AuditRecord]) -> Result<Option<(Seq, [u8; 32])>, AuditError> {
    let mut resume: Option<(Seq, [u8; 32])> = None;
    for record in records {
        if record.class == AuditEventClass::Checkpoint {
            continue;
        }
        let (seq, previous) = chain_headers(record)?;
        let next_seq = match resume {
            None => seq
                .checked_add(1)
                .filter(|next| *next < u64::MAX)
                .ok_or_else(|| poisoned("chain sequence exhausted"))?,
            Some((expected, head)) => match chain_step(expected.0, seq, previous == head) {
                ChainStep::Continue(next) => next,
                ChainStep::SequenceMismatch => {
                    return Err(poisoned("noncontiguous chain sequence"));
                }
                ChainStep::HeadMismatch => return Err(poisoned("chain head mismatch")),
                ChainStep::Exhausted => return Err(poisoned("chain sequence exhausted")),
            },
        };
        resume = Some((Seq(next_seq), chain_hash(&previous, seq, &record.value)));
    }
    Ok(resume)
}

fn chain_headers(record: &AuditRecord) -> Result<(u64, [u8; 32]), AuditError> {
    let seq = record
        .headers
        .iter()
        .find(|(key, _)| key == HEADER_SEQ)
        .and_then(|(_, value)| std::str::from_utf8(value).ok())
        .and_then(|value| value.parse().ok());
    let previous = record
        .headers
        .iter()
        .find(|(key, _)| key == HEADER_PREV_HASH)
        .and_then(|(_, value)| std::str::from_utf8(value).ok())
        .and_then(from_hex32);
    seq.zip(previous)
        .ok_or_else(|| poisoned("missing or invalid chain headers"))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            chain::{GENESIS_HEAD, to_hex},
            spool::test_support::{ROOMY_CAP, chained_record},
        },
        assert2::check,
    };

    #[test]
    fn resume_point_is_from_last_chained_record_skipping_checkpoints() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let prev0 = GENESIS_HEAD;
        let r0 = chained_record(0, &prev0, b"v0");
        let head0 = chain_hash(&prev0, 0, b"v0");
        let r1 = chained_record(1, &head0, b"v1");
        let head1 = chain_hash(&head0, 1, b"v1");
        // a checkpoint record (no seq/prev_hash) must be skipped
        let cp = AuditRecord {
            class: AuditEventClass::Checkpoint,
            value: b"{\"type\":\"checkpoint\"}".to_vec(),
            headers: vec![("event_class".into(), b"checkpoint".to_vec())],
        };
        s.append(&r0).unwrap();
        s.append(&r1).unwrap();
        s.append(&cp).unwrap();
        let (next_seq, head) = s.resume_point().unwrap().unwrap();
        // after seq 1; the hex projection also checks r1's chain math.
        check!((next_seq, head, to_hex(&head)) == (2, head1, to_hex(&head1)));
        let _ = (HEADER_SEQ, HEADER_PREV_HASH); // used by impl
    }

    #[test]
    fn resume_rejects_a_malformed_or_disconnected_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        let record = chained_record(0, &GENESIS_HEAD, b"valid");
        let mut malformed = chained_record(1, &chain_hash(&GENESIS_HEAD, 0, b"valid"), b"bad");
        malformed.headers.retain(|(key, _)| key != HEADER_SEQ);
        spool.append(&record).unwrap();
        spool.append(&malformed).unwrap();
        check!(matches!(spool.resume_point(), Err(AuditError::Poisoned(_))));

        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        spool.append(&record).unwrap();
        spool
            .append(&chained_record(2, &GENESIS_HEAD, b"gap"))
            .unwrap();
        check!(matches!(spool.resume_point(), Err(AuditError::Poisoned(_))));
    }

    #[test]
    fn resume_rejects_sequence_exhaustion() {
        let dir = tempfile::tempdir().unwrap();
        let mut spool = Spool::open(dir.path(), ROOMY_CAP).unwrap();
        spool
            .append(&chained_record(u64::MAX - 1, &GENESIS_HEAD, b"last"))
            .unwrap();
        check!(matches!(spool.resume_point(), Err(AuditError::Poisoned(_))));
    }
}
