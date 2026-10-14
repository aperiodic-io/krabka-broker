//! Pure reassembly state machine for a follower that fetches a KIP-630
//! snapshot over `FetchSnapshot` (api key 59).
//!
//! This module is IO-free. The async engine (`controller.rs`) owns the
//! transport and applies the [`SnapshotFetchStep`] that this module returns.
//!
//! Chunks must arrive in order, that is, `position` must equal the number of
//! bytes received so far. Any mismatch or a changed snapshot id aborts the
//! transfer, so the engine restarts cleanly against the current leader.

use {
    crate::types::NodeId,
    bytes::{Bytes, BytesMut},
    krabka_macros::RefinedNewtype,
    krabka_units::prelude::{ByteSize, ByteSizeExt as _, gibibytes},
    krabka_verified::{SnapshotChunkDecision, snapshot_chunk_admission},
    refined_type::rule::MinMaxU64,
};

/// Snapshot identity: (`end_offset` exclusive, epoch), as KIP-630 `SnapshotId`.
pub type SnapshotId = (i64, i32);

/// Hard ceiling on the total bytes a follower will reassemble for one snapshot.
///
/// The size of the cluster metadata bounds the `__cluster_metadata` snapshot:
/// topics, partitions, ACLs, configs, and broker registrations. That is tens of
/// MiB even for very large clusters. 1 GiB is generous but finite. No
/// legitimate cluster reaches it, and it caps the memory that a peer which the
/// follower believes is the leader can force it to allocate (denial-of-service
/// finding M-3). A peer that declares or streams more than this aborts the
/// transfer.
pub const METADATA_SNAPSHOT_FETCH_HARD_MAX: ByteSize = gibibytes(1);

const METADATA_SNAPSHOT_FETCH_HARD_MAX_BYTES: u64 = 1_073_741_824;

/// Validated deployment limit for one metadata snapshot fetch.
///
/// Operators may lower this limit. No configuration can raise the fixed 1 GiB
/// security ceiling.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(MinMaxU64<1, METADATA_SNAPSHOT_FETCH_HARD_MAX_BYTES>),
    quantity = ByteSize,
    label = "metadata snapshot fetch max",
    quantity_getter = byte_size,
    default = METADATA_SNAPSHOT_FETCH_HARD_MAX
)]
pub struct MetadataSnapshotFetchMax(u64);

/// In-flight reassembly of one snapshot from one leader.
#[derive(Debug)]
pub struct SnapshotFetchState {
    pub snapshot_id: SnapshotId,
    pub leader_id: NodeId,
    buf: BytesMut,
    size: Option<i64>,
    max_size: MetadataSnapshotFetchMax,
}

/// What the engine should do after it feeds a chunk in.
#[derive(Debug, PartialEq, Eq)]
pub enum SnapshotFetchStep {
    /// Request the next byte range that starts at `next_position`.
    Continue { next_position: i64 },
    /// All bytes arrived; this variant holds the assembled snapshot.
    Complete(Bytes),
    /// Abort after an id mismatch, an out-of-order chunk, or a leader change.
    /// The engine discards this state and falls back to a normal Fetch.
    Restart,
}

impl SnapshotFetchState {
    #[must_use]
    pub fn new(snapshot_id: SnapshotId, leader_id: NodeId) -> Self {
        Self::with_max(snapshot_id, leader_id, MetadataSnapshotFetchMax::default())
    }

    /// Start reassembly with a validated deployment-specific maximum.
    #[must_use]
    pub fn with_max(
        snapshot_id: SnapshotId,
        leader_id: NodeId,
        max_size: MetadataSnapshotFetchMax,
    ) -> Self {
        Self {
            snapshot_id,
            leader_id,
            buf: BytesMut::new(),
            size: None,
            max_size,
        }
    }

    /// The byte position to request next, that is, the bytes received so far.
    #[must_use]
    pub fn next_position(&self) -> i64 { i64::try_from(self.buf.len()).unwrap_or(i64::MAX) }

    /// Feed one response chunk.
    ///
    /// `id`, `size`, and `position` come from the `FetchSnapshot` response.
    /// `chunk` holds the `unaligned_records` bytes.
    pub fn on_chunk(
        &mut self,
        id: SnapshotId,
        size: i64,
        position: i64,
        chunk: &[u8],
    ) -> SnapshotFetchStep {
        let Ok(received) = i64::try_from(self.buf.len()) else {
            return SnapshotFetchStep::Restart;
        };
        let Ok(chunk_len) = i64::try_from(chunk.len()) else {
            return SnapshotFetchStep::Restart;
        };
        let decision = snapshot_chunk_admission(
            id == self.snapshot_id,
            self.size,
            received,
            size,
            position,
            chunk_len,
            self.max_size.byte_size().bytes_i64(),
        );
        match decision {
            SnapshotChunkDecision::Restart => SnapshotFetchStep::Restart,
            SnapshotChunkDecision::Continue { next_position } => {
                self.size = Some(size);
                self.buf.extend_from_slice(chunk);
                SnapshotFetchStep::Continue { next_position }
            }
            SnapshotChunkDecision::Complete => {
                self.size = Some(size);
                self.buf.extend_from_slice(chunk);
                SnapshotFetchStep::Complete(self.buf.split().freeze())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, krabka_units::prelude::bytes};

    fn start_three_byte_chunk(s: &mut SnapshotFetchState) {
        assert2::assert!(
            s.on_chunk((10, 1), 6, 0, b"abc") == SnapshotFetchStep::Continue { next_position: 3 }
        );
    }

    #[test]
    fn snapshot_fetch_max_accepts_only_whole_bytes_within_the_security_ceiling() {
        assert2::assert!(MetadataSnapshotFetchMax::new(bytes(1)).is_ok());
        assert2::assert!(MetadataSnapshotFetchMax::new(gibibytes(1)).is_ok());
        assert2::assert!(MetadataSnapshotFetchMax::new(ByteSize::from_bytes_f64(0.5)).is_err());
        assert2::assert!(MetadataSnapshotFetchMax::new(bytes(0)).is_err());
        assert2::assert!(MetadataSnapshotFetchMax::new(bytes(1_073_741_825)).is_err());
    }

    #[test]
    fn configured_snapshot_fetch_max_may_lower_but_not_raise_the_ceiling() {
        let max = MetadataSnapshotFetchMax::new(bytes(3)).expect("valid configured maximum");
        let mut oversized = SnapshotFetchState::with_max((10, 1), NodeId(2), max);
        assert2::assert!(oversized.on_chunk((10, 1), 4, 0, b"abc") == SnapshotFetchStep::Restart);

        let mut exact = SnapshotFetchState::with_max((10, 1), NodeId(2), max);
        assert2::assert!(
            exact.on_chunk((10, 1), 3, 0, b"abc")
                == SnapshotFetchStep::Complete(Bytes::from_static(b"abc"))
        );
    }

    /// A declared total the receiver will not accept: negative, changed
    /// mid-transfer, or contradicted by a chunk that runs past it.
    ///
    /// Each is its own guard and each is silent unless something asks for it.
    /// A leader that could shrink `size` after the fact, or overrun it, would
    /// push `buf` past the bound the cap is supposed to give.
    #[test]
    fn a_declared_size_that_cannot_be_trusted_restarts_the_transfer() {
        // A negative total, rejected on its own rather than only alongside a
        // wrong id or position.
        let mut negative = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(negative.on_chunk((10, 1), -1, 0, b"") == SnapshotFetchStep::Restart);

        // Zero is a real total: an empty snapshot completes immediately.
        let mut empty = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(
            empty.on_chunk((10, 1), 0, 0, b"") == SnapshotFetchStep::Complete(Bytes::new())
        );

        // The total may not change once declared.
        let mut changed = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(
            changed.on_chunk((10, 1), 6, 0, b"abc")
                == SnapshotFetchStep::Continue { next_position: 3 }
        );
        assert2::assert!(changed.on_chunk((10, 1), 9, 3, b"def") == SnapshotFetchStep::Restart);

        // A chunk may not carry more than the remaining bytes.
        let mut overshoot = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(
            overshoot.on_chunk((10, 1), 6, 0, b"abc")
                == SnapshotFetchStep::Continue { next_position: 3 }
        );
        assert2::assert!(overshoot.on_chunk((10, 1), 6, 3, b"defgh") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn assembles_in_order_chunks_to_complete() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(s.next_position() == 0);
        let step = s.on_chunk((10, 1), 6, 0, b"abc");
        assert2::assert!(step == SnapshotFetchStep::Continue { next_position: 3 });
        let step = s.on_chunk((10, 1), 6, 3, b"def");
        match step {
            SnapshotFetchStep::Complete(b) => assert2::assert!(b.as_ref() == b"abcdef"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }

    #[test]
    fn out_of_order_position_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        let _ = s.on_chunk((10, 1), 6, 0, b"abc");
        assert2::assert!(s.on_chunk((10, 1), 6, 99, b"def") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn mismatched_id_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(s.on_chunk((11, 1), 6, 0, b"abc") == SnapshotFetchStep::Restart);
    }

    #[test]
    fn chunk_overshooting_declared_size_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        // Leader declares size 3 but streams 5 bytes in one chunk.
        let step = s.on_chunk((10, 1), 3, 0, b"abcde");
        // buf must not have been blown past the declared size.
        assert2::assert!(step == SnapshotFetchStep::Restart);
        assert2::assert!(s.next_position() == 0);
    }

    #[yare::parameterized(
        duplicate_chunk = { (10, 1), 6, 0, b"abc".as_slice() },
        overlapping_chunk = { (10, 1), 6, 2, b"cde".as_slice() },
        starts_after_gap = { (10, 1), 6, 4, b"ef".as_slice() },
        changed_snapshot_id = { (11, 1), 6, 3, b"def".as_slice() },
        changed_declared_size = { (10, 1), 7, 3, b"def".as_slice() },
    )]
    fn rejected_retries_do_not_change_progress_or_latch_size(
        id: SnapshotId,
        size: i64,
        position: i64,
        chunk: &[u8],
    ) {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        start_three_byte_chunk(&mut s);
        assert2::assert!(s.on_chunk(id, size, position, chunk) == SnapshotFetchStep::Restart);
        assert2::assert!(s.next_position() == 3);
    }

    #[test]
    fn accepted_chunks_complete_the_latched_snapshot() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        start_three_byte_chunk(&mut s);
        assert2::assert!(
            s.on_chunk((10, 1), 6, 3, b"def")
                == SnapshotFetchStep::Complete(Bytes::from_static(b"abcdef"))
        );
    }

    #[test]
    fn fresh_transfer_can_accept_a_different_declared_size() {
        let mut fresh = SnapshotFetchState::new((10, 1), NodeId(2));
        assert2::assert!(fresh.on_chunk((10, 1), 3, 0, b"toolong") == SnapshotFetchStep::Restart);
        assert2::assert!(
            fresh.on_chunk((10, 1), 7, 0, b"toolong")
                == SnapshotFetchStep::Complete(Bytes::from_static(b"toolong"))
        );
    }

    #[test]
    fn declared_size_over_cap_restarts() {
        let mut s = SnapshotFetchState::new((10, 1), NodeId(2));
        let too_big = METADATA_SNAPSHOT_FETCH_HARD_MAX.bytes_i64() + 1;
        let step = s.on_chunk((10, 1), too_big, 0, b"abc");
        assert2::assert!(step == SnapshotFetchStep::Restart);
        assert2::assert!(s.next_position() == 0);
    }

    #[test]
    fn reassembly_cap_is_exactly_one_gibibyte() {
        // The cap bounds the memory a peer claiming to be the leader can force
        // this follower to allocate, so its magnitude is the security property
        // (finding M-3) — not just that some cap exists.
        assert2::assert!(METADATA_SNAPSHOT_FETCH_HARD_MAX.bytes_i64() == 1_073_741_824);
    }

    #[test]
    fn single_chunk_completes() {
        let mut s = SnapshotFetchState::new((5, 0), NodeId(1));
        match s.on_chunk((5, 0), 3, 0, b"xyz") {
            SnapshotFetchStep::Complete(b) => assert2::assert!(b.as_ref() == b"xyz"),
            other => panic!("expected Complete, got {other:?}"),
        }
    }
}
