//! The per-partition row that a `ShareFetch` carries from request resolution,
//! through the acquire passes, to the encoded response.
//!
//! Every stage of the handler reads or writes this row, so it lives on its own
//! rather than in any one of them.

use {super::request::AckBatch, krabka_protocol::owned::share_fetch_response::PartitionData};

/// One resolved `(topic, partition)` request row. It travels through the
/// acquire passes, so the handler assembles the response once at the end.
pub(super) struct PendingPartition {
    pub(super) topic_id: uuid::Uuid,
    pub(super) topic_name: Option<String>,
    pub(super) partition_index: i32,
    pub(super) partition_max_bytes: i32,
    /// `Some` only when this broker leads the partition and the ACL check
    /// allowed the topic, that is when an acquire pass should run. A `None` row
    /// already has a complete `out`, because it is an error row.
    pub(super) leadable: bool,
    /// Whether this partition remains in the effective session subscription.
    /// A forgotten or final-request row can still carry acknowledgements, but
    /// must not acquire more records.
    pub(super) fetchable: bool,
    /// Whether the request named this partition. When the request carries
    /// acknowledgements, every request partition gets an acknowledge result.
    pub(super) in_request: bool,
    /// Acknowledgement batches piggybacked on this fetch that passed every
    /// check, so the acquire pass applies them. It is empty for a row whose
    /// acknowledge half already failed.
    pub(super) ack_batches: Vec<AckBatch>,
    pub(super) out: PartitionData,
}
