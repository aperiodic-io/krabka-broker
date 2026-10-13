//! Resolution of one `ShareFetch` partition row into a [`PendingPartition`]:
//! the fetch error and the acknowledge error that Kafka gives the row before
//! any share-partition state is touched.
//!
//! Kafka answers the two halves of a row separately. The fetch half covers
//! the partitions of the share session (`handleFetchFromShareFetchRequest`),
//! and the acknowledge half covers the request partitions when any of them
//! carries acknowledgements (`handleAcknowledgements`). Each half runs its own
//! checks in its own order, so a row can fail one half and pass the other.

use {
    super::{
        acknowledge::acknowledgement_batches_are_valid,
        authorization::topic_read_denied,
        pending::PendingPartition,
        request::collect_ack_batches,
        response::{not_leader_response, partition_response},
    },
    crate::{
        broker::Broker, codes, handlers::RequestContext,
        share_partition::manager::SharePartitionLeaderManager,
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::share_fetch_request::FetchPartition,
};

/// The request-wide facts that every row resolution reads.
pub(super) struct RowContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) manager: &'a SharePartitionLeaderManager,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
    /// Some request partition carries acknowledgement batches, so every
    /// request partition gets an acknowledge result.
    pub(super) has_acknowledgements: bool,
    /// The request version accepts the acknowledge type `Renew`.
    pub(super) supports_renew: bool,
    /// The request set `IsRenewAck`.
    pub(super) is_renew_ack: bool,
}

/// Resolves one `(topic, partition)` row.
///
/// `fetchable` says whether the row is a partition of the share session that
/// this request fetches, and `request_row` is the request partition, if the
/// request named it.
///
/// The fetch half follows `KafkaApis.handleFetchFromShareFetchRequest`: an
/// unresolved topic id is `UNKNOWN_TOPIC_ID`, then a topic without `Read` is
/// `TOPIC_AUTHORIZATION_FAILED`, then a partition that the metadata does not
/// hold is `UNKNOWN_TOPIC_OR_PARTITION`, then a partition that another broker
/// leads is `NOT_LEADER_OR_FOLLOWER`.
///
/// The acknowledge half follows `KafkaApis.handleAcknowledgements`: an
/// unresolved topic id is `UNKNOWN_TOPIC_ID`, then batches that
/// `validateAcknowledgementBatches` refuses are `INVALID_REQUEST`, then the
/// topic `Read` check, then the metadata check. A partition with no batches
/// then answers `NONE` without reaching the share partition, and a partition
/// that another broker leads answers `UNKNOWN_TOPIC_OR_PARTITION`, as
/// `SharePartitionManager.acknowledge` does for a partition with no cached
/// share partition. The row keeps its batches only when they are to be
/// applied.
pub(super) fn resolve_row(
    row: &RowContext<'_>,
    (topic_id, partition_index): (uuid::Uuid, i32),
    fetchable: bool,
    request_row: Option<&FetchPartition>,
) -> PendingPartition {
    let topic_name = row.manager.topic_name_for(topic_id);
    let acknowledges = row.has_acknowledgements && request_row.is_some();
    let mut ack_batches = request_row.map_or_else(Vec::new, collect_ack_batches);
    let partition_max_bytes = request_row.map_or(0, |partition| partition.partition_max_bytes);
    let mut out = partition_response(partition_index);
    let pending = |out, leadable, ack_batches| PendingPartition {
        topic_id,
        topic_name: topic_name.clone(),
        partition_index,
        partition_max_bytes,
        leadable,
        fetchable,
        in_request: request_row.is_some(),
        ack_batches,
        out,
    };

    let Some(name) = topic_name.as_deref() else {
        if fetchable {
            out.error_code = codes::UNKNOWN_TOPIC_ID;
        }
        if acknowledges {
            out.acknowledge_error_code = codes::UNKNOWN_TOPIC_ID;
        }
        return pending(out, false, Vec::new());
    };

    let denied = topic_read_denied(row.broker, row.image, row.ctx, name);
    let exists = row.image.partition(name, partition_index).is_some();
    let fetch_error = if denied {
        Some(codes::TOPIC_AUTHORIZATION_FAILED)
    } else if exists {
        None
    } else {
        Some(codes::UNKNOWN_TOPIC_OR_PARTITION)
    };
    let ack_error = if !acknowledges {
        None
    } else if !acknowledgement_batches_are_valid(
        ack_batches
            .iter()
            .map(|(first, last, types)| (*first, *last, types.as_slice())),
        row.supports_renew,
        row.is_renew_ack,
    ) {
        Some(codes::INVALID_REQUEST)
    } else {
        fetch_error
    };
    if !acknowledges || ack_error.is_some() {
        ack_batches.clear();
    }
    if fetchable && let Some(code) = fetch_error {
        out.error_code = code;
    }
    if let Some(code) = ack_error {
        out.acknowledge_error_code = code;
    }
    if !exists || denied || (!fetchable && ack_batches.is_empty()) {
        return pending(out, false, ack_batches);
    }

    if !row.manager.topic_leader_is_self(topic_id, partition_index) {
        // The leader hint belongs to the fetch half: Kafka sets it only on a
        // row whose fetch error is NOT_LEADER_OR_FOLLOWER.
        if fetchable {
            let (leader_id, leader_epoch) =
                row.manager.current_leader_of(topic_id, partition_index);
            let acknowledge_error_code = out.acknowledge_error_code;
            out = not_leader_response(partition_index, leader_id, leader_epoch);
            out.acknowledge_error_code = acknowledge_error_code;
        }
        // Kafka's `SharePartitionManager.acknowledge` has no leadership check:
        // a broker that does not lead the partition holds no share partition
        // for it, so the acknowledge half answers UNKNOWN_TOPIC_OR_PARTITION,
        // and no leader hint goes with it.
        if !ack_batches.is_empty() {
            out.acknowledge_error_code = codes::UNKNOWN_TOPIC_OR_PARTITION;
        }
        return pending(out, false, Vec::new());
    }
    pending(out, true, ack_batches)
}
