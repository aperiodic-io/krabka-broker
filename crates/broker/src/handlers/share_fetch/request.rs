//! The values that the `ShareFetch` handler derives straight from a decoded
//! `ShareFetchRequest`, before it reaches any share-partition state.
//!
//! The share-session update needs to know whether the request carried
//! acknowledgements and whether it added partitions, and each acquire pass
//! needs the piggybacked acknowledgement batches in a shape that no longer
//! borrows the request.

use {
    crate::{
        broker::Broker,
        codes,
        coordinator::unified::share::config::ShareGroupConfig,
        features::share_groups_enabled,
        handlers::{RequestContext, group_read_denied},
        share_partition::group_settings::GroupShareSettings,
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::share_fetch_request::{FetchPartition, ShareFetchRequest},
};

/// The authorized member and resolved group settings shared by fetch and ack.
pub(crate) struct ShareRequestIdentity {
    pub(crate) group: String,
    pub(crate) settings: GroupShareSettings,
    pub(crate) lock_timeout_ms: i32,
    pub(crate) member: String,
}

/// Apply Kafka's feature, group-id, group-read and member-id gates in order.
/// Settings resolve after authorization and before the member's format check.
pub(crate) fn resolve_share_identity(
    broker: &Broker,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    defaults: &ShareGroupConfig,
    group_id: Option<&String>,
    member_id: Option<&String>,
) -> Result<ShareRequestIdentity, i16> {
    if !share_groups_enabled(image) {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    let group = group_id.cloned().ok_or(codes::INVALID_REQUEST)?;
    if group_read_denied(broker.config.authorizer.as_ref(), image, ctx, &group) {
        return Err(codes::GROUP_AUTHORIZATION_FAILED);
    }
    let settings = GroupShareSettings::resolve(image, &group, defaults);
    let lock_timeout_ms = settings.record_lock_duration_ms();
    let member = member_id
        .cloned()
        .filter(|id| super::member_id_is_valid(id))
        .ok_or(codes::INVALID_REQUEST)?;
    Ok(ShareRequestIdentity {
        group,
        settings,
        lock_timeout_ms,
        member,
    })
}

/// Keep the settings snapshot and identity bindings alive for the whole handler.
macro_rules! share_request_identity {
    (
        (
            $defaults:ident,
            $image:ident,
            $group:ident,
            $settings:ident,
            $lock_timeout:ident,
            $member:ident
        ),
        $response:ident,
        $broker:ident,
        $request:ident,
        $context:ident
    ) => {
        let $defaults = $broker.config.share_group.clone();
        let $image = $broker.controller.current_image();
        let crate::handlers::share_fetch::ShareRequestIdentity {
            group: $group,
            settings: $settings,
            lock_timeout_ms: $lock_timeout,
            member: $member,
        } = match crate::handlers::share_fetch::resolve_share_identity(
            $broker,
            &$image,
            $context,
            &$defaults,
            $request.group_id.as_ref(),
            $request.member_id.as_ref(),
        ) {
            Ok(identity) => identity,
            Err(code) => return Ok($response::error(code, None)),
        };
    };
}
pub(crate) use share_request_identity;

/// One piggybacked acknowledgement batch, that is
/// `(first_offset, last_offset, per-offset acknowledge_types)`.
pub(super) type AckBatch = (i64, i64, Vec<i8>);

/// Whether any request partition carries acknowledgement batches: Kafka's
/// `isAcknowledgeDataPresentInFetchRequest`.
pub(super) fn has_acknowledgements(req: &ShareFetchRequest) -> bool {
    req.topics
        .iter()
        .flat_map(|topic| &topic.partitions)
        .any(|partition| !partition.acknowledgement_batches.is_empty())
}

/// Collects the piggybacked acknowledgement batches from a request partition
/// into `(first, last, acknowledge_types)` triples.
pub(super) fn collect_ack_batches(fp: &FetchPartition) -> Vec<AckBatch> {
    fp.acknowledgement_batches
        .iter()
        .map(|b| (b.first_offset, b.last_offset, b.acknowledge_types.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_protocol::owned::share_fetch_request::{AcknowledgementBatch, FetchTopic},
    };

    #[test]
    fn collect_ack_batches_preserves_offsets_and_ack_types() {
        let partition = FetchPartition {
            partition_index: 6,
            acknowledgement_batches: vec![
                AcknowledgementBatch {
                    first_offset: 10,
                    last_offset: 12,
                    acknowledge_types: vec![0, 1, 1],
                    ..Default::default()
                },
                AcknowledgementBatch {
                    first_offset: 30,
                    last_offset: 30,
                    acknowledge_types: Vec::new(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let batches = collect_ack_batches(&partition);

        assert!(batches == vec![(10, 12, vec![0, 1, 1]), (30, 30, Vec::new())]);
    }

    #[test]
    fn acknowledgements_are_present_when_any_partition_carries_a_batch() {
        let request = |partitions| ShareFetchRequest {
            topics: vec![FetchTopic {
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        };
        let addition = FetchPartition {
            partition_index: 1,
            ..Default::default()
        };
        let acknowledgement = FetchPartition {
            partition_index: 2,
            acknowledgement_batches: vec![AcknowledgementBatch::default()],
            ..Default::default()
        };

        assert!(
            [
                has_acknowledgements(&ShareFetchRequest::default()),
                has_acknowledgements(&request(vec![addition.clone()])),
                has_acknowledgements(&request(vec![acknowledgement.clone()])),
                has_acknowledgements(&request(vec![addition, acknowledgement])),
            ] == [false, false, true, true]
        );
    }
}
