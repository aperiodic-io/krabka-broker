//! Validation of one WAL fetch response. The follower trusts nothing the leader
//! sends, so the frontiers, the reset offset, and the offset the append reached
//! each pass through a check here before the follower acts on them.

use {
    crate::wal::quorum::registry::ShardId,
    krabka_ids::Offset,
    krabka_kraft_core::LogOffsetMetadata,
    krabka_protocol::owned::fetch_response::{FetchResponse, PartitionData},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WalFetchFrontiers {
    pub(super) start: Offset,
    pub(super) end: Offset,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FetchProgress {
    Idle,
    Advanced,
}

pub(super) fn validate_fetch_frontiers(
    partition: &PartitionData,
) -> Result<WalFetchFrontiers, String> {
    let start = Offset(partition.log_start_offset);
    let high_watermark = Offset(partition.high_watermark);
    let end = Offset(partition.last_stable_offset);
    let (true, true) = (
        (Offset(0)..=end).contains(&start),
        (start..=end).contains(&high_watermark),
    ) else {
        return Err("leader returned invalid WAL frontiers".into());
    };
    Ok(WalFetchFrontiers { start, end })
}

pub(super) fn validate_reset_offset(partition: &PartitionData) -> Result<Offset, String> {
    let start = Offset(partition.log_start_offset);
    let end = Offset(partition.last_stable_offset);
    let true = (Offset(0)..=end).contains(&start) else {
        return Err("leader returned invalid WAL reset offset".into());
    };
    Ok(start)
}

/// The diverging epoch a response names, or `None` when it names none.
///
/// Kafka's `FetchResponseData.EpochEndOffset` defaults to epoch `-1` and end
/// offset `-1`, and a leader leaves it at that default for a consistent
/// fetch, so a negative end offset means "no divergence". A named divergence
/// must carry a real epoch.
pub(super) fn validate_diverging_epoch(
    partition: &PartitionData,
) -> Result<Option<LogOffsetMetadata>, String> {
    let diverging = &partition.diverging_epoch;
    if diverging.end_offset < 0 {
        return Ok(None);
    }
    let epoch = u32::try_from(diverging.epoch)
        .map_err(|_| String::from("leader returned an invalid WAL diverging epoch"))?;
    Ok(Some(LogOffsetMetadata {
        offset: diverging.end_offset,
        epoch,
    }))
}

pub(super) fn fetch_progress(requested: Offset, appended: Offset) -> Result<FetchProgress, String> {
    match appended.cmp(&requested) {
        std::cmp::Ordering::Less => Err(format!(
            "WAL follower regressed from requested offset {} to {}",
            requested.0, appended.0
        )),
        std::cmp::Ordering::Equal => Ok(FetchProgress::Idle),
        std::cmp::Ordering::Greater => Ok(FetchProgress::Advanced),
    }
}

pub(super) fn response_partition(response: FetchResponse, shard: ShardId) -> Option<PartitionData> {
    let topic_id = krabka_protocol::primitives::uuid::Uuid(*shard.topic_id.as_bytes());
    response
        .responses
        .into_iter()
        .find(|topic| topic.topic_id == topic_id)?
        .partitions
        .into_iter()
        .find(|partition| partition.partition_index == shard.partition)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn frontiers(start: i64, high_watermark: i64, end: i64) -> PartitionData {
        PartitionData {
            log_start_offset: start,
            high_watermark,
            last_stable_offset: end,
            ..Default::default()
        }
    }

    #[test]
    fn fetch_frontiers_require_an_ordered_nonnegative_range() {
        assert!(
            validate_fetch_frontiers(&frontiers(5, 6, 7))
                == Ok(WalFetchFrontiers {
                    start: Offset(5),
                    end: Offset(7),
                })
        );
        for invalid in [
            frontiers(-1, 0, 1),
            frontiers(0, -1, 1),
            frontiers(0, 2, 1),
            frontiers(2, 2, 1),
            frontiers(0, 0, -1),
        ] {
            assert!(validate_fetch_frontiers(&invalid).is_err());
        }
    }

    #[test]
    fn reset_offset_must_fall_inside_the_leader_log() {
        assert!(validate_reset_offset(&frontiers(5, 5, 7)) == Ok(Offset(5)));
        for invalid in [frontiers(-1, 0, 7), frontiers(8, 8, 7), frontiers(0, 0, -1)] {
            assert!(validate_reset_offset(&invalid).is_err());
        }
    }

    #[test]
    fn diverging_epoch_is_absent_at_the_kafka_default_and_needs_a_real_epoch() {
        for (name, epoch, end_offset, expected) in [
            ("kafka default", -1, -1, Ok(None)),
            ("negative end offset", 3, -1, Ok(None)),
            (
                "named divergence",
                3,
                10,
                Ok(Some(LogOffsetMetadata {
                    offset: 10,
                    epoch: 3,
                })),
            ),
            (
                "epoch zero",
                0,
                0,
                Ok(Some(LogOffsetMetadata {
                    offset: 0,
                    epoch: 0,
                })),
            ),
            (
                "negative epoch",
                -1,
                10,
                Err(String::from(
                    "leader returned an invalid WAL diverging epoch",
                )),
            ),
        ] {
            let mut partition = frontiers(0, 0, 10);
            partition.diverging_epoch.epoch = epoch;
            partition.diverging_epoch.end_offset = end_offset;
            assert!(
                validate_diverging_epoch(&partition) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn fetch_progress_distinguishes_idle_advance_and_regression() {
        assert!(fetch_progress(Offset(4), Offset(4)) == Ok(FetchProgress::Idle));
        assert!(fetch_progress(Offset(4), Offset(5)) == Ok(FetchProgress::Advanced));
        assert!(fetch_progress(Offset(4), Offset(3)).is_err());
    }
}
