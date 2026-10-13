//! Reading a group's offset state out of its coordinator actor.
//!
//! Both `OffsetFetch` request shapes need the same name-keyed offset map, and
//! both reach it the same way: find the group's actor and ask it for its
//! offsets. A group with no actor, or an actor that has gone away, yields an
//! empty view rather than an error, which is the "no committed offsets" answer
//! the response already encodes. The read never creates a group: Kafka's
//! `OffsetMetadataManager.fetchOffsets` answers -1 rows for a group it does
//! not know and creates nothing.
//!
//! The reply carries the stable offsets and the `(topic, partition)` keys an
//! unresolved transaction has written, because KIP-447's `require_stable`
//! decides between the two per partition and must see one consistent snapshot
//! of both.

use {
    crate::{
        broker::Broker,
        coordinator::unified::{
            actor::GroupActorMessage,
            group::{GroupOffsets, OffsetEntry, OffsetFetchMember},
        },
        task_util::ask,
    },
    krabka_protocol::owned::offset_fetch_response as wire,
};

/// Fetches the group's committed offsets, keyed by topic name and partition,
/// together with the keys its open transactions have not resolved yet.
///
/// `member_id` and `member_epoch` are the v9+ request fields (`None` and -1
/// before v9). A consumer group checks them first, and a refusal is the group
/// error code Kafka's `ConsumerGroup.validateOffsetFetch` answers.
pub(super) async fn fetch_offsets(
    broker: &Broker,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: i32,
) -> Result<GroupOffsets, i16> {
    let Some(handle) = broker.group_coordinator.find(group_id) else {
        return Ok(GroupOffsets::default());
    };
    ask(&handle.tx, |reply| {
        GroupActorMessage::FetchOffsetsForMember {
            member: OffsetFetchMember {
                member_id: member_id.map(str::to_string),
                member_epoch,
            },
            reply,
        }
    })
    .await
    .unwrap_or_else(|_| Ok(GroupOffsets::default()))
}

/// Build the topic-name ordered fetch-all view using either wire row shape.
pub(super) fn committed_topics<T>(
    offsets: &GroupOffsets,
    mut row: impl FnMut(&str, i32) -> T,
) -> std::collections::BTreeMap<&str, Vec<T>> {
    let mut by_topic = std::collections::BTreeMap::<&str, Vec<T>>::new();
    for (topic, partition) in offsets.committed.keys() {
        by_topic
            .entry(topic.as_str())
            .or_default()
            .push(row(topic, *partition));
    }
    by_topic
}

// The two wire shapes carry identical committed and invalid-offset fields.
// Missing rows carry offset/epoch -1 and an empty metadata string, never null.
krabka_macros::offset_fetch_rows! { wire::OffsetFetchResponsePartition { missing: missing_legacy_row, stable: stable_legacy_row }
wire::OffsetFetchResponsePartitions { missing: missing_group_row, stable: stable_group_row } }
