//! Assembly of the wire response: the resolved reads are grouped back into
//! per-topic entries, down-converted for a v0-v3 fetcher, and metered.

use {
    super::plan::PendingRead,
    crate::{broker::Broker, handlers::fetch_downconvert::down_convert_payload_for_fetch},
    krabka_protocol::{
        owned::fetch_response::{FetchableTopicResponse, LeaderIdAndEpoch},
        records::RecordsPayload,
    },
};

/// Group the resolved `PendingRead`s back into per-topic response entries.
///
/// This is Kafka's `FetchResponse.toMessage`: it walks the rows in order and
/// adds a row to the previous topic entry only when the row's topic is that
/// entry's topic; otherwise the row opens a new entry. A topic whose rows are
/// not adjacent, as the refused rows that `build_pending_reads` moves to the
/// end can make them, therefore answers in more than one entry.
///
/// The topic's identity is the whole of it: the id and the name. A v13
/// request names topics by id only, so every topic whose id does not resolve
/// carries the empty name. A key of the name alone would fold those topics
/// into one entry under the first id.
///
/// The function also returns the per-topic `cpu_micros` accumulators. They
/// line up by position with the returned `Vec`, so `cpu_micros[ti][pi]`
/// matches `responses[ti].partitions[pi]`. The caller can then attribute CPU
/// without a re-key by topic name.
pub(super) type GroupedResponses = (Vec<FetchableTopicResponse>, Vec<Vec<u64>>);

pub(super) fn group_into_topic_responses(pending: Vec<PendingRead>) -> GroupedResponses {
    let mut responses: Vec<FetchableTopicResponse> = Vec::new();
    let mut cpu_micros: Vec<Vec<u64>> = Vec::new();
    for p in pending {
        let continues_previous = responses
            .last()
            .is_some_and(|topic| topic.topic_id == p.topic_id && topic.topic == p.topic_name);
        if !continues_previous {
            responses.push(FetchableTopicResponse {
                topic: p.topic_name,
                topic_id: p.topic_id,
                partitions: Vec::new(),
                ..Default::default()
            });
            cpu_micros.push(Vec::new());
        }
        responses
            .last_mut()
            .expect("an entry was just pushed")
            .partitions
            .push(p.out);
        cpu_micros
            .last_mut()
            .expect("an entry was just pushed")
            .push(p.cpu_micros);
    }
    (responses, cpu_micros)
}

/// Clear every row's KIP-951 `CurrentLeader` below Fetch v16.
///
/// The schema carries `CurrentLeader` as a tagged field from v12, so a row
/// that names a leader would encode it at v12 to v15. Kafka's
/// `KafkaApis.handleFetchRequest` fills it only when `versionId >= 16`, for
/// a `NOT_LEADER_OR_FOLLOWER` or `FENCED_LEADER_EPOCH` row. The planning
/// and read paths name the leader whatever the version, and this is the one
/// place that withholds it from an older fetcher.
pub(super) fn withhold_current_leader_before_kip_951(
    version: i16,
    responses: &mut [FetchableTopicResponse],
) {
    if version >= super::KIP_951_FETCH_VERSION {
        return;
    }
    for partition in responses
        .iter_mut()
        .flat_map(|topic| topic.partitions.iter_mut())
    {
        partition.current_leader = LeaderIdAndEpoch::default();
    }
}

pub(super) fn downconvert_legacy_responses(
    broker: &Broker,
    version: i16,
    responses: &mut [FetchableTopicResponse],
) {
    if version >= 4 {
        return;
    }
    for topic in responses {
        // Resolve the owned name once per topic, not once per converted
        // partition: the counter's label set holds an `Arc<str>`, and the
        // registry hands back the copy it already keys the topic by.
        let topic_name = broker.partitions.shared_topic_name(&topic.topic);
        for partition in &mut topic.partitions {
            let Some(payload) = partition.records.take() else {
                continue;
            };
            match down_convert_payload_for_fetch(&payload, version) {
                Ok(Some(converted)) => {
                    if converted.payload_len() > 0 {
                        partition.records = Some(converted);
                    }
                    if !topic.topic.is_empty() {
                        broker.metrics.record_fetch_message_conversion(&topic_name);
                    }
                }
                Ok(None) => {}
                Err(error_code) => partition.error_code = error_code,
            }
        }
    }
}

/// One partition row as the fetch metrics count it: its index, its record
/// bytes, and whether it carried an error.
type PartitionMetricRow = (i32, u64, bool);

/// The per-topic rows a fetch read, as the fetch metrics count them.
pub(super) struct FetchMetricRows(Vec<(std::sync::Arc<str>, Vec<PartitionMetricRow>)>);

/// Charge each partition's read time to its CPU metric, which counts work
/// done whether or not the response is sent, and keep the rows the byte and
/// error metrics count once the response is known to go out unthrottled.
pub(super) fn charge_fetch_cpu(
    broker: &Broker,
    responses: &[FetchableTopicResponse],
    cpu_micros_by_index: &[Vec<u64>],
) -> FetchMetricRows {
    let mut rows = Vec::with_capacity(responses.len());
    for (topic_index, topic) in responses.iter().enumerate() {
        if topic.topic.is_empty() {
            continue;
        }
        // Resolve the owned topic name once for the whole topic: the wire
        // response carries a `String`, but the label sets hold an `Arc<str>`.
        // The registry hands back the copy it already keys the topic by, so a
        // locally hosted topic — every topic a `Fetch` reads from — costs
        // nothing here.
        let topic_name = broker.partitions.shared_topic_name(&topic.topic);
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            let bytes = partition
                .records
                .as_ref()
                .map_or(0, RecordsPayload::payload_len) as u64;
            partitions.push((partition.partition_index, bytes, partition.error_code != 0));
            if let Some(micros) = cpu_micros_by_index
                .get(topic_index)
                .and_then(|partitions| partitions.get(partition_index))
            {
                broker.metrics.record_partition_cpu_micros(
                    &topic_name,
                    partition.partition_index,
                    *micros,
                );
            }
        }
        rows.push((topic_name, partitions));
    }
    FetchMetricRows(rows)
}

/// Count the bytes and errors of a response that goes out. A throttled
/// consumer fetch sends no rows, so it counts none.
pub(super) fn record_fetch_metrics(
    broker: &Broker,
    rows: FetchMetricRows,
    is_follower_fetch: bool,
) {
    for (topic_name, partitions) in rows.0 {
        let mut topic_bytes = 0;
        for (partition_index, bytes, failed) in partitions {
            broker
                .metrics
                .record_partition_fetch(&topic_name, partition_index, bytes);
            if failed {
                broker.metrics.record_failed_fetch(&topic_name);
            }
            if is_follower_fetch {
                broker
                    .metrics
                    .record_replication_out(&topic_name, partition_index, bytes);
            }
            topic_bytes += bytes;
        }
        broker.metrics.record_fetch(&topic_name, topic_bytes);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{group_into_topic_responses, withhold_current_leader_before_kip_951},
        crate::{
            codes::{FENCED_LEADER_EPOCH, TOPIC_AUTHORIZATION_FAILED, UNKNOWN_TOPIC_ID},
            handlers::fetch::{
                PendingRead,
                plan::{refused_partition, refused_read},
            },
        },
        assert2::check,
        krabka_protocol::{
            owned::fetch_response::{FetchableTopicResponse, LeaderIdAndEpoch, PartitionData},
            primitives::uuid::Uuid as WireUuid,
        },
    };

    fn row(topic: &str, topic_id: u8, out: PartitionData) -> PendingRead {
        PendingRead {
            topic_name: topic.to_owned(),
            topic_id: WireUuid([topic_id; 16]),
            partition_index: out.partition_index,
            current_leader_epoch: -1,
            last_fetched_epoch: -1,
            fetch_offset: 0,
            max_bytes: 1024,
            read_committed: false,
            is_follower_fetch: false,
            fetch_only_leader: false,
            follower_position: None,
            partition: None,
            cpu_micros: u64::try_from(out.partition_index).expect("non-negative index"),
            out,
        }
    }

    fn topic(topic: &str, topic_id: u8, partitions: Vec<PartitionData>) -> FetchableTopicResponse {
        FetchableTopicResponse {
            topic: topic.to_owned(),
            topic_id: WireUuid([topic_id; 16]),
            partitions,
            ..Default::default()
        }
    }

    /// Kafka's `FetchResponse.toMessage` adds a row to the previous topic
    /// entry only when the topic matches, so the refused rows that follow
    /// the rows read open entries of their own: a request
    /// `[t1-0, unknown-0, t1-1]` answers `t1 [0, 1]`, then `unknown [0]`, and
    /// a topic whose refused row lands after another topic answers twice.
    #[test]
    fn rows_group_by_adjacent_topic_in_plan_order() {
        let read = |index| PartitionData {
            partition_index: index,
            ..Default::default()
        };
        let unknown = refused_partition(0, UNKNOWN_TOPIC_ID);
        let denied = refused_partition(2, TOPIC_AUTHORIZATION_FAILED);
        let (responses, cpu_micros) = group_into_topic_responses(vec![
            row("t1", 1, read(0)),
            row("t1", 1, read(1)),
            row("t2", 2, read(0)),
            row("", 9, unknown.clone()),
            row("t1", 1, denied.clone()),
        ]);
        check!(
            responses
                == vec![
                    topic("t1", 1, vec![read(0), read(1)]),
                    topic("t2", 2, vec![read(0)]),
                    topic("", 9, vec![unknown]),
                    topic("t1", 1, vec![denied]),
                ]
        );
        check!(cpu_micros == vec![vec![0, 1], vec![0], vec![0], vec![2]]);
    }

    /// `CurrentLeader` is a tagged field from v12, but Kafka fills it only
    /// from v16: the fenced row keeps it at v16 and loses it at v12 and v15.
    #[test]
    fn current_leader_is_withheld_below_v16() {
        let leader = LeaderIdAndEpoch {
            leader_id: 2,
            leader_epoch: 7,
            ..Default::default()
        };
        let fenced = PartitionData {
            current_leader: leader.clone(),
            ..refused_read(0, FENCED_LEADER_EPOCH)
        };
        for (version, expected_leader) in [
            (12, LeaderIdAndEpoch::default()),
            (15, LeaderIdAndEpoch::default()),
            (16, leader.clone()),
        ] {
            let mut responses = vec![topic("t", 1, vec![fenced.clone()])];
            withhold_current_leader_before_kip_951(version, &mut responses);
            let expected = vec![topic("t", 1, vec![PartitionData {
                current_leader: expected_leader,
                ..fenced.clone()
            }])];
            check!(responses == expected, "v{version}");
        }
    }
}
