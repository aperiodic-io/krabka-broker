//! Proves that the markers a cut names are really in the log.
//!
//! A cut is a list of offsets, and it is worth exactly as much as the markers
//! behind it. This reads the log at each of those offsets and checks that the
//! batch there is a barrier control batch carrying this group and this epoch.
//!
//! The read cannot go through [`krabka_client_core::fetch_partition`], because
//! that drops control batches the way every Kafka consumer does, which is the
//! whole reason a marker is invisible in the first place. So this sends a raw
//! `Fetch` and decodes the batches itself.
//!
//! The client negotiates the highest `Fetch` version the broker speaks, and
//! from version 13 the request names a topic by id rather than by name. The
//! metadata read that finds each leader therefore also carries the topic id
//! back, so the fetch is correct at any version.

use {
    crate::report::described_error,
    krabka_protocol::{
        krabka::barrier as api,
        owned::{
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            metadata_request::{MetadataRequest, MetadataRequestTopic},
        },
        records::RecordBatch,
    },
};

/// One offset that does not hold the marker the cut claims.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// The topic the cut named.
    pub topic: String,
    /// The partition the cut named.
    pub partition: i32,
    /// The offset the cut named.
    pub offset: i64,
    /// Why the log does not agree.
    pub reason: String,
}

/// What one verify found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyOutcome {
    /// The epoch that was verified.
    pub epoch: i64,
    /// Every offset that holds the marker it should, as topic, partition and
    /// offset.
    pub checked: Vec<(String, i32, i64)>,
    /// Every offset that does not.
    pub mismatches: Vec<Mismatch>,
}

/// Read the log at each of a cut's offsets and report what is there.
///
/// # Errors
///
/// Returns a message when the broker refuses the cut read, when the group has
/// no cut at `epoch`, or when a `Fetch` cannot be sent at all. A partition the
/// log disagrees with is a [`Mismatch`] in the outcome rather than an error:
/// the point of the command is to report every one of them, not to stop at the
/// first.
pub(crate) async fn verify(
    client: &krabka_client_core::Client,
    group: &str,
    epoch: i64,
) -> Result<VerifyOutcome, String> {
    let cut = fetch_cut(client, group, epoch).await?;
    let mut outcome = VerifyOutcome {
        epoch,
        checked: Vec::new(),
        mismatches: Vec::new(),
    };

    for topic in &cut.topics {
        let (topic_id, leaders) = resolve_topic(client, &topic.topic).await?;
        for partition in &topic.partitions {
            let Some(&leader) = leaders.get(&partition.partition) else {
                outcome.mismatches.push(Mismatch {
                    topic: topic.topic.clone(),
                    partition: partition.partition,
                    offset: partition.offset,
                    reason: "the partition has no leader, so the log cannot be read".to_owned(),
                });
                continue;
            };
            let batch = fetch_batch_at(
                client,
                leader,
                &topic.topic,
                topic_id,
                partition.partition,
                partition.offset,
            )
            .await;
            match batch
                .and_then(|batch| check_batch(batch.as_ref(), group, epoch, partition.offset))
            {
                Ok(()) => outcome.checked.push((
                    topic.topic.clone(),
                    partition.partition,
                    partition.offset,
                )),
                Err(reason) => outcome.mismatches.push(Mismatch {
                    topic: topic.topic.clone(),
                    partition: partition.partition,
                    offset: partition.offset,
                    reason,
                }),
            }
        }
    }
    Ok(outcome)
}

/// Read the one cut a group holds at `epoch`.
async fn fetch_cut(
    client: &krabka_client_core::Client,
    group: &str,
    epoch: i64,
) -> Result<api::BarrierCut, String> {
    let response = client
        .send(api::ListBarrierCutsRequest {
            group: group.to_owned(),
            from_epoch: epoch,
            max_results: 1,
            ..api::ListBarrierCutsRequest::default()
        })
        .await
        .map_err(|e| format!("cannot read the cuts of {group}: {e}"))?;
    if response.error_code != 0 {
        return Err(format!(
            "cannot read the cuts of {group}: {}",
            described_error(response.error_code, response.error_message.as_deref())
        ));
    }
    response
        .cuts
        .into_iter()
        .find(|cut| cut.epoch == epoch)
        .ok_or_else(|| format!("{group} has no retained cut at epoch {epoch}"))
}

/// The id of one topic, and the current leader of each of its partitions.
///
/// The id is what a modern `Fetch` names the topic by, and the leaders are
/// where each fetch has to go.
async fn resolve_topic(
    client: &krabka_client_core::Client,
    topic: &str,
) -> Result<
    (
        krabka_protocol::primitives::uuid::Uuid,
        std::collections::BTreeMap<i32, i32>,
    ),
    String,
> {
    let response = client
        .send(MetadataRequest {
            topics: Some(vec![MetadataRequestTopic {
                name: Some(topic.to_owned()),
                ..MetadataRequestTopic::default()
            }]),
            ..MetadataRequest::default()
        })
        .await
        .map_err(|e| format!("cannot read the metadata of {topic}: {e}"))?;
    let found = response
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some(topic))
        .ok_or_else(|| format!("the cluster does not know the topic {topic}"))?;
    if found.error_code != 0 {
        return Err(format!(
            "the cluster refused the metadata of {topic} with error {}",
            found.error_code
        ));
    }
    Ok((
        found.topic_id,
        found
            .partitions
            .iter()
            .map(|p| (p.partition_index, p.leader_id))
            .collect(),
    ))
}

/// Fetch the batch that starts at `offset`, if the leader serves one.
async fn fetch_batch_at(
    client: &krabka_client_core::Client,
    leader: i32,
    topic: &str,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
    partition: i32,
    offset: i64,
) -> Result<Option<RecordBatch>, String> {
    let response = client
        .broker(leader)
        .send(FetchRequest {
            replica_id: -1,
            max_wait_ms: 1_000,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.to_owned(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition,
                    fetch_offset: offset,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .map_err(|e| format!("cannot fetch {topic}-{partition} at {offset}: {e}"))?;

    let Some(part) = response
        .responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .find(|p| p.partition_index == partition)
    else {
        return Err(format!(
            "the leader served no response for {topic}-{partition}"
        ));
    };
    if part.error_code != 0 {
        return Err(format!(
            "the leader refused the fetch of {topic}-{partition} with error {}",
            part.error_code
        ));
    }
    Ok(part
        .records
        .as_ref()
        .and_then(krabka_protocol::records::RecordsPayload::as_v2)
        .and_then(|batches| {
            batches
                .iter()
                .find(|batch| batch.base_offset == offset)
                .cloned()
        }))
}

/// Decide whether one fetched batch is the marker a cut claims.
///
/// Pulled out of the read so the reasons can be driven directly. Every branch
/// names something that would make a cut a lie: no batch there at all, an
/// ordinary data batch, a transaction marker, or a marker from another group or
/// another epoch.
fn check_batch(
    batch: Option<&RecordBatch>,
    group: &str,
    epoch: i64,
    offset: i64,
) -> Result<(), String> {
    let Some(batch) = batch else {
        return Err(format!("the log holds no batch starting at {offset}"));
    };
    if !batch.attributes.is_control_batch() {
        return Err(format!(
            "the batch at {offset} is a data batch, not a control batch"
        ));
    }
    let Some(record) = batch.records.first() else {
        return Err(format!("the control batch at {offset} holds no record"));
    };
    let marker = krabka_broker::parse_barrier_marker(record)
        .map_err(|e| format!("the control batch at {offset} is not a barrier marker: {e}"))?;
    if marker.group != group {
        return Err(format!(
            "the marker at {offset} belongs to group {}, not {group}",
            marker.group
        ));
    }
    if marker.epoch != epoch {
        return Err(format!(
            "the marker at {offset} carries epoch {}, not {epoch}",
            marker.epoch
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        bytes::Bytes,
        krabka_protocol::records::{Attributes, Record},
    };

    /// A barrier marker for `group` at `epoch`, built the way the broker builds
    /// one so the key layout cannot drift from the parser.
    fn marker_batch(group: &str, epoch: i64, offset: i64) -> RecordBatch {
        let mut key = Vec::new();
        key.extend_from_slice(&0i16.to_be_bytes());
        key.extend_from_slice(&1000i16.to_be_bytes());
        let mut value = Vec::new();
        value.extend_from_slice(&0i16.to_be_bytes());
        let bytes = group.as_bytes();
        value.extend_from_slice(
            &i16::try_from(bytes.len())
                .expect("short group")
                .to_be_bytes(),
        );
        value.extend_from_slice(bytes);
        value.extend_from_slice(&epoch.to_be_bytes());
        value.extend_from_slice(&1_724_500_000_000i64.to_be_bytes());
        RecordBatch {
            base_offset: offset,
            attributes: Attributes::default().with_control(true),
            records: vec![Record {
                key: Some(Bytes::from(key)),
                value: Some(Bytes::from(value)),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    #[test]
    fn a_matching_marker_verifies() {
        let batch = marker_batch("orders-cut", 7, 42);
        check!(check_batch(Some(&batch), "orders-cut", 7, 42).is_ok());
    }

    /// Every one of these would make a published cut a lie, so each has to be
    /// reported rather than passed over.
    #[test]
    fn a_cut_that_the_log_does_not_back_is_reported() {
        let good = marker_batch("orders-cut", 7, 42);
        let other_group = marker_batch("payments-cut", 7, 42);
        let other_epoch = marker_batch("orders-cut", 8, 42);
        let data = RecordBatch {
            base_offset: 42,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        let empty_control = RecordBatch {
            base_offset: 42,
            attributes: Attributes::default().with_control(true),
            ..RecordBatch::default()
        };

        let cases: &[(&str, Option<&RecordBatch>, &str)] = &[
            ("no batch at the offset", None, "holds no batch"),
            ("a data batch", Some(&data), "not a control batch"),
            (
                "a control batch with no record",
                Some(&empty_control),
                "holds no record",
            ),
            (
                "another group's marker",
                Some(&other_group),
                "belongs to group",
            ),
            (
                "another epoch's marker",
                Some(&other_epoch),
                "carries epoch",
            ),
        ];
        for (case, batch, expected) in cases {
            let reason = check_batch(*batch, "orders-cut", 7, 42).expect_err(case);
            check!(reason.contains(expected), "{case}: got {reason}");
        }

        // The good batch is the control: the table above must be rejecting for
        // real reasons, not because check_batch rejects everything.
        check!(check_batch(Some(&good), "orders-cut", 7, 42).is_ok());
    }

    /// A transaction marker is a control batch too, and its key parses to a
    /// different control type, so it must not pass as a barrier.
    #[test]
    fn a_transaction_marker_is_not_a_barrier_marker() {
        let mut key = Vec::new();
        key.extend_from_slice(&0i16.to_be_bytes());
        key.extend_from_slice(&1i16.to_be_bytes());
        let commit = RecordBatch {
            base_offset: 42,
            attributes: Attributes::default().with_control(true),
            records: vec![Record {
                key: Some(Bytes::from(key)),
                value: Some(Bytes::from_static(&[0, 0, 0, 0])),
                ..Record::default()
            }],
            ..RecordBatch::default()
        };
        let reason = check_batch(Some(&commit), "orders-cut", 7, 42).expect_err("rejects");
        check!(reason.contains("not a barrier marker"), "got {reason}");
    }
}
