//! The two throttles a finished fetch passes: the KIP-73 leader-side
//! replication throttle on a follower fetch, and the KIP-13 consumer byte
//! rate together with the KIP-124 request quota on a client fetch.

use {
    crate::{
        broker::Broker,
        handlers::RequestContext,
        metrics::{BrokerMetrics, QuotaType},
        quota::{
            QuotaBuckets, QuotaDelay, debt_tokens, entity_field, lookup_quota_with_key,
            throttle_time_ms,
        },
        throttle::{REPLICATION_QUOTA_WINDOW, TokenBucket, TopicThrottle},
    },
    krabka_protocol::{
        Encode as _,
        owned::fetch_response::{FetchResponse, FetchableTopicResponse},
        records::RecordsPayload,
    },
    krabka_units::{Time, convert::TimeExt},
    num_traits::ToPrimitive as _,
};

pub(super) fn throttle_follower_responses(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    follower_id: i32,
    responses: &mut [FetchableTopicResponse],
) {
    let Ok(follower) = u64::try_from(follower_id) else {
        return;
    };
    throttle_replica_traffic(
        &broker.throttle_state.leader_out,
        &broker.metrics,
        image,
        ReplicaPair {
            leader: broker.config.node_id,
            follower: krabka_metadata::NodeId(follower),
        },
        responses,
    );
}

/// The leader and the follower of one replica fetch.
#[derive(Clone, Copy)]
struct ReplicaPair {
    leader: krabka_metadata::NodeId,
    follower: krabka_metadata::NodeId,
}

/// The KIP-73 leader-side throttle of one follower fetch, run by the leader
/// `leader` for the replica `follower`.
///
/// A partition is throttled when the leader's own id is on its
/// `leader.replication.throttled.replicas` list, whichever follower fetches it
/// (`ConfigHandler.parseThrottledPartitions` keeps the entries that name the
/// broker itself, and `ReplicationQuotaManager.isThrottled` is per partition,
/// #1210). A follower that is in the partition's ISR is never throttled, to
/// avoid ISR thrashing (`ReplicaManager.shouldLeaderThrottle`, #1211), but its
/// bytes still count against the quota, as `KafkaApis.sizeOfThrottledPartitions`
/// records the size of every throttled partition, in sync or not.
///
/// The out-of-sync partitions share what the bucket holds, and a partition
/// past it is answered without records. The in-sync bytes are recorded after
/// that, so a burst of them leaves the bucket in debt and the next fetch of an
/// out-of-sync replica finds nothing to draw. The debt is kept for one
/// replication quota window at most ([`REPLICATION_QUOTA_WINDOW`]), as Kafka's
/// quota forgets a sample that leaves its window, so the replica is starved for
/// that long and not for as long as the burst was big.
///
/// [`REPLICATION_QUOTA_WINDOW`]: crate::throttle::REPLICATION_QUOTA_WINDOW
fn throttle_replica_traffic(
    leader_out: &TokenBucket,
    metrics: &BrokerMetrics,
    image: &krabka_metadata::MetadataImage,
    ReplicaPair { leader, follower }: ReplicaPair,
    responses: &mut [FetchableTopicResponse],
) {
    let mut in_sync_bytes = 0_u64;
    let mut out_of_sync_bytes = 0_u64;
    let mut out_of_sync = Vec::new();
    for (topic_index, topic) in responses.iter().enumerate() {
        let throttle = TopicThrottle::for_topic(image, &topic.topic, leader);
        for (partition_index, partition) in topic.partitions.iter().enumerate() {
            if !throttle.leader.contains(partition.partition_index) {
                continue;
            }
            let bytes = partition
                .records
                .as_ref()
                .map_or(0, RecordsPayload::payload_len) as u64;
            let in_sync = image
                .partition(&topic.topic, partition.partition_index)
                .is_some_and(|record| record.isr.contains(&follower));
            if in_sync {
                in_sync_bytes += bytes;
            } else {
                out_of_sync_bytes += bytes;
                out_of_sync.push(ResponseSlot {
                    topic_index,
                    partition_index,
                });
            }
        }
    }
    let mut sent_out_of_sync = out_of_sync_bytes;
    if out_of_sync_bytes > 0 {
        let granted = leader_out.try_consume(out_of_sync_bytes);
        if granted < out_of_sync_bytes {
            sent_out_of_sync = truncate_throttled_responses(
                responses,
                &out_of_sync,
                granted,
                leader_out.token_burst(),
            );
            // Whole partitions are dropped, so the budget a dropped partition
            // did not fit goes back to the bucket. A partition larger than
            // the bucket's burst is the exception: Kafka must make progress
            // on an oversized partition payload, so one such partition may pass
            // once this round had positive credit. Charge its overage as
            // bounded debt, which keeps later fetches throttled without
            // allowing the same oversized response to spin through for free.
            if sent_out_of_sync < granted {
                leader_out.refund(granted - sent_out_of_sync);
            } else if sent_out_of_sync > granted {
                leader_out.record_bounded(sent_out_of_sync - granted, REPLICATION_QUOTA_WINDOW);
            }
            // The bucket had less than the round asked for, so some of what
            // this follower was owed is held back. Kafka delays the fetch;
            // krabka truncates it and the follower re-asks next round.
            metrics.record_replication_throttle_sleep();
        }
    }
    leader_out.record_bounded(in_sync_bytes, REPLICATION_QUOTA_WINDOW);
    // KIP-73: the measured leader-side throttled-replication rate, which
    // Kafka publishes as
    // `kafka.server:type=LeaderReplication,name=byte-rate`. It is what
    // tells an operator whose reassignment is not moving whether the
    // throttle is biting or something else is wrong.
    let sent = in_sync_bytes + sent_out_of_sync;
    if sent > 0 {
        metrics.record_replication_throttled_out(sent);
    }
}

/// The bytes a consumer may read in one fetch without a throttle: Kafka's
/// `ClientQuotaManager.maxValueInQuotaWindow`, which is
/// `consumer_byte_rate * (quota.window.num - 1) * quota.window.size.seconds`.
/// `quota_window` is the whole `quota.window.num * quota.window.size.seconds`
/// and `quota_throttle_max` is one `quota.window.size.seconds`. Without a
/// `consumer_byte_rate` the read is not capped.
pub(super) fn consumer_quota_window_bytes(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
) -> usize {
    let Some((_, rate)) = lookup_quota_with_key(
        image,
        &context.principal.name,
        context.client_id,
        "consumer_byte_rate",
    ) else {
        return usize::MAX;
    };
    if !rate.is_finite() || rate <= 0.0 {
        return usize::MAX;
    }
    let window_secs = (broker.config.quota_window - broker.config.quota_throttle_max)
        .secs_f64()
        .max(0.0);
    (rate * window_secs).to_usize().unwrap_or(usize::MAX)
}

/// The byte-rate charge a consumer fetch made, in bytes, which a throttled
/// fetch gives back.
pub(super) struct ConsumerCharge(Option<ChargedBucket>);

/// The bucket a consumer fetch drew from and the bytes it drew.
struct ChargedBucket {
    bucket: std::sync::Arc<TokenBucket>,
    bytes: u64,
}

impl ConsumerCharge {
    /// Kafka's `quotas.fetch.unrecordQuotaSensor`: a throttled fetch sends
    /// no records, so the bytes it was charged come off the quota. The whole
    /// charge comes back, the debt it left included.
    pub(super) fn refund(self) {
        if let Some(ChargedBucket { bucket, bytes }) = self.0 {
            bucket.refund(bytes);
        }
    }
}

/// Charge a consumer fetch whose whole response is `response_bytes` long to
/// its `consumer_byte_rate` and `request_percentage` quotas, as Kafka's
/// `KafkaApis.handleFetchRequest` records both with
/// `fetchContext.getResponseSize`. Returns the throttle the response reports,
/// and the byte-rate charge to give back when that throttle is above zero.
pub(super) fn apply_consumer_fetch_quota(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    context: &RequestContext<'_>,
    handler_start: std::time::Instant,
    response_bytes: u64,
) -> (i32, ConsumerCharge) {
    let ConsumerQuotaOutcome {
        throttle: data_delay,
        charge,
    } = consume_consumer_quota(
        image,
        &broker.quota_buckets,
        &context.principal.name,
        context.client_id,
        response_bytes,
    );
    let request_delay = context.charge_request_quota(broker, image, handler_start);
    // KIP-219: the connection is muted for the larger of the two delays.
    // Resolving it through the metric records the throttle phase and the quota
    // that caused it, and hands back the delay the response reports.
    let delay = broker
        .metrics
        .record_applied_throttle(super::FETCH_API_KEY, &[
            (QuotaType::Fetch, data_delay).into(),
            (QuotaType::Request, request_delay).into(),
        ]);
    if delay <= <Time as TimeExt>::ZERO {
        return (0, charge);
    }
    // KIP-219: the window goes back to the connection loop, which mutes the
    // connection after the response is written.
    context.record_throttle(delay);
    (throttle_time_ms(delay), charge)
}

/// The encoded size of a whole Fetch response at `version`, which is what
/// Kafka's `FetchContext.getResponseSize` charges to the consumer byte rate.
pub(super) fn fetch_response_size(response: &FetchResponse, version: i16) -> u64 {
    let size = if version < 4 {
        let legacy: krabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            response.clone().into();
        legacy.encoded_len(version)
    } else {
        response.encoded_len(version)
    };
    u64::try_from(size).unwrap_or(u64::MAX)
}

/// The position of one partition within a fetch response's topics.
#[derive(Clone, Copy)]
struct ResponseSlot {
    topic_index: usize,
    partition_index: usize,
}

/// KIP-73 leader-side throttle: walk `throttled_idxs` in order and drop
/// whole-partition chunks until the remaining throttled bytes fit in
/// `budget`, allowing one over-burst partition through when this round had credit.
///
/// The function drops a partition completely and sets its records to `None`.
/// It never truncates a partition payload in the middle of its record batches.
/// It returns the bytes it left in place.
fn truncate_throttled_responses(
    responses: &mut [FetchableTopicResponse],
    throttled_idxs: &[ResponseSlot],
    budget: u64,
    burst: u64,
) -> u64 {
    let mut remaining = budget;
    let mut kept = 0;
    let mut sent_oversized_partition = false;
    for &ResponseSlot {
        topic_index,
        partition_index,
    } in throttled_idxs
    {
        let part = &mut responses[topic_index].partitions[partition_index];
        let chunk_size = part.records.as_ref().map_or(0, RecordsPayload::payload_len) as u64;
        if chunk_size <= remaining {
            remaining -= chunk_size;
            kept += chunk_size;
        } else if budget > 0 && burst < chunk_size && !sent_oversized_partition {
            // Kafka permits a fetch to return an indivisible record batch
            // larger than the entire burst. Since this throttle drops whole
            // partition payloads, give one such payload a chance to advance;
            // the caller records its excess as debt.
            kept += chunk_size;
            sent_oversized_partition = true;
            remaining = 0;
        } else {
            // Budget exhausted — drop this chunk and all subsequent throttled ones.
            part.records = None;
            remaining = 0;
        }
    }
    kept
}

/// The throttle a consumer fetch earned from its byte-rate quota, with the
/// charge it made.
struct ConsumerQuotaOutcome {
    throttle: QuotaDelay,
    charge: ConsumerCharge,
}

/// KIP-13 `consumer_byte_rate` enforcement.
///
/// The function looks up the matching quota for `(principal, client_id)`,
/// takes `bytes` from the bucket, and returns the throttle delay with the
/// charge it made. The delay is zero when the config sets no quota, or when
/// the bucket has enough capacity.
fn consume_consumer_quota(
    image: &krabka_metadata::MetadataImage,
    buckets: &QuotaBuckets,
    principal: &str,
    client_id: Option<&str>,
    bytes: u64,
) -> ConsumerQuotaOutcome {
    let Some((entity_key, rate)) =
        lookup_quota_with_key(image, principal, client_id, "consumer_byte_rate")
    else {
        return ConsumerQuotaOutcome {
            throttle: QuotaDelay::zero(),
            charge: ConsumerCharge(None),
        };
    };
    if !rate.is_finite() || rate <= 0.0 {
        return ConsumerQuotaOutcome {
            throttle: QuotaDelay::zero(),
            charge: ConsumerCharge(None),
        };
    }
    let user = entity_field(&entity_key, "user");
    let client_id_opt = entity_field(&entity_key, "client-id");
    // Kafka holds the quota as a double (`ClientQuotaManager`), so the bucket
    // runs at the configured rate, fractional part included. Like the
    // producer path it records the whole response and turns the debt into the
    // throttle (#1212); a throttled fetch then gives the whole charge back.
    let bucket = buckets.get_or_create("consumer_byte_rate", &entity_key, rate);
    let debt = bucket.record(bytes);
    let charge = ConsumerCharge(Some(ChargedBucket {
        bucket: std::sync::Arc::clone(&bucket),
        bytes,
    }));
    let Some(overage) = debt_tokens(debt) else {
        return ConsumerQuotaOutcome {
            throttle: QuotaDelay::zero(),
            charge,
        };
    };
    let delay_secs = overage / rate;
    // Kafka's `ClientQuotaManager.throttleTime` does not bound a byte-rate
    // throttle.
    let delay = Time::from_secs_f64(delay_secs);
    ConsumerQuotaOutcome {
        throttle: QuotaDelay::new(delay, user, client_id_opt),
        charge,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::ConsumerQuotaOutcome,
        crate::{
            handlers::test_support::replicated_partition,
            metrics::BrokerMetrics,
            quota::{
                QuotaBuckets,
                test_support::{QuotaEntityInput, check_fractional_bandwidth, image_with_quota},
            },
            throttle::{LEADER_THROTTLED_REPLICAS_KEY, TokenBucket},
        },
        assert2::{assert, check},
        krabka_units::{Time, convert::TimeExt, millis, secs},
    };

    #[test]
    fn consume_consumer_quota_tuple_match_overage_throttles() {
        let img = image_with_quota()
            .entity(vec![
                QuotaEntityInput::user(Some("alice")),
                QuotaEntityInput::client_id(Some("app-x")),
            ])
            .key("consumer_byte_rate")
            .value(1024.0)
            .call();
        // A one-second window, so 4096 bytes at 1024 B/s is over the burst
        // rather than inside the default 11-second one.
        let buckets = QuotaBuckets::with_window(secs(1));
        // 3072 bytes over at 1024 B/s is three seconds, reported whole: Kafka
        // does not bound a byte-rate throttle (#709).
        let delay_match =
            super::consume_consumer_quota(&img, &buckets, "alice", Some("app-x"), 4096).throttle;
        assert!(
            delay_match > millis(2_900) && delay_match <= secs(3),
            "tuple quota match should throttle for the whole overage; got {delay_match:?}"
        );
        let buckets2 = QuotaBuckets::with_window(secs(1));
        let delay_other =
            super::consume_consumer_quota(&img, &buckets2, "alice", Some("other"), 4096).throttle;
        assert!(
            delay_other == <Time as TimeExt>::ZERO,
            "non-matching client_id should not throttle; got {delay_other:?}"
        );
    }

    /// A fetch is charged in full and, when that throttles it, the whole
    /// charge comes back (`ClientQuotaManager.unrecordQuotaSensor`), the debt
    /// it left included: the fetch that follows the throttle finds the bucket
    /// as it was. A throttled fetch that is not refunded stays charged.
    #[test]
    fn a_throttled_fetch_gives_the_whole_charge_back() {
        let img = image_with_quota().key("consumer_byte_rate").call();
        let consume = |buckets: &QuotaBuckets, bytes| {
            super::consume_consumer_quota(&img, buckets, "alice", Some("app"), bytes)
        };

        let refunded = QuotaBuckets::with_window(secs(1));
        let ConsumerQuotaOutcome { throttle, charge } = consume(&refunded, 1_500);
        charge.refund();
        let after_refund = consume(&refunded, 1_000).throttle;

        let kept = QuotaBuckets::with_window(secs(1));
        let _ = consume(&kept, 1_500);
        let after_kept = consume(&kept, 1_000).throttle;

        assert!(throttle > <Time as TimeExt>::ZERO, "{throttle:?}");
        assert!(after_refund == <Time as TimeExt>::ZERO, "{after_refund:?}");
        assert!(after_kept > millis(1_400), "{after_kept:?}");
    }

    /// Kafka's `ClientQuotaManager` holds `consumer_byte_rate` as a double,
    /// so a fractional rate throttles a fetch at that rate: the shortfall
    /// over the rate, neither unbounded nor rounded to a whole byte per
    /// second.
    #[test]
    fn a_fractional_consumer_byte_rate_throttles() {
        // `(consumer_byte_rate, response bytes, expected throttle)`. The
        // one-second window gives the bucket a burst of exactly its rate.
        check_fractional_bandwidth("consumer_byte_rate", |image, buckets, bytes| {
            super::consume_consumer_quota(image, buckets, "alice", Some("app"), bytes)
                .throttle
                .delay
        });
    }

    /// The image of leader broker 1 with topic `t`, whose partition `i` has
    /// the follower, broker 2, in its ISR when `isr_has_follower[i]` says so,
    /// and the given leader-side throttled-replicas list.
    fn leader_image(list: &str, isr_has_follower: &[bool]) -> krabka_metadata::MetadataImage {
        use krabka_metadata::{
            MetadataImage, MetadataRecord, NodeId, PartitionRecord, TopicConfigRecord, TopicRecord,
        };
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: uuid::Uuid::from_u128(7),
            partitions: i32::try_from(isr_has_follower.len()).expect("few partitions"),
            replication_factor: 2,
        }));
        for (partition, in_sync) in (0..).zip(isr_has_follower) {
            image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                isr: if *in_sync {
                    vec![NodeId(1), NodeId(2)]
                } else {
                    vec![NodeId(1)]
                },
                ..replicated_partition()
                    .topic("t")
                    .partition(partition)
                    .leader(1)
                    .replicas(&[NodeId(1), NodeId(2)])
                    .call()
            }));
        }
        image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
            topic: "t".into(),
            overrides: [(LEADER_THROTTLED_REPLICAS_KEY.to_owned(), list.to_owned())]
                .into_iter()
                .collect(),
        }));
        image
    }

    /// One partition of a follower fetch: the bytes of records it carries and
    /// whether the follower is in its ISR.
    #[derive(Clone, Copy)]
    struct FetchedPartition {
        bytes: usize,
        in_sync: bool,
    }

    fn synced(bytes: usize) -> FetchedPartition {
        FetchedPartition {
            bytes,
            in_sync: true,
        }
    }

    fn unsynced(bytes: usize) -> FetchedPartition {
        FetchedPartition {
            bytes,
            in_sync: false,
        }
    }

    /// Runs the leader-side throttle of the follower fetch of broker 2 over
    /// one partition per entry of `partitions`, each with that many bytes of
    /// records and with the follower in its ISR or not, and returns the bytes
    /// each row still carries.
    fn throttle_leader_fetch(
        leader_out: &TokenBucket,
        list: &str,
        partitions: &[FetchedPartition],
    ) -> Vec<usize> {
        use krabka_protocol::{
            owned::fetch_response::{FetchableTopicResponse, PartitionData},
            records::RecordsPayload,
        };
        let in_isr: Vec<bool> = partitions.iter().map(|row| row.in_sync).collect();
        let image = leader_image(list, &in_isr);
        let mut responses = vec![FetchableTopicResponse {
            topic: "t".into(),
            partitions: (0..)
                .zip(partitions)
                .map(|(partition_index, row)| PartitionData {
                    partition_index,
                    records: Some(RecordsPayload::Raw(bytes::Bytes::from(vec![
                        0_u8;
                        row.bytes
                    ]))),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }];

        super::throttle_replica_traffic(
            leader_out,
            &BrokerMetrics::new(),
            &image,
            super::ReplicaPair {
                leader: krabka_metadata::NodeId(1),
                follower: krabka_metadata::NodeId(2),
            },
            &mut responses,
        );

        responses[0]
            .partitions
            .iter()
            .map(|partition| {
                partition
                    .records
                    .as_ref()
                    .map_or(0, RecordsPayload::payload_len)
            })
            .collect()
    }

    fn leader_bucket(rate: u32) -> TokenBucket {
        let bucket = TokenBucket::new();
        bucket.set_byte_rate(krabka_units::bytes_per_sec(rate));
        bucket
    }

    /// A partition is throttled on the leader when the leader's own id is on
    /// its `leader.replication.throttled.replicas` list, whichever follower
    /// fetches it (#1210): the list `kafka-reassign-partitions --throttle`
    /// writes names the source replicas, and never the destination that
    /// fetches. Each row is one fetch by broker 2, an out-of-sync follower,
    /// against a bucket that holds 500 bytes.
    #[test]
    fn a_leader_throttles_the_partitions_that_name_its_own_id() {
        // (label, list, bytes of each partition, bytes each keeps)
        let cases = [
            (
                "the leader's id is listed: over the budget is dropped",
                "0:1,1:1",
                vec![300, 300],
                vec![300, 0],
            ),
            (
                "the follower's id is listed, which is the inverted reading: no throttle",
                "0:2,1:2",
                vec![300, 300],
                vec![300, 300],
            ),
            (
                "only the listed partition is throttled and an oversized payload progresses",
                "1:1",
                vec![800, 800],
                vec![800, 800],
            ),
            (
                "the wildcard throttles every partition",
                "*",
                vec![300, 300],
                vec![300, 0],
            ),
            ("no list throttles nothing", "", vec![800, 800], vec![
                800, 800,
            ]),
        ];
        for (label, list, bytes, want) in cases {
            let partitions: Vec<FetchedPartition> = bytes.iter().map(|b| unsynced(*b)).collect();
            check!(
                throttle_leader_fetch(&leader_bucket(500), list, &partitions) == want,
                "{label}"
            );
        }
    }

    /// An indivisible partition payload larger than the one-second burst
    /// must eventually pass, and its overage leaves bounded debt so the next
    /// out-of-sync fetch cannot immediately pass as well (#1340).
    #[test]
    fn an_over_burst_replication_payload_progresses_and_is_charged() {
        let bucket = leader_bucket(500);

        let first = throttle_leader_fetch(&bucket, "*", &[unsynced(800)]);
        let second = throttle_leader_fetch(&bucket, "*", &[unsynced(800)]);

        check!((first, second) == (vec![800], vec![0]));
    }

    /// A follower in the ISR is never throttled, however far over the budget
    /// it is (#1211). An over-burst out-of-sync payload now progresses once,
    /// and its charge leaves the bucket with no immediate follow-up credit.
    #[test]
    fn an_in_sync_follower_is_never_throttled() {
        let kept = throttle_leader_fetch(&leader_bucket(100), "*", &[synced(5_000), synced(300)]);
        let bucket = leader_bucket(100);
        let first_out_of_sync = throttle_leader_fetch(&bucket, "*", &[unsynced(300)]);
        let next_out_of_sync = throttle_leader_fetch(&bucket, "*", &[unsynced(300)]);

        check!(
            (kept, first_out_of_sync, next_out_of_sync) == (vec![5_000, 300], vec![300], vec![0])
        );
    }

    /// The bytes an in-sync follower reads still count against the quota
    /// (`KafkaApis.sizeOfThrottledPartitions`), so a burst of them leaves the
    /// bucket in debt and an out-of-sync replica that fetches next is held
    /// back until the debt is repaid (#1211).
    #[test]
    fn in_sync_bytes_are_charged_against_the_leader_quota() {
        let bucket = leader_bucket(1_000);

        // The out-of-sync partition draws first and fits the 1000 bytes the
        // bucket holds; the in-sync partition's 4000 bytes are then recorded.
        let mixed = throttle_leader_fetch(&bucket, "*", &[synced(4_000), unsynced(600)]);
        let next = throttle_leader_fetch(&bucket, "*", &[unsynced(600)]);

        check!((mixed, next) == (vec![4_000, 600], vec![0]));
    }

    /// The debt an in-sync burst leaves is kept for one replication quota
    /// window at most, as Kafka's quota forgets a sample that leaves its
    /// window. Without the bound a lagging follower on a throttled partition
    /// is starved for as long as the burst was big, drops out of the ISR, and
    /// `acks=all` produces fail with `NOT_ENOUGH_REPLICAS`. The burst here is
    /// `100_000` bytes on a `1_000` B/s quota: 99 s of debt unbounded, 11 s bounded.
    #[test]
    fn an_in_sync_burst_is_forgotten_after_the_replication_window() {
        let clock = qubit_clock::ManualMonotonicClock::new_shared();
        let bucket = TokenBucket::with_clock(clock.clone());
        bucket.set_byte_rate(krabka_units::bytes_per_sec(1_000));

        throttle_leader_fetch(&bucket, "*", &[synced(100_000)]);
        let during = throttle_leader_fetch(&bucket, "*", &[unsynced(600)]);
        clock
            .advance(std::time::Duration::from_secs(12))
            .expect("manual time moves forward");
        let after = throttle_leader_fetch(&bucket, "*", &[unsynced(600)]);

        check!((during, after) == (vec![0], vec![600]));
    }

    /// A partition that does not fit the budget goes back to the bucket
    /// unspent, so the next fetch still has it to draw. Both chunks are below
    /// the burst; the first partition consumes part of this round's grant and
    /// the second partition is dropped without incurring oversized-batch debt.
    #[test]
    fn the_budget_of_a_dropped_partition_goes_back_to_the_bucket() {
        let bucket = leader_bucket(500);

        let first = throttle_leader_fetch(&bucket, "*", &[unsynced(400), unsynced(400)]);
        let second = throttle_leader_fetch(&bucket, "*", &[unsynced(100)]);

        check!((first, second) == (vec![400, 0], vec![100]));
    }

    /// A rate of zero is no throttle, in sync or not.
    #[test]
    fn an_unthrottled_leader_leaves_every_partition_alone() {
        let kept = throttle_leader_fetch(&TokenBucket::new(), "*", &[unsynced(800), synced(800)]);

        check!(kept == vec![800, 800]);
    }
}
