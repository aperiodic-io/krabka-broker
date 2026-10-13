//! Sampling of the two lag gauges: how far each follower trails the leader of
//! a partition this broker leads, and how far each consumer group this broker
//! coordinates trails the high watermark of a partition it committed an offset
//! for.
//!
//! Both are subtractions the broker can do and no client can. The follower's
//! last-fetched offset is leader-side state that never leaves the leader, and a
//! group's committed offsets are coordinator-side state; putting the numbers on
//! the scrape endpoint is what lets an operator alert on "a follower is
//! drifting" or "a consumer is stuck" without standing a lag exporter beside
//! the cluster.
//!
//! Sampling is a poll rather than an event because lag changes when *nothing*
//! happens on the lagging side: a follower that stops fetching falls further
//! behind on every leader append, and a consumer that stops committing falls
//! further behind on every produce. Only the leader's own clock can observe
//! that.
//!
//! Each pass publishes the whole set of label sets it can justify, so
//! publishing also releases the series the pass no longer justifies —
//! see [`crate::metrics::lag`].

use {
    crate::{
        broker::registered_listener_endpoint,
        codes,
        coordinator::{
            GroupCoordinator,
            bootstrap::OFFSETS_TOPIC,
            partitioner,
            unified::{actor::GroupActorMessage, classic_state::OffsetEntry},
        },
        metadata_source::MetadataSource,
        metrics::{BrokerMetrics, ConsumerGroupLabel, ReplicaLagLabel},
        network::client::InterBrokerClient,
        partition_registry::PartitionRegistry,
    },
    krabka_client_core::ConnectionOptions,
    krabka_ids::PartitionIndex,
    krabka_metadata::{MetadataImage, NodeId},
    krabka_protocol::{
        owned::{
            fetch_request::{FetchPartition, FetchRequest, FetchTopic},
            fetch_response::FetchResponse,
            list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
            list_offsets_response::ListOffsetsResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    krabka_security::ListenerProtocol,
    krabka_units::{Time, convert::TimeExt as _, secs},
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::{Arc, atomic::Ordering},
    },
    tokio::sync::oneshot,
    tokio_util::sync::CancellationToken,
};

/// One `(topic, partition)` pair, as both samplers key their lookups.
type TopicPartition = (String, i32);

/// How often both lag families are resampled.
///
/// Lag is read at the granularity an operator alerts on, which is minutes of
/// backlog rather than seconds, so the interval is set by what a pass costs
/// rather than by how fresh the number could be. A pass walks this broker's
/// led partitions and asks each group's actor for its committed offsets, and
/// half a minute keeps that off the data path's back while still catching a
/// stalled follower well inside a scrape window.
pub(crate) const LAG_POLL_INTERVAL: Time = secs(30);

/// How long one pass waits for any single answer it does not compute itself:
/// one group actor's round trip, or one remote leader's watermark probe.
///
/// A pass has to end for the poller to return to its interval, and every
/// gauge the pass has not reached yet is stale until it does. An answer that
/// has not arrived well inside [`LAG_POLL_INTERVAL`] costs its own series this
/// pass and the pass carries on with the rest, which is the same rule the
/// samplers already follow for a watermark they cannot read.
pub(crate) const SAMPLE_WAIT: Time = secs(5);

/// The periodic sampler behind `replica_lag_records`,
/// `replica_lag_max_records` and `consumer_group_lag_records`.
pub(crate) struct LagPoller {
    pub node_id: NodeId,
    pub coordinator: Arc<GroupCoordinator>,
    pub metadata: Arc<dyn MetadataSource>,
    pub partitions: Arc<PartitionRegistry>,
    pub inter_broker: Arc<InterBrokerClient>,
    pub listener_protocol: ListenerProtocol,
    pub listener_name: String,
    /// The TLS SNI and SASL server name every inter-broker connection this
    /// broker opens is authenticated against, from
    /// `BrokerConfig::inter_broker_server_name`. A probe that used anything
    /// else would fail its handshake on a cluster that configures one.
    pub inter_broker_server_name: String,
    pub period: Time,
    pub metrics: BrokerMetrics,
    pub shutdown: CancellationToken,
}

impl LagPoller {
    /// Run the sampler until `shutdown`, then release every series it holds.
    ///
    /// Shutdown clears both families rather than freezing them: a broker that
    /// stopped sampling knows nothing about the lag, and a stale number is
    /// worse than a missing one for the alert an operator writes on it.
    pub(crate) fn spawn(self) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.period.to_std());
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    () = self.shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        // The pass itself is a cancellation branch, and not
                        // only the wait between passes. A pass reads a group
                        // actor and a remote leader, so it can be parked on an
                        // answer when shutdown arrives, and dropping it there
                        // is what lets the cleanup publishes below run at all.
                        tokio::select! {
                            () = self.shutdown.cancelled() => break,
                            () = self.sample() => {}
                        }
                    }
                }
            }
            self.metrics.publish_replica_lag(&HashMap::new());
            self.metrics.publish_consumer_group_lag(&HashMap::new());
        });
    }

    /// One pass over both families.
    ///
    /// Both halves read the same metadata image, so a partition that appears
    /// or moves mid-pass cannot make the two families disagree about who leads
    /// it.
    async fn sample(&self) {
        let image = self.metadata.current_image();
        self.metrics.publish_replica_lag(
            &replica_lag_samples(&self.partitions, self.node_id, &image).await,
        );
        self.metrics
            .publish_consumer_group_lag(&self.consumer_group_lag_samples(&image).await);
    }

    /// Lag of every group this broker coordinates, for every partition that
    /// group has a committed offset on.
    async fn consumer_group_lag_samples(
        &self,
        image: &MetadataImage,
    ) -> HashMap<ConsumerGroupLabel, i64> {
        let mut committed: HashMap<String, HashMap<TopicPartition, i64>> = HashMap::new();
        let mut wanted: HashSet<TopicPartition> = HashSet::new();
        for group_id in self.coordinator.consumer_group_ids() {
            if !coordinates_group(image, self.node_id, &group_id) {
                continue;
            }
            let Some(handle) = self.coordinator.find(&group_id) else {
                continue;
            };
            let offsets = committed_offsets(&handle.tx).await;
            let mut group_offsets = HashMap::new();
            for ((topic, partition), entry) in offsets {
                // A negative offset is the "no committed offset" sentinel the
                // OffsetFetch response encodes, and a partition the image no
                // longer holds has no watermark to lag behind. Neither is a
                // series.
                if entry.offset.0 < 0 || image.partition(&topic, partition).is_none() {
                    continue;
                }
                wanted.insert((topic.clone(), partition));
                group_offsets.insert((topic, partition), entry.offset.0);
            }
            if !group_offsets.is_empty() {
                committed.insert(group_id, group_offsets);
            }
        }
        let watermarks = self.high_watermarks(image, &wanted).await;
        let mut samples = HashMap::new();
        for (group_id, offsets) in committed {
            for ((topic, partition), offset) in offsets {
                let Some(high_watermark) = watermarks.get(&(topic.clone(), partition)) else {
                    continue;
                };
                samples.insert(
                    ConsumerGroupLabel {
                        group_id: group_id.clone(),
                        topic,
                        partition,
                    },
                    // A commit can sit ahead of the watermark for as long as it
                    // takes the leader to advance it. Lag is a backlog, so it
                    // floors at zero rather than going negative.
                    (high_watermark - offset).max(0),
                );
            }
        }
        samples
    }

    /// The high watermark of each wanted partition, read locally where this
    /// broker leads and probed over the inter-broker listener where it does
    /// not.
    ///
    /// A partition whose watermark cannot be read is absent from the result,
    /// and therefore has no series this pass. The probe is batched by leader,
    /// so a pass costs two round trips per remote broker rather than two per
    /// partition.
    async fn high_watermarks(
        &self,
        image: &MetadataImage,
        wanted: &HashSet<TopicPartition>,
    ) -> HashMap<TopicPartition, i64> {
        let mut watermarks = HashMap::new();
        let mut remote: HashMap<NodeId, Vec<TopicPartition>> = HashMap::new();
        for (topic, partition) in wanted {
            let Some(record) = image.partition(topic, *partition) else {
                continue;
            };
            if record.leader == self.node_id {
                if let Some(local) = self.partitions.get(topic, PartitionIndex(*partition)) {
                    watermarks.insert((topic.clone(), *partition), local.high_watermark().await.0);
                }
            } else {
                remote
                    .entry(record.leader)
                    .or_default()
                    .push((topic.clone(), *partition));
            }
        }
        for (leader, partitions) in remote {
            match self.probe_high_watermarks(image, leader, &partitions).await {
                Ok(probed) => watermarks.extend(probed),
                Err(error) => tracing::debug!(
                    %leader,
                    %error,
                    "consumer-group lag could not read a remote leader's high watermarks",
                ),
            }
        }
        watermarks
    }

    /// Resolve valid offsets with `ListOffsets`, then probe high watermarks
    /// with one zero-budget `Fetch` covering that leader's successful rows.
    ///
    /// The exchange is bounded by [`SAMPLE_WAIT`]: an unreachable leader must
    /// cost this pass its own series rather than the pass itself, because the
    /// poller cannot resample any family until it returns.
    async fn probe_high_watermarks(
        &self,
        image: &MetadataImage,
        leader: NodeId,
        partitions: &[TopicPartition],
    ) -> Result<HashMap<TopicPartition, i64>, String> {
        let broker = image
            .broker(leader)
            .ok_or_else(|| format!("unknown leader broker {leader}"))?;
        let (host, port) = registered_listener_endpoint(broker, &self.listener_name);
        let Some(offset_request) = probe_offsets_request(image, partitions) else {
            return Ok(HashMap::new());
        };
        let options = ConnectionOptions {
            client_id: format!("krabka-consumer-lag-{}", self.node_id),
            ..ConnectionOptions::default()
        };
        let exchange = async {
            let connection = self
                .inter_broker
                .connect_as_connection(
                    host,
                    port,
                    self.listener_protocol,
                    &self.inter_broker_server_name,
                    options,
                )
                .await
                .map_err(|error| error.to_string())?;
            let offsets: ListOffsetsResponse = connection
                .send(offset_request)
                .await
                .map_err(|error| error.to_string())?;
            let Some((request, names)) = probe_request(image, partitions, &offsets) else {
                connection.close();
                return Ok(HashMap::new());
            };
            let response: FetchResponse = connection
                .send(request)
                .await
                .map_err(|error| error.to_string())?;
            connection.close();
            Ok(probed_high_watermarks(&response, &names))
        };
        tokio::time::timeout(SAMPLE_WAIT.to_std(), exchange)
            .await
            .map_err(|_| format!("watermark probe to broker {leader} timed out"))?
    }
}

/// Read-committed LATEST gives each partition its stable consumer boundary.
/// Group topics in name order and fence both exchanges with the same epochs.
fn probe_offsets_request(
    image: &MetadataImage,
    partitions: &[TopicPartition],
) -> Option<ListOffsetsRequest> {
    let mut topics: BTreeMap<&str, ListOffsetsTopic> = BTreeMap::new();
    for (topic, partition) in partitions {
        let Some(record) = image.partition(topic, *partition) else {
            continue;
        };
        if image.topic(topic).is_none() {
            continue;
        }
        topics
            .entry(topic.as_str())
            .or_insert_with(|| ListOffsetsTopic {
                name: topic.clone(),
                ..Default::default()
            })
            .partitions
            .push(ListOffsetsPartition {
                partition_index: *partition,
                current_leader_epoch: record.leader_epoch.0,
                timestamp: -1,
                ..Default::default()
            });
    }
    (!topics.is_empty()).then(|| ListOffsetsRequest {
        replica_id: -1,
        isolation_level: 1,
        topics: topics.into_values().collect(),
        ..Default::default()
    })
}

/// The `Fetch` that asks one leader for the high watermark of each of
/// `partitions`, and the topic names its reply has to be read back through.
///
/// Skip failed, missing or negative `ListOffsets` rows. Fetch v13 dropped the
/// topic name from the response, so carry names by UUID across both exchanges.
/// Topics go in name order. A zero byte budget asks for no payload, though
/// first-batch progress can still return records if the visible end advances
/// between the exchanges; the sampler discards them and reads only watermarks.
fn probe_request<'a>(
    image: &MetadataImage,
    partitions: &'a [TopicPartition],
    offsets: &ListOffsetsResponse,
) -> Option<(FetchRequest, HashMap<WireUuid, &'a str>)> {
    let offsets: HashMap<_, _> = offsets
        .topics
        .iter()
        .flat_map(|topic| {
            topic.partitions.iter().filter_map(|row| {
                (row.error_code == codes::NONE && row.offset >= 0)
                    .then_some(((topic.name.as_str(), row.partition_index), row.offset))
            })
        })
        .collect();
    let mut topics: BTreeMap<&str, FetchTopic> = BTreeMap::new();
    let mut names: HashMap<WireUuid, &str> = HashMap::new();
    for (topic, partition) in partitions {
        let Some(record) = image.partition(topic, *partition) else {
            continue;
        };
        let Some(topic_id) = image.topic(topic).map(|topic| topic.topic_id) else {
            continue;
        };
        let Some(&fetch_offset) = offsets.get(&(topic.as_str(), *partition)) else {
            continue;
        };
        names.insert(WireUuid(*topic_id.as_bytes()), topic.as_str());
        topics
            .entry(topic.as_str())
            .or_insert_with(|| FetchTopic {
                topic: topic.clone(),
                topic_id: WireUuid(*topic_id.as_bytes()),
                ..Default::default()
            })
            .partitions
            .push(FetchPartition {
                partition: *partition,
                current_leader_epoch: record.leader_epoch.0,
                fetch_offset,
                partition_max_bytes: 0,
                ..Default::default()
            });
    }
    if topics.is_empty() {
        return None;
    }
    Some((
        FetchRequest {
            replica_id: -1,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            isolation_level: 1,
            topics: topics.into_values().collect(),
            ..Default::default()
        },
        names,
    ))
}

/// The high watermark of every partition row `response` reported without an
/// error, named by `names`.
///
/// A row is identified by topic id, because Fetch v13 removed the name from the
/// response and a v13 reply carries an empty one. A row whose id `names` does
/// not hold is a topic this probe did not ask about, and a row that carries an
/// error code is dropped rather than failing the pass: a single partition that
/// moved between the image read and the probe must not cost the other
/// partitions on the same broker their series.
fn probed_high_watermarks(
    response: &FetchResponse,
    names: &HashMap<WireUuid, &str>,
) -> HashMap<TopicPartition, i64> {
    if response.error_code != codes::NONE {
        return HashMap::new();
    }
    let mut watermarks = HashMap::new();
    for topic in &response.responses {
        let Some(name) = names.get(&topic.topic_id).copied().or_else(|| {
            names
                .values()
                .find(|name| **name == topic.topic.as_str())
                .copied()
        }) else {
            continue;
        };
        for partition in &topic.partitions {
            if partition.error_code == codes::NONE && partition.high_watermark >= 0 {
                watermarks.insert(
                    (name.to_owned(), partition.partition_index),
                    partition.high_watermark,
                );
            }
        }
    }
    watermarks
}

/// `true` when `node_id` leads the `__consumer_offsets` partition that hosts
/// `group_id`, which is what makes it that group's coordinator.
fn coordinates_group(image: &MetadataImage, node_id: NodeId, group_id: &str) -> bool {
    let p = partitioner::partition_for_group(image, group_id);
    matches!(image.partition(OFFSETS_TOPIC, p), Some(rec) if rec.leader == node_id)
}

/// Every committed offset the group's actor holds.
///
/// The offsets live on the protocol-agnostic `CoordinatorGroup`, so one message
/// serves a classic group and a KIP-848 group alike. An actor that has gone
/// away yields nothing, which is the same answer `OffsetFetch` gives, and so
/// does one that has not answered within [`SAMPLE_WAIT`]: a wedged actor costs
/// its own group's series this pass rather than the pass, which every other
/// group's series and both replica families would otherwise wait behind.
///
/// `FetchOffsets` also reports the keys an open transaction has not resolved
/// yet, and this pass drops them. `OffsetFetch` answers those
/// `UNSTABLE_OFFSET_COMMIT` under `read_committed` because a reader must not
/// see an offset that may still abort, but the gauge is a distance and the
/// stable commit under the pending key is the last offset the group is known
/// to have kept, so reporting lag from it is the honest reading of where the
/// group stands.
async fn committed_offsets(
    actor: &tokio::sync::mpsc::Sender<GroupActorMessage>,
) -> HashMap<TopicPartition, OffsetEntry> {
    let (reply, response) = oneshot::channel();
    let exchange = async {
        actor
            .send(GroupActorMessage::FetchOffsets { reply })
            .await
            .ok()?;
        response.await.ok()
    };
    let Ok(offsets) = tokio::time::timeout(SAMPLE_WAIT.to_std(), exchange).await else {
        tracing::debug!("a group actor did not answer the lag sampler in time");
        return HashMap::new();
    };
    offsets.unwrap_or_default().committed
}

/// Lag of every follower of every partition `node_id` currently leads.
///
/// The follower's tracked offset is what it last told the leader it had
/// persisted, so the difference from the leader's log end offset is the work
/// the follower still owes. It is read per partition rather than accumulated,
/// which is why a follower that stops fetching still shows a climbing value:
/// its tracked offset stands still while the leader's moves.
///
/// The followers come from the image's replica assignment rather than from
/// `ReplicaState::per_follower`. That map is fetch-driven — `install_isr`
/// seeds only ISR members, deliberately, so that `isr_maintenance` cannot
/// re-admit a replica on progress it never made — so a replica a reassignment
/// has just added, or one that has not fetched since this broker took the
/// leadership, has no entry in it. That is precisely the follower an operator
/// needs to see, so a missing entry reads as no progress rather than as no
/// series, and the gauge falls to the real lag on the follower's first fetch.
/// A partition the image no longer names is skipped: there is no assignment
/// left to justify a series.
pub(crate) async fn replica_lag_samples(
    partitions: &PartitionRegistry,
    node_id: NodeId,
    image: &MetadataImage,
) -> HashMap<ReplicaLagLabel, i64> {
    let mut samples = HashMap::new();
    for partition in partitions.arcs() {
        if partition.current_leader.load(Ordering::Acquire) != node_id.0 {
            continue;
        }
        let index = partition.index.get();
        let Some(record) = image.partition(&partition.topic, index) else {
            continue;
        };
        let leader_log_end = partition.log_end_offset().0;
        let state = partition.replica_state.lock().await;
        for follower in record
            .replicas
            .iter()
            .filter(|replica| **replica != node_id)
        {
            let follower_leo = state
                .per_follower
                .get(follower)
                .map_or(0, |stats| stats.leo.0);
            samples.insert(
                ReplicaLagLabel {
                    topic: partition.topic.clone(),
                    partition: index,
                    replica: follower.0,
                },
                (leader_log_end - follower_leo).max(0),
            );
        }
    }
    samples
}

#[cfg(test)]
mod tests;
