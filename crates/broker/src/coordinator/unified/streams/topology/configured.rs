//! Topology configuration: the task count of every subtopology, the partition
//! count of every internal topic, and the status that keeps a group
//! `NotReady`.
//!
//! This module ports Kafka's `InternalTopicManager.configureTopics` with its
//! helpers `RepartitionTopics`, `CopartitionedTopicsEnforcer` and
//! `ChangelogTopics`. The rules, their order, the status codes and the detail
//! strings are Kafka's:
//!
//! 1. A missing source topic stops the configuration with one
//!    `MISSING_SOURCE_TOPICS` status.
//! 2. A repartition topic with no explicit partition count gets the largest
//!    partition count of the source and repartition source topics of the
//!    subtopology that writes it.
//! 3. In a copartition group, the flexible repartition topics get the
//!    partition count of the external topics, which must all agree, or the
//!    count of the fixed repartition topics.
//! 4. A changelog topic gets the largest partition count of the source and
//!    repartition source topics of its subtopology.
//! 5. An internal topic that exists with another partition count is
//!    `INCORRECTLY_PARTITIONED_TOPICS`, and the internal topics that do not
//!    exist are `MISSING_INTERNAL_TOPICS`.
//!
//! Where Kafka iterates a `HashMap` or a `HashSet`, this port iterates in name
//! order, so that a message that names one of several topics is deterministic.

use {
    super::{metadata_hash::required_topics, status},
    crate::{
        codes,
        coordinator::unified::streams::persistence::{
            StoredSubtopology, StoredTopicInfo, StreamsGroupTopologyValue,
        },
    },
    krabka_metadata::MetadataImage,
    std::collections::{BTreeMap, BTreeSet},
};

/// Kafka's `ConfiguredInternalTopic`: a repartition or changelog topic with
/// its decided partition count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredInternalTopic {
    pub name: String,
    pub partitions: i32,
    /// The replication factor of the topology, or `None` when the topology
    /// leaves it at 0.
    pub replication_factor: Option<i16>,
    pub configs: BTreeMap<String, String>,
}

/// Kafka's `ConfiguredSubtopology`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredSubtopology {
    pub number_of_tasks: i32,
    pub source_topics: BTreeSet<String>,
    pub repartition_source_topics: BTreeMap<String, ConfiguredInternalTopic>,
    pub repartition_sink_topics: BTreeSet<String>,
    pub state_changelog_topics: BTreeMap<String, ConfiguredInternalTopic>,
}

/// Kafka's `ConfiguredTopology`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredTopology {
    pub topology_epoch: i32,
    /// The configured subtopologies by id. `None` when a status stopped the
    /// configuration before the subtopologies were sized.
    pub subtopologies: Option<BTreeMap<String, ConfiguredSubtopology>>,
    /// The internal topics that the metadata image does not hold, by name.
    pub internal_topics_to_create: BTreeMap<String, ConfiguredInternalTopic>,
    /// The `(status code, status detail)` that keeps the group `NotReady`:
    /// Kafka's `topicConfigurationException`.
    pub status: Option<(i8, String)>,
}

impl ConfiguredTopology {
    /// Kafka's `isReady`: no status keeps the group from an assignment.
    #[must_use]
    pub fn is_ready(&self) -> bool { self.status.is_none() }

    /// The number of tasks of each configured subtopology, by id. Empty when
    /// the subtopologies were not sized.
    #[must_use]
    pub fn number_of_tasks(&self) -> BTreeMap<String, i32> {
        self.subtopologies
            .iter()
            .flatten()
            .map(|(id, subtopology)| (id.clone(), subtopology.number_of_tasks))
            .collect()
    }
}

/// A topology that cannot be configured. The heartbeat answers with the error
/// code and message, and it changes nothing.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigureTopicsError {
    /// Kafka's `StreamsInvalidTopologyException`.
    #[error("{0}")]
    InvalidTopology(String),
    /// Kafka's `InvalidTopicException` from `Topic.validate` on an internal
    /// topic name.
    #[error("{0}")]
    InvalidTopic(String),
    /// An exception that Kafka does not map to a code (an
    /// `IllegalStateException` or an `IllegalArgumentException`). Kafka
    /// answers `UNKNOWN_SERVER_ERROR` with no message.
    #[error("{0}")]
    Internal(String),
}

impl ConfigureTopicsError {
    /// The wire error code of the heartbeat response.
    #[must_use]
    pub fn error_code(&self) -> i16 {
        match self {
            Self::InvalidTopology(_) => codes::STREAMS_INVALID_TOPOLOGY,
            Self::InvalidTopic(_) => codes::INVALID_TOPIC_EXCEPTION,
            Self::Internal(_) => codes::UNKNOWN_SERVER_ERROR,
        }
    }

    /// The `error_message` of the heartbeat response.
    #[must_use]
    pub fn error_message(&self) -> Option<String> {
        match self {
            Self::InvalidTopology(message) | Self::InvalidTopic(message) => Some(message.clone()),
            Self::Internal(_) => None,
        }
    }
}

/// Why [`configure`] stopped: a status that Kafka catches in
/// `configureTopics`, or an error that leaves the heartbeat.
enum Stop {
    Status(i8, String),
    Error(ConfigureTopicsError),
}

impl From<ConfigureTopicsError> for Stop {
    fn from(error: ConfigureTopicsError) -> Self { Self::Error(error) }
}

/// Kafka's `InternalTopicManager.configureTopics`.
///
/// # Errors
///
/// Returns the error that Kafka throws out of the heartbeat for a topology
/// that cannot be configured.
pub fn configure_topics(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<ConfiguredTopology, ConfigureTopicsError> {
    let copartition_groups = topology
        .subtopologies
        .iter()
        .map(copartition_groups)
        .collect::<Result<Vec<_>, _>>()?;
    match configure(topology, image, &copartition_groups) {
        Ok((subtopologies, internal_topics_to_create)) => {
            let status = (!internal_topics_to_create.is_empty()).then(|| {
                (
                    status::MISSING_INTERNAL_TOPICS,
                    format!(
                        "Internal topics are missing: {}",
                        summarize_topics(internal_topics_to_create.keys())
                    ),
                )
            });
            Ok(ConfiguredTopology {
                topology_epoch: topology.epoch,
                subtopologies: Some(subtopologies),
                internal_topics_to_create,
                status,
            })
        }
        Err(Stop::Status(code, detail)) => Ok(ConfiguredTopology {
            topology_epoch: topology.epoch,
            subtopologies: None,
            internal_topics_to_create: BTreeMap::new(),
            status: Some((code, detail)),
        }),
        Err(Stop::Error(error)) => Err(error),
    }
}

type Configured = (
    BTreeMap<String, ConfiguredSubtopology>,
    BTreeMap<String, ConfiguredInternalTopic>,
);

fn configure(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
    copartition_groups: &[Vec<BTreeSet<String>>],
) -> Result<Configured, Stop> {
    throw_on_missing_source_topics(topology, image)?;
    let decided = decide_partition_counts(topology, image, copartition_groups)?;
    let mut subtopologies = BTreeMap::new();
    for subtopology in &topology.subtopologies {
        let configured = from_persisted_subtopology(subtopology, image, &decided)?;
        if subtopologies
            .insert(subtopology.subtopology_id.clone(), configured)
            .is_some()
        {
            return Err(ConfigureTopicsError::Internal(format!(
                "Duplicate key {}",
                subtopology.subtopology_id
            ))
            .into());
        }
    }
    let internal_topics_to_create = missing_internal_topics(&subtopologies, topology, image)?;
    Ok((subtopologies, internal_topics_to_create))
}

/// Kafka's `throwOnMissingSourceTopics`.
fn throw_on_missing_source_topics(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<(), Stop> {
    let missing: BTreeSet<&str> = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.source_topics.iter())
        .filter(|topic| image.topic(topic).is_none())
        .map(String::as_str)
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(Stop::Status(
            status::MISSING_SOURCE_TOPICS,
            format!("Source topics {} are missing.", summarize_topics(missing)),
        ))
    }
}

/// Kafka's `decidePartitionCounts`: repartition topics, then copartition
/// coercion, then changelog topics.
fn decide_partition_counts(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
    copartition_groups: &[Vec<BTreeSet<String>>],
) -> Result<BTreeMap<String, i32>, Stop> {
    let mut decided = repartition_topic_partition_counts(topology, image)?;

    let (fixed, flexible): (Vec<&StoredTopicInfo>, Vec<&StoredTopicInfo>) = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.repartition_source_topics.iter())
        .partition(|topic| topic.partitions != 0);
    let fixed: BTreeSet<&str> = fixed.into_iter().map(|topic| topic.name.as_str()).collect();
    let flexible: BTreeSet<&str> = flexible
        .into_iter()
        .map(|topic| topic.name.as_str())
        .collect();
    for group in copartition_groups.iter().flatten() {
        let coerced = enforce_copartitioning(group, &fixed, &flexible, image, &decided)?;
        decided.extend(coerced);
    }

    let changelogs = changelog_topic_partition_counts(topology, image, &decided)?;
    decided.extend(changelogs);
    Ok(decided)
}

/// Kafka's `getPartitionCount`: the partition count in the image, else the
/// decided count of an internal topic.
fn partition_count(
    image: &MetadataImage,
    topic: &str,
    decided: &BTreeMap<String, i32>,
) -> Option<i32> {
    if image.topic(topic).is_some() {
        Some(image.topic_partition_count(topic))
    } else {
        decided.get(topic).copied()
    }
}

/// Kafka's `RepartitionTopics.setup`.
fn repartition_topic_partition_counts(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<BTreeMap<String, i32>, Stop> {
    let no_decisions = BTreeMap::new();
    let mut counts: BTreeMap<String, i32> = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.repartition_source_topics.iter())
        .filter(|topic| topic.partitions != 0)
        .map(|topic| (topic.name.clone(), topic.partitions))
        .collect();
    loop {
        let mut partition_count_needed = false;
        let mut progress_made = false;
        for subtopology in &topology.subtopologies {
            for sink in &subtopology.repartition_sink_topics {
                if counts.contains_key(sink) {
                    continue;
                }
                // The largest count of the repartition source topics known so
                // far and of the external source topics.
                let candidate = subtopology
                    .repartition_source_topics
                    .iter()
                    .filter_map(|topic| counts.get(&topic.name).copied())
                    .chain(
                        subtopology
                            .source_topics
                            .iter()
                            .filter_map(|topic| partition_count(image, topic, &no_decisions)),
                    )
                    .max();
                if let Some(partitions) = candidate {
                    counts.insert(sink.clone(), partitions);
                    progress_made = true;
                } else {
                    partition_count_needed = true;
                }
            }
        }
        if !progress_made && partition_count_needed {
            return Err(ConfigureTopicsError::InvalidTopology(
                "Failed to compute number of partitions for all repartition topics. There may be \
                 loops in the topology that cannot be resolved."
                    .into(),
            )
            .into());
        }
        if !partition_count_needed {
            break;
        }
    }
    let never_written = topology
        .subtopologies
        .iter()
        .flat_map(|subtopology| subtopology.repartition_source_topics.iter())
        .any(|topic| !counts.contains_key(&topic.name));
    if never_written {
        return Err(ConfigureTopicsError::InvalidTopology(
            "Failed to compute number of partitions for all repartition topics, because a \
             repartition source topic is never used as a sink topic."
                .into(),
        )
        .into());
    }
    Ok(counts)
}

/// Kafka's `CopartitionedTopicsEnforcer.enforce`. `fixed` and `flexible` are
/// the repartition source topics of the whole topology with and without an
/// explicit partition count.
fn enforce_copartitioning(
    group: &BTreeSet<String>,
    fixed: &BTreeSet<&str>,
    flexible: &BTreeSet<&str>,
    image: &MetadataImage,
    decided: &BTreeMap<String, i32>,
) -> Result<BTreeMap<String, i32>, Stop> {
    if group.is_empty() {
        return Ok(BTreeMap::new());
    }
    let count_of = |topic: &str| {
        partition_count(image, topic, decided).ok_or_else(|| {
            Stop::Error(ConfigureTopicsError::Internal(format!(
                "Number of partitions is not set for topic: {topic}"
            )))
        })
    };
    let mut repartition_counts = BTreeMap::new();
    let mut external_counts = BTreeMap::new();
    for topic in group {
        let count = count_of(topic)?;
        if fixed.contains(topic.as_str()) || flexible.contains(topic.as_str()) {
            repartition_counts.insert(topic.clone(), count);
        } else {
            external_counts.insert(topic.clone(), count);
        }
    }

    let partitions = if external_counts.is_empty() {
        if fixed.is_empty() {
            let max = repartition_counts
                .values()
                .copied()
                .max()
                .unwrap_or(0)
                .max(0);
            if max == 0 {
                return Err(ConfigureTopicsError::InvalidTopology(format!(
                    "All topics in the copartition group had undefined partition number: [{}]",
                    repartition_counts
                        .keys()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
                .into());
            }
            max
        } else {
            // Kafka compares every fixed repartition topic of the topology,
            // not only the ones in this group.
            let mut first = None;
            for topic in fixed {
                let count = count_of(topic)?;
                match first {
                    None => first = Some(count),
                    Some(expected) if expected != count => {
                        return Err(Stop::Status(
                            status::INCORRECTLY_PARTITIONED_TOPICS,
                            format!(
                                "Following topics do not have the same number of partitions: \
                                 [{}]",
                                java_map_string(&repartition_counts)
                            ),
                        ));
                    }
                    Some(_) => {}
                }
            }
            first.unwrap_or_default()
        }
    } else {
        let mut counts = external_counts.values().copied();
        let first = counts.next().unwrap_or_default();
        if counts.any(|count| count != first) {
            return Err(Stop::Status(
                status::INCORRECTLY_PARTITIONED_TOPICS,
                format!(
                    "Following topics do not have the same number of partitions: [{}]",
                    java_map_string(&external_counts)
                ),
            ));
        }
        first
    };

    let mut coerced = BTreeMap::new();
    for (topic, count) in repartition_counts {
        if fixed.contains(topic.as_str()) && count != partitions {
            return Err(Stop::Status(
                status::INCORRECTLY_PARTITIONED_TOPICS,
                format!(
                    "Number of partitions [{count}] of repartition topic [{topic}] doesn't match \
                     number of partitions [{partitions}] of the source topic."
                ),
            ));
        }
        coerced.insert(topic, partitions);
    }
    Ok(coerced)
}

/// Kafka's `ChangelogTopics.setup`.
fn changelog_topic_partition_counts(
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
    decided: &BTreeMap<String, i32>,
) -> Result<BTreeMap<String, i32>, Stop> {
    let mut counts = BTreeMap::new();
    for subtopology in &topology.subtopologies {
        let mut max = None;
        for topic in input_topics(subtopology) {
            let count = partition_count(image, topic, decided).ok_or_else(|| {
                ConfigureTopicsError::Internal(format!(
                    "No partition count for source topic {topic}"
                ))
            })?;
            max = Some(max.map_or(count, |max: i32| max.max(count)));
        }
        let Some(max) = max else {
            return Err(ConfigureTopicsError::InvalidTopology(format!(
                "No source topics found for subtopology {}",
                subtopology.subtopology_id
            ))
            .into());
        };
        for topic in &subtopology.state_changelog_topics {
            counts.insert(topic.name.clone(), max);
        }
    }
    Ok(counts)
}

/// The source topics and then the repartition source topics of a
/// subtopology.
pub(super) fn input_topics(subtopology: &StoredSubtopology) -> impl Iterator<Item = &str> {
    subtopology.source_topics.iter().map(String::as_str).chain(
        subtopology
            .repartition_source_topics
            .iter()
            .map(|topic| topic.name.as_str()),
    )
}

/// Kafka's `fromPersistedSubtopology`.
fn from_persisted_subtopology(
    subtopology: &StoredSubtopology,
    image: &MetadataImage,
    decided: &BTreeMap<String, i32>,
) -> Result<ConfiguredSubtopology, ConfigureTopicsError> {
    let mut number_of_tasks = None;
    for topic in input_topics(subtopology) {
        let count = partition_count(image, topic, decided).ok_or_else(|| {
            ConfigureTopicsError::Internal(format!(
                "Number of partitions must be set for topic {topic}"
            ))
        })?;
        number_of_tasks = Some(number_of_tasks.map_or(count, |max: i32| max.max(count)));
    }
    let number_of_tasks = number_of_tasks.ok_or_else(|| {
        ConfigureTopicsError::Internal("Subtopology does not contain any source topics".into())
    })?;
    let internal = |topics: &[StoredTopicInfo]| {
        topics
            .iter()
            .map(|topic| {
                from_persisted_topic_info(topic, decided).map(|topic| (topic.name.clone(), topic))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()
    };
    Ok(ConfiguredSubtopology {
        number_of_tasks,
        source_topics: subtopology.source_topics.iter().cloned().collect(),
        repartition_source_topics: internal(&subtopology.repartition_source_topics)?,
        repartition_sink_topics: subtopology
            .repartition_sink_topics
            .iter()
            .cloned()
            .collect(),
        state_changelog_topics: internal(&subtopology.state_changelog_topics)?,
    })
}

/// Kafka's `fromPersistedTopicInfo` and the `ConfiguredInternalTopic` checks.
fn from_persisted_topic_info(
    topic: &StoredTopicInfo,
    decided: &BTreeMap<String, i32>,
) -> Result<ConfiguredInternalTopic, ConfigureTopicsError> {
    let partitions = if topic.partitions == 0 {
        *decided.get(&topic.name).ok_or_else(|| {
            ConfigureTopicsError::Internal(format!(
                "Number of partitions must be set for topic {}",
                topic.name
            ))
        })?
    } else {
        topic.partitions
    };
    // Java evaluates the config map before the `ConfiguredInternalTopic`
    // constructor checks the name and the partition count.
    let mut configs = BTreeMap::new();
    for (key, value) in &topic.topic_configs {
        if configs.insert(key.clone(), value.clone()).is_some() {
            return Err(ConfigureTopicsError::Internal(format!(
                "Duplicate key {key}"
            )));
        }
    }
    krabka_log::topic_name::validate_topic_name(&topic.name)
        .map_err(|invalid| ConfigureTopicsError::InvalidTopic(invalid.to_string()))?;
    if partitions < 1 {
        return Err(ConfigureTopicsError::Internal(
            "Number of partitions must be at least 1.".into(),
        ));
    }
    Ok(ConfiguredInternalTopic {
        name: topic.name.clone(),
        partitions,
        replication_factor: (topic.replication_factor != 0).then_some(topic.replication_factor),
        configs,
    })
}

/// Kafka's `copartitionGroupsFromPersistedSubtopology`: each group as the set
/// of topic names that its indices select.
fn copartition_groups(
    subtopology: &StoredSubtopology,
) -> Result<Vec<BTreeSet<String>>, ConfigureTopicsError> {
    let out_of_range = |index: i16, length: usize| {
        ConfigureTopicsError::Internal(format!("Index {index} out of bounds for length {length}"))
    };
    subtopology
        .copartition_groups
        .iter()
        .map(|group| {
            let sources = group.source_topics.iter().map(|&index| {
                usize::try_from(index)
                    .ok()
                    .and_then(|position| subtopology.source_topics.get(position))
                    .cloned()
                    .ok_or_else(|| out_of_range(index, subtopology.source_topics.len()))
            });
            let repartition_sources = group.repartition_source_topics.iter().map(|&index| {
                usize::try_from(index)
                    .ok()
                    .and_then(|position| subtopology.repartition_source_topics.get(position))
                    .map(|topic| topic.name.clone())
                    .ok_or_else(|| out_of_range(index, subtopology.repartition_source_topics.len()))
            });
            sources.chain(repartition_sources).collect()
        })
        .collect()
}

/// Kafka's `missingInternalTopics`.
fn missing_internal_topics(
    subtopologies: &BTreeMap<String, ConfiguredSubtopology>,
    topology: &StreamsGroupTopologyValue,
    image: &MetadataImage,
) -> Result<BTreeMap<String, ConfiguredInternalTopic>, Stop> {
    let mut to_create: BTreeMap<String, ConfiguredInternalTopic> = subtopologies
        .values()
        .flat_map(|subtopology| {
            subtopology
                .repartition_source_topics
                .values()
                .chain(subtopology.state_changelog_topics.values())
        })
        .map(|topic| (topic.name.clone(), topic.clone()))
        .collect();
    for topic in required_topics(topology) {
        if image.topic(topic).is_none() {
            continue;
        }
        if let Some(expected) = to_create.remove(topic) {
            let found = image.topic_partition_count(topic);
            if found != expected.partitions {
                return Err(Stop::Status(
                    status::INCORRECTLY_PARTITIONED_TOPICS,
                    format!(
                        "Existing topic {topic} has different number of partitions: expected {}, \
                         found {found}",
                        expected.partitions
                    ),
                ));
            }
        }
    }
    Ok(to_create)
}

/// Kafka's `InternalTopicManager.summarizeTopics`: at most three names, then
/// the count of the others.
fn summarize_topics<S: AsRef<str>>(topics: impl IntoIterator<Item = S>) -> String {
    let topics: Vec<S> = topics.into_iter().collect();
    if topics.is_empty() {
        return "<none>".into();
    }
    let shown = topics
        .iter()
        .take(3)
        .map(AsRef::as_ref)
        .collect::<Vec<_>>()
        .join(", ");
    if topics.len() > 3 {
        format!("{shown} and {} additional topics", topics.len() - 3)
    } else {
        shown
    }
}

/// Java's `TreeMap.toString`: `{a=1, b=2}`.
fn java_map_string(map: &BTreeMap<String, i32>) -> String {
    let entries = map
        .iter()
        .map(|(topic, count)| format!("{topic}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{entries}}}")
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::streams::{
            persistence::StoredCopartitionGroup,
            topology::test_support::{image_with, sub},
        },
        assert2::check,
    };

    type Row = (
        &'static str,
        StreamsGroupTopologyValue,
        MetadataImage,
        Result<ConfiguredTopology, ConfigureTopicsError>,
    );

    fn info(name: &str, partitions: i32) -> StoredTopicInfo {
        StoredTopicInfo {
            name: name.into(),
            partitions,
            replication_factor: 0,
            topic_configs: vec![],
        }
    }

    fn internal(name: &str, partitions: i32) -> (String, ConfiguredInternalTopic) {
        (name.to_string(), ConfiguredInternalTopic {
            name: name.into(),
            partitions,
            replication_factor: None,
            configs: BTreeMap::new(),
        })
    }

    fn names(topics: &[&str]) -> BTreeSet<String> {
        topics.iter().map(|topic| (*topic).to_string()).collect()
    }

    fn copartition(sources: Vec<i16>, repartition_sources: Vec<i16>) -> StoredCopartitionGroup {
        StoredCopartitionGroup {
            source_topics: sources,
            source_topic_regex: vec![],
            repartition_source_topics: repartition_sources,
        }
    }

    fn topology(subtopologies: Vec<StoredSubtopology>) -> StreamsGroupTopologyValue {
        StreamsGroupTopologyValue {
            epoch: 1,
            subtopologies,
        }
    }

    fn status_only(code: i8, detail: &str) -> ConfiguredTopology {
        ConfiguredTopology {
            topology_epoch: 1,
            subtopologies: None,
            internal_topics_to_create: BTreeMap::new(),
            status: Some((code, detail.into())),
        }
    }

    /// Subtopology 0 reads `orders` (6 partitions) and writes `rp`.
    /// Subtopology 1 reads `rp` and `customers` (3 partitions) in one
    /// copartition group, and keeps `store-changelog`. Kafka coerces `rp` to 3.
    fn copartition_coercion() -> Row {
        let mut s0 = sub("0");
        s0.source_topics = vec!["orders".into()];
        s0.repartition_sink_topics = vec!["rp".into()];
        let mut s1 = sub("1");
        s1.source_topics = vec!["customers".into()];
        s1.repartition_source_topics = vec![info("rp", 0)];
        s1.state_changelog_topics = vec![info("store-changelog", 0)];
        s1.copartition_groups = vec![copartition(vec![0], vec![0])];
        let expected = ConfiguredTopology {
            topology_epoch: 1,
            subtopologies: Some(BTreeMap::from([
                ("0".to_string(), ConfiguredSubtopology {
                    number_of_tasks: 6,
                    source_topics: names(&["orders"]),
                    repartition_source_topics: BTreeMap::new(),
                    repartition_sink_topics: names(&["rp"]),
                    state_changelog_topics: BTreeMap::new(),
                }),
                ("1".to_string(), ConfiguredSubtopology {
                    number_of_tasks: 3,
                    source_topics: names(&["customers"]),
                    repartition_source_topics: BTreeMap::from([internal("rp", 3)]),
                    repartition_sink_topics: BTreeSet::new(),
                    state_changelog_topics: BTreeMap::from([internal("store-changelog", 3)]),
                }),
            ])),
            internal_topics_to_create: BTreeMap::from([
                internal("rp", 3),
                internal("store-changelog", 3),
            ]),
            status: Some((
                status::MISSING_INTERNAL_TOPICS,
                "Internal topics are missing: rp, store-changelog".into(),
            )),
        };
        (
            "copartition coerces the flexible repartition topic",
            topology(vec![s0, s1]),
            image_with(&[("orders", 1, 6), ("customers", 2, 3)]),
            Ok(expected),
        )
    }

    /// Subtopology 1 reads `rp` from the 4-partition subtopology 0 and `other`
    /// (8 partitions) with no copartition group. Kafka sizes `rp` with 4.
    fn writer_partition_count() -> Row {
        let mut s0 = sub("0");
        s0.source_topics = vec!["in".into()];
        s0.repartition_sink_topics = vec!["rp".into()];
        let mut s1 = sub("1");
        s1.source_topics = vec!["other".into()];
        s1.repartition_source_topics = vec![info("rp", 0)];
        let expected = ConfiguredTopology {
            topology_epoch: 1,
            subtopologies: Some(BTreeMap::from([
                ("0".to_string(), ConfiguredSubtopology {
                    number_of_tasks: 4,
                    source_topics: names(&["in"]),
                    repartition_source_topics: BTreeMap::new(),
                    repartition_sink_topics: names(&["rp"]),
                    state_changelog_topics: BTreeMap::new(),
                }),
                ("1".to_string(), ConfiguredSubtopology {
                    number_of_tasks: 8,
                    source_topics: names(&["other"]),
                    repartition_source_topics: BTreeMap::from([internal("rp", 4)]),
                    repartition_sink_topics: BTreeSet::new(),
                    state_changelog_topics: BTreeMap::new(),
                }),
            ])),
            internal_topics_to_create: BTreeMap::new(),
            status: None,
        };
        (
            "a flexible repartition topic takes its writer's partition count",
            topology(vec![s0, s1]),
            image_with(&[("in", 1, 4), ("other", 2, 8), ("rp", 3, 4)]),
            Ok(expected),
        )
    }

    /// The rows that stop with a status.
    fn status_rows() -> Vec<Row> {
        let mut fixed = [sub("0"), sub("1")];
        fixed[0].source_topics = vec!["in".into()];
        fixed[0].repartition_sink_topics = vec!["rp1".into(), "rp2".into()];
        fixed[1].repartition_source_topics = vec![info("rp1", 2), info("rp2", 3)];
        fixed[1].copartition_groups = vec![copartition(vec![], vec![0, 1])];

        let mut external = sub("0");
        external.source_topics = vec!["a".into(), "b".into()];
        external.copartition_groups = vec![copartition(vec![0, 1], vec![])];

        let mut stateful = sub("0");
        stateful.source_topics = vec!["in".into()];
        stateful.state_changelog_topics = vec![info("store-changelog", 0)];

        let mut missing = sub("0");
        missing.source_topics = ["b", "a", "c", "d", "e"].map(String::from).to_vec();
        missing.state_changelog_topics = vec![info("store-changelog", 0)];

        vec![
            (
                "fixed repartition topics disagree",
                topology(fixed.to_vec()),
                image_with(&[("in", 1, 2)]),
                Ok(status_only(
                    status::INCORRECTLY_PARTITIONED_TOPICS,
                    "Following topics do not have the same number of partitions: [{rp1=2, rp2=3}]",
                )),
            ),
            (
                "external copartitioned topics disagree",
                topology(vec![external]),
                image_with(&[("a", 1, 2), ("b", 2, 3)]),
                Ok(status_only(
                    status::INCORRECTLY_PARTITIONED_TOPICS,
                    "Following topics do not have the same number of partitions: [{a=2, b=3}]",
                )),
            ),
            (
                "an existing changelog topic has another partition count",
                topology(vec![stateful]),
                image_with(&[("in", 1, 2), ("store-changelog", 2, 5)]),
                Ok(status_only(
                    status::INCORRECTLY_PARTITIONED_TOPICS,
                    "Existing topic store-changelog has different number of partitions: \
                     expected 2, found 5",
                )),
            ),
            (
                "missing source topics stop before any internal topic",
                topology(vec![missing]),
                image_with(&[("c", 1, 1)]),
                Ok(status_only(
                    status::MISSING_SOURCE_TOPICS,
                    "Source topics a, b, d and 1 additional topics are missing.",
                )),
            ),
        ]
    }

    /// The rows that Kafka throws out of the heartbeat.
    fn error_rows() -> Vec<Row> {
        let mut never_written = sub("0");
        never_written.source_topics = vec!["in".into()];
        never_written.repartition_source_topics = vec![info("rp", 0)];

        let mut looped = [sub("0"), sub("1")];
        looped[0].repartition_source_topics = vec![info("rp-b", 0)];
        looped[0].repartition_sink_topics = vec!["rp-a".into()];
        looped[1].repartition_source_topics = vec![info("rp-a", 0)];
        looped[1].repartition_sink_topics = vec!["rp-b".into()];

        let mut invalid_name = sub("0");
        invalid_name.source_topics = vec!["in".into()];
        invalid_name.state_changelog_topics = vec![info("bad/name", 0)];

        vec![
            (
                "a repartition source topic is never written",
                topology(vec![never_written]),
                image_with(&[("in", 1, 1)]),
                Err(ConfigureTopicsError::InvalidTopology(
                    "Failed to compute number of partitions for all repartition topics, because \
                     a repartition source topic is never used as a sink topic."
                        .into(),
                )),
            ),
            (
                "repartition topics loop",
                topology(looped.to_vec()),
                image_with(&[]),
                Err(ConfigureTopicsError::InvalidTopology(
                    "Failed to compute number of partitions for all repartition topics. There \
                     may be loops in the topology that cannot be resolved."
                        .into(),
                )),
            ),
            (
                "a subtopology has no source topics",
                topology(vec![sub("0")]),
                image_with(&[]),
                Err(ConfigureTopicsError::InvalidTopology(
                    "No source topics found for subtopology 0".into(),
                )),
            ),
            (
                "an internal topic name is invalid",
                topology(vec![invalid_name]),
                image_with(&[("in", 1, 1)]),
                Err(ConfigureTopicsError::InvalidTopic(
                    krabka_log::topic_name::validate_topic_name("bad/name")
                        .unwrap_err()
                        .to_string(),
                )),
            ),
        ]
    }

    /// Kafka's `InternalTopicManager.configureTopics`. Each row is a topology
    /// and an image, and the whole configured topology or error it gives.
    #[test]
    fn configure_topics_follows_kafka_internal_topic_manager() {
        let rows = [copartition_coercion(), writer_partition_count()]
            .into_iter()
            .chain(status_rows())
            .chain(error_rows());
        for (name, topology, image, expected) in rows {
            check!(configure_topics(&topology, &image) == expected, "{name}");
        }
    }

    #[test]
    fn configure_topics_error_codes_and_messages() {
        let rows = [
            (
                ConfigureTopicsError::InvalidTopology("t".into()),
                codes::STREAMS_INVALID_TOPOLOGY,
                Some("t".to_string()),
            ),
            (
                ConfigureTopicsError::InvalidTopic("n".into()),
                codes::INVALID_TOPIC_EXCEPTION,
                Some("n".to_string()),
            ),
            (
                ConfigureTopicsError::Internal("i".into()),
                codes::UNKNOWN_SERVER_ERROR,
                None,
            ),
        ];
        for (error, code, message) in rows {
            check!((error.error_code(), error.error_message()) == (code, message));
        }
    }
}
