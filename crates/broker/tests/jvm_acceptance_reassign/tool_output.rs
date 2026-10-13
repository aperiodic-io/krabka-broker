//! The JSON files `kafka-leader-election` and `kafka-reassign-partitions`
//! read, and the output they print.
//!
//! Both tools take a plan in a file and answer in prose, so a suite about them
//! needs the two halves this module holds: builders for the file shapes, and
//! parsers for the answers. Everything here is pure, and unit-tested below, so
//! the part of these cases that is not a container is verified without one.
//!
//! # Why the answers are parsed rather than matched
//!
//! `kafka-leader-election` reports three outcomes per partition, and only one
//! of them is an error line: `ELECTION_NOT_NEEDED` (84) is folded into a
//! `Valid replica already elected for partitions …` line, while
//! `PREFERRED_LEADER_NOT_AVAILABLE` (80) becomes an
//! `Error completing leader election …` line. A case that grepped for a
//! substring would silently pass on a broker that reported the wrong one of
//! the two, which is exactly the divergence this suite exists to catch.
//! [`parse_election`] therefore reduces the whole output to one outcome per
//! partition, and the oracle and krabka are compared on that map.

use {serde_json::json, std::collections::BTreeMap};

/// A partition, in the `topic-index` spelling the tools print.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct TopicPartition {
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

impl TopicPartition {
    pub(crate) fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_owned(),
            partition,
        }
    }

    /// Read one back out of a rendered `TopicPartition`.
    ///
    /// Split from the right: a topic name may contain `-`, and
    /// `orders-eu-3` is partition 3 of `orders-eu`, not partition `eu-3` of
    /// `orders`.
    fn parse(text: &str) -> Option<Self> {
        let (topic, partition) = text.trim().rsplit_once('-')?;
        Some(Self {
            topic: topic.to_owned(),
            partition: partition.parse().ok()?,
        })
    }
}

/// What `kafka-leader-election` said about one partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ElectionOutcome {
    /// The tool moved the leadership.
    Elected,
    /// `ELECTION_NOT_NEEDED` (84): the partition was already led by the
    /// replica the election would have chosen.
    AlreadyElected,
    /// Anything the tool reported as an error, named by the Kafka exception
    /// class it printed -- `PREFERRED_LEADER_NOT_AVAILABLE` (80) becomes
    /// `PreferredLeaderNotAvailableException`.
    Failed(String),
}

/// Reduce a whole `kafka-leader-election` run to one outcome per partition.
pub(crate) fn parse_election(text: &str) -> BTreeMap<TopicPartition, ElectionOutcome> {
    let mut outcomes = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(list) = after(line, "for partitions ") {
            let outcome = if line.starts_with("Valid replica already elected") {
                ElectionOutcome::AlreadyElected
            } else {
                ElectionOutcome::Elected
            };
            for partition in list.split(',').filter_map(TopicPartition::parse) {
                outcomes.insert(partition, outcome.clone());
            }
        } else if let Some(rest) = after(line, "for partition: ") {
            // `Error completing leader election (PREFERRED) for partition:
            // <tp>: <exception>: <message>`. The partition ends at the colon
            // that introduces the exception.
            let Some((partition, tail)) = rest.split_once(':') else {
                continue;
            };
            let Some(partition) = TopicPartition::parse(partition) else {
                continue;
            };
            outcomes.insert(
                partition,
                ElectionOutcome::Failed(
                    exception_class(tail).unwrap_or_else(|| tail.trim().into()),
                ),
            );
        }
    }
    outcomes
}

/// The partitions a `--cancel` said it cancelled.
///
/// `ReassignPartitionsCommand` agrees the noun with the count, so cancelling
/// one partition prints "reassignment for:" and cancelling several prints
/// "reassignments for:". cp-kafka:7.5.0 emits the singular for a one-partition
/// plan, which is the shape this suite drives, so the marker stops before the
/// plural `s` and the ` for: ` that follows either spelling is what the list
/// is taken after.
pub(crate) fn parse_cancelled(text: &str) -> Vec<TopicPartition> {
    let mut cancelled: Vec<TopicPartition> = text
        .lines()
        .filter_map(|line| {
            let tail = after(line.trim(), "Successfully cancelled partition reassignment")?;
            after(tail, " for: ")
        })
        .flat_map(|list| {
            list.split(',')
                .filter_map(TopicPartition::parse)
                .collect::<Vec<_>>()
        })
        .collect();
    cancelled.sort();
    cancelled
}

/// One partition's replica list, as `--generate` proposes it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Assignment {
    pub(crate) partition: TopicPartition,
    pub(crate) replicas: Vec<i32>,
}

/// The current and the proposed assignment `--generate` printed, in that
/// order.
///
/// The tool writes two labelled JSON documents separated by a blank line. The
/// labels are what tells them apart, so they are what is looked for; the
/// documents themselves are parsed as JSON rather than compared as text,
/// because neither tool promises key order and `--generate` is free to emit
/// the partitions in any order at all.
pub(crate) fn parse_generate(stdout: &str) -> Option<(Vec<Assignment>, Vec<Assignment>)> {
    let current = json_after(stdout, "Current partition replica assignment")?;
    let proposed = json_after(stdout, "Proposed partition reassignment configuration")?;
    Some((parse_assignments(&current)?, parse_assignments(&proposed)?))
}

/// The first non-empty line after `label`, which is where each tool puts the
/// document that belongs to it.
fn json_after(stdout: &str, label: &str) -> Option<String> {
    let mut lines = stdout.lines().skip_while(|line| !line.contains(label));
    lines.next()?;
    lines
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
}

/// Read a reassignment document into a sorted assignment list.
pub(crate) fn parse_assignments(document: &str) -> Option<Vec<Assignment>> {
    let value: serde_json::Value = serde_json::from_str(document).ok()?;
    let mut assignments: Vec<Assignment> = value
        .get("partitions")?
        .as_array()?
        .iter()
        .filter_map(|entry| {
            Some(Assignment {
                partition: TopicPartition {
                    topic: entry.get("topic")?.as_str()?.to_owned(),
                    partition: i32::try_from(entry.get("partition")?.as_i64()?).ok()?,
                },
                replicas: entry
                    .get("replicas")?
                    .as_array()?
                    .iter()
                    .filter_map(|id| i32::try_from(id.as_i64()?).ok())
                    .collect(),
            })
        })
        .collect();
    assignments.sort();
    Some(assignments)
}

/// The `--path-to-json-file` document `kafka-leader-election` reads.
pub(crate) fn election_json(partitions: &[TopicPartition]) -> String {
    json!({
        "partitions": partitions
            .iter()
            .map(|tp| json!({"topic": tp.topic, "partition": tp.partition}))
            .collect::<Vec<_>>(),
    })
    .to_string()
}

/// The `--topics-to-move-json-file` document `--generate` reads.
pub(crate) fn topics_to_move_json(topics: &[&str]) -> String {
    json!({
        "topics": topics.iter().map(|topic| json!({"topic": topic})).collect::<Vec<_>>(),
        "version": 1,
    })
    .to_string()
}

/// The `--reassignment-json-file` document `--execute`, `--verify` and
/// `--cancel` read.
pub(crate) fn reassignment_json(assignments: &[Assignment]) -> String {
    json!({
        "version": 1,
        "partitions": assignments
            .iter()
            .map(|a| json!({
                "topic": a.partition.topic,
                "partition": a.partition.partition,
                "replicas": a.replicas,
            }))
            .collect::<Vec<_>>(),
    })
    .to_string()
}

/// Everything after `marker` on a line that has it.
fn after<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    line.find(marker).map(|at| &line[at + marker.len()..])
}

/// The fully-qualified Kafka exception class named in `text`.
fn exception_class(text: &str) -> Option<String> {
    const PREFIX: &str = "org.apache.kafka.common.errors.";
    let at = text.find(PREFIX)?;
    let rest = &text[at..];
    let end = rest[PREFIX.len()..]
        .find(|c: char| !c.is_ascii_alphanumeric())
        .map_or(rest.len(), |offset| PREFIX.len() + offset);
    Some(rest[..end].to_owned())
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn each_of_the_three_election_outcomes_is_read_off_its_own_line() {
        let text = concat!(
            "Successfully completed leader election (PREFERRED) for partitions orders-0\n",
            "Valid replica already elected for partitions orders-1, orders-eu-2\n",
            "Error completing leader election (PREFERRED) for partition: orders-3: ",
            "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException: ",
            "The preferred leader was not available.\n",
        );
        check!(
            parse_election(text)
                == BTreeMap::from([
                    (TopicPartition::new("orders", 0), ElectionOutcome::Elected),
                    (
                        TopicPartition::new("orders", 1),
                        ElectionOutcome::AlreadyElected
                    ),
                    (
                        TopicPartition::new("orders-eu", 2),
                        ElectionOutcome::AlreadyElected
                    ),
                    (
                        TopicPartition::new("orders", 3),
                        ElectionOutcome::Failed(
                            "org.apache.kafka.common.errors.PreferredLeaderNotAvailableException"
                                .to_owned()
                        )
                    ),
                ])
        );
    }

    #[test]
    fn a_hyphenated_topic_keeps_its_hyphens() {
        check!(
            TopicPartition::parse("orders-eu-west-12")
                == Some(TopicPartition::new("orders-eu-west", 12))
        );
        check!(TopicPartition::parse("orders") == None);
        check!(TopicPartition::parse("orders-x") == None);
    }

    #[test]
    fn a_cancel_reports_the_partitions_it_cancelled() {
        check!(
            parse_cancelled(
                "Successfully cancelled partition reassignments for: orders-0,orders-1\n"
            ) == vec![
                TopicPartition::new("orders", 0),
                TopicPartition::new("orders", 1),
            ]
        );
        check!(
            parse_cancelled("None of the specified partition reassignments exist.\n").is_empty()
        );
    }

    /// The tool agrees the noun with the count, and a one-partition cancel --
    /// which is what the container case drives -- prints the singular.
    #[test]
    fn a_one_partition_cancel_is_read_out_of_the_singular_sentence() {
        check!(
            parse_cancelled(
                "Successfully cancelled partition reassignment for: orders-0\n\
                 None of the specified partition moves are active.\n"
            ) == vec![TopicPartition::new("orders", 0)]
        );
    }

    #[test]
    fn generate_yields_the_current_assignment_and_the_proposed_one() {
        let stdout = concat!(
            "Current partition replica assignment\n",
            r#"{"version":1,"partitions":[{"topic":"orders","partition":0,"replicas":[1,2],"#,
            r#""log_dirs":["any","any"]}]}"#,
            "\n\n",
            "Proposed partition reassignment configuration\n",
            r#"{"version":1,"partitions":[{"topic":"orders","partition":0,"replicas":[2,3],"#,
            r#""log_dirs":["any","any"]}]}"#,
            "\n",
        );
        let current = vec![Assignment {
            partition: TopicPartition::new("orders", 0),
            replicas: vec![1, 2],
        }];
        let proposed = vec![Assignment {
            partition: TopicPartition::new("orders", 0),
            replicas: vec![2, 3],
        }];
        check!(parse_generate(stdout) == Some((current, proposed)));
        check!(parse_generate("nothing was generated") == None);
    }

    #[test]
    fn the_partitions_of_a_generated_plan_sort_the_same_however_they_were_printed() {
        let one = r#"{"version":1,"partitions":[{"topic":"t","partition":1,"replicas":[2]},
             {"topic":"t","partition":0,"replicas":[1]}]}"#;
        let other = r#"{"version":1,"partitions":[{"topic":"t","partition":0,"replicas":[1]},
             {"topic":"t","partition":1,"replicas":[2]}]}"#;
        check!(parse_assignments(one) == parse_assignments(other));
        check!(parse_assignments(one).is_some());
    }

    #[test]
    fn every_document_the_tools_read_round_trips_through_its_own_parser() {
        let assignments = vec![
            Assignment {
                partition: TopicPartition::new("orders", 0),
                replicas: vec![1, 2],
            },
            Assignment {
                partition: TopicPartition::new("orders", 1),
                replicas: vec![2, 3],
            },
        ];
        check!(parse_assignments(&reassignment_json(&assignments)) == Some(assignments.clone()));

        let elected = election_json(&[TopicPartition::new("orders", 0)]);
        check!(elected == r#"{"partitions":[{"partition":0,"topic":"orders"}]}"#);
        check!(
            topics_to_move_json(&["orders"]) == r#"{"topics":[{"topic":"orders"}],"version":1}"#
        );
    }
}
