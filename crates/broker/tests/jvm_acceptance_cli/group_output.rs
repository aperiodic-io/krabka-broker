//! Reading what `kafka-consumer-groups` printed.
//!
//! Every function here is pure: text in, a comparable value out. That is what
//! lets the oracle differential compare *answers* rather than bytes. The two
//! sides run the same binary from the same image, so their column widths and
//! their header lines are identical and worth nothing; what has to agree is
//! the offset the tool computed, the order it was reached in, and the
//! exception a refusal named.
//!
//! Parsing is deliberately loose about layout and strict about content. The
//! tool pads its table with `%-30s`-style formats that no case should depend
//! on, so a row is whitespace-split; but a row is only a row when its
//! partition and its offset are numbers, which is what stops a header, a
//! warning line or a stack frame from being read as data.

use std::collections::BTreeSet;

/// One row of the `GROUP TOPIC PARTITION NEW-OFFSET` table that
/// `--reset-offsets` prints under both `--dry-run` and `--execute`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ResetRow {
    pub(crate) group: String,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) new_offset: i64,
}

/// Parse the reset table out of a `--reset-offsets` stdout, sorted.
///
/// Sorted because the tool walks a map: the same broker answers the same
/// partitions in a different order between two runs, and a case that compared
/// the raw order would be flaky on one side and correct on neither.
pub(crate) fn parse_reset_table(stdout: &str) -> Vec<ResetRow> {
    let mut rows: Vec<ResetRow> = stdout
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [group, topic, partition, new_offset] = fields[..] else {
                return None;
            };
            Some(ResetRow {
                group: group.to_owned(),
                topic: topic.to_owned(),
                partition: partition.parse().ok()?,
                new_offset: new_offset.parse().ok()?,
            })
        })
        .collect();
    rows.sort();
    rows
}

/// One line of the CSV that `--reset-offsets --export` writes and
/// `--from-file` reads back.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ExportRow {
    /// `None` for the three-column form. The tool drops the group column when
    /// exactly one `--group` was named, and keeps it under `--all-groups`, so
    /// both shapes are legitimate output and the parser accepts either.
    pub(crate) group: Option<String>,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
}

/// Parse an exported offsets CSV, sorted.
///
/// The tool writes this file with a Jackson `CsvMapper`, and
/// apache/kafka:4.3.1 quotes the string columns while leaving the numeric ones
/// bare -- `"orders",0,3`. The quoting is not part of the claim these cases
/// make, so [`unquote`] takes it back off before the fields are compared.
///
/// Splitting on `,` before unquoting would mis-split a field that contained a
/// comma. A Kafka topic name cannot (`[a-zA-Z0-9._-]` only), and the group ids
/// this suite uses cannot either, so the simple split is enough here.
pub(crate) fn parse_export_csv(stdout: &str) -> Vec<ExportRow> {
    let mut rows: Vec<ExportRow> = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(',').map(str::trim).collect();
            let (group, topic, partition, offset) = match fields[..] {
                [topic, partition, offset] => (None, topic, partition, offset),
                [group, topic, partition, offset] => (Some(group), topic, partition, offset),
                _ => return None,
            };
            Some(ExportRow {
                group: group.map(unquote),
                topic: unquote(topic),
                partition: unquote(partition).parse().ok()?,
                offset: unquote(offset).parse().ok()?,
            })
        })
        .collect();
    rows.sort();
    rows
}

/// One CSV field with its surrounding quotes removed, and any doubled quote
/// inside it read back as the single quote it stands for. A field that is not
/// quoted is returned as it came.
fn unquote(field: &str) -> String {
    let Some(inner) = field
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    else {
        return field.to_owned();
    };
    inner.replace("\"\"", "\"")
}

/// The fully-qualified Kafka exception class names a tool printed, sorted and
/// deduplicated.
///
/// This is the part of a refusal worth comparing across the two sides. The
/// sentence after the class name is the broker's own and the two brokers word
/// theirs differently; the class is Kafka's, built by `Errors.forCode` from
/// the numeric code the broker sent, so it is exactly the claim a differential
/// about error codes wants to make.
pub(crate) fn kafka_exceptions(text: &str) -> BTreeSet<String> {
    const PREFIX: &str = "org.apache.kafka.common.errors.";
    let mut found = BTreeSet::new();
    let mut rest = text;
    while let Some(at) = rest.find(PREFIX) {
        rest = &rest[at + PREFIX.len()..];
        let end = rest
            .find(|c: char| !c.is_ascii_alphanumeric())
            .unwrap_or(rest.len());
        if end > 0 {
            found.insert(format!("{PREFIX}{}", &rest[..end]));
        }
        rest = &rest[end..];
    }
    found
}

/// What `--delete` said about one group.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct DeleteOutcome {
    pub(crate) group: String,
    /// The exception class the tool named, or `None` when the delete worked.
    pub(crate) failure: Option<String>,
}

/// Read the per-group outcome of a `kafka-consumer-groups --delete`.
///
/// The tool prints one of two shapes: a single success sentence naming every
/// group it deleted, or `Error: Deletion of some consumer groups failed:`
/// followed by one `* Group 'g' could not be deleted due to: <exception>` line
/// per group. Both are reduced to the same list here, so a case states the
/// rule once for the group that existed and the group that did not.
pub(crate) fn parse_delete_groups(text: &str) -> Vec<DeleteOutcome> {
    let mut outcomes: Vec<DeleteOutcome> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(group) = quoted_group(line, "Group ") {
            outcomes.push(DeleteOutcome {
                group,
                failure: kafka_exceptions(line).into_iter().next(),
            });
        } else if line.starts_with("Deletion of requested consumer groups") {
            outcomes.extend(quoted_groups(line).into_iter().map(|group| DeleteOutcome {
                group,
                failure: None,
            }));
        }
    }
    outcomes.sort();
    outcomes
}

/// The group named by the first `'...'` after `marker`, when the line has one.
fn quoted_group(line: &str, marker: &str) -> Option<String> {
    let after = line.find(marker)? + marker.len();
    let rest = &line[after..];
    let open = rest.find('\'')? + 1;
    let close = rest[open..].find('\'')? + open;
    Some(rest[open..close].to_owned())
}

/// Every `'...'`-quoted name on a line.
fn quoted_groups(line: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = line;
    while let Some(open) = rest.find('\'') {
        rest = &rest[open + 1..];
        let Some(close) = rest.find('\'') else { break };
        names.push(rest[..close].to_owned());
        rest = &rest[close + 1..];
    }
    names
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// The tool's own table, padded the way `%-30s` pads it, with the header
    /// and a blank line the parser has to drop.
    const RESET_TABLE: &str = "\n\
        GROUP                          TOPIC                          PARTITION  NEW-OFFSET     \n\
        krabka-grp                     krabka-topic                   0          7              \n\
        krabka-grp                     krabka-topic                   1          0              \n";

    #[test]
    fn the_reset_table_keeps_only_numeric_rows_and_sorts_them() {
        check!(
            parse_reset_table(RESET_TABLE)
                == vec![
                    ResetRow {
                        group: "krabka-grp".to_owned(),
                        topic: "krabka-topic".to_owned(),
                        partition: 0,
                        new_offset: 7,
                    },
                    ResetRow {
                        group: "krabka-grp".to_owned(),
                        topic: "krabka-topic".to_owned(),
                        partition: 1,
                        new_offset: 0,
                    },
                ]
        );
    }

    #[test]
    fn a_table_printed_in_another_order_parses_to_the_same_rows() {
        let reversed: String = {
            let mut lines: Vec<&str> = RESET_TABLE.lines().collect();
            lines.reverse();
            lines.join("\n")
        };
        check!(parse_reset_table(&reversed) == parse_reset_table(RESET_TABLE));
    }

    #[test]
    fn lines_that_are_not_rows_are_not_rows() {
        for text in [
            "GROUP TOPIC PARTITION NEW-OFFSET",
            "Error: Assignments can only be reset if the group is inactive",
            "\tat org.apache.kafka.tools.ConsumerGroupCommand.main(x.java:1)",
            "",
        ] {
            check!(
                parse_reset_table(text).is_empty(),
                "parsed a row from {text:?}"
            );
        }
    }

    #[test]
    fn the_export_csv_parses_in_both_of_the_shapes_the_tool_writes() {
        let three = ExportRow {
            group: None,
            topic: "t".to_owned(),
            partition: 0,
            offset: 12,
        };
        let four = ExportRow {
            group: Some("g".to_owned()),
            ..three.clone()
        };
        check!(parse_export_csv("t,0,12\n") == vec![three]);
        check!(parse_export_csv("g,t,0,12\n") == vec![four]);
        check!(parse_export_csv("\n\n") == Vec::new());
    }

    /// apache/kafka:4.3.1 writes this file through a Jackson `CsvMapper` that
    /// quotes the string columns and leaves the numeric ones bare, so the same
    /// row arrives quoted from one side and plain from the other and the two
    /// must still compare equal.
    #[test]
    fn a_quoted_export_row_reads_the_same_as_a_plain_one() {
        check!(parse_export_csv("\"t\",0,12\n") == parse_export_csv("t,0,12\n"));
        check!(parse_export_csv("\"g\",\"t\",0,12\n") == parse_export_csv("g,t,0,12\n"));
    }

    /// A quote inside a quoted field is written doubled, and reads back as one.
    #[test]
    fn a_doubled_quote_inside_a_field_reads_back_as_one() {
        check!(
            parse_export_csv("\"a\"\"b\",0,12\n")
                == vec![ExportRow {
                    group: None,
                    topic: "a\"b".to_owned(),
                    partition: 0,
                    offset: 12,
                }]
        );
    }

    #[test]
    fn every_kafka_exception_in_a_tool_dump_is_found_once() {
        let text = "Error: org.apache.kafka.common.errors.GroupNotEmptyException: no\n\
                    caused by org.apache.kafka.common.errors.GroupNotEmptyException\n\
                    and org.apache.kafka.common.errors.GroupIdNotFoundException: gone";
        check!(
            kafka_exceptions(text)
                == BTreeSet::from([
                    "org.apache.kafka.common.errors.GroupIdNotFoundException".to_owned(),
                    "org.apache.kafka.common.errors.GroupNotEmptyException".to_owned(),
                ])
        );
        check!(kafka_exceptions("nothing here").is_empty());
    }

    #[test]
    fn delete_reports_reduce_to_one_outcome_per_group() {
        let failed = "Error: Deletion of some consumer groups failed:\n\
             * Group 'ghost' could not be deleted due to: \
             org.apache.kafka.common.errors.GroupIdNotFoundException: The group id does not exist.\n\
             * Group 'busy' could not be deleted due to: \
             org.apache.kafka.common.errors.GroupNotEmptyException: The group is not empty.\n";
        check!(
            parse_delete_groups(failed)
                == vec![
                    DeleteOutcome {
                        group: "busy".to_owned(),
                        failure: Some(
                            "org.apache.kafka.common.errors.GroupNotEmptyException".to_owned()
                        ),
                    },
                    DeleteOutcome {
                        group: "ghost".to_owned(),
                        failure: Some(
                            "org.apache.kafka.common.errors.GroupIdNotFoundException".to_owned()
                        ),
                    },
                ]
        );
        check!(
            parse_delete_groups("Deletion of requested consumer groups ('gone') was successful.\n")
                == vec![DeleteOutcome {
                    group: "gone".to_owned(),
                    failure: None,
                }]
        );
    }
}
