//! The compound values the bound flags take, and the parsers clap calls to
//! build them.
//!
//! A `--to-offset`, `--exclude-offset`, or `--exclude-header` value is a small
//! grammar of its own, and every one of these parsers is a clap `value_parser`
//! that turns one such string into the type the restore later matches records
//! against. They live beside those types because a parser and the value it
//! builds are one contract: the parser is the only thing that constructs the
//! type, and every rejection an operator can see is written here.

use {
    base64::Engine as _,
    krabka_ids::{Offset, ProducerId},
    krabka_metadata::NodeId,
    regex::Regex,
    uuid::Uuid,
};

/// Kafka's limit on a topic name, which a bound may not exceed either.
const MAX_TOPIC_NAME_LEN: usize = 249;

/// A topic partition an operator names in a bound.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, derive_more::Display)]
#[display("{topic}-{partition}")]
pub struct PartitionRef {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
}

/// One `--to-offset` bound: the last offset the restore keeps in a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetBound {
    /// The partition the bound applies to.
    pub partition: PartitionRef,
    /// The highest offset the restore keeps. It is inclusive.
    pub last_offset: Offset,
}

/// One `--exclude-offset` range, normalized to a half-open interval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetRange {
    /// The partition the range applies to.
    pub partition: PartitionRef,
    /// First excluded offset.
    pub start: Offset,
    /// First offset past the range. It is not excluded.
    pub end_exclusive: Offset,
}

/// One `--exclude-header` pattern.
#[derive(Debug, Clone)]
pub struct HeaderPattern {
    /// The header name, matched byte for byte.
    pub name: String,
    /// The pattern the header value must match for the record to be dropped.
    pub pattern: Regex,
}

impl PartialEq for HeaderPattern {
    /// Two patterns are equal when they name the same header and hold the same
    /// source pattern. `Regex` has no equality of its own, and the compiled
    /// program is a function of the source.
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.pattern.as_str() == other.pattern.as_str()
    }
}

impl Eq for HeaderPattern {}

/// Parse a node id as `krabka-format --node-id` parses it: Kafka's
/// `node.id`, an `int` that is not negative. The restore formats the target
/// with the id, so an id that the formatter refuses is refused here first.
pub(super) fn parse_node_id(s: &str) -> Result<NodeId, String> { krabka_format::parse_node_id(s) }

/// Parses a `--cluster-id` value in Kafka's base64 `Uuid` form (what
/// `Metadata` and `DescribeCluster` report, e.g. `AQIDBAUGBwgJCgsMDQ4PEA`) or
/// `java.util.UUID`'s hyphenated form.
pub(super) fn parse_cluster_id(s: &str) -> Result<Uuid, String> {
    if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(s)
        && let Ok(bytes) = <[u8; 16]>::try_from(bytes)
    {
        return Ok(Uuid::from_bytes(bytes));
    }
    Uuid::parse_str(s).map_err(|_| {
        format!("{s:?} is not a cluster id: neither a base64 Uuid nor a hyphenated UUID")
    })
}

/// Parse a producer id. A negative value is the "no producer" sentinel and
/// never identifies a writer, so it cannot be excluded.
pub(super) fn parse_producer_id(s: &str) -> Result<ProducerId, String> {
    let id: i64 = s
        .trim()
        .parse()
        .map_err(|error| format!("producer id: {error}"))?;
    if id < 0 {
        return Err(format!("producer id must not be negative, got {id}"));
    }
    Ok(ProducerId(id))
}

/// Compile one operator-supplied pattern.
pub(super) fn parse_regex(s: &str) -> Result<Regex, String> {
    Regex::new(s).map_err(|error| format!("pattern {s:?}: {error}"))
}

/// Check a Kafka topic name: it is not empty, it is at most 249 characters, it
/// is neither `.` nor `..`, and it holds only `[a-zA-Z0-9._-]`.
pub(super) fn parse_topic_name(s: &str) -> Result<String, String> {
    if s.is_empty() {
        return Err("topic name must not be empty".into());
    }
    if s.len() > MAX_TOPIC_NAME_LEN {
        return Err(format!(
            "topic name must be at most {MAX_TOPIC_NAME_LEN} characters, got {}",
            s.len()
        ));
    }
    if s == "." || s == ".." {
        return Err(format!("topic name must not be {s:?}"));
    }
    if let Some(bad) = s
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')))
    {
        return Err(format!(
            "topic name may hold only [a-zA-Z0-9._-], found {bad:?}"
        ));
    }
    Ok(s.to_owned())
}

/// Parse the `topic:partition` half of a bound.
fn parse_partition_ref(s: &str) -> Result<PartitionRef, String> {
    let (topic, partition) = s
        .split_once(':')
        .ok_or_else(|| format!("expected topic:partition, got {s:?}"))?;
    let topic = parse_topic_name(topic)?;
    let partition: i32 = partition
        .parse()
        .map_err(|error| format!("partition index: {error}"))?;
    if partition < 0 {
        return Err(format!(
            "partition index must not be negative, got {partition}"
        ));
    }
    Ok(PartitionRef { topic, partition })
}

/// Parse one `--to-offset topic:partition=N`.
pub(super) fn parse_offset_bound(s: &str) -> Result<OffsetBound, String> {
    let (partition, last) = s
        .split_once('=')
        .ok_or_else(|| format!("expected topic:partition=N, got {s:?}"))?;
    let partition = parse_partition_ref(partition)?;
    let last_offset: i64 = last
        .parse()
        .map_err(|error| format!("offset bound: {error}"))?;
    if last_offset < 0 {
        return Err(format!(
            "offset bound must not be negative, got {last_offset}"
        ));
    }
    Ok(OffsetBound {
        partition,
        last_offset: Offset(last_offset),
    })
}

/// Parse one `--exclude-offset topic:partition=A..B` or `topic:partition=A..=B`.
pub(super) fn parse_offset_range(s: &str) -> Result<OffsetRange, String> {
    let (partition, range) = s
        .split_once('=')
        .ok_or_else(|| format!("expected topic:partition=A..B, got {s:?}"))?;
    let partition = parse_partition_ref(partition)?;
    let (start, end, end_included) = if let Some((start, end)) = range.split_once("..=") {
        (start, end, true)
    } else {
        let (start, end) = range
            .split_once("..")
            .ok_or_else(|| format!("expected an offset range A..B, got {range:?}"))?;
        (start, end, false)
    };
    let start: i64 = start
        .parse()
        .map_err(|error| format!("range start: {error}"))?;
    let end: i64 = end.parse().map_err(|error| format!("range end: {error}"))?;
    if start < 0 || end < 0 {
        return Err(format!(
            "offset range must not be negative, got {start}..{end}"
        ));
    }
    let end_exclusive = if end_included {
        end.checked_add(1)
            .ok_or_else(|| format!("range end {end} overflows when made exclusive"))?
    } else {
        end
    };
    if start >= end_exclusive {
        return Err(format!(
            "offset range must exclude at least one offset, got {range:?}"
        ));
    }
    Ok(OffsetRange {
        partition,
        start: Offset(start),
        end_exclusive: Offset(end_exclusive),
    })
}

/// Parse one `--exclude-header NAME=REGEX`.
///
/// The split is on the first `=`, so a pattern may hold `=` and a header name
/// may not.
pub(super) fn parse_header_pattern(s: &str) -> Result<HeaderPattern, String> {
    let (name, pattern) = s
        .split_once('=')
        .ok_or_else(|| format!("expected NAME=REGEX, got {s:?}"))?;
    if name.is_empty() {
        return Err("header name must not be empty".into());
    }
    Ok(HeaderPattern {
        name: name.to_owned(),
        pattern: parse_regex(pattern)?,
    })
}

#[cfg(test)]
mod tests {
    use {super::*, crate::args::test_support::partition, assert2::check};

    /// `--cluster-id` accepts Kafka's base64 `Uuid` form -- what `Metadata`
    /// and `DescribeCluster` report (#1082) -- as well as the hyphenated
    /// `java.util.UUID` form.
    #[test]
    fn parse_cluster_id_accepts_kafka_base64_and_hyphenated_uuid_forms() {
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        check!(parse_cluster_id("AQIDBAUGBwgJCgsMDQ4PEA") == Ok(id));
        check!(parse_cluster_id(&id.to_string()) == Ok(id));
        check!(parse_cluster_id("not-a-cluster-id").is_err());
    }

    #[test]
    fn topic_names_follow_kafka_rules() {
        for good in [
            "orders",
            "a",
            "a.b_c-d",
            "0",
            &"x".repeat(MAX_TOPIC_NAME_LEN),
        ] {
            check!(parse_topic_name(good) == Ok(good.to_owned()), "{good:?}");
        }
        for bad in [
            "",
            ".",
            "..",
            "has space",
            "has/slash",
            "has:colon",
            "has=equals",
            "café",
            &"x".repeat(MAX_TOPIC_NAME_LEN + 1),
        ] {
            check!(parse_topic_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn partition_refs_split_on_the_colon() {
        check!(parse_partition_ref("orders:0") == Ok(partition("orders", 0)));
        check!(parse_partition_ref("a.b-c:12") == Ok(partition("a.b-c", 12)));
    }

    #[test]
    fn partition_refs_reject_malformed_input() {
        for bad in [
            "orders",
            "orders:",
            ":0",
            "orders:-1",
            "orders:x",
            "orders:0:1",
            "or ders:0",
            "orders:99999999999",
        ] {
            check!(parse_partition_ref(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn partition_ref_displays_the_kafka_spelling() {
        check!(partition("orders", 3).to_string() == "orders-3");
    }

    #[test]
    fn offset_bounds_parse_to_an_inclusive_last_offset() {
        check!(
            parse_offset_bound("orders:0=1000")
                == Ok(OffsetBound {
                    partition: partition("orders", 0),
                    last_offset: Offset(1000),
                })
        );
        check!(
            parse_offset_bound("orders:7=0")
                == Ok(OffsetBound {
                    partition: partition("orders", 7),
                    last_offset: Offset(0),
                })
        );
    }

    #[test]
    fn offset_bounds_reject_malformed_input() {
        for bad in [
            "orders:0",
            "orders=1000",
            "orders:0=",
            "orders:0=-1",
            "orders:0=x",
            "=1000",
            "orders:0=1000=2000",
        ] {
            check!(parse_offset_bound(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn offset_ranges_normalize_to_a_half_open_interval() {
        check!(
            parse_offset_range("orders:0=100..200")
                == Ok(OffsetRange {
                    partition: partition("orders", 0),
                    start: Offset(100),
                    end_exclusive: Offset(200),
                })
        );
        check!(
            parse_offset_range("orders:0=100..=200")
                == Ok(OffsetRange {
                    partition: partition("orders", 0),
                    start: Offset(100),
                    end_exclusive: Offset(201),
                })
        );
        check!(
            parse_offset_range("orders:0=0..1")
                == Ok(OffsetRange {
                    partition: partition("orders", 0),
                    start: Offset(0),
                    end_exclusive: Offset(1),
                })
        );
    }

    #[test]
    fn offset_ranges_reject_empty_inverted_and_malformed_input() {
        for bad in [
            "orders:0=100..100",
            "orders:0=200..100",
            "orders:0=100..=99",
            "orders:0=100",
            "orders:0=..200",
            "orders:0=100..",
            "orders:0=-1..5",
            "orders:0=1..-5",
            "orders:0",
            "orders:0=a..b",
            &format!("orders:0=0..={}", i64::MAX),
        ] {
            check!(parse_offset_range(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn header_patterns_split_on_the_first_equals() {
        let parsed = parse_header_pattern("trace-id=^abc[0-9]+$").expect("pattern");
        check!(
            parsed
                == HeaderPattern {
                    name: "trace-id".to_owned(),
                    pattern: Regex::new("^abc[0-9]+$").expect("pattern"),
                }
        );
        let with_equals = parse_header_pattern("op=a=b").expect("pattern");
        check!(with_equals.name == "op");
        check!(with_equals.pattern.as_str() == "a=b");
    }

    #[test]
    fn header_patterns_reject_malformed_input() {
        for bad in ["trace-id", "=^abc$", "trace-id=[unclosed"] {
            check!(parse_header_pattern(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn producer_ids_reject_the_no_producer_sentinel() {
        check!(parse_producer_id("42") == Ok(ProducerId(42)));
        check!(parse_producer_id(" 7 ") == Ok(ProducerId(7)));
        for bad in ["-1", "x", "", "1.5"] {
            check!(parse_producer_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn node_ids_take_kafkas_node_id_range() {
        check!(parse_node_id("3") == Ok(NodeId(3)));
        check!(parse_node_id("2147483647") == Ok(NodeId(2_147_483_647)));
        for bad in ["-1", "x", "", "2147483648"] {
            check!(parse_node_id(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn regexes_compile_or_report_why_not() {
        check!(parse_regex("^ok$").is_ok());
        check!(parse_regex("[unclosed").is_err());
    }
}
