//! Kafka's group metadata hash: one number over the metadata of the topics
//! that a consumer, share or streams group reads.
//!
//! Kafka 4.3.1 writes the number as the `MetadataHash` of
//! `ConsumerGroupMetadataValue` (tagged field 0), `ShareGroupMetadataValue`
//! and `StreamsGroupMetadataValue` (both plain fields). A heartbeat computes
//! it again when the group's metadata expired, and a new value bumps the group
//! epoch, so the next target assignment sees the change.
//!
//! The byte layout is the one of `Utils.computeTopicHash` and
//! `Utils.computeGroupHash`, which stream into hash4j's XXH3-64 with seed 0
//! (`Hashing.xxh3_64().hashStream()`). The streamed bytes of one topic are:
//!
//! 1. the magic byte `TOPIC_HASH_MAGIC_BYTE`, `0x00`;
//! 2. the most and then the least significant 64 bits of the topic id;
//! 3. the topic name;
//! 4. the partition count, an `int`;
//! 5. for each partition from 0, its index, an `int`, then for each replica
//!    rack in sorted order the rack's length, an `int`, and the rack.
//!
//! hash4j writes every `long` and `int` as little-endian bytes, and a `String`
//! as its UTF-16 code units, each little-endian, followed by its length as an
//! `int`. So a rack is its length twice: once from `computeTopicHash` and once
//! from `putString`. The group hash streams the topic hashes as `long`s, in
//! topic name order, and is 0 for no topic.
//!
//! Java orders `String`s by UTF-16 code unit, which sorts a character above
//! the Basic Multilingual Plane before `U+E000`..`U+FFFF`, where UTF-8 byte
//! order puts it after. Both sorts here follow Java.

use {krabka_metadata::MetadataImage, std::cmp::Ordering, twox_hash::XxHash3_64};

/// `Utils.TOPIC_HASH_MAGIC_BYTE`: the version of the topic hash layout.
const TOPIC_HASH_MAGIC_BYTE: u8 = 0x00;

/// Java's `String.compareTo`: the order of the UTF-16 code units.
fn java_string_order(left: &str, right: &str) -> Ordering {
    left.encode_utf16().cmp(right.encode_utf16())
}

/// Java's `String.length()`: the number of UTF-16 code units.
fn utf16_len(value: &str) -> i32 { i32::try_from(value.encode_utf16().count()).unwrap_or(i32::MAX) }

/// hash4j's `putString`: the UTF-16 code units, then their count as an `int`.
fn put_string(stream: &mut Vec<u8>, value: &str) {
    for unit in value.encode_utf16() {
        stream.extend_from_slice(&unit.to_le_bytes());
    }
    stream.extend_from_slice(&utf16_len(value).to_le_bytes());
}

fn xxh3(stream: &[u8]) -> i64 { i64::from_le_bytes(XxHash3_64::oneshot(stream).to_le_bytes()) }

/// Kafka's `Utils.computeTopicHash` for a topic that exists.
///
/// `partition_racks` holds one entry per partition, from partition 0, so its
/// length is the partition count. Each entry names the rack of every replica
/// of the partition whose broker has one, in any order and with repeats, as
/// `CoordinatorMetadataImage.TopicMetadata.partitionRacks` returns them.
#[must_use]
pub fn topic_hash<'a>(
    topic_id: [u8; 16],
    name: &str,
    partition_racks: impl ExactSizeIterator<Item = Vec<&'a str>>,
) -> i64 {
    let (most, least) = topic_id.split_at(8);
    let mut stream = vec![TOPIC_HASH_MAGIC_BYTE];
    // A Java `long` from `Uuid.getMostSignificantBits` is the big-endian
    // reading of the id's first eight bytes; hash4j writes it little-endian.
    for half in [most, least] {
        let mut bits = [0; 8];
        bits.copy_from_slice(half);
        stream.extend_from_slice(&u64::from_be_bytes(bits).to_le_bytes());
    }
    put_string(&mut stream, name);
    let partition_count = i32::try_from(partition_racks.len()).unwrap_or(i32::MAX);
    stream.extend_from_slice(&partition_count.to_le_bytes());
    for (partition, mut racks) in (0..partition_count).zip(partition_racks) {
        stream.extend_from_slice(&partition.to_le_bytes());
        racks.sort_by(|left, right| java_string_order(left, right));
        for rack in racks {
            stream.extend_from_slice(&utf16_len(rack).to_le_bytes());
            put_string(&mut stream, rack);
        }
    }
    xxh3(&stream)
}

/// Kafka's `Utils.computeGroupHash`: the topic hashes in topic name order, or
/// 0 for no topic.
#[must_use]
pub fn group_hash<'a>(topic_hashes: impl IntoIterator<Item = (&'a str, i64)>) -> i64 {
    let mut sorted: Vec<(&str, i64)> = topic_hashes.into_iter().collect();
    if sorted.is_empty() {
        return 0;
    }
    sorted.sort_by(|(left, _), (right, _)| java_string_order(left, right));
    let mut stream = Vec::with_capacity(sorted.len() * 8);
    for (_, hash) in sorted {
        stream.extend_from_slice(&hash.to_le_bytes());
    }
    xxh3(&stream)
}

/// Kafka's `Utils.computeTopicHash` against a metadata image, or `None` for a
/// topic that the image does not hold.
///
/// The partition count is the number of partitions that the image holds, as
/// `KRaftCoordinatorMetadataImage` counts `topicImage.partitions()`, and the
/// racks of a partition are the racks of its replicas' registered brokers.
#[must_use]
pub fn image_topic_hash(topic: &str, image: &MetadataImage) -> Option<i64> {
    let record = image.topic(topic)?;
    let partition_count = image.topic_partition_count(topic);
    let racks = (0..partition_count).map(|partition| {
        image
            .partition(topic, partition)
            .into_iter()
            .flat_map(|record| record.replicas.iter())
            .filter_map(|replica| image.broker(*replica))
            .filter_map(|broker| broker.rack.as_deref())
            .collect::<Vec<&str>>()
    });
    let racks: Vec<Vec<&str>> = racks.collect();
    Some(topic_hash(
        *record.topic_id.as_bytes(),
        &record.name,
        racks.into_iter(),
    ))
}

/// Kafka's `ModernGroup.computeMetadataHash` and
/// `StreamsGroup.computeMetadataHash`: the group hash over the topic hash of
/// every topic in `topics` that `image` holds. A name that repeats counts
/// once.
#[must_use]
pub fn image_metadata_hash<'a>(
    topics: impl IntoIterator<Item = &'a str>,
    image: &MetadataImage,
) -> i64 {
    let topics: std::collections::BTreeSet<&str> = topics.into_iter().collect();
    group_hash(
        topics
            .into_iter()
            .filter_map(|topic| image_topic_hash(topic, image).map(|hash| (topic, hash))),
    )
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// The topic id `0123456789abcdef-fedcba9876543210`.
    const ID: [u8; 16] = [
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32,
        0x10,
    ];

    fn racks<'a>(partitions: &[&[&'a str]]) -> Vec<Vec<&'a str>> {
        partitions.iter().map(|racks| racks.to_vec()).collect()
    }

    /// One topic of the golden table.
    struct TopicCase {
        case: &'static str,
        topic_id: [u8; 16],
        name: &'static str,
        racks: Vec<Vec<&'static str>>,
        kafka: i64,
    }

    /// One group of the golden table: its topic hashes, by name.
    struct GroupCase {
        case: &'static str,
        topics: Vec<(&'static str, i64)>,
        kafka: i64,
    }

    /// Golden values from hash4j 0.22.0, the version Kafka 4.3.1 pins in
    /// `gradle/dependencies.gradle`. Each was computed by running a line by
    /// line transcription of `Utils.computeTopicHash` and
    /// `Utils.computeGroupHash` against the published jar (SHA-1
    /// `2d61bfb5b38469cb76141b06dc68d36a9007e9ad`), with the metadata image
    /// lookups replaced by the arguments below.
    #[test]
    fn topic_hashes_match_kafka() {
        let mut ids = [0x11; 16];
        ids[8..].fill(0x22);
        let rows = [
            TopicCase {
                case: "UtilsTest.testComputeTopicHash's layout: two partitions, two racks each",
                topic_id: ID,
                name: "foo",
                racks: racks(&[&["rack1", "rack0"], &["rack2", "rack1"]]),
                kafka: 4_231_092_367_357_902_416,
            },
            TopicCase {
                case: "one partition without racks",
                topic_id: ids,
                name: "bar",
                racks: racks(&[&[]]),
                kafka: 8_733_119_781_167_439_863,
            },
            TopicCase {
                case: "no partitions",
                topic_id: [0; 16],
                name: "empty",
                racks: racks(&[]),
                kafka: 728_769_691_835_767_132,
            },
            TopicCase {
                case: "two replicas on one rack",
                topic_id: ID,
                name: "foo",
                racks: racks(&[&["r", "r"]]),
                kafka: 2_883_780_561_648_472_600,
            },
            TopicCase {
                case: "racks in UTF-16 order, a supplementary character first",
                topic_id: ID,
                name: "foo",
                racks: racks(&[&["\u{e000}", "\u{1f600}"]]),
                kafka: 4_767_223_826_197_171_634,
            },
            TopicCase {
                case: "a name and a rack outside ASCII",
                topic_id: ID,
                name: "t\u{e9}",
                racks: racks(&[&["\u{e9}"]]),
                kafka: 480_043_309_326_700_168,
            },
        ];
        for row in rows {
            check!(
                topic_hash(row.topic_id, row.name, row.racks.into_iter()) == row.kafka,
                "{}",
                row.case
            );
        }
    }

    #[test]
    fn group_hashes_match_kafka() {
        let foo = 4_231_092_367_357_902_416;
        let bar = 8_733_119_781_167_439_863;
        let rows = [
            GroupCase {
                case: "no topic",
                topics: vec![],
                kafka: 0,
            },
            GroupCase {
                case: "one topic",
                topics: vec![("foo", foo)],
                kafka: -3_651_539_661_068_893_492,
            },
            GroupCase {
                case: "two topics, sorted by name",
                topics: vec![("foo", foo), ("bar", bar)],
                kafka: -556_879_919_459_959_918,
            },
            GroupCase {
                case: "the same topics in the other order",
                topics: vec![("bar", bar), ("foo", foo)],
                kafka: -556_879_919_459_959_918,
            },
            GroupCase {
                case: "UtilsTest's literal hashes",
                topics: vec![("foo", 456), ("bar", 123)],
                kafka: 1_482_712_172_080_382_793,
            },
        ];
        for row in rows {
            check!(group_hash(row.topics) == row.kafka, "{}", row.case);
        }
    }
}
