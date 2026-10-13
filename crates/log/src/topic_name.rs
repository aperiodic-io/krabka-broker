//! The topic-name rules of Apache Kafka's `org.apache.kafka.common.internals.Topic`.
//!
//! A topic name becomes part of a filesystem path: the partition directory is `<log_dir>/<topic>-<partition>`. A name that holds a `/`, or that is `.` or `..`, gives a path outside the log directory. [`validate_topic_name`] is the one check. Every path that creates a topic, and every path that creates or removes a partition directory from a name, calls it. A name that passes gives a partition directory that is a direct child of the log directory.
//!
//! The error text is Kafka's text, because `CreateTopics` sends it to the client as the error message.

use thiserror::Error;

/// Longest topic name Kafka accepts, in UTF-16 code units (`Topic.MAX_NAME_LENGTH`).
pub const MAX_TOPIC_NAME_LENGTH: usize = 249;

/// Why a topic name is invalid. The display text is the message of Kafka's `InvalidTopicException` from `Topic.validate`.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InvalidTopicName {
    /// The name is the empty string.
    #[error("Topic name is invalid: the empty string is not allowed")]
    Empty,
    /// The name is `.`.
    #[error("Topic name is invalid: '.' is not allowed")]
    Dot,
    /// The name is `..`.
    #[error("Topic name is invalid: '..' is not allowed")]
    DotDot,
    /// The name is longer than [`MAX_TOPIC_NAME_LENGTH`].
    #[error(
        "Topic name is invalid: the length of '{0}' is longer than the max allowed length {MAX_TOPIC_NAME_LENGTH}"
    )]
    TooLong(String),
    /// The name holds a character other than ASCII alphanumerics, `.`, `_` and `-`.
    #[error(
        "Topic name is invalid: '{0}' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'"
    )]
    IllegalCharacter(String),
}

/// Checks `name` against Kafka's `Topic.validate`, in Kafka's order.
///
/// # Errors
///
/// Returns the first [`InvalidTopicName`] rule that `name` breaks.
pub fn validate_topic_name(name: &str) -> Result<(), InvalidTopicName> {
    if name.is_empty() {
        return Err(InvalidTopicName::Empty);
    }
    if name == "." {
        return Err(InvalidTopicName::Dot);
    }
    if name == ".." {
        return Err(InvalidTopicName::DotDot);
    }
    // Java's `String.length()` counts UTF-16 code units.
    if name.encode_utf16().count() > MAX_TOPIC_NAME_LENGTH {
        return Err(InvalidTopicName::TooLong(name.to_owned()));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(InvalidTopicName::IllegalCharacter(name.to_owned()));
    }
    Ok(())
}

/// Whether `a` and `b` are the same name after `.` becomes `_` (Kafka's `Topic.hasCollision`).
///
/// Kafka metric names cannot tell `.` from `_`, so Kafka refuses to create a topic that collides with an existing topic.
#[must_use]
pub fn topic_names_collide(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .all(|(x, y)| unify_collision_byte(x) == unify_collision_byte(y))
}

fn unify_collision_byte(byte: u8) -> u8 { if byte == b'.' { b'_' } else { byte } }

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn validate_topic_name_matches_kafka() {
        let long_ok = "a".repeat(MAX_TOPIC_NAME_LENGTH);
        let too_long = "a".repeat(MAX_TOPIC_NAME_LENGTH + 1);
        // Each `é` is one UTF-16 unit and two UTF-8 bytes, so the length rule must count units.
        let non_ascii_short = "é".repeat(200);
        for (name, expected) in [
            ("orders", Ok(())),
            ("a.b_c-D9", Ok(())),
            (long_ok.as_str(), Ok(())),
            ("", Err(InvalidTopicName::Empty)),
            (".", Err(InvalidTopicName::Dot)),
            ("..", Err(InvalidTopicName::DotDot)),
            ("...", Ok(())),
            (
                too_long.as_str(),
                Err(InvalidTopicName::TooLong(too_long.clone())),
            ),
            (
                "a/b",
                Err(InvalidTopicName::IllegalCharacter("a/b".to_owned())),
            ),
            (
                "../x",
                Err(InvalidTopicName::IllegalCharacter("../x".to_owned())),
            ),
            (
                "/tmp/x",
                Err(InvalidTopicName::IllegalCharacter("/tmp/x".to_owned())),
            ),
            (
                "a b",
                Err(InvalidTopicName::IllegalCharacter("a b".to_owned())),
            ),
            (
                non_ascii_short.as_str(),
                Err(InvalidTopicName::IllegalCharacter(non_ascii_short.clone())),
            ),
        ] {
            check!(validate_topic_name(name) == expected, "{name:?}");
        }
    }

    #[test]
    fn invalid_topic_name_messages_are_kafka_text() {
        for (error, message) in [
            (
                InvalidTopicName::Empty,
                "Topic name is invalid: the empty string is not allowed",
            ),
            (
                InvalidTopicName::Dot,
                "Topic name is invalid: '.' is not allowed",
            ),
            (
                InvalidTopicName::DotDot,
                "Topic name is invalid: '..' is not allowed",
            ),
            (
                InvalidTopicName::TooLong("x".to_owned()),
                "Topic name is invalid: the length of 'x' is longer than the max allowed length 249",
            ),
            (
                InvalidTopicName::IllegalCharacter("a/b".to_owned()),
                "Topic name is invalid: 'a/b' contains one or more characters other than ASCII \
                 alphanumerics, '.', '_' and '-'",
            ),
        ] {
            check!(error.to_string() == message);
        }
    }

    #[test]
    fn topic_names_collide_unifies_dot_and_underscore() {
        for (a, b, expected) in [
            ("a.b", "a_b", true),
            ("a_b", "a.b", true),
            ("a.b", "a.b", true),
            ("a.b", "a-b", false),
            ("a.b", "a.bc", false),
            ("orders", "orders2", false),
        ] {
            check!(topic_names_collide(a, b) == expected, "{a} {b}");
        }
    }
}
