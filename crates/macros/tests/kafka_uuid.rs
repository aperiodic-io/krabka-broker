//! The derive delegates its string representation, parser and errors to the local codec.

use assert2::assert;

type KafkaUuidError = &'static str;

fn encode(value: u32) -> String { format!("kafka-{value}") }

fn decode(input: &str) -> Result<u32, KafkaUuidError> {
    input
        .strip_prefix("kafka-")
        .ok_or("expected Kafka UUID")?
        .parse()
        .map_err(|_| "invalid Kafka UUID")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::KafkaUuid)]
struct ClusterId(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::KafkaUuid)]
struct DirectoryId(u32);

#[test]
fn display_obeys_width_alignment_fill_and_precision() {
    let cluster = ClusterId(7);
    let directory = DirectoryId(7);
    assert!(cluster.to_string() == "kafka-7");
    assert!(directory.to_string() == cluster.to_string());
    assert!(format!("{cluster:>10}") == "   kafka-7");
    assert!(format!("{directory:*<10}") == "kafka-7***");
    assert!(format!("{cluster:_^10}") == "_kafka-7__");
    assert!(format!("{directory:.5}") == "kafka");
}

#[test]
fn parsing_and_serde_preserve_the_local_codec_contract() {
    assert!("kafka-7".parse::<ClusterId>() == Ok(ClusterId(7)));
    assert!("kafka-9".parse::<DirectoryId>() == Ok(DirectoryId(9)));
    assert!("hyphenated".parse::<ClusterId>() == Err("expected Kafka UUID"));
    assert!("kafka-many".parse::<DirectoryId>() == Err("invalid Kafka UUID"));
    assert!(serde_json::to_string(&ClusterId(7)).unwrap() == r#""kafka-7""#);
    assert!(serde_json::to_string(&DirectoryId(9)).unwrap() == r#""kafka-9""#);
    assert!(serde_json::from_str::<ClusterId>(r#""kafka-7""#).unwrap() == ClusterId(7));
    assert!(serde_json::from_str::<DirectoryId>(r#""kafka-9""#).unwrap() == DirectoryId(9));
    assert!(serde_json::from_str::<ClusterId>("7").is_err());
    let error = serde_json::from_str::<DirectoryId>(r#""kafka-many""#).unwrap_err();
    assert!(error.to_string().starts_with("invalid Kafka UUID"));
}
