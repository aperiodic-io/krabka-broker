//! Independent wire expectations and input metadata for configuration tests.

use krabka_metadata::{MetadataImage, MetadataRecord, TopicConfigRecord};
use krabka_protocol::owned::describe_configs_response::DescribeConfigsSynonym;

pub(super) fn synonym(name: &str, value: &str, source: i8) -> DescribeConfigsSynonym {
    tagged_wire!(DescribeConfigsSynonym {
        name: name.to_owned(),
        value: Some(value.to_owned()),
        source,
    })
}

#[derive(Clone, Copy)]
pub(super) struct TopicConfigSetup<'a> {
    pub topic: &'a str,
    pub key: &'a str,
    pub value: &'a str,
}

impl Default for TopicConfigSetup<'_> {
    fn default() -> Self {
        Self {
            topic: "orders",
            key: "retention.ms",
            value: "60000",
        }
    }
}

pub(super) fn set_topic_config(image: &mut MetadataImage, setup: TopicConfigSetup<'_>) {
    let TopicConfigSetup { topic, key, value } = setup;
    image.apply(&MetadataRecord::V1TopicConfig(TopicConfigRecord {
        topic: topic.into(),
        overrides: maplit::btreemap! {key.to_string() => value.to_string()},
    }));
}
