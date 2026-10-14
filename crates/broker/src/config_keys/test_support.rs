use krabka_metadata::TopicConfigRecord;

#[derive(Debug)]
pub struct TopicOverride<'a> {
    pub key: &'a str,
    pub value: &'a str,
}

pub fn topic_config_record(overrides: &[TopicOverride<'_>]) -> TopicConfigRecord {
    TopicConfigRecord {
        topic: "t".into(),
        overrides: overrides
            .iter()
            .map(|TopicOverride { key, value }| ((*key).to_owned(), (*value).to_owned()))
            .collect(),
    }
}
