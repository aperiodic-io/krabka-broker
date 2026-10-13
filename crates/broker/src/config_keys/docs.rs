//! The generated topic-config reference table, one row for each whitelisted
//! key.
//!
//! The rows come from [`super::registry`], so the reference page, the
//! `AlterConfigs` validator, and the typed metadata `DescribeConfigs` reports
//! cannot drift apart. A key the page documents is a key the validator
//! accepts, spelled with the type the JVM `AdminClient` parses it with.

use super::registry::{self, ConfigScope};

/// One whitelisted topic-config key, for the generated reference page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicConfigDoc {
    pub key: &'static str,
    /// The `ConfigDef` type name, and the unit or range that narrows it.
    pub value_type: String,
    pub default: Option<&'static str>,
    pub kip: Option<&'static str>,
    pub description: &'static str,
}

/// The full whitelist documented on the topic-configs reference page.
///
/// A key no alter path may write stays off the page, because no operator can
/// set one. `WRITE_FREEZE` is synthesised and never stored.
#[must_use]
pub fn topic_config_docs() -> Vec<TopicConfigDoc> {
    registry::keys_in(ConfigScope::Topic)
        .filter(|row| row.is_alterable())
        .map(|row| TopicConfigDoc {
            key: row.name,
            value_type: row.value_type(),
            default: row.default,
            kip: row.kip,
            description: row.doc,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                RETENTION_MS, WRITE_FREEZE,
                validation::{is_recognized, validate_topic_config},
            },
            *,
        },
        assert2::{assert, check},
    };

    #[test]
    fn every_documented_key_is_a_key_the_validator_accepts() {
        use std::collections::HashSet;
        let docs = topic_config_docs();
        let doc_keys: HashSet<&str> = docs.iter().map(|d| d.key).collect();

        check!(
            doc_keys.len() == docs.len(),
            "duplicate key in the doc table"
        );
        for key in &doc_keys {
            check!(
                is_recognized(key),
                "documented key `{key}` not recognized by the validator"
            );
        }
        check!(docs.iter().all(|d| !d.description.is_empty()));
    }

    #[test]
    fn every_key_the_validator_accepts_is_documented() {
        // The validator and the page read the same table, so the way this can
        // break is a row the page filters out. Only unstored keys may be.
        for row in super::registry::keys_in(ConfigScope::Topic) {
            let documented = topic_config_docs().iter().any(|doc| doc.key == row.name);
            check!(
                documented == is_recognized(row.name),
                "`{}` is documented={documented} but recognized={}",
                row.name,
                is_recognized(row.name)
            );
        }
    }

    #[test]
    fn the_synthesised_freeze_key_is_neither_documented_nor_alterable() {
        let docs = topic_config_docs();

        check!(!docs.iter().any(|doc| doc.key == WRITE_FREEZE));
        check!(!is_recognized(WRITE_FREEZE));
        check!(validate_topic_config(WRITE_FREEZE, "false").is_err());
    }

    #[test]
    fn a_row_carries_the_type_the_default_and_the_kip_from_the_registry() {
        let docs = topic_config_docs();
        let retention = docs
            .iter()
            .find(|doc| doc.key == RETENTION_MS)
            .expect(RETENTION_MS);

        assert!(
            *retention
                == TopicConfigDoc {
                    key: RETENTION_MS,
                    value_type: "long (ms)".to_owned(),
                    default: Some("604800000"),
                    kip: None,
                    description: "Retention time before log segments become eligible for deletion.",
                }
        );
    }
}
