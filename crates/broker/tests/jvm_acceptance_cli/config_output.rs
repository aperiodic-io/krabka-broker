//! Reading what `kafka-configs --describe` printed, for each of the entity
//! types it prints differently.
//!
//! `ConfigCommand` has two describe paths and they do not look alike. A
//! resource-backed entity -- `topics`, `brokers`, `groups`, `client-metrics`
//! -- goes through `DescribeConfigs` and prints a header line followed by one
//! indented `name=value sensitive=… synonyms={…}` line per key. A quota-backed
//! entity -- `clients`, `users`, `ips`, and the `users`+`clients` tuple --
//! goes through `DescribeClientQuotas` and prints a single
//! `Quota configs for <entity> are <k=v, k=v>` line.
//!
//! Both are reduced here to the same `BTreeMap<String, String>`, so one case
//! can state the same rule -- what was altered comes back -- over every entity
//! type in a table, and the oracle differential compares maps rather than two
//! renderings of them.

use std::collections::BTreeMap;

/// The configuration one `--describe` reported, whichever path printed it.
pub(crate) type Configs = BTreeMap<String, String>;

/// Parse a `kafka-configs --describe` stdout into the keys it reported.
///
/// Both shapes are read in one pass because a case does not know, and should
/// not have to know, which path its entity type took: that is `ConfigCommand`'s
/// private business, and a krabka change that moved an entity from one path to
/// the other would be a divergence this suite must catch rather than a reason
/// to fail parsing.
pub(crate) fn parse_describe(stdout: &str) -> Configs {
    let mut configs = Configs::new();
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(entries) = line.split_once(" are ").filter(|_| is_quota_line(line)) {
            for entry in entries.1.split(',') {
                if let Some((key, value)) = entry.trim().split_once('=') {
                    configs.insert(key.trim().to_owned(), value.trim().to_owned());
                }
            }
        } else if let Some((assignment, _)) = line.split_once(" sensitive=")
            && let Some((key, value)) = assignment.split_once('=')
        {
            configs.insert(key.trim().to_owned(), value.trim().to_owned());
        }
    }
    configs
}

/// Whether a line is the quota path's one-line summary.
fn is_quota_line(line: &str) -> bool { line.starts_with("Quota configs for ") }

/// The entity a quota line named, as the tool spelled it.
///
/// The tuple case is the reason this exists: `--entity-type users
/// --entity-name u --entity-type clients --entity-name c` is one quota entity
/// with two components, and the tool renders it as
/// `user-principal 'u', client-id 'c'`. A case that only compared the values
/// would pass on a broker that quietly dropped the client half and answered
/// for the user alone.
pub(crate) fn quota_entities(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| is_quota_line(line))
        .filter_map(|line| {
            let rest = line.strip_prefix("Quota configs for ")?;
            let (entity, _) = rest.split_once(" are ")?;
            Some(entity.trim().to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn the_resource_path_yields_its_keys_without_the_provenance() {
        let stdout = "Dynamic configs for group 'krabka-grp' are:\n  \
             consumer.session.timeout.ms=50000 sensitive=false \
             synonyms={DYNAMIC_GROUP_CONFIG:consumer.session.timeout.ms=50000}\n";
        check!(
            parse_describe(stdout)
                == Configs::from([("consumer.session.timeout.ms".to_owned(), "50000".to_owned())])
        );
    }

    #[test]
    fn the_quota_path_yields_the_same_shape_as_the_resource_path() {
        let stdout = "Quota configs for client-id 'krabka-client' are \
             consumer_byte_rate=1024.0, producer_byte_rate=2048.0\n";
        check!(
            parse_describe(stdout)
                == Configs::from([
                    ("consumer_byte_rate".to_owned(), "1024.0".to_owned()),
                    ("producer_byte_rate".to_owned(), "2048.0".to_owned()),
                ])
        );
    }

    #[test]
    fn a_tuple_entity_is_reported_with_both_of_its_components() {
        let stdout = "Quota configs for user-principal 'u', client-id 'c' are \
             request_percentage=42.0\n";
        check!(quota_entities(stdout) == vec!["user-principal 'u', client-id 'c'".to_owned()]);
        check!(
            parse_describe(stdout)
                == Configs::from([("request_percentage".to_owned(), "42.0".to_owned())])
        );
    }

    #[test]
    fn a_header_line_contributes_nothing() {
        for stdout in [
            "Dynamic configs for topic 't' are:\n",
            "All configs for client-metrics 'm' are:\n",
            "",
        ] {
            check!(
                parse_describe(stdout).is_empty(),
                "parsed keys out of {stdout:?}"
            );
            check!(quota_entities(stdout).is_empty());
        }
    }
}
