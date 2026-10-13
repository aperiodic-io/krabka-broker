//! Dominating DENY patterns for Kafka's resource-type authorization scan.

use {
    krabka_metadata::{AclEntry, PatternType},
    std::collections::HashSet,
};

pub struct DeniedNames<'a> {
    wildcard: bool,
    literals: HashSet<&'a str>,
    prefixes: Vec<&'a str>,
}

impl<'a> DeniedNames<'a> {
    pub fn new(entries: impl IntoIterator<Item = &'a AclEntry>) -> Self {
        let mut names = Self {
            wildcard: false,
            literals: HashSet::new(),
            prefixes: Vec::new(),
        };
        for entry in entries {
            match entry.pattern_type {
                PatternType::Literal if entry.resource_name == "*" => names.wildcard = true,
                PatternType::Literal => {
                    names.literals.insert(&entry.resource_name);
                }
                PatternType::Prefixed => names.prefixes.push(&entry.resource_name),
            }
        }
        names
    }

    pub fn allows_literal(&self, name: &str) -> bool {
        !self.literals.contains(name) && self.allows_prefix(name)
    }

    pub fn allows_prefix(&self, prefix: &str) -> bool {
        // Kafka's Authorizer.authorizeByResourceType ignores literal and
        // narrower-prefix exclusions of a prefix grant. Only the wildcard or
        // a prefix containing the whole grant dominates it. No chosen witness
        // can be singled out by another literal DENY to change this decision.
        !self.wildcard
            && !self
                .prefixes
                .iter()
                .any(|denied| prefix.starts_with(denied))
    }

    pub fn allows_wildcard(&self) -> bool {
        // Kafka returns immediately for a wildcard ALLOW once it has ruled
        // out a wildcard DENY, irrespective of literal and prefix exclusions.
        !self.wildcard
    }
}
