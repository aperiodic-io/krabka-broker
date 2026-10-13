//! Reading the ACL set that `kafka-acls --list` printed.
//!
//! The shorthand flags are the reason this module exists. `--producer`,
//! `--consumer`, `--idempotent` and the rest are not requests the broker ever
//! sees: `AclCommand` expands each one, client-side, into several
//! `AclBinding`s across several resource types, and sends those. What a suite
//! can check is therefore not the flag but its consequence -- the set that
//! comes back from `--list` afterwards -- and a set is only comparable once it
//! is parsed out of the tool's two-level rendering.
//!
//! That rendering nests: a "Current ACLs for resource ResourcePattern(…):"
//! header, then one indented `(principal=…, host=…, operation=…,
//! permissionType=…)` line per entry under it. [`parse_acls`] flattens the two
//! levels into one binding per entry, which is the form the two sides are
//! compared in and the form a case can write an expectation in.

use std::collections::BTreeSet;

/// One ACL binding, flattened out of the resource it was printed under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct AclBinding {
    /// `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, `DELEGATION_TOKEN`…
    pub(crate) resource_type: String,
    pub(crate) resource_name: String,
    /// `LITERAL` or `PREFIXED`.
    pub(crate) pattern_type: String,
    pub(crate) principal: String,
    pub(crate) host: String,
    pub(crate) operation: String,
    /// `ALLOW` or `DENY`.
    pub(crate) permission: String,
}

/// The resource a `ResourcePattern(` line names, which the entries under it
/// belong to.
#[derive(Clone)]
struct ResourcePattern {
    resource_type: String,
    resource_name: String,
    pattern_type: String,
}

/// Parse every binding a `--list` printed, as a set.
///
/// A set rather than a list: the tool walks the authorizer's own iteration
/// order, which neither broker promises and which differs between the two
/// sides for the same bindings. What the shorthands are about is *which*
/// bindings exist, and that is exactly what a set states.
pub(crate) fn parse_acls(stdout: &str) -> BTreeSet<AclBinding> {
    let mut bindings = BTreeSet::new();
    let mut resource: Option<ResourcePattern> = None;
    for line in stdout.lines() {
        if line.contains("ResourcePattern(") {
            resource = Some(ResourcePattern {
                resource_type: field(line, "resourceType=").unwrap_or_default(),
                resource_name: field(line, "name=").unwrap_or_default(),
                pattern_type: field(line, "patternType=").unwrap_or_default(),
            });
        } else if line.contains("principal=") {
            let Some(ResourcePattern {
                resource_type,
                resource_name,
                pattern_type,
            }) = resource.clone()
            else {
                continue;
            };
            bindings.insert(AclBinding {
                resource_type,
                resource_name,
                pattern_type,
                principal: field(line, "principal=").unwrap_or_default(),
                host: field(line, "host=").unwrap_or_default(),
                operation: field(line, "operation=").unwrap_or_default(),
                permission: field(line, "permissionType=").unwrap_or_default(),
            });
        }
    }
    bindings
}

/// The value of one `key=value` field of a rendered Java object.
///
/// The fields are separated by `, ` and the object is closed by `)`, so a
/// value ends at whichever of the two comes first. `name=` needs the second
/// terminator as often as the first, because it is the last field of an
/// `AccessControlEntry`-shaped render and the first of a `ResourcePattern`.
fn field(line: &str, key: &str) -> Option<String> {
    let at = line.find(key)? + key.len();
    let rest = &line[at..];
    let end = rest
        .find(", ")
        .into_iter()
        .chain(rest.find(')'))
        .min()
        .unwrap_or(rest.len());
    Some(rest[..end].trim().to_owned())
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// What `--list` prints after `--producer --topic orders`: the two topic
    /// operations the shorthand expands to, and the cluster `IDEMPOTENT_WRITE`
    /// it adds unless `--idempotent` is spelled out.
    const PRODUCER_LISTING: &str = concat!(
        "Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, ",
        "patternType=LITERAL)`: \n",
        "\t(principal=User:alice, host=*, operation=WRITE, permissionType=ALLOW)\n",
        "\t(principal=User:alice, host=*, operation=DESCRIBE, permissionType=ALLOW)\n",
        "\n",
        "Current ACLs for resource `ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, ",
        "patternType=LITERAL)`: \n",
        "\t(principal=User:alice, host=*, operation=IDEMPOTENT_WRITE, permissionType=ALLOW)\n",
    );

    fn binding(
        resource_type: &str,
        resource_name: &str,
        operation: &str,
        permission: &str,
    ) -> AclBinding {
        AclBinding {
            resource_type: resource_type.to_owned(),
            resource_name: resource_name.to_owned(),
            pattern_type: "LITERAL".to_owned(),
            principal: "User:alice".to_owned(),
            host: "*".to_owned(),
            operation: operation.to_owned(),
            permission: permission.to_owned(),
        }
    }

    #[test]
    fn every_entry_is_flattened_onto_the_resource_it_was_printed_under() {
        check!(
            parse_acls(PRODUCER_LISTING)
                == BTreeSet::from([
                    binding("TOPIC", "orders", "WRITE", "ALLOW"),
                    binding("TOPIC", "orders", "DESCRIBE", "ALLOW"),
                    binding("CLUSTER", "kafka-cluster", "IDEMPOTENT_WRITE", "ALLOW"),
                ])
        );
    }

    #[test]
    fn the_resources_may_be_printed_in_any_order() {
        let reordered: String = {
            let mut blocks: Vec<&str> = PRODUCER_LISTING.split("\n\n").collect();
            blocks.reverse();
            blocks.join("\n\n")
        };
        check!(parse_acls(&reordered) == parse_acls(PRODUCER_LISTING));
    }

    #[test]
    fn a_denied_host_is_read_off_the_entry_that_carries_it() {
        let listing = concat!(
            "Current ACLs for resource `ResourcePattern(resourceType=TOPIC, name=orders, ",
            "patternType=PREFIXED)`: \n",
            "\t(principal=User:mallory, host=10.0.0.1, operation=READ, permissionType=DENY)\n",
        );
        check!(
            parse_acls(listing)
                == BTreeSet::from([AclBinding {
                    resource_type: "TOPIC".to_owned(),
                    resource_name: "orders".to_owned(),
                    pattern_type: "PREFIXED".to_owned(),
                    principal: "User:mallory".to_owned(),
                    host: "10.0.0.1".to_owned(),
                    operation: "READ".to_owned(),
                    permission: "DENY".to_owned(),
                }])
        );
    }

    #[test]
    fn entries_printed_before_any_resource_header_are_dropped() {
        check!(
            parse_acls("\t(principal=User:alice, host=*, operation=READ, permissionType=ALLOW)\n")
                .is_empty()
        );
    }

    #[test]
    fn an_empty_listing_is_an_empty_set() {
        check!(parse_acls("").is_empty());
    }
}
