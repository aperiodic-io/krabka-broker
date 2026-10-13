//! Validation of the `AlterClientQuotas` entries and the metadata records
//! they become.
//!
//! This is a port of Kafka's `ClientQuotaControlManager.alterClientQuotas`:
//! the same checks, in the same order, with the same `INVALID_REQUEST`
//! messages. The quota keys and the entity types are the vocabulary of that
//! validation, so they live here beside the checks that use them.

use {
    crate::codes::INVALID_REQUEST,
    krabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity},
    krabka_protocol::owned::alter_client_quotas_request::EntryData,
    std::collections::{BTreeMap, HashMap},
};

/// Quota key: produce-side bandwidth cap in bytes/sec (KIP-13).
const PRODUCER_BYTE_RATE_KEY: &str = "producer_byte_rate";
/// Quota key: fetch-side bandwidth cap in bytes/sec (KIP-13).
const CONSUMER_BYTE_RATE_KEY: &str = "consumer_byte_rate";
/// Quota key: request-handler time cap as a percentage of one thread (KIP-124).
const REQUEST_PERCENTAGE_KEY: &str = "request_percentage";
/// Quota key: per-IP connection creation rate (KIP-612).
const CONNECTION_CREATION_RATE_KEY: &str = "connection_creation_rate";
/// Quota key: controller mutation rate for topic/partition creation and deletion (KIP-599).
const CONTROLLER_MUTATION_RATE_KEY: &str = "controller_mutation_rate";

/// The `ConfigDef` type Kafka's `QuotaConfig` gives a quota key. It decides
/// the range and fraction checks a new value goes through.
#[derive(Clone, Copy)]
enum QuotaValueType {
    Int,
    Long,
    Double,
}

/// One quota key of an entity type's key set, with its `ConfigDef` type.
struct QuotaKey {
    name: &'static str,
    value_type: QuotaValueType,
}

/// `QuotaConfig.userAndClientQuotaConfigs()`: the keys a `user` and/or
/// `client-id` entity accepts.
const USER_AND_CLIENT_KEYS: &[QuotaKey] = &[
    QuotaKey {
        name: PRODUCER_BYTE_RATE_KEY,
        value_type: QuotaValueType::Long,
    },
    QuotaKey {
        name: CONSUMER_BYTE_RATE_KEY,
        value_type: QuotaValueType::Long,
    },
    QuotaKey {
        name: REQUEST_PERCENTAGE_KEY,
        value_type: QuotaValueType::Double,
    },
    QuotaKey {
        name: CONTROLLER_MUTATION_RATE_KEY,
        value_type: QuotaValueType::Double,
    },
];

/// `QuotaConfig.ipConfigs()`: the keys an `ip` entity accepts.
const IP_KEYS: &[QuotaKey] = &[QuotaKey {
    name: CONNECTION_CREATION_RATE_KEY,
    value_type: QuotaValueType::Int,
}];

/// One requested change to a quota key: a set to a value, or a removal.
#[derive(Clone, Copy)]
struct QuotaChange<'a> {
    key: &'a str,
    value: Option<f64>,
}

/// `Integer.MAX_VALUE` as a double, the bound of an `INT` quota.
const INT_MAX: f64 = 2_147_483_647.0;
/// `Long.MAX_VALUE` as Java widens it to a double (2^63), the bound of a
/// `LONG` quota.
const LONG_MAX: f64 = 9_223_372_036_854_775_808.0;

/// Quota entity type: authenticated user principal (KIP-257).
const ENTITY_TYPE_USER: &str = "user";
/// Quota entity type: client id (KIP-257).
const ENTITY_TYPE_CLIENT_ID: &str = "client-id";
/// Quota entity type: client source IP address (KIP-612).
const ENTITY_TYPE_IP: &str = "ip";

/// The wire `(code, message)` pair of a rejected entity.
pub(crate) type EntityError = (i16, String);

/// The outcome of one `AlterClientQuotas` request.
#[derive(Debug, PartialEq)]
pub(crate) struct Alteration {
    /// One result per distinct entity, in the order each entity first
    /// appears. Kafka keys its result map by entity, so a repeated entity
    /// answers one row.
    pub(crate) results: Vec<(EntityKey, Result<(), EntityError>)>,
    /// The records to write. They are empty for a request that changes
    /// nothing.
    pub(crate) records: Vec<MetadataRecord>,
}

/// The entity an entry names, sorted by entity type.
///
/// Kafka's `AlterClientQuotasRequest.entries` builds a map from type to
/// name, so an entry that repeats a type keeps its last name.
pub(crate) fn entry_entity(entry: &EntryData) -> EntityKey {
    let mut by_type: BTreeMap<&str, Option<&str>> = BTreeMap::new();
    for e in &entry.entity {
        by_type.insert(e.entity_type.as_str(), e.entity_name.as_deref());
    }
    by_type
        .into_iter()
        .map(|(t, n)| (t.to_owned(), n.map(str::to_owned)))
        .collect()
}

/// Kafka's `ClientQuotaEntity.toString`, which its error messages embed.
///
/// A Java `HashMap` over `user`, `client-id` and `ip` iterates `client-id`
/// before `user`, the same order as the sorted key.
fn describe_entity(entity: &[(String, Option<String>)]) -> String {
    let entries = entity
        .iter()
        .map(|(t, n)| format!("{t}={}", n.as_deref().unwrap_or("null")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ClientQuotaEntity(entries={{{entries}}})")
}

fn invalid(message: String) -> EntityError { (INVALID_REQUEST, message) }

/// Validates every entry and builds the records they write, as Kafka's
/// `ClientQuotaControlManager.alterClientQuotas` does.
///
/// `current` is the quota state before the request: a removal writes a
/// record only for a key that is set, and a set writes one only for a value
/// that changes. `ip_is_valid` answers Kafka's `InetAddress.getByName` check
/// for a named `ip` entity.
pub(crate) fn alter_client_quotas(
    entries: &[EntryData],
    current: &HashMap<EntityKey, BTreeMap<String, f64>>,
    ip_is_valid: &dyn Fn(&str) -> bool,
) -> Alteration {
    let mut results: Vec<(EntityKey, Result<(), EntityError>)> = Vec::new();
    let mut records = Vec::new();
    for entry in entries {
        let entity = entry_entity(entry);
        let mut alterations: Vec<QuotaChange<'_>> = Vec::with_capacity(entry.ops.len());
        let mut duplicate_key = false;
        for op in &entry.ops {
            if alterations.iter().any(|change| change.key == op.key) {
                duplicate_key = true;
            } else {
                alterations.push(QuotaChange {
                    key: op.key.as_str(),
                    value: (!op.remove).then_some(op.value),
                });
            }
        }
        let slot = results.iter().position(|(seen, _)| *seen == entity);
        // Kafka records the "Duplicate quota key" error under the entity and
        // then finds the entity already in its result map, so both a
        // duplicate key and a repeated entity end as "Ignoring duplicate
        // entity" with no records for this entry.
        let outcome = if slot.is_some() || duplicate_key {
            Err(invalid(format!(
                "Ignoring duplicate entity {}",
                describe_entity(&entity)
            )))
        } else {
            alter_entity(
                &entity,
                &alterations,
                current.get(&entity),
                ip_is_valid,
                &mut records,
            )
        };
        match slot {
            Some(index) => results[index].1 = outcome,
            None => results.push((entity, outcome)),
        }
    }
    Alteration { results, records }
}

/// Kafka's `alterClientQuotaEntity`: the entity checks, the key set they
/// select, then each alteration. A removal is not validated.
fn alter_entity(
    entity: &[(String, Option<String>)],
    alterations: &[QuotaChange<'_>],
    current: Option<&BTreeMap<String, f64>>,
    ip_is_valid: &dyn Fn(&str) -> bool,
    records: &mut Vec<MetadataRecord>,
) -> Result<(), EntityError> {
    validate_entity(entity)?;
    let keys = config_keys_for_entity(entity, ip_is_valid)?;
    let mut new_records = Vec::with_capacity(alterations.len());
    for &QuotaChange { key, value } in alterations {
        let current_value = current.and_then(|quotas| quotas.get(key)).copied();
        match value {
            None => {
                if current_value.is_some() {
                    new_records.push(record(entity, key, None));
                }
            }
            Some(value) => {
                validate_quota_key_value(keys, key, value)?;
                // Java's `Objects.equals` on two `Double`s compares bits.
                if current_value.map(f64::to_bits) != Some(value.to_bits()) {
                    new_records.push(record(entity, key, Some(value)));
                }
            }
        }
    }
    records.extend(new_records);
    Ok(())
}

fn record(entity: &[(String, Option<String>)], key: &str, value: Option<f64>) -> MetadataRecord {
    MetadataRecord::V1ClientQuota(ClientQuotaRecord {
        entity: entity
            .iter()
            .map(|(entity_type, entity_name)| QuotaEntity {
                entity_type: entity_type.clone(),
                entity_name: entity_name.clone(),
            })
            .collect(),
        config_key: key.to_owned(),
        config_value: value,
    })
}

/// Kafka's `validateEntity`: a known type and a non-empty name for each
/// component.
fn validate_entity(entity: &[(String, Option<String>)]) -> Result<(), EntityError> {
    if entity.is_empty() {
        return Err(invalid("Invalid empty client quota entity".to_owned()));
    }
    for (entity_type, entity_name) in entity {
        if ![ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT_ID, ENTITY_TYPE_IP]
            .contains(&entity_type.as_str())
        {
            return Err(invalid(format!(
                "Unhandled client quota entity type: {entity_type}"
            )));
        }
        if entity_name.as_deref() == Some("") {
            return Err(invalid(format!("Empty {entity_type} not supported")));
        }
    }
    Ok(())
}

/// Kafka's `configKeysForEntityType`: the allowed combination of entity
/// types and the quota keys it accepts.
fn config_keys_for_entity(
    entity: &[(String, Option<String>)],
    ip_is_valid: &dyn Fn(&str) -> bool,
) -> Result<&'static [QuotaKey], EntityError> {
    let has = |wanted: &str| entity.iter().any(|(t, _)| t == wanted);
    let ip = entity.iter().find(|(t, _)| t == ENTITY_TYPE_IP);
    if let Some((_, ip_name)) = ip {
        if has(ENTITY_TYPE_USER) || has(ENTITY_TYPE_CLIENT_ID) {
            // Kafka's message has no space between "should" and "not".
            return Err(invalid(
                "Invalid quota entity combination, IP entity shouldnot be combined with User or \
                 ClientId"
                    .to_owned(),
            ));
        }
        match ip_name {
            Some(name) if !ip_is_valid(name) => Err(invalid(format!(
                "{name} is not a valid IP or resolvable host."
            ))),
            _ => Ok(IP_KEYS),
        }
    } else if has(ENTITY_TYPE_USER) || has(ENTITY_TYPE_CLIENT_ID) {
        Ok(USER_AND_CLIENT_KEYS)
    } else {
        Err(invalid("Invalid empty client quota entity".to_owned()))
    }
}

/// Kafka's `validateQuotaKeyValue`: a key of the entity's set, a positive
/// value, and for an integral key a whole value inside the type's range. A
/// `DOUBLE` key has no further check.
fn validate_quota_key_value(keys: &[QuotaKey], key: &str, value: f64) -> Result<(), EntityError> {
    let Some(&QuotaKey { value_type, .. }) = keys.iter().find(|known| known.name == key) else {
        return Err(invalid(format!("Invalid configuration key {key}")));
    };
    if value <= 0.0 {
        return Err(invalid(format!("Quota {key} must be greater than 0")));
    }
    match value_type {
        QuotaValueType::Double => Ok(()),
        QuotaValueType::Int if value > INT_MAX => Err(invalid(format!(
            "Proposed value for {key} is too large for an INT."
        ))),
        QuotaValueType::Long if value > LONG_MAX => Err(invalid(format!(
            "Proposed value for {key} is too large for a LONG."
        ))),
        QuotaValueType::Int | QuotaValueType::Long => {
            if (value % 1.0).abs() > 1.0e-6 {
                Err(invalid(format!("{key} cannot be a fractional value.")))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::alter_client_quotas::test_support::{
            EntityPart, QuotaOp, entry, op, op_remove, part,
        },
        assert2::check,
    };

    type Entity = Vec<EntityPart<'static>>;
    type Ops = Vec<QuotaOp>;

    /// One entry of a request: the entity it names and the operations on it.
    struct EntryCase {
        entity: Entity,
        ops: Ops,
    }

    fn key(parts: &[EntityPart<'_>]) -> EntityKey {
        krabka_metadata::canonicalize_entity(
            parts
                .iter()
                .map(|part| {
                    (
                        part.entity_type.to_owned(),
                        part.entity_name.map(str::to_owned),
                    )
                })
                .collect(),
        )
    }

    fn set(entity: &[EntityPart<'_>], quota: &str, value: f64) -> MetadataRecord {
        record(&key(entity), quota, Some(value))
    }

    fn remove(entity: &[EntityPart<'_>], quota: &str) -> MetadataRecord {
        record(&key(entity), quota, None)
    }

    fn err(message: &str) -> Result<(), EntityError> { Err((INVALID_REQUEST, message.to_owned())) }

    /// Resolves IP literals and the name `localhost`, as the handler's
    /// resolver would.
    fn resolver(name: &str) -> bool {
        name.parse::<std::net::IpAddr>().is_ok() || name == "localhost"
    }

    /// A case name, the request entries, and the whole expected outcome.
    struct CaseRow {
        name: &'static str,
        entries: Vec<EntryCase>,
        expected: Alteration,
    }

    /// Runs each row against a quota state where `user=carol` has
    /// `producer_byte_rate` 1024.
    fn run(rows: Vec<CaseRow>) {
        let existing: HashMap<EntityKey, BTreeMap<String, f64>> = HashMap::from([(
            key(&[part("user", Some("carol"))]),
            BTreeMap::from([("producer_byte_rate".to_owned(), 1024.0)]),
        )]);
        for CaseRow {
            name,
            entries,
            expected,
        } in rows
        {
            let entries: Vec<EntryData> = entries
                .into_iter()
                .map(|EntryCase { entity, ops }| entry(entity, ops))
                .collect();
            let got = alter_client_quotas(&entries, &existing, &resolver);
            check!(got == expected, "row {name}");
        }
    }

    /// Rows for setting, unknown keys and removals.
    fn key_rules_set_and_remove_rows() -> Vec<CaseRow> {
        let alice: Entity = vec![part("user", Some("alice"))];
        vec![
            CaseRow {
                name: "valid set",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("producer_byte_rate", 1024.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&alice), Ok(()))],
                    records: vec![set(&alice, "producer_byte_rate", 1024.0)],
                },
            },
            CaseRow {
                name: "unknown key, set",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("foo", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&alice), err("Invalid configuration key foo"))],
                    records: vec![],
                },
            },
            CaseRow {
                name: "unknown key, removed, is not validated and writes nothing",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op_remove("foo", 0.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&alice), Ok(()))],
                    records: vec![],
                },
            },
            CaseRow {
                name: "removal of a set key writes a removal",
                entries: vec![EntryCase {
                    entity: vec![part("user", Some("carol"))],
                    ops: vec![op_remove("producer_byte_rate", 0.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&[part("user", Some("carol"))]), Ok(()))],
                    records: vec![remove(&[part("user", Some("carol"))], "producer_byte_rate")],
                },
            },
            CaseRow {
                name: "an unchanged value writes nothing",
                entries: vec![EntryCase {
                    entity: vec![part("user", Some("carol"))],
                    ops: vec![op("producer_byte_rate", 1024.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&[part("user", Some("carol"))]), Ok(()))],
                    records: vec![],
                },
            },
        ]
    }

    /// Rows for the value and key-scope rules.
    fn key_rules_value_rows() -> Vec<CaseRow> {
        let alice: Entity = vec![part("user", Some("alice"))];
        let ip: Entity = vec![part("ip", Some("10.0.0.1"))];
        vec![
            CaseRow {
                name: "zero",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("producer_byte_rate", 0.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&alice),
                        err("Quota producer_byte_rate must be greater than 0"),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "negative",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("request_percentage", -1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&alice),
                        err("Quota request_percentage must be greater than 0"),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "fractional LONG",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("producer_byte_rate", 1024.5)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&alice),
                        err("producer_byte_rate cannot be a fractional value."),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "LONG past Long.MAX_VALUE",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("consumer_byte_rate", f64::INFINITY)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&alice),
                        err("Proposed value for consumer_byte_rate is too large for a LONG."),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "fractional INT",
                entries: vec![EntryCase {
                    entity: ip.clone(),
                    ops: vec![op("connection_creation_rate", 1.5)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&ip),
                        err("connection_creation_rate cannot be a fractional value."),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "INT past Integer.MAX_VALUE",
                entries: vec![EntryCase {
                    entity: ip.clone(),
                    ops: vec![op("connection_creation_rate", 3.0e9)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&ip),
                        err("Proposed value for connection_creation_rate is too large for an INT."),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "DOUBLE has no upper bound",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("request_percentage", 250.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&alice), Ok(()))],
                    records: vec![set(&alice, "request_percentage", 250.0)],
                },
            },
            CaseRow {
                name: "infinity on a DOUBLE key",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("controller_mutation_rate", f64::INFINITY)],
                }],
                expected: Alteration {
                    results: vec![(key(&alice), Ok(()))],
                    records: vec![set(&alice, "controller_mutation_rate", f64::INFINITY)],
                },
            },
            CaseRow {
                name: "ip key on a user entity",
                entries: vec![EntryCase {
                    entity: alice.clone(),
                    ops: vec![op("connection_creation_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&alice),
                        err("Invalid configuration key connection_creation_rate"),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "user key on an ip entity",
                entries: vec![EntryCase {
                    entity: ip.clone(),
                    ops: vec![op("producer_byte_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&ip),
                        err("Invalid configuration key producer_byte_rate"),
                    )],
                    records: vec![],
                },
            },
        ]
    }

    /// Rows for the entity types and names.
    fn entity_rules_name_rows() -> Vec<CaseRow> {
        vec![
            CaseRow {
                name: "ip combined with user",
                entries: vec![EntryCase {
                    entity: vec![part("ip", Some("10.0.0.1")), part("user", Some("alice"))],
                    ops: vec![op("connection_creation_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&[part("ip", Some("10.0.0.1")), part("user", Some("alice"))]),
                        err(
                            "Invalid quota entity combination, IP entity shouldnot be combined \
                             with User or ClientId",
                        ),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "IPv6 and a resolvable host name are valid ip names",
                entries: vec![
                    EntryCase {
                        entity: vec![part("ip", Some("::1"))],
                        ops: vec![op("connection_creation_rate", 5.0)],
                    },
                    EntryCase {
                        entity: vec![part("ip", Some("localhost"))],
                        ops: vec![op("connection_creation_rate", 6.0)],
                    },
                ],
                expected: Alteration {
                    results: vec![
                        (key(&[part("ip", Some("::1"))]), Ok(())),
                        (key(&[part("ip", Some("localhost"))]), Ok(())),
                    ],
                    records: vec![
                        set(&[part("ip", Some("::1"))], "connection_creation_rate", 5.0),
                        set(
                            &[part("ip", Some("localhost"))],
                            "connection_creation_rate",
                            6.0,
                        ),
                    ],
                },
            },
            CaseRow {
                name: "unresolvable ip name",
                entries: vec![EntryCase {
                    entity: vec![part("ip", Some("not-an-ip"))],
                    ops: vec![op("connection_creation_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&[part("ip", Some("not-an-ip"))]),
                        err("not-an-ip is not a valid IP or resolvable host."),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "empty user name",
                entries: vec![EntryCase {
                    entity: vec![part("user", Some(""))],
                    ops: vec![op("producer_byte_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&[part("user", Some(""))]),
                        err("Empty user not supported"),
                    )],
                    records: vec![],
                },
            },
        ]
    }

    /// Rows for repeated keys, entities and types.
    fn entity_rules_duplicate_rows() -> Vec<CaseRow> {
        let alice: Entity = vec![part("user", Some("alice"))];
        vec![
            CaseRow {
                name: "unknown entity type",
                entries: vec![EntryCase {
                    entity: vec![part("group", Some("g1"))],
                    ops: vec![op("producer_byte_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&[part("group", Some("g1"))]),
                        err("Unhandled client quota entity type: group"),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "empty entity",
                entries: vec![EntryCase {
                    entity: vec![],
                    ops: vec![op("producer_byte_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(vec![], err("Invalid empty client quota entity"))],
                    records: vec![],
                },
            },
            CaseRow {
                name: "the same key twice",
                entries: vec![EntryCase {
                    entity: vec![part("client-id", None), part("user", Some("alice"))],
                    ops: vec![op("producer_byte_rate", 1.0), op("producer_byte_rate", 2.0)],
                }],
                expected: Alteration {
                    results: vec![(
                        key(&[part("client-id", None), part("user", Some("alice"))]),
                        err(
                            "Ignoring duplicate entity ClientQuotaEntity(entries={client-id=null, \
                             user=alice})",
                        ),
                    )],
                    records: vec![],
                },
            },
            CaseRow {
                name: "the same entity twice keeps the first records and answers one row",
                entries: vec![
                    EntryCase {
                        entity: alice.clone(),
                        ops: vec![op("producer_byte_rate", 1.0)],
                    },
                    EntryCase {
                        entity: alice.clone(),
                        ops: vec![op("consumer_byte_rate", 2.0)],
                    },
                ],
                expected: Alteration {
                    results: vec![(
                        key(&alice),
                        err("Ignoring duplicate entity ClientQuotaEntity(entries={user=alice})"),
                    )],
                    records: vec![set(&alice, "producer_byte_rate", 1.0)],
                },
            },
            CaseRow {
                name: "a repeated entity type keeps the last name",
                entries: vec![EntryCase {
                    entity: vec![part("user", Some("a")), part("user", Some("b"))],
                    ops: vec![op("producer_byte_rate", 1.0)],
                }],
                expected: Alteration {
                    results: vec![(key(&[part("user", Some("b"))]), Ok(()))],
                    records: vec![set(&[part("user", Some("b"))], "producer_byte_rate", 1.0)],
                },
            },
        ]
    }

    /// The key and value rules of Kafka's `ClientQuotaControlManager`
    /// (#675), one row per rule.
    #[test]
    fn key_and_value_rules_match_kafkas_client_quota_control_manager() {
        let mut rows = key_rules_set_and_remove_rows();
        rows.extend(key_rules_value_rows());
        run(rows);
    }

    /// The entity rules of Kafka's `ClientQuotaControlManager` (#675): the
    /// allowed types and names, and how a repeated key, entity or type ends.
    #[test]
    fn entity_rules_match_kafkas_client_quota_control_manager() {
        let mut rows = entity_rules_name_rows();
        rows.extend(entity_rules_duplicate_rows());
        run(rows);
    }
}
