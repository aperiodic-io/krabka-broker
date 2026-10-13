//! Validation of a single `CreateAcls` binding, and the `AclEntry` a valid one
//! becomes.
//!
//! Kafka validates a binding in two places, and this module keeps its order:
//! `AclApis.handleCreateAcls` refuses a `CLUSTER` binding not named
//! `kafka-cluster` and an empty resource name, then the controller's
//! `AclControlManager.validateNewAcl` refuses a non-concrete resource type,
//! pattern type, operation or permission type, and a principal with no `:`.
//! Kafka trunk, and so `unstable.feature.versions.enable`, also refuses a
//! malformed host ([`HostCheck`]). Every refusal is a per-binding
//! `INVALID_REQUEST` (or `UNSUPPORTED_VERSION` for a CIDR host below its
//! metadata version) carrying Kafka's message text, which `kafka-acls` prints.
//!
//! Two whole-request refusals sit in front of this, in the handler. A wire
//! `UNKNOWN` (0) element fails Kafka's `CreateAclsRequest.validate` while the
//! request is parsed, which closes the connection ([`has_unknown_element`]).
//! An `ANY` or `MATCH` element makes the `ResourcePattern` or
//! `AccessControlEntry` constructor throw, which fails every creation
//! ([`has_filter_only_element`]).

use {
    crate::{
        codes,
        handlers::acl_wire::{
            CLUSTER_RESOURCE_NAME, operation_concrete, pattern_type_concrete, permission_concrete,
            resource_type_concrete,
        },
    },
    krabka_authz::cidr::Cidr,
    krabka_metadata::AclEntry,
    krabka_protocol::owned::create_acls_request::AclCreation,
};

/// The message `CreateAcls` answers with when a host containing `/` is
/// submitted below [`crate::features::CIDR_ACL_HOST_MIN_LEVEL`]. Verbatim
/// from Kafka's `AclControlManager.validateHostPattern`.
const CIDR_UNSUPPORTED_VERSION_MESSAGE: &str =
    "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.";

/// Wire `resource_type` of Kafka's `ResourceType.CLUSTER`.
const RESOURCE_TYPE_CLUSTER: i8 = 4;
/// Wire `UNKNOWN` (0), shared by every ACL enum axis.
const WIRE_UNKNOWN: i8 = 0;
/// Wire `ANY` (1), shared by every ACL enum axis.
const WIRE_ANY: i8 = 1;
/// Wire `PatternType.MATCH` (2).
const WIRE_PATTERN_MATCH: i8 = 2;

/// True when any creation carries a wire `UNKNOWN` (0) resource type, pattern
/// type, operation or permission type.
///
/// Kafka's `CreateAclsRequest.validate` throws for such a request while it is
/// parsed, so the broker sends no response and closes the connection.
pub(super) fn has_unknown_element(creations: &[AclCreation]) -> bool {
    creations.iter().any(|c| {
        c.resource_type == WIRE_UNKNOWN
            || c.resource_pattern_type == WIRE_UNKNOWN
            || c.operation == WIRE_UNKNOWN
            || c.permission_type == WIRE_UNKNOWN
    })
}

/// True when any creation carries a filter-only value: `ANY` for the resource
/// type, operation or permission type, or `ANY` or `MATCH` for the pattern
/// type.
///
/// Kafka's `AclApis.handleCreateAcls` builds every binding before it
/// validates any of them, and the `ResourcePattern` and `AccessControlEntry`
/// constructors throw `IllegalArgumentException` for these values. The
/// request's error path then answers every creation with
/// `UNKNOWN_SERVER_ERROR` and no message, because `ApiError.fromThrowable`
/// drops the text of an unknown server error.
pub(super) fn has_filter_only_element(creations: &[AclCreation]) -> bool {
    creations.iter().any(|c| {
        c.resource_type == WIRE_ANY
            || matches!(c.resource_pattern_type, WIRE_ANY | WIRE_PATTERN_MATCH)
            || c.operation == WIRE_ANY
            || c.permission_type == WIRE_ANY
    })
}

/// Kafka's `ResourceType` constant name for a wire code, as
/// `ResourceType.fromCode` resolves it: an unrecognized code is `UNKNOWN`.
fn resource_type_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "TOPIC",
        3 => "GROUP",
        4 => "CLUSTER",
        5 => "TRANSACTIONAL_ID",
        6 => "DELEGATION_TOKEN",
        7 => "USER",
        _ => "UNKNOWN",
    }
}

/// Kafka's `PatternType` constant name for a wire code.
fn pattern_type_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "MATCH",
        3 => "LITERAL",
        4 => "PREFIXED",
        _ => "UNKNOWN",
    }
}

/// Kafka's `AclOperation` constant name for a wire code.
fn operation_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "ALL",
        3 => "READ",
        4 => "WRITE",
        5 => "CREATE",
        6 => "DELETE",
        7 => "ALTER",
        8 => "DESCRIBE",
        9 => "CLUSTER_ACTION",
        10 => "DESCRIBE_CONFIGS",
        11 => "ALTER_CONFIGS",
        12 => "IDEMPOTENT_WRITE",
        13 => "CREATE_TOKENS",
        14 => "DESCRIBE_TOKENS",
        15 => "TWO_PHASE_COMMIT",
        _ => "UNKNOWN",
    }
}

/// Kafka's `AclPermissionType` constant name for a wire code.
fn permission_type_name(code: i8) -> &'static str {
    match code {
        1 => "ANY",
        2 => "DENY",
        3 => "ALLOW",
        _ => "UNKNOWN",
    }
}

/// A binding that `CreateAcls` refuses: Kafka's error code and message.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct AclRefusal {
    pub(super) code: i16,
    pub(super) message: String,
}

fn invalid(message: String) -> AclRefusal {
    AclRefusal {
        code: codes::INVALID_REQUEST,
        message,
    }
}

/// What `CreateAcls` checks about the host of a binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum HostCheck {
    /// Kafka 4.3.1, whose `AclControlManager.validateNewAcl` has no host check
    /// and whose `AccessControlEntry` only refuses a null host. Any host,
    /// empty or containing `/`, is stored as text.
    Unchecked,
    /// Kafka trunk's `AclControlManager.validateHostPattern`, which
    /// `unstable.feature.versions.enable` turns on. A host with a `/` is a
    /// CIDR range when `cidr_hosts_supported`, and is refused otherwise.
    Trunk { cidr_hosts_supported: bool },
}

pub(super) fn validate(c: &AclCreation, host_check: HostCheck) -> Result<AclEntry, AclRefusal> {
    // `AclApis.handleCreateAcls`, before the binding reaches the controller.
    // The authorizer only ever asks about `kafka-cluster`, so a CLUSTER ACL
    // under any other name would be stored and never match.
    if c.resource_type == RESOURCE_TYPE_CLUSTER && c.resource_name != CLUSTER_RESOURCE_NAME {
        return Err(invalid(format!(
            "The only valid name for the CLUSTER resource is {CLUSTER_RESOURCE_NAME}"
        )));
    }
    if c.resource_name.is_empty() {
        return Err(invalid("Invalid empty resource name".to_owned()));
    }

    // `AclControlManager.validateNewAcl`: a code Kafka does not recognize is
    // refused under Kafka's name for it, `UNKNOWN`.
    let resource_type = resource_type_concrete(c.resource_type).map_err(|_| {
        invalid(format!(
            "Invalid resourceType {}",
            resource_type_name(c.resource_type)
        ))
    })?;
    let pattern_type = pattern_type_concrete(c.resource_pattern_type).map_err(|_| {
        invalid(format!(
            "Invalid patternType {}",
            pattern_type_name(c.resource_pattern_type)
        ))
    })?;
    let operation = operation_concrete(c.operation)
        .map_err(|_| invalid(format!("Invalid operation {}", operation_name(c.operation))))?;
    let permission_type = permission_concrete(c.permission_type).map_err(|_| {
        invalid(format!(
            "Invalid permissionType {}",
            permission_type_name(c.permission_type)
        ))
    })?;
    // Kafka accepts any `<type>:<name>` principal and refuses only one with
    // no colon at all.
    if !c.principal.contains(':') {
        return Err(invalid(format!(
            "Could not parse principal from `{}` (no colon is present separating the principal \
             type from the principal name)",
            c.principal
        )));
    }
    if let HostCheck::Trunk {
        cidr_hosts_supported,
    } = host_check
    {
        validate_host(&c.host, cidr_hosts_supported)?;
    }
    Ok(AclEntry {
        resource_type,
        resource_name: c.resource_name.clone(),
        pattern_type,
        principal: c.principal.clone(),
        host: c.host.clone(),
        operation,
        permission_type,
    })
}

/// Validates an ACL host the way Kafka's `AclControlManager.validateHostPattern`
/// does: the wildcard and a plain address are always accepted; a CIDR range
/// (anything containing `/`) needs `cidr_hosts_supported` (KIP-1276, gated on
/// `metadata.version` `4.4-IV1`) and must parse; an empty host is always
/// rejected. `CreateAcls` stores whatever host string is accepted as literal
/// text -- this only rejects what Kafka would also reject, it does not
/// normalize the text that gets stored.
fn validate_host(host: &str, cidr_hosts_supported: bool) -> Result<(), AclRefusal> {
    if host.is_empty() {
        return Err(invalid("Host pattern cannot be null or empty".to_owned()));
    }
    if !host.contains('/') {
        return Ok(());
    }
    if !cidr_hosts_supported {
        return Err(AclRefusal {
            code: codes::UNSUPPORTED_VERSION,
            message: CIDR_UNSUPPORTED_VERSION_MESSAGE.to_owned(),
        });
    }
    Cidr::parse(host)
        .map(|_| ())
        .map_err(|reason| invalid(format!("Invalid CIDR notation '{host}': {reason}")))
}

#[cfg(test)]
mod tests {
    use {
        super::{AclRefusal, HostCheck, has_filter_only_element, has_unknown_element},
        crate::{
            codes::{self, INVALID_REQUEST, UNSUPPORTED_VERSION},
            handlers::create_acls::test_support::{OPERATION_READ, creation, validate},
            test_support::allow_acl,
        },
        assert2::assert,
        krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType},
        krabka_protocol::owned::create_acls_request::AclCreation,
    };

    #[test]
    fn validate_accepts_any_principal_type_and_the_kafka_cluster_name() {
        /// A mutation of the baseline creation, and the entry `validate` builds from it.
        struct ShapeCase {
            shape: fn(&mut AclCreation),
            expected: AclEntry,
        }

        let cases: [ShapeCase; 4] = [
            ShapeCase {
                shape: |_| {},
                expected: AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "topic-a".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Read,
                    permission_type: PermissionType::Allow,
                },
            },
            ShapeCase {
                shape: |c| c.principal = "Group:ops".into(),
                expected: AclEntry {
                    resource_type: ResourceType::Topic,
                    resource_name: "topic-a".into(),
                    pattern_type: PatternType::Literal,
                    principal: "Group:ops".into(),
                    host: "*".into(),
                    operation: AclOperation::Read,
                    permission_type: PermissionType::Allow,
                },
            },
            ShapeCase {
                shape: |c| {
                    c.resource_type = 4;
                    c.resource_name = "kafka-cluster".into();
                    c.operation = 7;
                    c.permission_type = 2;
                },
                expected: AclEntry {
                    resource_type: ResourceType::Cluster,
                    resource_name: "kafka-cluster".into(),
                    pattern_type: PatternType::Literal,
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: AclOperation::Alter,
                    permission_type: PermissionType::Deny,
                },
            },
            // KIP-373: Kafka's `validateNewAcl` accepts the `USER` resource
            // type and the `CREATE_TOKENS` operation like any other.
            ShapeCase {
                shape: |c| {
                    c.resource_type = 7;
                    c.resource_name = "User:bob".into();
                    c.operation = 13;
                },
                expected: allow_acl()
                    .resource_type(ResourceType::User)
                    .resource_name("User:bob")
                    .operation(AclOperation::CreateTokens)
                    .call(),
            },
        ];
        for ShapeCase { shape, expected } in cases {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            shape(&mut c);
            assert!(validate(&c) == Ok(expected));
        }
    }

    /// Every per-binding refusal, with Kafka's code and message from
    /// `AclApis.handleCreateAcls` and `AclControlManager.validateNewAcl`.
    #[test]
    fn validate_refuses_with_kafka_messages() {
        /// A corruption of the baseline creation, and the message `validate` refuses it with.
        struct CorruptCase {
            corrupt: fn(&mut AclCreation),
            want: &'static str,
        }

        let cases: [CorruptCase; 9] = [
            CorruptCase {
                corrupt: |c| {
                    c.resource_type = 4;
                    c.resource_name = "my-cluster".into();
                },
                want: "The only valid name for the CLUSTER resource is kafka-cluster",
            },
            CorruptCase {
                corrupt: |c| {
                    c.resource_type = 4;
                    c.resource_name.clear();
                },
                want: "The only valid name for the CLUSTER resource is kafka-cluster",
            },
            CorruptCase {
                corrupt: |c| c.resource_name.clear(),
                want: "Invalid empty resource name",
            },
            CorruptCase {
                corrupt: |c| c.resource_type = 99,
                want: "Invalid resourceType UNKNOWN",
            },
            CorruptCase {
                corrupt: |c| c.resource_pattern_type = 9,
                want: "Invalid patternType UNKNOWN",
            },
            CorruptCase {
                corrupt: |c| c.operation = 42,
                want: "Invalid operation UNKNOWN",
            },
            CorruptCase {
                corrupt: |c| c.permission_type = 5,
                want: "Invalid permissionType UNKNOWN",
            },
            CorruptCase {
                corrupt: |c| c.principal = "alice".into(),
                want: "Could not parse principal from `alice` (no colon is present separating the \
                 principal type from the principal name)",
            },
            CorruptCase {
                corrupt: |c| c.host.clear(),
                want: "Host pattern cannot be null or empty",
            },
        ];
        for CorruptCase { corrupt, want } in cases {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            corrupt(&mut c);
            assert!(
                validate(&c)
                    == Err(AclRefusal {
                        code: codes::INVALID_REQUEST,
                        message: want.to_owned()
                    }),
                "expected {want:?}"
            );
        }
    }

    /// A resource name with a NUL byte is a valid Kafka resource name.
    #[test]
    fn validate_accepts_a_nul_in_the_resource_name() {
        let c = creation("bad\0name", "User:alice", OPERATION_READ);
        assert!(validate(&c).map(|entry| entry.resource_name) == Ok("bad\0name".to_owned()));
    }

    #[test]
    fn whole_request_element_checks_match_kafka_parse_and_binding_construction() {
        /// A mutation of the baseline creation, whether it makes an UNKNOWN element, and
        /// whether it makes a filter-only element.
        struct ElementCase {
            shape: fn(&mut AclCreation),
            unknown: bool,
            filter_only: bool,
        }

        let cases: [ElementCase; 10] = [
            ElementCase {
                shape: |_| {},
                unknown: false,
                filter_only: false,
            },
            ElementCase {
                shape: |c| c.resource_type = 0,
                unknown: true,
                filter_only: false,
            },
            ElementCase {
                shape: |c| c.resource_pattern_type = 0,
                unknown: true,
                filter_only: false,
            },
            ElementCase {
                shape: |c| c.operation = 0,
                unknown: true,
                filter_only: false,
            },
            ElementCase {
                shape: |c| c.permission_type = 0,
                unknown: true,
                filter_only: false,
            },
            ElementCase {
                shape: |c| c.resource_type = 1,
                unknown: false,
                filter_only: true,
            },
            ElementCase {
                shape: |c| c.resource_pattern_type = 1,
                unknown: false,
                filter_only: true,
            },
            ElementCase {
                shape: |c| c.resource_pattern_type = 2,
                unknown: false,
                filter_only: true,
            },
            ElementCase {
                shape: |c| c.operation = 1,
                unknown: false,
                filter_only: true,
            },
            ElementCase {
                shape: |c| c.permission_type = 1,
                unknown: false,
                filter_only: true,
            },
        ];
        for ElementCase {
            shape,
            unknown,
            filter_only,
        } in cases
        {
            let valid = creation("topic-a", "User:alice", OPERATION_READ);
            let mut c = valid.clone();
            shape(&mut c);
            let creations = [valid, c];
            assert!(
                (
                    has_unknown_element(&creations),
                    has_filter_only_element(&creations)
                ) == (unknown, filter_only)
            );
        }
    }

    /// KIP-1276 host validation, table-driven against
    /// `AclControlManager.validateHostPattern`'s four outcomes: the wildcard
    /// and a plain address always pass; a CIDR host needs
    /// `cidr_hosts_supported` and must parse; an empty host is always
    /// rejected regardless of the gate.
    #[test]
    fn validate_host_matches_kafka_cidr_gating() {
        /// The refusal expected for a host: Kafka's code and message.
        struct HostRefusal {
            code: i16,
            message: &'static str,
        }

        /// A host, whether CIDR hosts are supported, and the refusal expected, if any.
        struct HostCase {
            host: &'static str,
            cidr_hosts_supported: bool,
            expected_err: Option<HostRefusal>,
        }

        let cases: &[HostCase] = &[
            HostCase {
                host: "*",
                cidr_hosts_supported: false,
                expected_err: None,
            },
            HostCase {
                host: "*",
                cidr_hosts_supported: true,
                expected_err: None,
            },
            HostCase {
                host: "10.0.0.1",
                cidr_hosts_supported: false,
                expected_err: None,
            },
            HostCase {
                host: "10.0.0.1",
                cidr_hosts_supported: true,
                expected_err: None,
            },
            HostCase {
                host: "10.0.0.0/8",
                cidr_hosts_supported: true,
                expected_err: None,
            },
            HostCase {
                host: "10.0.0.0/8",
                cidr_hosts_supported: false,
                expected_err: Some(HostRefusal {
                    code: UNSUPPORTED_VERSION,
                    message: "CIDR-based ACL host patterns require metadata version 4.4-IV1 or higher.",
                }),
            },
            HostCase {
                host: "10.0.0.0/33",
                cidr_hosts_supported: true,
                expected_err: Some(HostRefusal {
                    code: INVALID_REQUEST,
                    message: "Invalid CIDR notation '10.0.0.0/33': Value [33] not in range [0,32]",
                }),
            },
            HostCase {
                host: "2001:db8::/32",
                cidr_hosts_supported: true,
                expected_err: None,
            },
            HostCase {
                host: "",
                cidr_hosts_supported: true,
                expected_err: Some(HostRefusal {
                    code: INVALID_REQUEST,
                    message: "Host pattern cannot be null or empty",
                }),
            },
            HostCase {
                host: "",
                cidr_hosts_supported: false,
                expected_err: Some(HostRefusal {
                    code: INVALID_REQUEST,
                    message: "Host pattern cannot be null or empty",
                }),
            },
        ];
        for HostCase {
            host,
            cidr_hosts_supported,
            expected_err,
        } in cases
        {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            c.host = (*host).into();
            let got = super::validate(&c, HostCheck::Trunk {
                cidr_hosts_supported: *cidr_hosts_supported,
            })
            .err();
            let expected = expected_err.as_ref().map(|refusal| AclRefusal {
                code: refusal.code,
                message: refusal.message.to_owned(),
            });
            assert!(
                got == expected,
                "host {host:?} cidr_hosts_supported {cidr_hosts_supported}"
            );
        }
    }

    /// Kafka 4.3.1 has no host check, so every host is stored as the text it
    /// arrived as: the wildcard, an address, a range, a malformed range and
    /// the empty string.
    #[test]
    fn validate_accepts_any_host_without_the_trunk_check() {
        for host in ["*", "10.0.0.1", "10.0.0.0/8", "10.0.0.0/33", "a/b", ""] {
            let mut c = creation("topic-a", "User:alice", OPERATION_READ);
            c.host = host.into();
            assert!(
                super::validate(&c, HostCheck::Unchecked).map(|entry| entry.host)
                    == Ok(host.to_owned()),
                "host {host:?}"
            );
        }
    }
}
