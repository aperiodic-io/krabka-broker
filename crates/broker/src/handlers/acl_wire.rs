//! Wire ↔ metadata enum mapping for ACL handlers.
//!
//! Kafka serializes ACL enums as `i8` discriminants. This module gives the
//! conversions and a small error type. The error type covers an unknown
//! discriminant and an ANY value where a concrete value is necessary.
//! [`binding_filter`] decodes and matches the filters of `DescribeAcls` and
//! `DeleteAcls`.

use krabka_metadata::{AclOperation, PatternType, PermissionType, ResourceType};

pub mod binding_filter;

/// Topic patterns shared by the filter and handler matching fixtures.
#[cfg(test)]
pub(crate) const PATTERN_MATCHING_FIXTURE: [(&str, PatternType); 6] = [
    ("foo", PatternType::Literal),
    ("*", PatternType::Literal),
    ("f", PatternType::Prefixed),
    ("fo", PatternType::Prefixed),
    ("bar", PatternType::Prefixed),
    ("food", PatternType::Literal),
];

/// Kafka's singleton cluster resource name (`Resource.CLUSTER_NAME`).
/// Every cluster-scoped ACL and authorization check targets this name.
pub const CLUSTER_RESOURCE_NAME: &str = "kafka-cluster";

/// What Kafka's `AclApis.handleDescribeAcls` writes into the error message of
/// the `DescribeAcls` response when the cluster runs no authorizer.
pub const NO_AUTHORIZER_MESSAGE: &str = "No Authorizer is configured on the broker";

/// The message of the `SecurityDisabledException` that Kafka's
/// `AclApis.handleCreateAcls` and `handleDeleteAcls` build their responses
/// from when the cluster runs no authorizer. It is not the `DescribeAcls`
/// text: those two go through `getErrorResponse`, which keeps the exception
/// message.
pub const NO_AUTHORIZER_EXCEPTION_MESSAGE: &str = "No Authorizer is configured.";

/// Kafka's `QuorumController.MAX_RECORDS_PER_USER_OP`: the most ACL records a
/// single `CreateAcls` or `DeleteAcls` request may write. `AclControlManager`
/// bounds both requests by it.
pub const MAX_ACL_RECORDS_PER_REQUEST: usize = 10_000;

/// Wire `i8` discriminant of an ACL `resource_type` field.
pub type ResourceTypeCode = i8;
/// Wire `i8` discriminant of an ACL `pattern_type` field.
pub type PatternTypeCode = i8;
/// Wire `i8` discriminant of an ACL `operation` field.
pub type OperationCode = i8;
/// Wire `i8` discriminant of an ACL `permission_type` field.
pub type PermissionTypeCode = i8;

/// Wire byte for `UNKNOWN` (0). Every ACL enum axis shares it.
const WIRE_UNKNOWN: i8 = 0;
/// Wire byte for `ANY` (1), a filter wildcard and never a concrete value.
/// Every ACL enum axis shares it.
const WIRE_ANY: i8 = 1;
/// Wire byte for `PatternType::MATCH` (2, KIP-290), a filter-only
/// wildcard that matches both literal and prefixed patterns.
const WIRE_PATTERN_MATCH: i8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireAclError {
    UnknownDiscriminant,
    /// A caller passed ANY or MATCH where a concrete value is necessary.
    AnyRequiresFilter,
}

/// Parse a wire `resource_type` byte into a concrete `ResourceType`.
/// `CreateAcls` uses this function, because its resource must be concrete.
pub fn resource_type_concrete(b: ResourceTypeCode) -> Result<ResourceType, WireAclError> {
    match b {
        2 => Ok(ResourceType::Topic),
        3 => Ok(ResourceType::Group),
        4 => Ok(ResourceType::Cluster),
        5 => Ok(ResourceType::TransactionalId),
        6 => Ok(ResourceType::DelegationToken),
        7 => Ok(ResourceType::User),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn resource_type_to_wire(rt: ResourceType) -> ResourceTypeCode {
    match rt {
        ResourceType::Topic => 2,
        ResourceType::Group => 3,
        ResourceType::Cluster => 4,
        ResourceType::TransactionalId => 5,
        ResourceType::DelegationToken => 6,
        ResourceType::User => 7,
    }
}

/// Parse a wire `pattern_type` byte into a concrete `PatternType`.
/// `CreateAcls` uses this function, because its pattern must be concrete.
pub fn pattern_type_concrete(b: PatternTypeCode) -> Result<PatternType, WireAclError> {
    match b {
        3 => Ok(PatternType::Literal),
        4 => Ok(PatternType::Prefixed),
        WIRE_UNKNOWN..=WIRE_PATTERN_MATCH => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn pattern_type_to_wire(pt: PatternType) -> PatternTypeCode {
    match pt {
        PatternType::Literal => 3,
        PatternType::Prefixed => 4,
    }
}

/// Parse a wire `operation` byte into a concrete `AclOperation`.
/// `CreateAcls` uses this function, because its operation must be concrete.
pub fn operation_concrete(b: OperationCode) -> Result<AclOperation, WireAclError> {
    match b {
        2 => Ok(AclOperation::All),
        3 => Ok(AclOperation::Read),
        4 => Ok(AclOperation::Write),
        5 => Ok(AclOperation::Create),
        6 => Ok(AclOperation::Delete),
        7 => Ok(AclOperation::Alter),
        8 => Ok(AclOperation::Describe),
        9 => Ok(AclOperation::ClusterAction),
        10 => Ok(AclOperation::DescribeConfigs),
        11 => Ok(AclOperation::AlterConfigs),
        12 => Ok(AclOperation::IdempotentWrite),
        13 => Ok(AclOperation::CreateTokens),
        14 => Ok(AclOperation::DescribeTokens),
        15 => Ok(AclOperation::TwoPhaseCommit),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn operation_to_wire(op: AclOperation) -> OperationCode {
    match op {
        AclOperation::All => 2,
        AclOperation::Read => 3,
        AclOperation::Write => 4,
        AclOperation::Create => 5,
        AclOperation::Delete => 6,
        AclOperation::Alter => 7,
        AclOperation::Describe => 8,
        AclOperation::ClusterAction => 9,
        AclOperation::DescribeConfigs => 10,
        AclOperation::AlterConfigs => 11,
        AclOperation::IdempotentWrite => 12,
        AclOperation::CreateTokens => 13,
        AclOperation::DescribeTokens => 14,
        AclOperation::TwoPhaseCommit => 15,
    }
}

/// Parse a wire `permission_type` byte into a concrete `PermissionType`.
/// `CreateAcls` uses this function, because its permission must be concrete.
pub fn permission_concrete(b: PermissionTypeCode) -> Result<PermissionType, WireAclError> {
    match b {
        2 => Ok(PermissionType::Deny),
        3 => Ok(PermissionType::Allow),
        WIRE_UNKNOWN | WIRE_ANY => Err(WireAclError::AnyRequiresFilter),
        _ => Err(WireAclError::UnknownDiscriminant),
    }
}

#[must_use]
pub fn permission_to_wire(pt: PermissionType) -> PermissionTypeCode {
    match pt {
        PermissionType::Deny => 2,
        PermissionType::Allow => 3,
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn resource_type_concrete_rejects_any() {
        let cases = [
            (1, Err(WireAclError::AnyRequiresFilter)),
            (2, Ok(ResourceType::Topic)),
            (3, Ok(ResourceType::Group)),
            (4, Ok(ResourceType::Cluster)),
        ];
        for (byte, want) in cases {
            assert!(resource_type_concrete(byte) == want, "byte {byte}");
        }
    }

    #[test]
    fn concrete_parsers_distinguish_wildcards_from_unknown_discriminants() {
        // The wildcard bytes (UNKNOWN/ANY, and MATCH for pattern types) must
        // report AnyRequiresFilter — a different wire error than a junk
        // discriminant — and the concrete codes must map, not fall through.
        let pattern_cases = [
            (3, Ok(PatternType::Literal)),
            (4, Ok(PatternType::Prefixed)),
            (WIRE_UNKNOWN, Err(WireAclError::AnyRequiresFilter)),
            (WIRE_ANY, Err(WireAclError::AnyRequiresFilter)),
            (WIRE_PATTERN_MATCH, Err(WireAclError::AnyRequiresFilter)),
            (5, Err(WireAclError::UnknownDiscriminant)),
        ];
        for (byte, want) in pattern_cases {
            check!(pattern_type_concrete(byte) == want, "pattern byte {byte}");
        }

        for byte in [WIRE_UNKNOWN, WIRE_ANY] {
            check!(
                operation_concrete(byte) == Err(WireAclError::AnyRequiresFilter),
                "operation byte {byte}"
            );
            check!(
                permission_concrete(byte) == Err(WireAclError::AnyRequiresFilter),
                "permission byte {byte}"
            );
        }
    }

    #[test]
    fn operation_round_trip_through_wire() {
        for op in [
            AclOperation::All,
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::IdempotentWrite,
            // KIP-939: TWO_PHASE_COMMIT, wire byte 15.
            AclOperation::TwoPhaseCommit,
        ] {
            let b = operation_to_wire(op);
            assert!(operation_concrete(b).unwrap() == op);
        }
    }

    /// KIP-939: the `TWO_PHASE_COMMIT` operation is wire byte 15 and must
    /// round-trip through the concrete codec and the encoder, so that
    /// `CreateAcls` and `DescribeAcls` can carry the 2PC grant on a
    /// `TransactionalId`.
    #[test]
    fn two_phase_commit_operation_is_byte_15() {
        check!(operation_to_wire(AclOperation::TwoPhaseCommit) == 15);
        check!(operation_concrete(15) == Ok(AclOperation::TwoPhaseCommit));
    }

    /// KIP-373's `USER` resource type (7) and its `CREATE_TOKENS` (13) and
    /// `DESCRIBE_TOKENS` (14) operations round-trip through the wire codec,
    /// with Kafka's `ResourceType` and `AclOperation` codes.
    #[test]
    fn kip_373_user_resource_and_token_operations_round_trip() {
        check!(resource_type_concrete(7) == Ok(ResourceType::User));
        check!(resource_type_to_wire(ResourceType::User) == 7);
        for (code, operation) in [
            (13, AclOperation::CreateTokens),
            (14, AclOperation::DescribeTokens),
        ] {
            check!(operation_concrete(code) == Ok(operation));
            check!(operation_to_wire(operation) == code);
        }
    }

    /// The KIP-48 `TOKEN` resource type, also named `DELEGATION_TOKEN`, is
    /// wire byte 6. It must round-trip through the wire codec, so that
    /// `CreateAcls`, `DeleteAcls`, and `DescribeAcls` can carry ACLs that
    /// guard delegation tokens.
    #[test]
    fn delegation_token_resource_type_now_accepted() {
        use krabka_metadata::AclEntry;

        // Concrete (CreateAcls) codec.
        check!(resource_type_concrete(6) == Ok(ResourceType::DelegationToken));
        // The filter codec is `binding_filter`, tested beside it.
        // Encoder.
        check!(resource_type_to_wire(ResourceType::DelegationToken) == 6);

        // Build a concrete AclEntry at the canonical (Describe, Allow)
        // shape KIP-48 token ACLs use; verify the wire bytes line up.
        let entry = AclEntry {
            resource_type: resource_type_concrete(6).unwrap(),
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: operation_concrete(8).unwrap(),
            permission_type: permission_concrete(3).unwrap(),
        };
        assert!(resource_type_to_wire(entry.resource_type) == 6);
        let expected = AclEntry {
            resource_type: ResourceType::DelegationToken,
            resource_name: "User:alice".into(),
            pattern_type: PatternType::Literal,
            principal: "User:bob".into(),
            host: "*".into(),
            operation: AclOperation::Describe,
            permission_type: PermissionType::Allow,
        };
        assert!(entry == expected);
    }
}
