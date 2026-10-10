//! KIP-430: computes the `(cluster|topic|group)_authorized_operations`
//! bitfield.
//!
//! The `Metadata`, `DescribeCluster`, `DescribeGroups`, and
//! `ConsumerGroupDescribe` responses carry the bitfield when the matching
//! request flag is set.
//!
//! The encoding works like this. For each operation in the supported set of
//! the resource type, the module asks the authorizer. On Allow it ORs
//! `1 << op.code()` into the bitfield. `op.code()` is the same wire
//! discriminant the ACL handlers serialize. See
//! [`super::acl_wire::operation_to_wire`].
//!
//! Kafka's convention is that the field is `i32::MIN`, the "not present"
//! sentinel, when the include flag is *not* set. That is already the
//! schema-level default, so the handlers fill the field only when the request
//! opts in.
//!
//! The map from a resource to its supported-operation set follows
//! `org.apache.kafka.security.authorizer.AclEntry#supportedOperations`:
//!
//! | resource         | operations                                                        |
//! |------------------|-------------------------------------------------------------------|
//! | Topic            | Read, Write, Create, Delete, Alter, Describe, DescribeConfigs,    |
//! |                  | AlterConfigs                                                      |
//! | Group            | Read, Describe, Delete, DescribeConfigs, AlterConfigs             |
//! | Cluster          | Create, Alter, Describe, ClusterAction, AlterConfigs,             |
//! |                  | DescribeConfigs, IdempotentWrite                                  |
//! | TransactionalId  | Describe, Write, TwoPhaseCommit                                    |
//! | DelegationToken  | Describe                                                          |

use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

use super::{RequestContext, acl_wire::operation_to_wire};
use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    codes,
};

/// Returns the operations whose Allow decision adds to the
/// authorized-operations bitfield for `resource_type`. It matches Kafka's
/// `AclEntry.supportedOperations(...)`.
#[must_use]
pub fn supported_operations(resource_type: ResourceType) -> &'static [AclOperation] {
    match resource_type {
        ResourceType::Topic => &[
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ],
        // KIP-848 group configs made DescribeConfigs and AlterConfigs
        // grantable on a group.
        ResourceType::Group => &[
            AclOperation::Read,
            AclOperation::Describe,
            AclOperation::Delete,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ],
        ResourceType::Cluster => &[
            AclOperation::Create,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            AclOperation::IdempotentWrite,
        ],
        ResourceType::TransactionalId => &[
            AclOperation::Describe,
            AclOperation::Write,
            // KIP-939: 2PC participation is a grantable TransactionalId
            // permission, so it surfaces in the authorized-operations bitfield.
            AclOperation::TwoPhaseCommit,
        ],
        ResourceType::DelegationToken => &[AclOperation::Describe],
        // KIP-373.
        ResourceType::User => &[AclOperation::CreateTokens, AclOperation::DescribeTokens],
    }
}

/// Computes the authorized-operations bitfield for
/// `(resource_type, resource_name)` from the point of view of the principal
/// and peer of `ctx`. The bit for an operation is
/// `1 << operation_to_wire(op)`, which matches Kafka's
/// `AuthorizationHelper.authorizedOperations(...)`.
///
/// Kafka builds each `Action` with `logIfAllowed` and `logIfDenied` off, since
/// a probe of every supported operation is no request to do any of them, so the
/// checks go through [`Authorizer::authorize_quiet`]: a Deny is not audited or
/// counted.
#[must_use]
pub(crate) fn authorized_operations_bits(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    resource_type: ResourceType,
    resource_name: &str,
) -> i32 {
    let mut bits: i32 = 0;
    for &op in supported_operations(resource_type) {
        let allow = authorizer.authorize_quiet(
            image,
            &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type,
                resource_name,
                operation: op,
            },
        );
        if allow == AuthorizationResult::Allow {
            bits |= 1_i32 << operation_to_wire(op);
        }
    }
    bits
}

/// A row of a group describe response: `DescribeGroups`,
/// `ConsumerGroupDescribe`, `ShareGroupDescribe` or `StreamsGroupDescribe`.
pub(crate) trait DescribedGroupRow {
    /// The row that answers `group_id` with `error_code` and `error_message`
    /// alone, every other field at its generated default.
    fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> Self;
    fn group_id(&self) -> &str;
    fn error_code(&self) -> i16;
    fn set_authorized_operations(&mut self, bits: i32);
}

macro_rules! impl_described_group_row {
    ($($ty:path),* $(,)?) => {
        $(impl DescribedGroupRow for $ty {
            fn error_row(group_id: &str, error_code: i16, error_message: Option<String>) -> Self {
                Self {
                    group_id: group_id.into(),
                    error_code,
                    error_message,
                    ..Self::default()
                }
            }

            fn group_id(&self) -> &str {
                &self.group_id
            }

            fn error_code(&self) -> i16 {
                self.error_code
            }

            fn set_authorized_operations(&mut self, bits: i32) {
                self.authorized_operations = bits;
            }
        })*
    };
}

impl_described_group_row!(
    krabka_protocol::owned::consumer_group_describe_response::DescribedGroup,
    krabka_protocol::owned::describe_groups_response::DescribedGroup,
    krabka_protocol::owned::share_group_describe_response::DescribedGroup,
    krabka_protocol::owned::streams_group_describe_response::DescribedGroup,
);

/// KIP-430: when `include` is set, gives every row whose error is `NONE` the
/// bitfield of the group operations the principal of `ctx` holds. Every other
/// row keeps the wire-default `i32::MIN` "not present" sentinel.
pub(crate) fn fill_group_authorized_operations<R: DescribedGroupRow>(
    authorizer: &dyn Authorizer,
    image: &MetadataImage,
    ctx: &RequestContext<'_>,
    include: bool,
    rows: &mut [R],
) {
    if !include {
        return;
    }
    for row in rows {
        if row.error_code() == codes::NONE {
            let bits = authorized_operations_bits(
                authorizer,
                image,
                ctx,
                ResourceType::Group,
                row.group_id(),
            );
            row.set_authorized_operations(bits);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, net::SocketAddr};

    use assert2::assert;
    use krabka_metadata::{AclEntry, MetadataRecord, PermissionType, ResourceType};
    use krabka_security::Principal;
    use uuid::Uuid;

    use super::*;
    use crate::authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer};

    fn principal(name: &str) -> Principal {
        crate::test_support::sasl_principal(name)
    }

    fn ctx<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "authorized-operations-test")
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    use crate::test_support::allow_acl;

    fn acl_image(setup: crate::test_support::AllowAclSetup<'_>) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1AccessControlEntry(allow_acl(setup)));
        image
    }

    fn bit(op: AclOperation) -> i32 {
        1_i32 << operation_to_wire(op)
    }

    /// Bind the empty ACL authorizer and Alice's existing test identity before
    /// calculating one resource's bitfield, retaining their caller lifetimes.
    macro_rules! alice_acl_bits {
        (($auth:ident, $principal:ident, $host:ident, $bits:ident), $image:expr, $resource:expr, $name:expr) => {
            let $auth = SimpleAclAuthorizer::new(HashSet::new());
            let $principal = principal("alice");
            let $host = addr();
            let $bits = authorized_operations_bits(
                &$auth,
                $image,
                &ctx(&$principal, &$host),
                $resource,
                $name,
            );
        };
    }

    #[test]
    fn supported_operations_topic_matches_kafka() {
        let ops = supported_operations(ResourceType::Topic);
        // Order doesn't matter for callers but the set must match.
        let got: HashSet<_> = ops.iter().copied().collect();
        let want: HashSet<_> = [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Create,
            AclOperation::Delete,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ]
        .into_iter()
        .collect();
        assert!(got == want);
    }

    #[test]
    fn supported_operations_group_matches_kafka() {
        let got: HashSet<_> = supported_operations(ResourceType::Group)
            .iter()
            .copied()
            .collect();
        let want: HashSet<_> = [
            AclOperation::Read,
            AclOperation::Describe,
            AclOperation::Delete,
            AclOperation::DescribeConfigs,
            AclOperation::AlterConfigs,
        ]
        .into_iter()
        .collect();
        assert!(got == want);
    }

    #[test]
    fn supported_operations_cluster_matches_kafka() {
        let got: HashSet<_> = supported_operations(ResourceType::Cluster)
            .iter()
            .copied()
            .collect();
        let want: HashSet<_> = [
            AclOperation::Create,
            AclOperation::Alter,
            AclOperation::Describe,
            AclOperation::ClusterAction,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            AclOperation::IdempotentWrite,
        ]
        .into_iter()
        .collect();
        assert!(got == want);
    }

    #[test]
    fn allow_all_authorizer_sets_every_supported_bit_for_each_resource() {
        let auth = AllowAllAuthorizer;
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("anyone");
        let h = addr();

        for rt in [
            ResourceType::Topic,
            ResourceType::Group,
            ResourceType::Cluster,
            ResourceType::TransactionalId,
            ResourceType::DelegationToken,
        ] {
            let bits = authorized_operations_bits(&auth, &img, &ctx(&p, &h), rt, "name");
            let expected = supported_operations(rt)
                .iter()
                .copied()
                .fold(0_i32, |acc, op| acc | bit(op));
            assert!(bits == expected, "{rt:?}: full mask under AllowAll");
        }
    }

    #[test]
    fn simple_acl_with_no_acls_yields_zero() {
        let mut supers = HashSet::new();
        supers.insert("ignored".to_string());
        let auth = SimpleAclAuthorizer::new(supers);
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("alice");
        let h = addr();
        // alice is not a super-user and the image has no ACLs → every
        // supported op denies → bitfield is 0.
        let bits =
            authorized_operations_bits(&auth, &img, &ctx(&p, &h), ResourceType::Topic, "foo");
        assert!(bits == 0);
    }

    #[test]
    fn super_user_gets_full_mask_per_resource() {
        let mut supers = HashSet::new();
        supers.insert("admin".to_string());
        let auth = SimpleAclAuthorizer::new(supers);
        let img = MetadataImage::new(Uuid::nil());
        let p = principal("admin");
        let h = addr();

        let topic_bits =
            authorized_operations_bits(&auth, &img, &ctx(&p, &h), ResourceType::Topic, "foo");
        let topic_want = supported_operations(ResourceType::Topic)
            .iter()
            .copied()
            .fold(0_i32, |acc, op| acc | bit(op));
        assert!(topic_bits == topic_want);

        let group_bits =
            authorized_operations_bits(&auth, &img, &ctx(&p, &h), ResourceType::Group, "g");
        let group_want = supported_operations(ResourceType::Group)
            .iter()
            .copied()
            .fold(0_i32, |acc, op| acc | bit(op));
        assert!(group_bits == group_want);
    }

    #[test]
    fn read_allow_on_topic_sets_read_and_describe_bits_only() {
        let img = acl_image(crate::test_support::AllowAclSetup {
            resource_name: "foo",
            ..Default::default()
        });
        alice_acl_bits!((auth, p, h, bits), &img, ResourceType::Topic, "foo");
        // Read ACL grants Read directly and Describe via implication.
        // No other supported op should be set.
        let expected = bit(AclOperation::Read) | bit(AclOperation::Describe);
        assert!(bits == expected);
    }

    #[test]
    fn write_allow_on_topic_sets_write_and_describe_only() {
        let img = acl_image(crate::test_support::AllowAclSetup {
            resource_name: "foo",

            operation: AclOperation::Write,
            ..Default::default()
        });
        alice_acl_bits!((auth, p, h, bits), &img, ResourceType::Topic, "foo");
        let expected = bit(AclOperation::Write) | bit(AclOperation::Describe);
        assert!(bits == expected);
    }

    /// The group bitfield per granted ACL, as Kafka's `AclEntry` supported
    /// set and its implication table give it: `Read` and `Delete` imply
    /// `Describe`, `AlterConfigs` implies `DescribeConfigs`, and `All`
    /// grants all five group operations.
    #[test]
    fn group_bits_follow_the_granted_acl() {
        let rows = [
            (
                AclOperation::Read,
                bit(AclOperation::Read) | bit(AclOperation::Describe),
            ),
            (
                AclOperation::Delete,
                bit(AclOperation::Delete) | bit(AclOperation::Describe),
            ),
            (
                AclOperation::DescribeConfigs,
                bit(AclOperation::DescribeConfigs),
            ),
            (
                AclOperation::AlterConfigs,
                bit(AclOperation::DescribeConfigs) | bit(AclOperation::AlterConfigs),
            ),
            (
                AclOperation::All,
                bit(AclOperation::Read)
                    | bit(AclOperation::Describe)
                    | bit(AclOperation::Delete)
                    | bit(AclOperation::DescribeConfigs)
                    | bit(AclOperation::AlterConfigs),
            ),
        ];
        for (granted, expected) in rows {
            let img = acl_image(crate::test_support::AllowAclSetup {
                resource_type: ResourceType::Group,
                resource_name: "cg",

                operation: granted,
                ..Default::default()
            });
            alice_acl_bits!((auth, p, h, bits), &img, ResourceType::Group, "cg");
            assert!(bits == expected, "{granted:?}");
        }
    }

    #[test]
    fn deny_wins_the_exact_operation_but_not_its_implied_describe() {
        let mut img = acl_image(crate::test_support::AllowAclSetup {
            resource_name: "foo",
            ..Default::default()
        });
        img.apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
            permission_type: PermissionType::Deny,
            ..allow_acl(crate::test_support::AllowAclSetup {
                resource_name: "foo",
                ..Default::default()
            })
        }));
        alice_acl_bits!((auth, p, h, bits), &img, ResourceType::Topic, "foo");
        // Read itself is denied: both ACL rows match the exact Read request,
        // and DENY wins precedence. But Kafka's operation-implication table
        // only ever widens what an ALLOW ACL matches -- a DENY Read ACL does
        // not also deny the implied Describe. Only the ALLOW row matches a
        // Describe request (via the Read -> Describe arrow); the DENY row
        // does not apply to it at all. So Describe is allowed.
        assert!(bits == bit(AclOperation::Describe));
    }

    /// The group bitfield goes only on an opted-in row whose error is `NONE`;
    /// every other row keeps the `i32::MIN` "not present" sentinel.
    #[test]
    fn fill_group_authorized_operations_fills_only_clean_opted_in_rows() {
        use krabka_protocol::owned::consumer_group_describe_response::DescribedGroup;

        let img = MetadataImage::new(Uuid::nil());
        let p = principal("anyone");
        let h = addr();
        let all_group_bits = supported_operations(ResourceType::Group)
            .iter()
            .fold(0_i32, |acc, &op| acc | bit(op));
        let rows = || {
            vec![
                DescribedGroup::error_row("clean", codes::NONE, None),
                DescribedGroup::error_row("missing", codes::GROUP_ID_NOT_FOUND, None),
            ]
        };
        for (include, clean_bits) in [(true, all_group_bits), (false, i32::MIN)] {
            let mut got = rows();
            fill_group_authorized_operations(
                &AllowAllAuthorizer,
                &img,
                &ctx(&p, &h),
                include,
                &mut got,
            );
            let want = vec![
                DescribedGroup {
                    group_id: "clean".into(),
                    authorized_operations: clean_bits,
                    ..DescribedGroup::default()
                },
                DescribedGroup {
                    group_id: "missing".into(),
                    error_code: codes::GROUP_ID_NOT_FOUND,
                    ..DescribedGroup::default()
                },
            ];
            assert!(got == want, "include = {include}");
        }
    }

    #[test]
    fn bit_values_match_kafka_int8_codes() {
        // Sanity: spot-check that the bit positions equal Kafka's wire
        // discriminants. If `operation_to_wire` ever drifts from
        // Kafka's `AclOperation.code()`, the wire field would become
        // unintelligible to JVM clients.
        for (op, want) in [
            (AclOperation::Read, 1 << 3),
            (AclOperation::Write, 1 << 4),
            (AclOperation::Create, 1 << 5),
            (AclOperation::Delete, 1 << 6),
            (AclOperation::Alter, 1 << 7),
            (AclOperation::Describe, 1 << 8),
            (AclOperation::ClusterAction, 1 << 9),
            (AclOperation::DescribeConfigs, 1 << 10),
            (AclOperation::AlterConfigs, 1 << 11),
            (AclOperation::IdempotentWrite, 1 << 12),
        ] {
            assert!(bit(op) == want, "{op:?}");
        }
    }
}
