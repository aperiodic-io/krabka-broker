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

use {
    super::{RequestContext, acl_wire::operation_to_wire},
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        codes,
    },
    krabka_metadata::{AclOperation, MetadataImage, ResourceType},
    krabka_protocol::owned::{
        consumer_group_describe_response, describe_groups_response, share_group_describe_response,
        streams_group_describe_response,
    },
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
        let allow = authorizer.authorize_quiet(image, &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type,
            resource_name,
            operation: op,
        });
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

krabka_macros::described_group_rows! {
    consumer_group_describe_response::DescribedGroup,
    describe_groups_response::DescribedGroup,
    share_group_describe_response::DescribedGroup,
    streams_group_describe_response::DescribedGroup,
}

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
    use {
        super::*,
        crate::authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
        assert2::assert,
        krabka_metadata::{AclEntry, MetadataRecord, PermissionType, ResourceType},
        krabka_security::Principal,
        std::{collections::HashSet, net::SocketAddr},
        uuid::Uuid,
    };

    fn principal(name: &str) -> Principal { sasl_principal(name) }

    fn ctx<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
        request_context(principal, peer, "authorized-operations-test")
    }

    fn addr() -> SocketAddr { "127.0.0.1:0".parse().unwrap() }

    use crate::test_support::{allow_acl, request_context, sasl_principal};

    #[bon::builder]
    fn acl_image(
        #[builder(default = ResourceType::Topic)] resource_type: ResourceType,
        #[builder(default = "orders")] resource_name: &'_ str,
        #[builder(default = "User:alice")] principal: &'_ str,
        #[builder(default = AclOperation::Read)] operation: AclOperation,
    ) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1AccessControlEntry(
            allow_acl()
                .resource_type(resource_type)
                .resource_name(resource_name)
                .principal(principal)
                .operation(operation)
                .call(),
        ));
        image
    }

    fn bit(op: AclOperation) -> i32 { 1_i32 << operation_to_wire(op) }

    fn alice_acl_bits(image: &MetadataImage, resource: ResourceType, name: &str) -> i32 {
        let auth = SimpleAclAuthorizer::new(HashSet::new());
        let principal = principal("alice");
        let host = addr();
        authorized_operations_bits(&auth, image, &ctx(&principal, &host), resource, name)
    }

    #[test]
    fn every_describe_schema_preserves_error_fields_and_default_operation_bits() {
        #[derive(Debug, PartialEq)]
        struct RowFields<'a> {
            error_message: Option<&'a str>,
            authorized_operations: i32,
        }
        fn check_row<Row: DescribedGroupRow>(fields: impl for<'a> Fn(&'a Row) -> RowFields<'a>) {
            let mut row = Row::error_row("group", -1, Some("refusal".into()));
            assert!(row.group_id() == "group");
            assert!(row.error_code() == -1);
            assert!(
                fields(&row)
                    == RowFields {
                        error_message: Some("refusal"),
                        authorized_operations: i32::MIN
                    }
            );
            row.set_authorized_operations(1 << 8);
            assert!(
                fields(&row)
                    == RowFields {
                        error_message: Some("refusal"),
                        authorized_operations: 1 << 8
                    }
            );
            assert!(row.group_id() == "group");
            assert!(row.error_code() == -1);
        }

        check_row::<consumer_group_describe_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
        check_row::<describe_groups_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
        check_row::<share_group_describe_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
        check_row::<streams_group_describe_response::DescribedGroup>(|row| RowFields {
            error_message: row.error_message.as_deref(),
            authorized_operations: row.authorized_operations,
        });
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
        let img = acl_image().resource_name("foo").call();
        let bits = alice_acl_bits(&img, ResourceType::Topic, "foo");
        // Read ACL grants Read directly and Describe via implication.
        // No other supported op should be set.
        let expected = bit(AclOperation::Read) | bit(AclOperation::Describe);
        assert!(bits == expected);
    }

    #[test]
    fn write_allow_on_topic_sets_write_and_describe_only() {
        let img = acl_image()
            .resource_name("foo")
            .operation(AclOperation::Write)
            .call();
        let bits = alice_acl_bits(&img, ResourceType::Topic, "foo");
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
            let img = acl_image()
                .resource_type(ResourceType::Group)
                .resource_name("cg")
                .operation(granted)
                .call();
            let bits = alice_acl_bits(&img, ResourceType::Group, "cg");
            assert!(bits == expected, "{granted:?}");
        }
    }

    #[test]
    fn deny_wins_the_exact_operation_but_not_its_implied_describe() {
        let mut img = acl_image().resource_name("foo").call();
        img.apply(&MetadataRecord::V1AccessControlEntry(AclEntry {
            permission_type: PermissionType::Deny,
            ..allow_acl().resource_name("foo").call()
        }));
        let bits = alice_acl_bits(&img, ResourceType::Topic, "foo");
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

    #[yare::parameterized(
        read = { AclOperation::Read, 1 << 3 },
        write = { AclOperation::Write, 1 << 4 },
        create = { AclOperation::Create, 1 << 5 },
        delete = { AclOperation::Delete, 1 << 6 },
        alter = { AclOperation::Alter, 1 << 7 },
        describe = { AclOperation::Describe, 1 << 8 },
        cluster_action = { AclOperation::ClusterAction, 1 << 9 },
        describe_configs = { AclOperation::DescribeConfigs, 1 << 10 },
        alter_configs = { AclOperation::AlterConfigs, 1 << 11 },
        idempotent_write = { AclOperation::IdempotentWrite, 1 << 12 },
    )]
    fn bit_values_match_kafka_int8_codes(op: AclOperation, want: i32) {
        // Sanity: spot-check that the bit positions equal Kafka's wire
        // discriminants. If `operation_to_wire` ever drifts from
        // Kafka's `AclOperation.code()`, the wire field would become
        // unintelligible to JVM clients.
        assert2::assert!(bit(op) == want, "{op:?}");
    }
}
