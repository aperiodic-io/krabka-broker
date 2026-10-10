//! Predicates that decide whether one stored ACL entry applies to an
//! authorization request.
//!
//! An entry applies only when its principal, its host, and its operation all
//! match the request. The principal and host tests are equality-or-wildcard
//! checks. The operation test additionally carries Kafka's one-way
//! operation-implication table, which is why it lives beside them rather than
//! in the decision loop.

use std::net::IpAddr;

use krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType};
use krabka_verified::{
    AclOperationKind, AclPatternKind, acl_identity_match, acl_operation_match, acl_resource_match,
    authz::{AclResourceFacts, AclResourceTypeMatch},
};

use crate::cidr::Cidr;

pub(super) fn matches_principal(entry: &AclEntry, user_pattern: &str) -> bool {
    acl_identity_match(entry.principal == "User:*", entry.principal == user_pattern)
}

/// True when `entry`'s host applies to a request from `host` (the JDK
/// `getHostAddress()` text of the peer, as `SimpleAclAuthorizer::authorize`
/// derives it) whose original address is `peer_ip`.
///
/// A stored host is the wildcard, a literal address compared as text, or --
/// KIP-1276, when `cidr_hosts_supported` -- a CIDR range compared against
/// `peer_ip` numerically. An entry host that fails to parse as a CIDR (no `/`,
/// or `/` in ordinary text that is not one) falls back to the literal
/// comparison, exactly as `CreateAcls`, `DescribeAcls` and `DeleteAcls`
/// filters treat it. Where CIDR hosts are not supported, as in Kafka 4.3.1,
/// a host containing `/` is only ever compared as text.
pub(super) fn matches_host(
    entry: &AclEntry,
    host: &str,
    peer_ip: IpAddr,
    cidr_hosts_supported: bool,
) -> bool {
    if acl_identity_match(entry.host == "*", entry.host == host) {
        return true;
    }
    cidr_hosts_supported
        && entry.host.contains('/')
        && Cidr::parse(&entry.host).is_ok_and(|cidr| cidr.contains(peer_ip))
}

pub(super) fn matches_resource(entry: &AclEntry, resource_type: ResourceType, name: &str) -> bool {
    let pattern = match entry.pattern_type {
        PatternType::Literal => AclPatternKind::Literal,
        PatternType::Prefixed => AclPatternKind::Prefixed,
    };
    acl_resource_match(
        pattern,
        AclResourceFacts {
            resource_type: if entry.resource_type == resource_type {
                AclResourceTypeMatch::Same
            } else {
                AclResourceTypeMatch::Different
            },
            exact_name: entry.resource_name == name,
            wildcard_name: entry.resource_name == "*",
            name_has_prefix: name.starts_with(entry.resource_name.as_str()),
        },
    )
}

/// Returns true when an ACL with the `stored` operation and `permission`
/// grants access for an authorization request with the `requested` operation.
///
/// Beyond an exact match and the `All` wildcard, this function applies Kafka's
/// operation-implication table, which only widens what an ALLOW ACL matches:
///
/// | stored          | implies                |
/// |-----------------|------------------------|
/// | Read            | Describe               |
/// | Write           | Describe               |
/// | Delete          | Describe               |
/// | Alter           | Describe               |
/// | `AlterConfigs`  | `DescribeConfigs`      |
/// | All             | Everything             |
///
/// The table is one-way: Describe does NOT imply Read, and so on. A DENY ACL
/// never gains the implied operations -- a DENY `Read` ACL does not also deny
/// `Describe`.
pub(super) fn matches_operation(
    stored: AclOperation,
    requested: AclOperation,
    permission: PermissionType,
) -> bool {
    acl_operation_match(
        operation_kind(stored),
        operation_kind(requested),
        permission == PermissionType::Allow,
    )
}

fn operation_kind(operation: AclOperation) -> AclOperationKind {
    match operation {
        AclOperation::All => AclOperationKind::All,
        AclOperation::Read => AclOperationKind::Read,
        AclOperation::Write => AclOperationKind::Write,
        AclOperation::Create => AclOperationKind::Create,
        AclOperation::Delete => AclOperationKind::Delete,
        AclOperation::Alter => AclOperationKind::Alter,
        AclOperation::Describe => AclOperationKind::Describe,
        AclOperation::ClusterAction => AclOperationKind::ClusterAction,
        AclOperation::DescribeConfigs => AclOperationKind::DescribeConfigs,
        AclOperation::AlterConfigs => AclOperationKind::AlterConfigs,
        AclOperation::IdempotentWrite => AclOperationKind::IdempotentWrite,
        AclOperation::TwoPhaseCommit => AclOperationKind::TwoPhaseCommit,
        AclOperation::CreateTokens => AclOperationKind::CreateTokens,
        AclOperation::DescribeTokens => AclOperationKind::DescribeTokens,
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use krabka_metadata::{
        AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    };

    use super::{matches_operation, matches_resource};
    use crate::{
        AuthorizationResult, Authorizer, SimpleAclAuthorizer,
        simple::test_support::{
            AclSetup, AliceAuthorizer, QuerySetup, acl_image, alice, check_topic_access, cidr_img,
            image_with_acls, img, no_super, req, topic_acl,
        },
    };

    #[test]
    fn operation_implications_are_exhaustive_and_one_way() {
        use AclOperation::{
            All, Alter, AlterConfigs, ClusterAction, Create, CreateTokens, Delete, Describe,
            DescribeConfigs, DescribeTokens, IdempotentWrite, Read, TwoPhaseCommit, Write,
        };
        // KIP-373's two token operations imply nothing and are implied only by
        // `All`, like every other operation outside the arrows below.
        let operations = [
            CreateTokens,
            DescribeTokens,
            All,
            Read,
            Write,
            Create,
            Delete,
            Alter,
            Describe,
            ClusterAction,
            DescribeConfigs,
            AlterConfigs,
            IdempotentWrite,
            TwoPhaseCommit,
        ];
        let arrows = [
            (Read, Describe),
            (Write, Describe),
            (Delete, Describe),
            (Alter, Describe),
            (AlterConfigs, DescribeConfigs),
        ];
        for stored in operations {
            for requested in operations {
                for permission in [PermissionType::Allow, PermissionType::Deny] {
                    let expected = stored == requested
                        || stored == All
                        || (permission == PermissionType::Allow
                            && arrows.contains(&(stored, requested)));
                    assert2::assert!(matches_operation(stored, requested, permission) == expected);
                }
            }
        }
    }

    /// #649: the operation-implication table (e.g. Read implies Describe)
    /// must apply only to ALLOW ACLs. A DENY Read ACL must not also deny
    /// Describe.
    #[test]
    fn implication_table_applies_only_to_allow_acls() {
        use AclOperation::{Alter, AlterConfigs, Delete, Describe, DescribeConfigs, Read, Write};

        let cases = [
            (PermissionType::Allow, Read, Describe, true),
            (PermissionType::Allow, Write, Describe, true),
            (PermissionType::Allow, Delete, Describe, true),
            (PermissionType::Allow, Alter, Describe, true),
            (PermissionType::Allow, AlterConfigs, DescribeConfigs, true),
            (PermissionType::Deny, Read, Describe, false),
            (PermissionType::Deny, Write, Describe, false),
            (PermissionType::Deny, Delete, Describe, false),
            (PermissionType::Deny, Alter, Describe, false),
            (PermissionType::Deny, AlterConfigs, DescribeConfigs, false),
        ];
        for (permission, stored, requested, expected) in cases {
            assert2::assert!(
                matches_operation(stored, requested, permission) == expected,
                "permission={permission:?} stored={stored:?} requested={requested:?}"
            );
        }
    }

    #[test]
    fn principal_wildcard_matches_any_user() {
        let img = acl_image([topic_acl(AclSetup {
            principal: "User:*",
            ..Default::default()
        })]);
        check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Allow);
    }

    #[test]
    fn host_filter_matches_specific_ip() {
        let img = acl_image([topic_acl(AclSetup {
            host: "127.0.0.1",
            ..Default::default()
        })]);
        let a = alice();
        let h_match: SocketAddr = "127.0.0.1:5000".parse().unwrap();
        let h_nomatch: SocketAddr = "127.0.0.2:5000".parse().unwrap();
        let auth = SimpleAclAuthorizer::new(no_super());
        for (_name, host, expected) in [
            ("host match", &h_match, AuthorizationResult::Allow),
            ("host mismatch", &h_nomatch, AuthorizationResult::Deny),
        ] {
            assert2::assert!(
                auth.authorize(&img, &req(&a, host, QuerySetup::default())) == expected
            );
        }
    }

    #[test]
    fn operation_all_matches_any_op() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::All,
            ..Default::default()
        })]);
        let caller = AliceAuthorizer::default();
        for op in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Describe,
            AclOperation::Delete,
        ] {
            assert2::assert!(caller.authorize(&img, "foo", op) == AuthorizationResult::Allow);
        }
    }

    #[test]
    fn operation_specific_does_not_match_others() {
        let img = acl_image([topic_acl(AclSetup::default())]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(
            [AclOperation::Read, AclOperation::Write]
                .map(|operation| { caller.authorize(&img, "foo", operation) })
                == [AuthorizationResult::Allow, AuthorizationResult::Deny]
        );
    }

    #[test]
    fn read_implies_describe_on_topic() {
        let img = acl_image([topic_acl(AclSetup::default())]);
        check_topic_access(
            &img,
            "foo",
            AclOperation::Describe,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn write_implies_describe_on_topic() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::Write,
            ..Default::default()
        })]);
        check_topic_access(
            &img,
            "foo",
            AclOperation::Describe,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn delete_implies_describe() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::Delete,
            ..Default::default()
        })]);
        check_topic_access(
            &img,
            "foo",
            AclOperation::Describe,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn alter_implies_describe() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::Alter,
            ..Default::default()
        })]);
        check_topic_access(
            &img,
            "foo",
            AclOperation::Describe,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn alter_configs_implies_describe_configs() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::AlterConfigs,
            ..Default::default()
        })]);
        check_topic_access(
            &img,
            "foo",
            AclOperation::DescribeConfigs,
            AuthorizationResult::Allow,
        );
    }

    #[test]
    fn describe_does_not_imply_read() {
        let img = acl_image([topic_acl(AclSetup {
            op: AclOperation::Describe,
            ..Default::default()
        })]);
        check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Deny);
    }

    #[test]
    fn implication_works_on_group_resource() {
        let img = acl_image([topic_acl(AclSetup {
            rt: ResourceType::Group,
            name: "cg-1",
            ..Default::default()
        })]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(
            caller.authorize_on(&img, ResourceType::Group, "cg-1", AclOperation::Describe)
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_cluster_resource() {
        let img = acl_image([topic_acl(AclSetup {
            rt: ResourceType::Cluster,
            op: AclOperation::Alter,
            name: "kafka-cluster",
            ..Default::default()
        })]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(
            caller.authorize_on(
                &img,
                ResourceType::Cluster,
                "kafka-cluster",
                AclOperation::Describe
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_transactional_id_resource() {
        let img = acl_image([topic_acl(AclSetup {
            rt: ResourceType::TransactionalId,
            op: AclOperation::Write,
            name: "tx-1",
            ..Default::default()
        })]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(
            caller.authorize_on(
                &img,
                ResourceType::TransactionalId,
                "tx-1",
                AclOperation::Describe
            ) == AuthorizationResult::Allow
        );
    }

    #[test]
    fn matches_resource_filters_by_type_name_and_pattern() {
        let entry = topic_acl(AclSetup {
            name: "orders",
            ..Default::default()
        });
        assert2::assert!(matches_resource(&entry, ResourceType::Topic, "orders"));
        assert2::assert!(!matches_resource(&entry, ResourceType::Topic, "payments"));
        assert2::assert!(!matches_resource(&entry, ResourceType::Group, "orders"));

        let prefix_entry = topic_acl(AclSetup {
            pattern: PatternType::Prefixed,
            name: "prefix_",
            ..Default::default()
        });
        assert2::assert!(matches_resource(
            &prefix_entry,
            ResourceType::Topic,
            "prefix_test"
        ));
        assert2::assert!(!matches_resource(
            &prefix_entry,
            ResourceType::Topic,
            "other_test"
        ));
        assert2::assert!(!matches_resource(
            &prefix_entry,
            ResourceType::Group,
            "prefix_test"
        ));
    }

    /// Check the same literal host ACL across peer addresses, with a caller-selected metadata version.
    fn check_host_cases(
        image: impl Fn() -> krabka_metadata::MetadataImage,
        cases: &[(&str, &str, AuthorizationResult)],
    ) {
        let principal = alice();
        let auth = SimpleAclAuthorizer::new(no_super());
        for (peer, acl_host, expected) in cases {
            let img = image_with_acls(
                image(),
                [topic_acl(AclSetup {
                    host: acl_host,
                    ..Default::default()
                })],
            );
            check_peer(&auth, &principal, &img, peer, *expected, Some(acl_host));
        }
    }

    fn check_peer(
        auth: &SimpleAclAuthorizer,
        principal: &krabka_security::Principal,
        image: &krabka_metadata::MetadataImage,
        peer: &str,
        expected: AuthorizationResult,
        acl_host: Option<&str>,
    ) {
        let host: SocketAddr = peer.parse().unwrap();
        let decision = auth.authorize(image, &req(principal, &host, QuerySetup::default()));
        if let Some(acl_host) = acl_host {
            assert2::assert!(decision == expected, "peer {peer} vs acl host {acl_host}");
        } else {
            assert2::assert!(decision == expected, "peer {peer}");
        }
    }

    /// The ACL host comparison must use the JDK's `getHostAddress()` text,
    /// not Rust's `Display` text, because that is what Kafka tooling writes
    /// into ACL host strings. See the module doc on
    /// [`crate::jdk_host_address`].
    #[test]
    fn host_matching_uses_jdk_address_text() {
        let cases: &[(&str, &str, AuthorizationResult)] = &[
            ("127.0.0.1:5000", "127.0.0.1", AuthorizationResult::Allow),
            (
                "[::ffff:10.0.0.5]:5000",
                "10.0.0.5",
                AuthorizationResult::Allow,
            ),
            (
                "[::ffff:10.0.0.5]:5000",
                "::ffff:10.0.0.5",
                AuthorizationResult::Deny,
            ),
            ("[::1]:5000", "0:0:0:0:0:0:0:1", AuthorizationResult::Allow),
            ("[::1]:5000", "::1", AuthorizationResult::Deny),
            (
                "[2001:db8::5]:5000",
                "2001:db8:0:0:0:0:0:5",
                AuthorizationResult::Allow,
            ),
        ];
        check_host_cases(img, cases);
    }

    /// KIP-1276: a CIDR ACL host matches by numeric range, not by comparing
    /// the peer's JDK host-address text against the CIDR literal.
    #[test]
    fn cidr_host_matches_peer_address_by_range() {
        let cases: &[(&str, &str, AuthorizationResult)] = &[
            ("10.1.2.3:5000", "10.0.0.0/8", AuthorizationResult::Allow),
            ("11.0.0.1:5000", "10.0.0.0/8", AuthorizationResult::Deny),
            (
                "[2001:db8::5]:5000",
                "2001:db8::/32",
                AuthorizationResult::Allow,
            ),
            (
                "[2001:db9::5]:5000",
                "2001:db8::/32",
                AuthorizationResult::Deny,
            ),
        ];
        check_host_cases(cidr_img, cases);
    }

    /// Kafka 4.3.1 compares every ACL host as text, and trunk reads a range
    /// only from `metadata.version` 4.4-IV1. Below it a host containing `/`
    /// applies to no peer, so a range ALLOW grants nothing and a range DENY
    /// denies nothing.
    #[test]
    fn a_host_with_a_slash_is_plain_text_below_the_cidr_metadata_version() {
        let a = alice();
        let auth = SimpleAclAuthorizer::new(no_super());
        let h: SocketAddr = "10.1.2.3:5000".parse().unwrap();
        // (stored entries, expected)
        let cases = [
            (
                vec![(PermissionType::Allow, "10.0.0.0/8")],
                AuthorizationResult::Deny,
            ),
            (
                vec![
                    (PermissionType::Allow, "*"),
                    (PermissionType::Deny, "10.0.0.0/8"),
                ],
                AuthorizationResult::Allow,
            ),
        ];
        for (entries, expected) in cases {
            let mut img = img();
            for (permission, host) in &entries {
                img.apply(&MetadataRecord::V1AccessControlEntry(topic_acl(AclSetup {
                    permission: *permission,
                    host,
                    ..Default::default()
                })));
            }
            assert2::assert!(
                auth.authorize(&img, &req(&a, &h, QuerySetup::default())) == expected,
                "{entries:?}"
            );
        }
    }

    /// A CIDR DENY still wins over a wildcard ALLOW for a peer inside the
    /// range, and the wildcard ALLOW still covers a peer outside it -- the
    /// same deny-wins-over-allow rule as a literal host ACL.
    #[test]
    fn cidr_deny_overrides_wildcard_allow_inside_range() {
        let img = image_with_acls(
            cidr_img(),
            [
                topic_acl(AclSetup::default()),
                topic_acl(AclSetup {
                    permission: PermissionType::Deny,
                    host: "10.0.0.0/8",
                    ..Default::default()
                }),
            ],
        );
        let a = alice();
        let auth = SimpleAclAuthorizer::new(no_super());
        let cases: &[(&str, AuthorizationResult)] = &[
            ("10.1.2.3:5000", AuthorizationResult::Deny),
            ("192.168.0.1:5000", AuthorizationResult::Allow),
        ];
        for (peer, expected) in cases {
            check_peer(&auth, &a, &img, peer, *expected, None);
        }
    }

    /// A DENY ACL written in Kafka's JDK host-address form must still deny an
    /// IPv4-mapped IPv6 peer even though a wildcard-host ALLOW exists --
    /// the security-relevant direction of the bug in #651.
    #[test]
    fn deny_acl_in_jdk_host_form_blocks_ipv4_mapped_peer() {
        let img = acl_image([
            topic_acl(AclSetup::default()),
            topic_acl(AclSetup {
                permission: PermissionType::Deny,
                host: "10.0.0.5",
                ..Default::default()
            }),
        ]);
        let a = alice();
        let h: SocketAddr = "[::ffff:10.0.0.5]:5000".parse().unwrap();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req(&a, &h, QuerySetup::default())) == AuthorizationResult::Deny
        );
    }
}
