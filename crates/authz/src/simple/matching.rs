//! Predicates that decide whether one stored ACL entry applies to an
//! authorization request.
//!
//! An entry applies only when its principal, its host, and its operation all
//! match the request. The principal and host tests are equality-or-wildcard
//! checks. The operation test additionally carries Kafka's one-way
//! operation-implication table, which is why it lives beside them rather than
//! in the decision loop.

use {
    crate::cidr::Cidr,
    krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType},
    krabka_verified::{
        AclOperationKind, AclPatternKind, acl_identity_match, acl_operation_match,
        acl_resource_match,
        authz::{AclResourceFacts, AclResourceTypeMatch},
    },
    std::net::IpAddr,
};

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
    acl_resource_match(pattern, AclResourceFacts {
        resource_type: if entry.resource_type == resource_type {
            AclResourceTypeMatch::Same
        } else {
            AclResourceTypeMatch::Different
        },
        exact_name: entry.resource_name == name,
        wildcard_name: entry.resource_name == "*",
        name_has_prefix: name.starts_with(entry.resource_name.as_str()),
    })
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
    use {
        super::{matches_operation, matches_resource},
        crate::{
            AuthorizationResult, Authorizer, SimpleAclAuthorizer,
            simple::test_support::{
                AliceAuthorizer, acl_image, alice, check_topic_access, cidr_img, image_with_acls,
                img, no_super, req, topic_acl,
            },
        },
        krabka_metadata::{
            AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
        },
        std::net::SocketAddr,
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
    #[yare::parameterized(
        allow_read_describe = {
            PermissionType::Allow,
            AclOperation::Read,
            AclOperation::Describe,
            true,
        },
        allow_write_describe = {
            PermissionType::Allow,
            AclOperation::Write,
            AclOperation::Describe,
            true,
        },
        allow_delete_describe = {
            PermissionType::Allow,
            AclOperation::Delete,
            AclOperation::Describe,
            true,
        },
        allow_alter_describe = {
            PermissionType::Allow,
            AclOperation::Alter,
            AclOperation::Describe,
            true,
        },
        allow_alter_configs_describe_configs = {
            PermissionType::Allow,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            true,
        },
        deny_read_describe = {
            PermissionType::Deny,
            AclOperation::Read,
            AclOperation::Describe,
            false,
        },
        deny_write_describe = {
            PermissionType::Deny,
            AclOperation::Write,
            AclOperation::Describe,
            false,
        },
        deny_delete_describe = {
            PermissionType::Deny,
            AclOperation::Delete,
            AclOperation::Describe,
            false,
        },
        deny_alter_describe = {
            PermissionType::Deny,
            AclOperation::Alter,
            AclOperation::Describe,
            false,
        },
        deny_alter_configs_describe_configs = {
            PermissionType::Deny,
            AclOperation::AlterConfigs,
            AclOperation::DescribeConfigs,
            false,
        },
    )]
    fn implication_table_applies_only_to_allow_acls(
        permission: PermissionType,
        stored: AclOperation,
        requested: AclOperation,
        expected: bool,
    ) {
        assert2::assert!(matches_operation(stored, requested, permission) == expected);
    }

    #[test]
    fn principal_wildcard_matches_any_user() {
        let img = acl_image([topic_acl().principal("User:*").call()]);
        check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Allow);
    }

    #[yare::parameterized(
        matching_address = { "127.0.0.1:5000", AuthorizationResult::Allow },
        different_address = { "127.0.0.2:5000", AuthorizationResult::Deny },
    )]
    fn host_filter_matches_specific_ip(host: &str, expected: AuthorizationResult) {
        let img = acl_image([topic_acl().host("127.0.0.1").call()]);
        let a = alice();
        let host = host.parse::<SocketAddr>().unwrap();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(auth.authorize(&img, &req().p(&a).host(&host).call()) == expected);
    }

    #[yare::parameterized(
        read = { AclOperation::Read },
        write = { AclOperation::Write },
        describe = { AclOperation::Describe },
        delete = { AclOperation::Delete },
    )]
    fn operation_all_matches_any_op(operation: AclOperation) {
        let img = acl_image([topic_acl().op(AclOperation::All).call()]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(caller.authorize(&img, "foo", operation) == AuthorizationResult::Allow);
    }

    #[test]
    fn operation_specific_does_not_match_others() {
        let img = acl_image([topic_acl().call()]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(
            [AclOperation::Read, AclOperation::Write]
                .map(|operation| { caller.authorize(&img, "foo", operation) })
                == [AuthorizationResult::Allow, AuthorizationResult::Deny]
        );
    }

    #[yare::parameterized(
        read = { AclOperation::Read, AclOperation::Describe },
        write = { AclOperation::Write, AclOperation::Describe },
        delete = { AclOperation::Delete, AclOperation::Describe },
        alter = { AclOperation::Alter, AclOperation::Describe },
        alter_configs = { AclOperation::AlterConfigs, AclOperation::DescribeConfigs },
    )]
    fn operation_implies_describe_on_topic(
        granted_operation: AclOperation,
        described_operation: AclOperation,
    ) {
        let img = acl_image([topic_acl().op(granted_operation).call()]);
        check_topic_access(&img, "foo", described_operation, AuthorizationResult::Allow);
    }

    #[test]
    fn describe_does_not_imply_read() {
        let img = acl_image([topic_acl().op(AclOperation::Describe).call()]);
        check_topic_access(&img, "foo", AclOperation::Read, AuthorizationResult::Deny);
    }

    #[test]
    fn implication_works_on_group_resource() {
        let img = acl_image([topic_acl().rt(ResourceType::Group).name("cg-1").call()]);
        let caller = AliceAuthorizer::default();
        assert2::assert!(
            caller.authorize_on(&img, ResourceType::Group, "cg-1", AclOperation::Describe)
                == AuthorizationResult::Allow
        );
    }

    #[test]
    fn implication_works_on_cluster_resource() {
        let img = acl_image([topic_acl()
            .rt(ResourceType::Cluster)
            .op(AclOperation::Alter)
            .name("kafka-cluster")
            .call()]);
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
        let img = acl_image([topic_acl()
            .rt(ResourceType::TransactionalId)
            .op(AclOperation::Write)
            .name("tx-1")
            .call()]);
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
        let entry = topic_acl().name("orders").call();
        assert2::assert!(matches_resource(&entry, ResourceType::Topic, "orders"));
        assert2::assert!(!matches_resource(&entry, ResourceType::Topic, "payments"));
        assert2::assert!(!matches_resource(&entry, ResourceType::Group, "orders"));

        let prefix_entry = topic_acl()
            .pattern(PatternType::Prefixed)
            .name("prefix_")
            .call();
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

    fn check_peer(
        auth: &SimpleAclAuthorizer,
        principal: &krabka_security::Principal,
        image: &krabka_metadata::MetadataImage,
        peer: &str,
        expected: AuthorizationResult,
        acl_host: Option<&str>,
    ) {
        let host: SocketAddr = peer.parse().unwrap();
        let decision = auth.authorize(image, &req().p(principal).host(&host).call());
        if let Some(acl_host) = acl_host {
            assert2::assert!(decision == expected, "peer {peer} vs acl host {acl_host}");
        } else {
            assert2::assert!(decision == expected, "peer {peer}");
        }
    }

    fn check_host_acl(
        base: krabka_metadata::MetadataImage,
        peer: &str,
        acl_host: &str,
        expected: AuthorizationResult,
    ) {
        let principal = alice();
        let auth = SimpleAclAuthorizer::new(no_super());
        let image = image_with_acls(base, [topic_acl().host(acl_host).call()]);
        check_peer(&auth, &principal, &image, peer, expected, Some(acl_host));
    }

    /// The ACL host comparison must use the JDK's `getHostAddress()` text,
    /// not Rust's `Display` text, because that is what Kafka tooling writes
    /// into ACL host strings. See the module doc on
    /// [`crate::jdk_host_address`].
    #[yare::parameterized(
        ipv4_text = { "127.0.0.1:5000", "127.0.0.1", AuthorizationResult::Allow },
        mapped_peer_ipv4_acl = { "[::ffff:10.0.0.5]:5000", "10.0.0.5", AuthorizationResult::Allow },
        mapped_peer_ipv6_acl = {
            "[::ffff:10.0.0.5]:5000",
            "::ffff:10.0.0.5",
            AuthorizationResult::Deny,
        },
        expanded_ipv6_acl = { "[::1]:5000", "0:0:0:0:0:0:0:1", AuthorizationResult::Allow },
        compressed_ipv6_acl = { "[::1]:5000", "::1", AuthorizationResult::Deny },
        expanded_global_ipv6_acl = {
            "[2001:db8::5]:5000",
            "2001:db8:0:0:0:0:0:5",
            AuthorizationResult::Allow,
        },
    )]
    fn host_matching_uses_jdk_address_text(
        peer: &'static str,
        acl_host: &'static str,
        expected: AuthorizationResult,
    ) {
        check_host_acl(img(), peer, acl_host, expected);
    }

    /// KIP-1276: a CIDR ACL host matches by numeric range, not by comparing
    /// the peer's JDK host-address text against the CIDR literal.
    #[yare::parameterized(
        ipv4_inside = { "10.1.2.3:5000", "10.0.0.0/8", AuthorizationResult::Allow },
        ipv4_outside = { "11.0.0.1:5000", "10.0.0.0/8", AuthorizationResult::Deny },
        ipv6_inside = { "[2001:db8::5]:5000", "2001:db8::/32", AuthorizationResult::Allow },
        ipv6_outside = { "[2001:db9::5]:5000", "2001:db8::/32", AuthorizationResult::Deny },
    )]
    fn cidr_host_matches_peer_address_by_range(
        peer: &'static str,
        acl_host: &'static str,
        expected: AuthorizationResult,
    ) {
        check_host_acl(cidr_img(), peer, acl_host, expected);
    }

    /// Kafka 4.3.1 compares every ACL host as text, and trunk reads a range
    /// only from `metadata.version` 4.4-IV1. Below it a host containing `/`
    /// applies to no peer, so a range ALLOW grants nothing and a range DENY
    /// denies nothing.
    #[yare::parameterized(
        range_allow_is_text = {
            vec![(PermissionType::Allow, "10.0.0.0/8")],
            AuthorizationResult::Deny,
        },
        wildcard_allow_wins = {
            vec![(PermissionType::Allow, "*"), (PermissionType::Deny, "10.0.0.0/8")],
            AuthorizationResult::Allow,
        },
    )]
    fn a_host_with_a_slash_is_plain_text_below_the_cidr_metadata_version(
        entries: Vec<(PermissionType, &'static str)>,
        expected: AuthorizationResult,
    ) {
        let a = alice();
        let auth = SimpleAclAuthorizer::new(no_super());
        let h: SocketAddr = "10.1.2.3:5000".parse().unwrap();
        let mut img = img();
        for (permission, host) in &entries {
            img.apply(&MetadataRecord::V1AccessControlEntry(
                topic_acl().permission(*permission).host(host).call(),
            ));
        }
        assert2::assert!(auth.authorize(&img, &req().p(&a).host(&h).call()) == expected);
    }

    /// A CIDR DENY still wins over a wildcard ALLOW for a peer inside the
    /// range, and the wildcard ALLOW still covers a peer outside it -- the
    /// same deny-wins-over-allow rule as a literal host ACL.
    #[yare::parameterized(
        peer_inside_range = { "10.1.2.3:5000", AuthorizationResult::Deny },
        peer_outside_range = { "192.168.0.1:5000", AuthorizationResult::Allow },
    )]
    fn cidr_deny_overrides_wildcard_allow_inside_range(peer: &str, expected: AuthorizationResult) {
        let img = image_with_acls(cidr_img(), [
            topic_acl().call(),
            topic_acl()
                .permission(PermissionType::Deny)
                .host("10.0.0.0/8")
                .call(),
        ]);
        let a = alice();
        let auth = SimpleAclAuthorizer::new(no_super());
        check_peer(&auth, &a, &img, peer, expected, None);
    }

    /// A DENY ACL written in Kafka's JDK host-address form must still deny an
    /// IPv4-mapped IPv6 peer even though a wildcard-host ALLOW exists --
    /// the security-relevant direction of the bug in #651.
    #[test]
    fn deny_acl_in_jdk_host_form_blocks_ipv4_mapped_peer() {
        let img = acl_image([
            topic_acl().call(),
            topic_acl()
                .permission(PermissionType::Deny)
                .host("10.0.0.5")
                .call(),
        ]);
        let a = alice();
        let h: SocketAddr = "[::ffff:10.0.0.5]:5000".parse().unwrap();
        let auth = SimpleAclAuthorizer::new(no_super());
        assert2::assert!(
            auth.authorize(&img, &req().p(&a).host(&h).call()) == AuthorizationResult::Deny
        );
    }
}
