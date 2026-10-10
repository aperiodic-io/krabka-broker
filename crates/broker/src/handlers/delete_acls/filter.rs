//! Translation of one wire `DeleteAclsFilter` into Kafka's `AclBindingFilter`,
//! and of one matched ACL into the deletion record that removes it.
//!
//! Kafka spells "no constraint" as `ANY` on the enum axes and as a null string
//! on the name, principal, and host axes. An empty string is a value, not a
//! wildcard, and `MATCH` selects every binding that applies to the named
//! resource. Getting that wrong would silently widen or narrow a delete, so
//! the decision sits in a file of its own.

use krabka_metadata::{AclEntry, AclEntryFilter};
use krabka_protocol::owned::delete_acls_request::DeleteAclsFilter;

use crate::handlers::acl_wire::binding_filter::{
    AclBindingFilter, UnknownElement, WireAclBindingFilter,
};

pub(super) fn build_filter(f: &DeleteAclsFilter) -> Result<AclBindingFilter, UnknownElement> {
    AclBindingFilter::from_wire(WireAclBindingFilter {
        resource_type: f.resource_type_filter,
        resource_name: f.resource_name_filter.as_deref(),
        pattern_type: f.pattern_type_filter,
        principal: f.principal_filter.as_deref(),
        host: f.host_filter.as_deref(),
        operation: f.operation,
        permission_type: f.permission_type,
    })
}

/// The deletion record filter that removes exactly `entry`.
///
/// Kafka's `AclControlManager.deleteAclsForFilter` writes one
/// `RemoveAccessControlEntryRecord` per matched ACL, so the log names each
/// removed binding rather than the request filter. Every axis is set, and the
/// metadata image holds each full tuple at most once, so this filter matches
/// that one entry.
pub(super) fn exact_filter(entry: &AclEntry) -> AclEntryFilter {
    AclEntryFilter {
        resource_type: Some(entry.resource_type),
        resource_name: Some(entry.resource_name.clone()),
        pattern_type: Some(entry.pattern_type),
        principal: Some(entry.principal.clone()),
        host: Some(entry.host.clone()),
        operation: Some(entry.operation),
        permission_type: Some(entry.permission_type),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{AclOperation, PatternType, PermissionType};

    use super::*;
    use crate::handlers::delete_acls::test_support::{acl, filter};

    #[test]
    fn build_filter_keeps_empty_strings_and_decodes_axes() {
        let f = empty_match_acl_filter!(DeleteAclsFilter);

        let built = build_filter(&f).expect("filter");

        let expected = crate::handlers::acl_test_support::expected_empty_match_filter();
        assert!(built == expected);
    }

    /// Kafka's `DeleteAclsRequest.normalizeAndValidate` refuses only the
    /// `UNKNOWN` (0) byte. Any other byte it does not define parses, and
    /// `AclControlManager.validateFilter` refuses that one filter later.
    #[test]
    fn build_filter_refuses_only_the_unknown_byte() {
        type CorruptFilter = fn(&mut DeleteAclsFilter, i8);
        let cases: [(&str, CorruptFilter); 4] = [
            ("resource_type_filter", |f, b| f.resource_type_filter = b),
            ("pattern_type_filter", |f, b| f.pattern_type_filter = b),
            ("operation", |f, b| f.operation = b),
            ("permission_type", |f, b| f.permission_type = b),
        ];
        for (axis, corrupt) in cases {
            let mut unknown = filter(Some("orders"), Some("User:alice"));
            corrupt(&mut unknown, 0);
            check!(build_filter(&unknown) == Err(UnknownElement), "axis {axis}");

            let mut undefined = filter(Some("orders"), Some("User:alice"));
            corrupt(&mut undefined, 99);
            check!(build_filter(&undefined).is_ok(), "axis {axis}");
        }
    }

    #[test]
    fn exact_filter_matches_only_its_entry() {
        let entry = acl(crate::test_support::AllowAclSetup::default());
        let neighbours = [
            AclEntry {
                pattern_type: PatternType::Prefixed,
                ..entry.clone()
            },
            AclEntry {
                permission_type: PermissionType::Deny,
                ..entry.clone()
            },
            AclEntry {
                host: "10.0.0.1".into(),
                ..entry.clone()
            },
            acl(crate::test_support::AllowAclSetup {
                operation: AclOperation::Write,
                ..Default::default()
            }),
            acl(crate::test_support::AllowAclSetup {
                principal: "User:bob",
                ..Default::default()
            }),
            acl(crate::test_support::AllowAclSetup {
                resource_name: "order",
                ..Default::default()
            }),
        ];

        let exact = exact_filter(&entry);

        check!(exact.matches(&entry));
        for other in &neighbours {
            check!(!exact.matches(other), "{other:?}");
        }
    }
}
