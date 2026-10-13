#[cfg(creusot)]
use super::{AclPatternKind, AclResourceTypeMatch};
use {
    super::{
        AclDecision, AclDefault, AclFacts, AclOperationKind, TokenApi, TokenApiAdmission,
        TokenDescriptionAcl, acl_decision, acl_identity_match, acl_operation_match,
        acl_resource_match, token_api_admission, token_describe_visible,
    },
    creusot_std::prelude::*,
};

// cargo-mutants: #[cfg(creusot)] specification, absent from runtime tests.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn token_description_resource_matches(row: TokenDescriptionAcl) -> bool {
    pearlite! { row.resource.resource_type == AclResourceTypeMatch::Same && match row.pattern {
        AclPatternKind::Literal => row.resource.exact_name || row.resource.wildcard_name,
        AclPatternKind::Prefixed => row.resource.name_has_prefix,
    } }
}

// cargo-mutants: #[cfg(creusot)] specification, absent from runtime tests.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn token_description_permission_matches(row: TokenDescriptionAcl) -> bool {
    pearlite! { token_description_resource_matches(row)
    && (row.principal.0 || row.principal.1)
    && (row.host.0 || row.host.1 || row.host.2)
    && (row.operation == AclOperationKind::All || row.operation == AclOperationKind::DescribeTokens) }
}

/// Fold complete User-owner ACLs, then compose connection admission and token
/// visibility. `CreateTokens` cannot confer `DescribeTokens` visibility; neither
/// ACL superuser status nor owner/requester/renewer relationships can bypass
/// the token-authenticated-session gate or an excluding owner filter.
/// Tuple fields name authentication, token relationships, and ACL policy.
/// Faithful identity/resource facts, complete enumeration, and the separate
/// exact-token Describe grant remain host obligations.
#[ensures(result == (identity.0 && !identity.1 && relationships.0
    && (relationships.1 || relationships.2 || relationships.3 || policy.2 || policy.0
        || (!(exists<i: Int> 0 <= i && i < rows@.len()
                && !rows@[i].allow && token_description_permission_matches(rows@[i]))
            && ((exists<i: Int> 0 <= i && i < rows@.len()
                    && rows@[i].allow && token_description_permission_matches(rows@[i]))
                || (policy.1 && !(exists<i: Int> 0 <= i && i < rows@.len()
                    && token_description_resource_matches(rows@[i]))))))))]
pub(super) fn token_description_preserves_authentication_and_acl_isolation(
    identity: (bool, bool), // authenticated non-anonymous principal, token-authenticated
    relationships: (bool, bool, bool, bool), // filter matches, owner, requester, renewer
    rows: &[TokenDescriptionAcl],
    policy: (bool, bool, bool), // superuser, allow when resource has no ACLs, exact-token grant
) -> bool {
    if matches!(
        token_api_admission(identity.0, identity.1, TokenApi::Describe),
        TokenApiAdmission::Reject
    ) {
        return false;
    }
    let mut has_resource_acls = false;
    let mut saw_allow = false;
    let mut saw_deny = false;
    let mut i = 0usize;
    #[invariant(i@ <= rows@.len())]
    #[invariant(has_resource_acls == (exists<j: Int> 0 <= j && j < i@
        && token_description_resource_matches(rows@[j])))]
    #[invariant(saw_allow == (exists<j: Int> 0 <= j && j < i@
        && rows@[j].allow && token_description_permission_matches(rows@[j])))]
    #[invariant(saw_deny == (exists<j: Int> 0 <= j && j < i@
        && !rows@[j].allow && token_description_permission_matches(rows@[j])))]
    #[variant(rows@.len() - i@)]
    while i < rows.len() {
        let row = rows[i];
        if acl_resource_match(row.pattern, row.resource) {
            has_resource_acls = true;
            if acl_identity_match(row.principal.0, row.principal.1)
                && (acl_identity_match(row.host.0, row.host.1) || row.host.2)
                && acl_operation_match(row.operation, AclOperationKind::DescribeTokens, row.allow)
            {
                if row.allow {
                    saw_allow = true;
                } else {
                    saw_deny = true;
                }
            }
        }
        i += 1;
    }
    let user_grant = matches!(
        acl_decision(AclFacts {
            super_user: policy.0,
            saw_allow,
            saw_deny,
            default_decision: if policy.1 && !has_resource_acls {
                AclDefault::Allow
            } else {
                AclDefault::Deny
            },
        }),
        AclDecision::AllowSuperuser | AclDecision::AllowAcl | AclDecision::AllowNoAcl
    );
    token_describe_visible(
        relationships.0,
        relationships.1,
        relationships.2,
        relationships.3,
        policy.2 || user_grant,
    )
}
