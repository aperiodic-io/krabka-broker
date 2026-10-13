use {
    super::{
        AclDecision, AclDefault, AclFacts, AclOperationKind, AclPatternKind, AclResourceFacts,
        AclResourceTypeMatch, RequestAuthState,
    },
    creusot_std::prelude::*,
};

/// Match a principal or host by exact equality or its axis-specific wildcard.
#[ensures(result == (wildcard || exact))]
#[must_use]
pub fn acl_identity_match(wildcard: bool, exact: bool) -> bool { wildcard || exact }

/// Match an ACL resource type and literal or prefixed name pattern.
///
/// Kafka's `AclAuthorizer.matchingAcls`: a LITERAL ACL applies to its exact
/// name and, when that name is `*`, to every name; a PREFIXED ACL applies to
/// every name that starts with its name. Neither applies across resource
/// types.
#[ensures(result == (facts.resource_type == AclResourceTypeMatch::Same && match pattern {
    AclPatternKind::Literal => facts.exact_name || facts.wildcard_name,
    AclPatternKind::Prefixed => facts.name_has_prefix,
}))]
#[must_use]
pub fn acl_resource_match(pattern: AclPatternKind, facts: AclResourceFacts) -> bool {
    match (facts.resource_type, pattern) {
        (AclResourceTypeMatch::Different, _) => false,
        (AclResourceTypeMatch::Same, AclPatternKind::Literal) => {
            facts.exact_name || facts.wildcard_name
        }
        (AclResourceTypeMatch::Same, AclPatternKind::Prefixed) => facts.name_has_prefix,
    }
}

/// Match exact operations, `All`, and Kafka's one-way implication arrows.
///
/// The implication arrows (`Read`/`Write`/`Delete`/`Alter` → `Describe`, and
/// `AlterConfigs` → `DescribeConfigs`) apply only when the stored ACL is an
/// ALLOW entry: Kafka's operation-implication table never widens what a DENY
/// ACL blocks.
#[ensures(result == (stored == requested
    || stored == AclOperationKind::All
    || (is_allow && stored == AclOperationKind::Read && requested == AclOperationKind::Describe)
    || (is_allow && stored == AclOperationKind::Write && requested == AclOperationKind::Describe)
    || (is_allow && stored == AclOperationKind::Delete && requested == AclOperationKind::Describe)
    || (is_allow && stored == AclOperationKind::Alter && requested == AclOperationKind::Describe)
    || (is_allow
        && stored == AclOperationKind::AlterConfigs
        && requested == AclOperationKind::DescribeConfigs)))]
#[must_use]
pub fn acl_operation_match(
    stored: AclOperationKind,
    requested: AclOperationKind,
    is_allow: bool,
) -> bool {
    match stored {
        AclOperationKind::All => true,
        AclOperationKind::Read => {
            matches!(requested, AclOperationKind::Read)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::Write => {
            matches!(requested, AclOperationKind::Write)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::Delete => {
            matches!(requested, AclOperationKind::Delete)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::Alter => {
            matches!(requested, AclOperationKind::Alter)
                || (is_allow && matches!(requested, AclOperationKind::Describe))
        }
        AclOperationKind::AlterConfigs => {
            matches!(requested, AclOperationKind::AlterConfigs)
                || (is_allow && matches!(requested, AclOperationKind::DescribeConfigs))
        }
        AclOperationKind::Create => matches!(requested, AclOperationKind::Create),
        AclOperationKind::Describe => matches!(requested, AclOperationKind::Describe),
        AclOperationKind::ClusterAction => matches!(requested, AclOperationKind::ClusterAction),
        AclOperationKind::DescribeConfigs => {
            matches!(requested, AclOperationKind::DescribeConfigs)
        }
        AclOperationKind::IdempotentWrite => {
            matches!(requested, AclOperationKind::IdempotentWrite)
        }
        AclOperationKind::TwoPhaseCommit => {
            matches!(requested, AclOperationKind::TwoPhaseCommit)
        }
        AclOperationKind::CreateTokens => matches!(requested, AclOperationKind::CreateTokens),
        AclOperationKind::DescribeTokens => {
            matches!(requested, AclOperationKind::DescribeTokens)
        }
    }
}

/// Decide whether an API key may run in the current authentication phase.
#[ensures(state == RequestAuthState::Authenticated ==> result)]
#[ensures(state == RequestAuthState::Failed ==> !result)]
#[ensures(state == RequestAuthState::Exchanging ==>
    result == (api_key@ == 36))]
#[ensures(state == RequestAuthState::PreHandshake ==>
    result == (api_key@ == 17 || api_key@ == 18))]
#[must_use]
pub fn request_auth_admission(state: RequestAuthState, api_key: i16) -> bool {
    match state {
        RequestAuthState::Authenticated => true,
        RequestAuthState::Failed => false,
        RequestAuthState::Exchanging => api_key == 36,
        RequestAuthState::PreHandshake => matches!(api_key, 17 | 18),
    }
}

/// Decide a request from whether any matching ACL allowed or denied it.
///
/// Kafka's `AclAuthorizer.authorizeAction` order: a super user is always
/// allowed; otherwise a matching DENY wins over any ALLOW; otherwise a
/// matching ALLOW allows; otherwise `default_decision` decides. It reflects
/// `allow.everyone.if.no.acl.found` (default `false`), which allows a request
/// only when no ACL at all applies to the resource, so it never overrides an
/// explicit ALLOW or DENY.
#[ensures(match result {
    AclDecision::AllowSuperuser => facts.super_user,
    AclDecision::DenyExplicit => !facts.super_user && facts.saw_deny,
    AclDecision::AllowAcl => !facts.super_user && !facts.saw_deny && facts.saw_allow,
    AclDecision::AllowNoAcl => !facts.super_user && !facts.saw_deny && !facts.saw_allow
        && facts.default_decision == AclDefault::Allow,
    AclDecision::DenyDefault => !facts.super_user && !facts.saw_deny && !facts.saw_allow
        && facts.default_decision == AclDefault::Deny,
})]
#[must_use]
pub fn acl_decision(facts: AclFacts) -> AclDecision {
    if facts.super_user {
        AclDecision::AllowSuperuser
    } else if facts.saw_deny {
        AclDecision::DenyExplicit
    } else if facts.saw_allow {
        AclDecision::AllowAcl
    } else {
        match facts.default_decision {
            AclDefault::Allow => AclDecision::AllowNoAcl,
            AclDefault::Deny => AclDecision::DenyDefault,
        }
    }
}
