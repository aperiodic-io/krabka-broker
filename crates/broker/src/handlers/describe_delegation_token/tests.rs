//! `DescribeDelegationToken` tests that drive [`super::handle`] against a live
//! single-voter controller.
//!
//! Every case here is about the KIP-48 visible set: `filterToken`'s
//! owner-or-renewer filter, the owner/renewer/ACL relationship it then
//! requires, the token id used as the ACL resource name, the empty-list
//! sentinel, and `allowTokenRequests`' blanket refusal of a
//! delegation-token-authenticated caller — the credential-disclosure gate
//! this suite exists to pin down.

use {
    super::handle,
    crate::{
        authorizer::{AllowAllAuthorizer, Authorizer, SimpleAclAuthorizer},
        handlers::delegation_token_test_support::{
            anonymous, authed, kp, seed_token, test_controller, token_authed,
        },
        metadata_source::MetadataSource,
        network::auth::ConnectionAuth,
        test_support::allow_acl,
    },
    assert2::assert,
    krabka_metadata::{
        AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
    },
    krabka_protocol::owned::describe_delegation_token_request::{
        DescribeDelegationTokenOwner, DescribeDelegationTokenRequest,
    },
    krabka_raft::ControllerHandle,
    krabka_security::SecretBytes,
    std::net::SocketAddr,
    tempfile::TempDir,
};

macro_rules! describe_token_response {
    ($controller:expr, $request:expr, $authentication:expr, $secret:expr) => {
        describe()
            .controller($controller)
            .authorizer(&simple_authz())
            .maybe_request($request)
            .maybe_authentication($authentication)
            .maybe_secret($secret)
            .call()
    };
}

#[derive(Clone, Copy, Default)]
struct DescribeTokenSetup<'a> {
    request: Option<&'a DescribeDelegationTokenRequest>,
    authentication: Option<&'a ConnectionAuth>,
    secret: Option<&'a SecretBytes>,
}

#[bon::builder]
fn describe(
    controller: &dyn MetadataSource,
    authorizer: &dyn Authorizer,
    request: Option<&'_ DescribeDelegationTokenRequest>,
    authentication: Option<&'_ ConnectionAuth>,
    secret: Option<&'_ SecretBytes>,
    #[builder(default)] assert_success: bool,
) -> krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse {
    let setup = DescribeTokenSetup {
        request,
        authentication,
        secret,
    };
    let default_request = no_owner_filter();
    let default_authentication = authed("alice");
    let response = handle(
        setup.request.unwrap_or(&default_request),
        setup.authentication.unwrap_or(&default_authentication),
        setup.secret,
        controller,
        &peer(),
        authorizer,
    );
    if assert_success {
        assert!(response.error_code == 0);
    }
    response
}

#[bon::builder]
fn describe_unrestricted(
    controller: &dyn MetadataSource,
    request: Option<&'_ DescribeDelegationTokenRequest>,
    authentication: Option<&'_ ConnectionAuth>,
    secret: Option<&'_ SecretBytes>,
) -> krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse {
    let setup = DescribeTokenSetup {
        request,
        authentication,
        secret,
    };
    describe()
        .controller(controller)
        .authorizer(&AllowAllAuthorizer)
        .maybe_request(setup.request)
        .maybe_authentication(setup.authentication)
        .maybe_secret(setup.secret)
        .call()
}

fn check_refused_token_description(
    response: &krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse,
) {
    assert!(response.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    assert!(
        response.tokens.is_empty(),
        "refused caller must never see any token HMAC, got {:?}",
        token_ids(response)
    );
}

async fn seed_alice_tokens(controller: &ControllerHandle) {
    seed_token().controller(controller).call().await;
    seed_token()
        .controller(controller)
        .token_id("t-b")
        .call()
        .await;
}

async fn seed_admin_minted_token(controller: &ControllerHandle) {
    seed_token()
        .controller(controller)
        .token_id("t-minted")
        .maybe_requester(Some(kp("admin")))
        .renewers(vec![kp("bob")])
        .call()
        .await;
}

fn peer() -> SocketAddr { "127.0.0.1:0".parse().unwrap() }

/// The tests below want the "real ACL" semantics: the ACL extension should
/// add a token if and only if the caller holds a matching `Describe` ACL on
/// `DelegationToken:<token_id>`. With [`crate::authorizer::AllowAllAuthorizer`]
/// every token would surface, which is correct under "allow everything" but
/// does not exercise the ACL filter these tests are written against.
fn simple_authz() -> SimpleAclAuthorizer {
    SimpleAclAuthorizer::new(std::collections::HashSet::new())
}

async fn seed_acl(controller: &ControllerHandle, entry: AclEntry) {
    controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(entry)])
        .await
        .expect("seed acl");
}

/// A `Describe` ACL on `DelegationToken:<token_id>` for `principal`, which is
/// the resource-name convention Kafka's own authorizer call
/// (`authHelper.authorize(..., DESCRIBE, DELEGATION_TOKEN, tokenId)`) and its
/// admin tooling use. A resource name of the owner's principal string
/// instead would grant every token of that owner from one ACL.
fn describe_token_acl(token_id: &str, principal: &str) -> AclEntry {
    allow_acl()
        .resource_type(ResourceType::DelegationToken)
        .resource_name(token_id)
        .principal(&format!("User:{principal}"))
        .operation(AclOperation::Describe)
        .call()
}

fn no_owner_filter() -> DescribeDelegationTokenRequest {
    DescribeDelegationTokenRequest {
        owners: None,
        ..Default::default()
    }
}

fn token_ids(
    resp: &krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse,
) -> std::collections::HashSet<&str> {
    resp.tokens.iter().map(|t| t.token_id.as_str()).collect()
}

fn check_visible_token_ids(
    response: &krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse,
    expected: &[&str],
) {
    assert!(response.error_code == 0);
    let expected: std::collections::HashSet<_> = expected.iter().copied().collect();
    assert!(token_ids(response) == expected);
}

#[tokio::test]
async fn returns_auth_disabled_when_no_secret_key() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;
    let req = no_owner_filter();
    let resp = describe()
        .controller(&*controller)
        .authorizer(&simple_authz())
        .maybe_request(Some(&req))
        .call();
    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_AUTH_DISABLED);
    controller.cancel().await;
}

#[tokio::test]
async fn anonymous_caller_is_rejected_without_exposing_token_hmacs() {
    token_fixture!(dir, controller, secret);
    seed_token().controller(&controller).call().await;

    let resp = describe_unrestricted()
        .controller(&*controller)
        .maybe_authentication(Some(&anonymous()))
        .maybe_secret(Some(&secret))
        .call();

    check_refused_token_description(&resp);
    controller.cancel().await;
}

/// `allowTokenRequests`: a delegation-token-authenticated caller is refused
/// entirely, with no tokens returned, regardless of what it owns. This is
/// the fix for the credential-disclosure bug: a caller holding one token
/// must never be able to read a sibling token's HMAC by calling
/// `DescribeDelegationToken`.
#[tokio::test]
async fn token_authed_caller_is_refused_entirely() {
    token_fixture!(dir, controller, secret);
    // alice owns both t-a and t-b: a token-authed session must not be able
    // to read either one, including its own token's sibling.
    seed_alice_tokens(&controller).await;

    let resp = describe_unrestricted()
        .controller(&*controller)
        .maybe_authentication(Some(&token_authed("alice")))
        .maybe_secret(Some(&secret))
        .call();

    check_refused_token_description(&resp);
    controller.cancel().await;
}

/// Check order: `allowTokenRequests` (64) fires before the
/// `tokenAuthEnabled` check (61), so a token-authed caller gets 64 even
/// when the broker also has no secret key configured.
#[tokio::test]
async fn token_authed_caller_gets_request_not_allowed_before_auth_disabled() {
    let dir = TempDir::new().unwrap();
    let controller = test_controller(dir.path().into()).await;

    let resp = describe_unrestricted()
        .controller(&*controller)
        .maybe_authentication(Some(&token_authed("alice")))
        .call();

    assert!(resp.error_code == crate::codes::DELEGATION_TOKEN_REQUEST_NOT_ALLOWED);
    controller.cancel().await;
}

/// `ownersListEmpty`: a present-but-empty `owners` list returns no tokens
/// with `error_code = NONE`, distinct from a missing (null) list.
#[tokio::test]
async fn empty_owners_list_returns_no_tokens_without_error() {
    token_fixture!(dir, controller, secret);
    seed_token().controller(&controller).call().await;

    let req = DescribeDelegationTokenRequest {
        owners: Some(vec![]),
        ..Default::default()
    };
    check_description_visibility()
        .controller(&*controller)
        .authorizer(&AllowAllAuthorizer)
        .maybe_request(Some(&req))
        .maybe_secret(Some(&secret))
        .expected(&[])
        .call();
    controller.cancel().await;
}

/// Table-driven: which tokens a caller sees, varying the caller's
/// relationship to each token and the ACL state, matching
/// `DelegationTokenManager.filterToken`.
#[tokio::test]
async fn filter_token_matches_owner_renewer_or_token_id_acl() {
    struct Case {
        name: &'static str,
        acl: Option<AclEntry>,
        expected: &'static [&'static str],
    }

    let cases = [
        Case {
            name: "owner sees own token, not a sibling owned by someone else",
            acl: None,
            expected: &["t-owner"],
        },
        Case {
            name: "describe acl on the exact token id grants only that token",
            acl: Some(describe_token_acl("t-other", "alice")),
            expected: &["t-owner", "t-other"],
        },
        Case {
            name: "describe acl on a different token id grants exactly that token, nothing else",
            acl: Some(describe_token_acl("t-unrelated", "alice")),
            expected: &["t-owner", "t-unrelated"],
        },
    ];

    for Case {
        name,
        acl,
        expected,
    } in cases
    {
        token_fixture!(dir, controller, secret);
        seed_token()
            .controller(&controller)
            .token_id("t-owner")
            .call()
            .await;
        seed_token()
            .controller(&controller)
            .token_id("t-renewer")
            .owner(kp("bob"))
            .renewers(vec![kp("carol")])
            .call()
            .await;
        seed_token()
            .controller(&controller)
            .token_id("t-other")
            .owner(kp("bob"))
            .call()
            .await;
        seed_token()
            .controller(&controller)
            .token_id("t-unrelated")
            .owner(kp("dave"))
            .call()
            .await;
        if let Some(entry) = acl {
            seed_acl(&controller, entry).await;
        }

        let resp = describe()
            .controller(&*controller)
            .authorizer(&simple_authz())
            .maybe_secret(Some(&secret))
            .call();
        assert!(resp.error_code == 0, "{name}");
        let expected: std::collections::HashSet<&str> = expected.iter().copied().collect();
        assert!(token_ids(&resp) == expected, "{name}");
        controller.cancel().await;
    }
}

/// A caller who is a listed renewer (not the owner) sees the token too, and
/// the owner filter matches on owner-OR-renewer, not owner alone.
#[tokio::test]
async fn renewer_sees_the_token_and_owner_filter_matches_renewer_too() {
    token_fixture!(dir, controller, secret);
    // bob's token: carol is a listed renewer.
    seed_token()
        .controller(&controller)
        .token_id("t-b")
        .owner(kp("bob"))
        .renewers(vec![kp("carol")])
        .call()
        .await;
    // dave's token: carol has no relationship.
    seed_token()
        .controller(&controller)
        .token_id("t-d")
        .owner(kp("dave"))
        .call()
        .await;

    // Filter asks for tokens whose owner-or-renewer is bob or dave. carol
    // is a renewer of t-b (owned by bob), so the filter matches it, and
    // carol's renewer relationship then makes it visible; t-d's owner
    // (dave) matches the filter too, but carol has no owner/renewer/ACL
    // relationship to it, so it stays hidden.
    let req = DescribeDelegationTokenRequest {
        owners: Some(vec![
            DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "bob".into(),
                ..Default::default()
            },
            DescribeDelegationTokenOwner {
                principal_type: "User".into(),
                principal_name: "dave".into(),
                ..Default::default()
            },
        ]),
        ..Default::default()
    };
    let resp = describe_token_response!(
        &*controller,
        Some(&req),
        Some(&authed("carol")),
        Some(&secret)
    );
    check_visible_token_ids(&resp, &["t-b"]);
    controller.cancel().await;
}

/// A caller with a cluster-wide `Describe` ACL on the token's own id sees
/// it even without an owner filter naming that owner, and a `Describe` ACL
/// on one token id does not leak a second token owned by the same
/// principal — the resource-name fix this issue is about.
#[tokio::test]
async fn describe_acl_on_token_id_grants_exactly_that_token() {
    token_fixture!(dir, controller, secret);
    // alice owns two tokens; bob has no owner/renewer relationship to
    // either.
    seed_alice_tokens(&controller).await;
    seed_acl(&controller, describe_token_acl("t-a", "bob")).await;

    let resp = describe()
        .controller(&*controller)
        .authorizer(&simple_authz())
        .maybe_authentication(Some(&authed("bob")))
        .maybe_secret(Some(&secret))
        .assert_success(true)
        .call();
    assert!(
        token_ids(&resp) == std::collections::HashSet::from(["t-a"]),
        "an ACL on t-a must not also surface t-b, same owner or not; got {:?}",
        token_ids(&resp)
    );
    controller.cancel().await;
}

/// KIP-373: `DescribeTokens` on `User:<owner>` grants every token that owner
/// holds, and nothing of another owner's (Kafka's `authorizeRequester`).
/// `CreateTokens` grants no description access, deny wins, and a resource ACL
/// for an unrelated principal suppresses the no-ACL default on that owner.
#[tokio::test]
async fn describe_tokens_acl_on_the_owner_grants_all_of_their_tokens() {
    use {
        AclOperation::{All, CreateTokens, DescribeTokens},
        PermissionType::{Allow, Deny},
    };
    type Case = (
        &'static [(AclOperation, PermissionType)],
        &'static str,
        bool,
        &'static [&'static str],
    );
    let cases: &[Case] = &[
        (&[(DescribeTokens, Allow)], "bob", false, &["t-a1", "t-a2"]),
        (&[(CreateTokens, Allow)], "bob", false, &[]),
        (
            &[(CreateTokens, Deny), (DescribeTokens, Allow)],
            "bob",
            false,
            &["t-a1", "t-a2"],
        ),
        (&[(All, Allow), (DescribeTokens, Deny)], "bob", false, &[]),
        (&[(DescribeTokens, Allow)], "mallory", true, &["t-c"]),
    ];
    for &(operations, principal, default_allow, expected) in cases {
        token_fixture!(dir, controller, secret);
        seed_token()
            .controller(&controller)
            .token_id("t-a1")
            .call()
            .await;
        seed_token()
            .controller(&controller)
            .token_id("t-a2")
            .call()
            .await;
        seed_token()
            .controller(&controller)
            .token_id("t-c")
            .owner(kp("carol"))
            .call()
            .await;
        // Isolate the User-owner grant from the independent exact-token path.
        let mut token_deny = describe_token_acl("*", "*");
        token_deny.permission_type = Deny;
        seed_acl(&controller, token_deny).await;
        for &(operation, permission_type) in operations {
            seed_acl(&controller, AclEntry {
                resource_type: ResourceType::User,
                resource_name: "User:alice".into(),
                pattern_type: PatternType::Literal,
                principal: format!("User:{principal}"),
                host: "*".into(),
                operation,
                permission_type,
            })
            .await;
        }
        let authorizer = simple_authz().with_allow_everyone_if_no_acl_found(default_allow);
        let resp = describe()
            .controller(&*controller)
            .authorizer(&authorizer)
            .maybe_authentication(Some(&authed("bob")))
            .maybe_secret(Some(&secret))
            .assert_success(true)
            .call();
        assert!(
            token_ids(&resp)
                == expected
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>(),
            "{operations:?}, {principal}, default={default_allow}"
        );
        controller.cancel().await;
    }
}

/// A caller with no owner/renewer relationship and no matching ACL sees
/// nothing — pure token possession (proven by nothing here, since the
/// handler never inspects HMACs) grants no visibility on its own.
#[tokio::test]
async fn unrelated_caller_sees_nothing() {
    token_fixture!(dir, controller, secret);
    seed_token().controller(&controller).call().await;

    check_description_visibility()
        .controller(&*controller)
        .authorizer(&simple_authz())
        .maybe_authentication(Some(&authed("eve")))
        .maybe_secret(Some(&secret))
        .expected(&[])
        .call();
    controller.cancel().await;
}

/// KIP-373: `TokenInformation.ownerOrRenewer` also matches the requester, the
/// principal that created a token for another owner. `filterToken` applies it
/// both to the `owners` filter and to the check that the caller may see the
/// token, so the minting principal finds and sees the token without any
/// `Describe` ACL.
#[tokio::test]
async fn requester_of_a_token_minted_for_another_owner_finds_and_sees_it() {
    // (caller, `owners` filter, expected token ids)
    type Case<'a> = (&'a str, Option<&'a [&'a str]>, &'a [&'a str]);
    let cases: [Case<'_>; 7] = [
        ("admin", None, &["t-minted"]),
        ("admin", Some(&["admin"]), &["t-minted"]),
        ("admin", Some(&["alice"]), &["t-minted"]),
        ("admin", Some(&["carol"]), &[]),
        ("alice", None, &["t-minted"]),
        ("bob", Some(&["admin"]), &["t-minted"]),
        ("eve", Some(&["admin"]), &[]),
    ];

    token_fixture!(dir, controller, secret);
    seed_admin_minted_token(&controller).await;
    seed_token()
        .controller(&controller)
        .token_id("t-own")
        .owner(kp("carol"))
        .call()
        .await;

    for (caller, owners, expected) in cases {
        let req = DescribeDelegationTokenRequest {
            owners: owners.map(|names| {
                names
                    .iter()
                    .map(|name| DescribeDelegationTokenOwner {
                        principal_type: "User".into(),
                        principal_name: (*name).into(),
                        ..Default::default()
                    })
                    .collect()
            }),
            ..Default::default()
        };
        let resp = describe_token_response!(
            &*controller,
            Some(&req),
            Some(&authed(caller)),
            Some(&secret)
        );
        let visible: std::collections::HashSet<&str> = token_ids(&resp);
        let expected: std::collections::HashSet<&str> = expected.iter().copied().collect();
        assert!(
            (resp.error_code, visible) == (0, expected),
            "caller {caller}, owners {owners:?}"
        );
    }
    controller.cancel().await;
}

/// The response names the requester that created the token, not the owner:
/// `DescribeDelegationTokenResponse` v3 `TokenRequesterPrincipalType` and
/// `TokenRequesterPrincipalName` (Kafka's `tokenInfo().tokenRequester()`).
#[tokio::test]
async fn describe_response_reports_the_requester_that_created_the_token() {
    use krabka_protocol::owned::describe_delegation_token_response::{
        DescribedDelegationToken, DescribedDelegationTokenRenewer,
    };

    token_fixture!(dir, controller, secret);
    seed_admin_minted_token(&controller).await;

    let resp = describe()
        .controller(&*controller)
        .authorizer(&simple_authz())
        .maybe_secret(Some(&secret))
        .call();
    assert!(
        resp.tokens
            == vec![DescribedDelegationToken {
                principal_type: "User".into(),
                principal_name: "alice".into(),
                token_requester_principal_type: "User".into(),
                token_requester_principal_name: "admin".into(),
                issue_timestamp: 1_000,
                expiry_timestamp: 2_000,
                max_timestamp: 3_000,
                token_id: "t-minted".into(),
                hmac: bytes::Bytes::from(krabka_security::compute_token_hmac(b"k", "t-minted")),
                renewers: vec![DescribedDelegationTokenRenewer {
                    principal_type: "User".into(),
                    principal_name: "bob".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
    controller.cancel().await;
}

#[bon::builder]
fn check_description_visibility(
    controller: &dyn MetadataSource,
    authorizer: &dyn Authorizer,
    request: Option<&'_ DescribeDelegationTokenRequest>,
    authentication: Option<&'_ ConnectionAuth>,
    secret: Option<&'_ SecretBytes>,
    expected: &[&str],
) {
    let setup = DescribeTokenSetup {
        request,
        authentication,
        secret,
    };
    check_visible_token_ids(
        &describe()
            .controller(controller)
            .authorizer(authorizer)
            .maybe_request(setup.request)
            .maybe_authentication(setup.authentication)
            .maybe_secret(setup.secret)
            .call(),
        expected,
    );
}
