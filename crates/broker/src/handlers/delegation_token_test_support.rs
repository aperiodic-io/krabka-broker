//! Controller and identity fixtures shared by the KIP-48 token handlers.

use {
    crate::{
        codes::{
            DELEGATION_TOKEN_AUTH_DISABLED, DELEGATION_TOKEN_NOT_FOUND,
            DELEGATION_TOKEN_OWNER_MISMATCH, DELEGATION_TOKEN_REQUEST_NOT_ALLOWED,
        },
        network::auth::ConnectionAuth,
        test_support::{KafkaErrorCode, UnixMillis, principal},
        time_util::now_ms,
    },
    assert2::assert,
    krabka_metadata::{DelegationTokenRecord, MetadataRecord},
    krabka_raft::{Controller, ControllerConfig, ControllerHandle, NodeId},
    krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism},
    std::{path::PathBuf, sync::Arc, time::Duration},
};

/// The controller, directory and HMAC secret used by token wire fixtures.
pub(super) struct TokenFixture {
    pub(super) directory: tempfile::TempDir,
    pub(super) controller: Arc<ControllerHandle>,
    pub(super) secret: krabka_security::SecretBytes,
}

impl TokenFixture {
    pub(super) async fn new() -> Self {
        let ControllerResources {
            directory,
            controller,
        } = controller_resources().await;
        Self::with_controller(directory, controller)
    }

    fn with_controller(directory: tempfile::TempDir, controller: Arc<ControllerHandle>) -> Self {
        let secret = krabka_security::SecretBytes::new(b"k".to_vec());
        Self {
            directory,
            controller,
            secret,
        }
    }
}

struct ControllerResources {
    directory: tempfile::TempDir,
    controller: Arc<ControllerHandle>,
}
async fn controller_resources() -> ControllerResources {
    let directory = tempfile::TempDir::new().unwrap();
    let controller = test_controller(directory.path().into()).await;
    ControllerResources {
        directory,
        controller,
    }
}

#[derive(Clone, Copy, Default, derive_more::Display, derive_more::From, derive_more::Into)]
pub(super) struct TokenMutationIndex(pub usize);

/// Kafka's default `delegation.token.expiry.time.ms`.
pub(super) const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

pub(super) struct RefusalToken {
    pub(super) hmac: Vec<u8>,
    pub(super) expires_at: i64,
}
pub(super) struct RefusalTokens {
    pub(super) live: RefusalToken,
    pub(super) expired: RefusalToken,
}

pub(super) struct RefusalFixture {
    pub(super) resources: TokenFixture,
    pub(super) live: RefusalToken,
    pub(super) expired: RefusalToken,
}

impl RefusalFixture {
    pub(super) async fn new() -> Self {
        let ControllerResources {
            directory,
            controller,
        } = controller_resources().await;
        let RefusalTokens { live, expired } = refusal_tokens(&controller).await;
        Self {
            resources: TokenFixture::with_controller(directory, controller),
            live,
            expired,
        }
    }
}

pub(super) struct SeededToken {
    pub(super) token_id: String,
    pub(super) hmac: Vec<u8>,
    pub(super) maximum: i64,
}

/// Seed one mutation case against one wall-clock reading for both limits.
#[bon::builder]
pub(super) async fn seed_mutation_token(
    controller: &ControllerHandle,
    #[builder(into)] index: TokenMutationIndex,
    #[builder(into, default = TokenPeriodMillis(60_000))] expiry_delta: TokenPeriodMillis,
    #[builder(into, default = TokenPeriodMillis(DAY_MS))] maximum_delta: TokenPeriodMillis,
) -> SeededToken {
    let token_id = format!("tok-{}", index.0);
    let hmac = hmac_for(&token_id);
    let seeded_at = now_ms();
    let maximum = seeded_at + maximum_delta.0;
    let defaults = TokenSetup::for_mutation();
    seed_token()
        .controller(controller)
        .token_id(&token_id)
        .owner(defaults.owner)
        .maybe_requester(defaults.requester)
        .issued_at(defaults.issued_at)
        .expires_at(seeded_at + expiry_delta.0)
        .max_expires_at(maximum)
        .renewers(defaults.renewers)
        .call()
        .await;
    SeededToken {
        token_id,
        hmac,
        maximum,
    }
}

pub(super) struct MutationResponse<Response> {
    pub(super) actual: Response,
    pub(super) expected: Response,
    pub(super) case: &'static str,
}
/// Retain the token resources through each ordered refusal and the final storage check.
#[bon::builder]
pub(super) async fn check_refusals<Cases, Response>(
    cases: impl FnOnce(&RefusalToken, &RefusalToken) -> Cases,
    mut mutation: impl AsyncFnMut(
        &ControllerHandle,
        &krabka_security::SecretBytes,
        TokenMutationRefusalSetup,
    ) -> MutationResponse<Response>,
    verify: impl FnOnce(&ControllerHandle, &RefusalToken, &RefusalToken),
) where
    Cases: IntoIterator<Item = TokenMutationRefusalSetup>,
    Response: PartialEq + std::fmt::Debug,
{
    let RefusalFixture {
        resources:
            TokenFixture {
                directory: _directory,
                controller,
                secret,
            },
        live,
        expired,
    } = RefusalFixture::new().await;
    for case in cases(&live, &expired) {
        let MutationResponse {
            actual,
            expected,
            case,
        } = mutation(&controller, &secret, case).await;
        assert!(actual == expected, "{case}");
    }
    verify(&controller, &live, &expired);
    controller.cancel().await;
}

#[derive(Clone, Copy, Default)]
pub(super) enum SecretProvision {
    #[default]
    Configured,
    Absent,
}

impl SecretProvision {
    pub(super) fn get(
        self,
        secret: &krabka_security::SecretBytes,
    ) -> Option<&krabka_security::SecretBytes> {
        match self {
            Self::Configured => Some(secret),
            Self::Absent => None,
        }
    }
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub(super) struct TokenPeriodMillis(pub i64);

#[derive(krabka_macros::FieldDefaults)]
pub(super) struct TokenMutationRefusalSetup {
    #[default("token refusal")]
    pub name: &'static str,
    #[default(authed("alice"))]
    pub authentication: ConnectionAuth,
    pub secret: SecretProvision,
    pub hmac: Vec<u8>,
    #[default(TokenPeriodMillis(-1))]
    pub period: TokenPeriodMillis,
    pub error: KafkaErrorCode,
    pub expiry: UnixMillis,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct OwnershipRefusalSetup {
    #[default("foreign caller")]
    pub name: &'static str,
    #[default("eve")]
    pub caller: &'static str,
    #[default(TokenPeriodMillis(-1))]
    pub period: TokenPeriodMillis,
}

#[bon::builder]
pub(super) fn ownership_refusal(
    hmac: &[u8],
    #[builder(default = "foreign caller")] name: &'static str,
    #[builder(default = "eve")] caller: &'static str,
    #[builder(into, default = TokenPeriodMillis (- 1))] period: TokenPeriodMillis,
) -> TokenMutationRefusalSetup {
    let setup = OwnershipRefusalSetup {
        name,
        caller,
        period,
    };
    TokenMutationRefusalSetup {
        name: setup.name,
        authentication: authed(setup.caller),
        hmac: hmac.to_vec(),
        period: setup.period,
        error: KafkaErrorCode(DELEGATION_TOKEN_OWNER_MISMATCH),
        ..Default::default()
    }
}

#[derive(Clone, Copy)]
struct OwnershipRefusalIdentity {
    name: &'static str,
    caller: &'static str,
}

const ORDINARY_OWNERSHIP_REFUSALS: [OwnershipRefusalIdentity; 3] = [
    OwnershipRefusalIdentity {
        name: "foreign caller",
        caller: "eve",
    },
    OwnershipRefusalIdentity {
        name: "super user",
        caller: "admin",
    },
    OwnershipRefusalIdentity {
        name: "requester of a token created for another owner",
        caller: "minter",
    },
];

pub(super) fn ordinary_ownership_refusals(hmac: &[u8]) -> [TokenMutationRefusalSetup; 3] {
    ORDINARY_OWNERSHIP_REFUSALS.map(|identity| {
        ownership_refusal()
            .hmac(hmac)
            .name(identity.name)
            .caller(identity.caller)
            .call()
    })
}

/// The shared credential and HMAC refusals precede each API's ownership and expiry checks.
pub(super) fn mutation_credential_refusals(live_hmac: &[u8]) -> Vec<TokenMutationRefusalSetup> {
    use crate::test_support::{KafkaErrorCode, UnixMillis};
    let token_authenticated = [
        ("token-authenticated owner", SecretProvision::Configured),
        (
            "token-authenticated owner, tokens disabled",
            SecretProvision::Absent,
        ),
    ]
    .into_iter()
    .map(|(name, secret)| TokenMutationRefusalSetup {
        name,
        authentication: authed_with_token("alice", CredentialSource::DelegationToken),
        secret,
        hmac: live_hmac.to_vec(),
        error: KafkaErrorCode(DELEGATION_TOKEN_REQUEST_NOT_ALLOWED),
        expiry: UnixMillis(-1),
        ..Default::default()
    });
    token_authenticated
        .chain([
            TokenMutationRefusalSetup {
                name: "tokens disabled",
                secret: SecretProvision::Absent,
                hmac: live_hmac.to_vec(),
                error: KafkaErrorCode(DELEGATION_TOKEN_AUTH_DISABLED),
                ..Default::default()
            },
            TokenMutationRefusalSetup {
                name: "unknown hmac",
                hmac: vec![0xFF; 32],
                error: KafkaErrorCode(DELEGATION_TOKEN_NOT_FOUND),
                ..Default::default()
            },
            TokenMutationRefusalSetup {
                name: "hmac computed under another secret key",
                hmac: krabka_security::compute_token_hmac(b"other", "live"),
                error: KafkaErrorCode(DELEGATION_TOKEN_NOT_FOUND),
                ..Default::default()
            },
        ])
        .collect()
}

/// Starts a single-voter controller and waits for its leader.
pub(super) async fn test_controller(log_dir: PathBuf) -> Arc<ControllerHandle> {
    let config = ControllerConfig {
        election_timeout: krabka_units::millis(200),
        heartbeat_interval: Some(krabka_units::millis(50)),
        client_id: "test".into(),
        ..ControllerConfig::for_tests(NodeId(1), log_dir)
    };
    let controller = Arc::new(Controller::start(config).await.unwrap());
    let mut leader = controller.watch_leader();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while leader.borrow().is_none() {
        assert!(std::time::Instant::now() < deadline, "no leader in 5s");
        let _ = tokio::time::timeout(Duration::from_millis(100), leader.changed()).await;
    }
    controller
}

#[derive(Clone, Copy, Default)]
pub(super) enum CredentialSource {
    #[default]
    Password,
    DelegationToken,
}

/// An authenticated SCRAM connection, optionally using a delegation token.
pub(super) fn authed_with_token(name: &str, source: CredentialSource) -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        },
        mechanism: SaslMechanism::ScramSha256,
        expires_at_ms: None,
        authenticated_via_token: matches!(source, CredentialSource::DelegationToken),
    }
}

/// An authenticated SCRAM connection established with a delegation-token credential.
pub(super) fn token_authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, CredentialSource::DelegationToken)
}

pub(super) fn authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, CredentialSource::Password)
}

/// The synthetic authenticated state of PLAINTEXT and SSL without mTLS.
/// Token APIs must refuse it.
pub(super) fn anonymous() -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: principal("ANONYMOUS"),
        mechanism: SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

pub(super) fn kp(name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: "User".into(),
        name: name.into(),
    }
}

/// The HMAC the fixture secret key `k` gives `token_id`.
pub(super) fn hmac_for(token_id: &str) -> Vec<u8> {
    krabka_security::compute_token_hmac(b"k", token_id)
}

/// Independent token metadata shared by description and mutation fixtures.
pub(super) struct TokenSetup<'a> {
    pub token_id: &'a str,
    pub owner: KafkaPrincipal,
    pub requester: Option<KafkaPrincipal>,
    pub issued_at: UnixMillis,
    pub expires_at: UnixMillis,
    pub max_expires_at: UnixMillis,
    pub renewers: Vec<KafkaPrincipal>,
}

impl Default for TokenSetup<'_> {
    fn default() -> Self {
        Self {
            token_id: "t-a",
            owner: kp("alice"),
            requester: None,
            issued_at: UnixMillis(1_000),
            expires_at: UnixMillis(2_000),
            max_expires_at: UnixMillis(3_000),
            renewers: vec![],
        }
    }
}

impl TokenSetup<'_> {
    /// The mutation tests use the minter and Bob as requester and renewer.
    pub(super) fn for_mutation() -> Self {
        Self {
            requester: Some(kp("minter")),
            issued_at: UnixMillis(0),
            renewers: vec![kp("bob")],
            ..Self::default()
        }
    }
}

#[bon::builder]
pub(super) async fn seed_token(
    controller: &ControllerHandle,
    #[builder(default = TokenSetup::default().token_id)] token_id: &'_ str,
    #[builder(default = TokenSetup::default().owner)] owner: KafkaPrincipal,
    requester: Option<KafkaPrincipal>,
    #[builder(into, default = TokenSetup::default().issued_at)] issued_at: UnixMillis,
    #[builder(into, default = TokenSetup::default().expires_at)] expires_at: UnixMillis,
    #[builder(into, default = TokenSetup::default().max_expires_at)] max_expires_at: UnixMillis,
    #[builder(default = TokenSetup::default().renewers)] renewers: Vec<KafkaPrincipal>,
) {
    let setup = TokenSetup {
        token_id,
        owner,
        requester,
        issued_at,
        expires_at,
        max_expires_at,
        renewers,
    };
    let TokenSetup {
        token_id,
        owner,
        requester,
        issued_at,
        expires_at,
        max_expires_at,
        renewers,
    } = setup;
    let token = DelegationTokenRecord {
        token_id: token_id.into(),
        requester: requester.unwrap_or_else(|| owner.clone()),
        owner,
        issue_timestamp_ms: issued_at.0,
        expiry_timestamp_ms: expires_at.0,
        max_timestamp_ms: max_expires_at.0,
        renewers,
    };
    controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(token)])
        .await
        .expect("seed token");
}

/// A live token and one just expired, both with the same future maximum.
pub(super) async fn refusal_tokens(controller: &ControllerHandle) -> RefusalTokens {
    struct TokenToSeed<'a> {
        id: &'a str,
        token: &'a RefusalToken,
    }
    let now = now_ms();
    let live = RefusalToken {
        hmac: hmac_for("live"),
        expires_at: now + 60_000,
    };
    let expired = RefusalToken {
        hmac: hmac_for("expired"),
        expires_at: now - 1,
    };

    for seed in [
        TokenToSeed {
            id: "live",
            token: &live,
        },
        TokenToSeed {
            id: "expired",
            token: &expired,
        },
    ] {
        seed_token()
            .controller(controller)
            .token_id(seed.id)
            .owner(TokenSetup::for_mutation().owner)
            .maybe_requester(TokenSetup::for_mutation().requester)
            .issued_at(TokenSetup::for_mutation().issued_at)
            .expires_at(seed.token.expires_at)
            .max_expires_at(now + DAY_MS)
            .renewers(TokenSetup::for_mutation().renewers)
            .call()
            .await;
    }
    RefusalTokens { live, expired }
}

#[derive(Clone, Copy, Default)]
pub(super) struct TokenMutationWindow {
    before: UnixMillis,
    after: UnixMillis,
}

pub(super) struct MutationExecution<Response> {
    pub(super) window: TokenMutationWindow,
    pub(super) response: Response,
}
/// Pin independent wall-clock bounds around execution of a token mutation.
pub(super) async fn in_mutation_window<F: std::future::Future>(
    mutation: F,
) -> MutationExecution<F::Output> {
    let before = UnixMillis(now_ms());
    let response = mutation.await;
    let after = UnixMillis(now_ms());
    MutationExecution {
        window: TokenMutationWindow { before, after },
        response,
    }
}

#[derive(Clone, Copy, Default)]
pub(super) enum TokenPersistence {
    #[default]
    Deleted,
    Stored,
}

#[derive(Clone, Copy)]
pub(super) enum TokenExpiryRule {
    RelativeToMutation(TokenPeriodMillis),
    LifetimeCap,
}

impl TokenExpiryRule {
    pub fn with_cap(self, maximum: UnixMillis) -> ExpiryExpectation {
        match self {
            Self::RelativeToMutation(period) => ExpiryExpectation::RelativeToMutation(period),
            Self::LifetimeCap => ExpiryExpectation::LifetimeCap(maximum),
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum ExpiryExpectation {
    RelativeToMutation(TokenPeriodMillis),
    LifetimeCap(UnixMillis),
}

impl ExpiryExpectation {
    pub fn for_period(period: Option<TokenPeriodMillis>, maximum: UnixMillis) -> Self {
        match period {
            Some(period) => Self::RelativeToMutation(period),
            None => Self::LifetimeCap(maximum),
        }
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct TokenExpirySetup {
    pub window: TokenMutationWindow,
    #[default(ExpiryExpectation::LifetimeCap(crate::test_support::UnixMillis(0)))]
    pub expectation: ExpiryExpectation,
}

#[derive(Clone, Copy)]
pub(super) struct TokenMutationOutcome {
    pub error: KafkaErrorCode,
    pub expiry: UnixMillis,
}

impl TokenMutationOutcome {
    pub(super) fn from_wire(error_code: i16, expiry_timestamp_ms: i64) -> Self {
        Self {
            error: KafkaErrorCode(error_code),
            expiry: UnixMillis(expiry_timestamp_ms),
        }
    }
}

impl TokenExpirySetup {
    pub(super) fn check(self, case: &str, outcome: TokenMutationOutcome) {
        check_expiry_window()
            .case(case)
            .outcome(outcome)
            .window(self.window)
            .expectation(self.expectation)
            .call();
    }
}
/// The expiry lies in the independently pinned clock window, or equals the lifetime cap.
#[bon::builder]
pub(super) fn check_expiry_window(
    case: &str,
    outcome: TokenMutationOutcome,
    #[builder(default)] window: TokenMutationWindow,
    #[builder(default = ExpiryExpectation :: LifetimeCap (crate :: test_support :: UnixMillis (0)))]
    expectation: ExpiryExpectation,
) {
    let setup = TokenExpirySetup {
        window,
        expectation,
    };
    assert!(outcome.error.0 == crate::codes::NONE, "{case}");
    match setup.expectation {
        ExpiryExpectation::RelativeToMutation(delta) => {
            let before = setup.window.before.0;
            let after = setup.window.after.0;
            assert!(
                (before + delta.0..=after + delta.0).contains(&outcome.expiry.0),
                "{case}: {} not in [{}, {}]",
                outcome.expiry.0,
                before + delta.0,
                after + delta.0
            );
        }
        ExpiryExpectation::LifetimeCap(maximum) => assert!(outcome.expiry == maximum, "{case}"),
    }
}

pub(super) fn stored_expiry(controller: &ControllerHandle, token_id: &str) -> Option<i64> {
    controller
        .current_image()
        .delegation_token_by_id(token_id)
        .map(|token| token.expiry_timestamp_ms)
}
