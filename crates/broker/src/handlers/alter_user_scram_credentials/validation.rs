//! Per-row validation of one SCRAM deletion or one SCRAM upsertion.
//!
//! Each row is checked on its own, and in the order Kafka checks it, because
//! the first failing check decides the per-user error code that the response
//! carries. The module also maps the KIP-554 wire mechanism byte to a
//! [`SaslMechanism`], because an unknown byte is itself a per-row error.

use {
    crate::{broker::Broker, codes},
    krabka_protocol::owned::alter_user_scram_credentials_request::{
        ScramCredentialDeletion, ScramCredentialUpsertion,
    },
    krabka_security::{
        SaslMechanism,
        scram::{MAX_SCRAM_ITERATIONS, MIN_SCRAM_ITERATIONS},
    },
    krabka_verified::{
        ScramAlterationDecision, ScramAlterationFacts, ScramAlterationKind, ScramPriorState,
        scram_alteration_decision,
    },
};

const EMPTY_USERNAME_MESSAGE: &str = "Username must not be empty";

/// The row message of a cluster `Alter` refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs ALTER permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
pub(super) const CLUSTER_ALTER_DENIED_MESSAGE: &str =
    "Request AlterUserScramCredentials needs ALTER permission.";

/// `ScramControlManager`'s refusal when `metadata.version` predates SCRAM.
pub(super) const SCRAM_UNSUPPORTED_MESSAGE: &str =
    "The current metadata.version does not support SCRAM";

/// KIP-554 wire byte that identifies a SCRAM mechanism. See
/// [`wire_to_mech`].
type MechanismWireByte = i8;

#[derive(Debug)]
pub(super) struct AlterationError {
    pub(super) code: i16,
    pub(super) message: &'static str,
}

#[cfg(test)]
pub(super) fn validate_deletion(
    broker: &Broker,
    deletion: &ScramCredentialDeletion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    let decision = decide_deletion(broker, deletion, authorized, ScramPriorState::Unseen);
    decision_mechanism(decision)
        .ok_or_else(|| decision_error(decision).expect("a first-row rejection has an error"))
}

pub(super) fn decide_deletion(
    broker: &Broker,
    deletion: &ScramCredentialDeletion,
    authorized: bool,
    prior: ScramPriorState,
) -> ScramAlterationDecision {
    let deletion_target_exists = wire_to_mech(deletion.mechanism).is_some_and(|mechanism| {
        broker
            .controller
            .current_image()
            .scram_credential(&deletion.name, mechanism)
            .is_some()
    });
    scram_alteration_decision(ScramAlterationFacts {
        kind: ScramAlterationKind::Delete,
        prior,
        authorized,
        name_empty: deletion.name.is_empty(),
        mechanism: deletion.mechanism,
        iterations: 0,
        min_iterations: MIN_SCRAM_ITERATIONS,
        max_iterations: MAX_SCRAM_ITERATIONS,
        deletion_target_exists,
    })
}

#[cfg(test)]
pub(super) fn validate_upsertion(
    upsertion: &ScramCredentialUpsertion,
    authorized: bool,
) -> Result<SaslMechanism, AlterationError> {
    let decision = decide_upsertion(upsertion, authorized, ScramPriorState::Unseen);
    decision_mechanism(decision)
        .ok_or_else(|| decision_error(decision).expect("a first-row rejection has an error"))
}

pub(super) fn decide_upsertion(
    upsertion: &ScramCredentialUpsertion,
    authorized: bool,
    prior: ScramPriorState,
) -> ScramAlterationDecision {
    scram_alteration_decision(ScramAlterationFacts {
        kind: ScramAlterationKind::Upsert,
        prior,
        authorized,
        name_empty: upsertion.name.is_empty(),
        mechanism: upsertion.mechanism,
        iterations: upsertion.iterations,
        min_iterations: MIN_SCRAM_ITERATIONS,
        max_iterations: MAX_SCRAM_ITERATIONS,
        deletion_target_exists: false,
    })
}

pub(super) fn decision_mechanism(decision: ScramAlterationDecision) -> Option<SaslMechanism> {
    match decision {
        ScramAlterationDecision::AcceptSha256 => Some(SaslMechanism::ScramSha256),
        ScramAlterationDecision::AcceptSha512 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

pub(super) fn decision_error(decision: ScramAlterationDecision) -> Option<AlterationError> {
    let error = match decision {
        ScramAlterationDecision::KeepPriorError
        | ScramAlterationDecision::AcceptSha256
        | ScramAlterationDecision::AcceptSha512 => return None,
        ScramAlterationDecision::Duplicate => AlterationError {
            code: codes::DUPLICATE_RESOURCE,
            message: "A user credential cannot be altered twice in the same request",
        },
        ScramAlterationDecision::Unauthorized => AlterationError {
            code: codes::CLUSTER_AUTHORIZATION_FAILED,
            message: CLUSTER_ALTER_DENIED_MESSAGE,
        },
        ScramAlterationDecision::EmptyName => AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: EMPTY_USERNAME_MESSAGE,
        },
        ScramAlterationDecision::UnsupportedMechanism => AlterationError {
            code: codes::UNSUPPORTED_SASL_MECHANISM,
            message: "Unknown SCRAM mechanism",
        },
        ScramAlterationDecision::TooFewIterations => AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "Too few iterations",
        },
        ScramAlterationDecision::TooManyIterations => AlterationError {
            code: codes::UNACCEPTABLE_CREDENTIAL,
            message: "Too many iterations",
        },
        ScramAlterationDecision::MissingCredential => AlterationError {
            code: codes::RESOURCE_NOT_FOUND,
            message: "Attempt to delete a user credential that does not exist",
        },
    };
    Some(error)
}

/// Maps the KIP-554 wire mechanism byte to a [`SaslMechanism`].
///
/// Per KIP-554:
/// - `0`: unknown, reserved
/// - `1`: SCRAM-SHA-256
/// - `2`: SCRAM-SHA-512
fn wire_to_mech(wire: MechanismWireByte) -> Option<SaslMechanism> {
    match wire {
        1 => Some(SaslMechanism::ScramSha256),
        2 => Some(SaslMechanism::ScramSha512),
        _ => None,
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            broker::{Broker, BrokerHandle},
            handlers::{
                alter_user_scram_credentials::{
                    answer,
                    test_support::{
                        KAFKA_MAX_SCRAM_ITERATIONS, KAFKA_UNACCEPTABLE_CREDENTIAL,
                        KAFKA_UNSUPPORTED_SASL_MECHANISM, deletion, expected_response,
                        expected_result, process_deletion, process_upsertion, start_broker,
                        test_context, valid_upsertion,
                    },
                },
                test_support::allow_all_authorizer,
            },
        },
        assert2::assert,
        krabka_metadata::{MetadataRecord, ScramCredentialRecord},
        krabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest,
        std::sync::Arc,
    };

    /// Deletion validation starts with an empty pending-record buffer.
    async fn deletion_fixture() -> (
        BrokerHandle,
        tempfile::TempDir,
        Arc<Broker>,
        Vec<MetadataRecord>,
    ) {
        let (handle, directory) = start_broker(allow_all_authorizer()).await;
        let broker = handle.broker_arc_for_test();
        let records = Vec::new();
        (handle, directory, broker, records)
    }

    #[test]
    fn wire_to_mech_maps_both_scram_variants() {
        let cases = [
            (1, Some(SaslMechanism::ScramSha256)),
            (2, Some(SaslMechanism::ScramSha512)),
            (0, None),
            (99, None),
        ];
        for (wire, expected) in cases {
            assert!(wire_to_mech(wire) == expected, "wire {wire}");
        }
    }

    #[test]
    fn process_upsertion_rejects_unknown_mechanism_with_unsupported_sasl_mechanism() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.mechanism = 99;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNSUPPORTED_SASL_MECHANISM,
                Some("Unknown SCRAM mechanism"),
            )
        );
        assert!(records.is_empty());
    }

    #[test]
    fn process_upsertion_rejects_iterations_above_kafka_maximum() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.iterations = KAFKA_MAX_SCRAM_ITERATIONS + 1;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNACCEPTABLE_CREDENTIAL,
                Some("Too many iterations"),
            )
        );
        assert!(records.is_empty());
    }

    #[test]
    fn process_upsertion_allows_kafka_maximum_iterations() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("alice");
        upsertion.iterations = KAFKA_MAX_SCRAM_ITERATIONS;

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("alice", 0, None));
        assert!(records.len() == 1);
    }

    #[test]
    fn process_upsertion_validates_boundaries_and_records_success() {
        let mut records = Vec::new();

        let rejections = [(
            {
                let mut u = valid_upsertion("too-few");
                u.iterations = MIN_SCRAM_ITERATIONS - 1;
                u
            },
            "Too few iterations",
        )];
        for (upsertion, msg) in rejections {
            let user = upsertion.name.clone();
            let r = process_upsertion(upsertion, true, &mut records);
            assert!(
                r == expected_result(&user, codes::UNACCEPTABLE_CREDENTIAL, Some(msg)),
                "case: {user}"
            );
            assert!(records.is_empty(), "case: {user}");
        }

        let r = process_upsertion(valid_upsertion("alice"), true, &mut records);
        assert!(r == expected_result("alice", 0, None));
        let (stored_key, server_key) = krabka_security::derive_keys_from_salted(
            SaslMechanism::ScramSha256,
            &valid_upsertion("alice").salted_password,
        );
        let expected_records = vec![MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha256,
            salt: b"salt".to_vec(),
            stored_key,
            server_key,
            iterations: u32::try_from(MIN_SCRAM_ITERATIONS).expect("min fits"),
        })];
        assert!(records == expected_records);
    }

    #[test]
    fn process_upsertion_rejects_unauthorized_users() {
        let mut records = Vec::new();

        let r = process_upsertion(valid_upsertion("bob"), false, &mut records);
        let expected = expected_result(
            "bob",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("Request AlterUserScramCredentials needs ALTER permission."),
        );
        assert!(r == expected);
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn process_deletion_rejects_missing_credentials() {
        let (broker_handle, _dir, broker, mut records) = deletion_fixture().await;

        let r = process_deletion(&broker, deletion("alice"), true, &mut records);
        assert!(
            r.error_code == 91,
            "missing SCRAM deletion target must use Kafka RESOURCE_NOT_FOUND (91), got {}",
            r.error_code
        );
        let expected = expected_result(
            "alice",
            codes::RESOURCE_NOT_FOUND,
            Some("Attempt to delete a user credential that does not exist"),
        );
        assert!(r == expected);
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn process_deletion_rejects_unauthorized_users() {
        let (broker_handle, _dir, broker, mut records) = deletion_fixture().await;

        let r = process_deletion(&broker, deletion("alice"), false, &mut records);
        let expected = expected_result(
            "alice",
            codes::CLUSTER_AUTHORIZATION_FAILED,
            Some("Request AlterUserScramCredentials needs ALTER permission."),
        );
        assert!(r == expected);
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn process_deletion_rejects_unknown_mechanism_with_unsupported_sasl_mechanism() {
        let (broker_handle, _dir, broker, mut records) = deletion_fixture().await;
        let mut deletion = deletion("alice");
        deletion.mechanism = 99;

        let r = process_deletion(&broker, deletion, true, &mut records);

        assert!(
            r == expected_result(
                "alice",
                KAFKA_UNSUPPORTED_SASL_MECHANISM,
                Some("Unknown SCRAM mechanism"),
            )
        );
        assert!(records.is_empty());
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_empty_deletion_username_is_unacceptable_credential() {
        check_empty_username(AlterUserScramCredentialsRequest {
            deletions: vec![deletion("")],
            ..Default::default()
        })
        .await;
    }

    #[tokio::test]
    async fn handle_empty_upsertion_username_is_unacceptable_credential() {
        check_empty_username(AlterUserScramCredentialsRequest {
            upsertions: vec![valid_upsertion("")],
            ..Default::default()
        })
        .await;
    }

    async fn check_empty_username(req: AlterUserScramCredentialsRequest) {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            ready controller_leader;
            user ["admin"];
        };
        let resp = answer(&broker, req, &ctx).await;
        let expected = expected_response(vec![expected_result(
            "",
            KAFKA_UNACCEPTABLE_CREDENTIAL,
            Some("Username must not be empty"),
        )]);
        assert!(resp == expected);
        assert!(
            broker
                .controller
                .current_image()
                .scram_credential("", SaslMechanism::ScramSha256)
                .is_none()
        );
        broker_handle.shutdown().await;
    }
}
