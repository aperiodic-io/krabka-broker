//! Staging of the requested deletions and upsertions into one plan.
//!
//! The rows are walked in Kafka's order, the deletions and then the
//! upsertions, and each user keeps the first validation or resource error it
//! hit. A second alteration for a user whose earlier row is still accepted
//! becomes `DUPLICATE_RESOURCE` instead. The stage returns the per-user
//! result rows in first-seen order, and the metadata records of the accepted
//! rows.

use {
    super::{
        records::{delete_record, upsertion_record},
        response::{err_result, ok_result},
        validation::{
            AlterationError, decide_deletion, decide_upsertion, decision_error, decision_mechanism,
        },
    },
    crate::broker::Broker,
    krabka_metadata::MetadataRecord,
    krabka_protocol::owned::{
        alter_user_scram_credentials_request::{
            AlterUserScramCredentialsRequest, ScramCredentialDeletion, ScramCredentialUpsertion,
        },
        alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
    },
    krabka_security::SaslMechanism,
    krabka_verified::{ScramAlterationDecision, ScramPriorState},
    std::collections::HashMap,
};

pub(super) struct AlterationPlan {
    pub(super) user_results: Vec<AlterUserScramCredentialsResult>,
    pub(super) records: Vec<MetadataRecord>,
}

/// A deletion that passed validation, with the mechanism it removes.
struct StagedDeletion {
    deletion: ScramCredentialDeletion,
    mechanism: SaslMechanism,
}

/// An upsertion that passed validation, with the mechanism it writes.
struct StagedUpsertion {
    upsertion: ScramCredentialUpsertion,
    mechanism: SaslMechanism,
}

pub(super) fn plan_alterations(
    broker: &Broker,
    req: AlterUserScramCredentialsRequest,
    authorized: bool,
) -> AlterationPlan {
    let mut user_order = Vec::new();
    let mut deletions = HashMap::new();
    let mut upsertions = HashMap::new();
    let mut errors = HashMap::new();

    for deletion in req.deletions {
        remember_user(&mut user_order, &deletion.name);
        stage_deletion(broker, deletion, authorized, &mut deletions, &mut errors);
    }

    for upsertion in req.upsertions {
        remember_user(&mut user_order, &upsertion.name);
        stage_upsertion(
            upsertion,
            authorized,
            &mut deletions,
            &mut upsertions,
            &mut errors,
        );
    }

    let mut user_results = Vec::with_capacity(user_order.len());
    let mut records = Vec::new();
    for user in user_order {
        if let Some(error) = errors.remove(&user) {
            user_results.push(err_result(user, error.code, error.message));
            continue;
        }

        if let Some(StagedDeletion {
            deletion,
            mechanism,
        }) = deletions.remove(&user)
        {
            records.push(delete_record(&deletion, mechanism));
            user_results.push(ok_result(deletion.name));
            continue;
        }

        if let Some(StagedUpsertion {
            upsertion,
            mechanism,
        }) = upsertions.remove(&user)
        {
            records.push(upsertion_record(&upsertion, mechanism));
            user_results.push(ok_result(upsertion.name));
        }
    }

    AlterationPlan {
        user_results,
        records,
    }
}

fn remember_user(user_order: &mut Vec<String>, user: &str) {
    if user_order.iter().any(|seen| seen == user) {
        return;
    }
    user_order.push(user.to_string());
}

pub(super) fn distinct_requested_users(req: &AlterUserScramCredentialsRequest) -> Vec<String> {
    let mut users = Vec::new();
    for deletion in &req.deletions {
        remember_user(&mut users, &deletion.name);
    }
    for upsertion in &req.upsertions {
        remember_user(&mut users, &upsertion.name);
    }
    users
}

fn stage_deletion(
    broker: &Broker,
    deletion: ScramCredentialDeletion,
    authorized: bool,
    deletions: &mut HashMap<String, StagedDeletion>,
    errors: &mut HashMap<String, AlterationError>,
) {
    let prior = prior_state(
        &deletion.name,
        deletions.contains_key(&deletion.name),
        false,
        errors,
    );
    let decision = decide_deletion(broker, &deletion, authorized, prior);
    match decision {
        ScramAlterationDecision::KeepPriorError => {}
        ScramAlterationDecision::Duplicate => {
            deletions.remove(&deletion.name);
            let error = decision_error(decision).expect("duplicate has an error");
            errors.insert(deletion.name, error);
        }
        ScramAlterationDecision::AcceptSha256 | ScramAlterationDecision::AcceptSha512 => {
            let mechanism =
                decision_mechanism(decision).expect("accepted decision has a mechanism");
            deletions.insert(deletion.name.clone(), StagedDeletion {
                deletion,
                mechanism,
            });
        }
        _ => {
            let error = decision_error(decision).expect("rejected decision has an error");
            errors.insert(deletion.name, error);
        }
    }
}

fn prior_state(
    user: &str,
    has_deletion: bool,
    has_upsertion: bool,
    errors: &HashMap<String, AlterationError>,
) -> ScramPriorState {
    if errors.contains_key(user) {
        ScramPriorState::Rejected
    } else if has_deletion || has_upsertion {
        ScramPriorState::Accepted
    } else {
        ScramPriorState::Unseen
    }
}

fn stage_upsertion(
    upsertion: ScramCredentialUpsertion,
    authorized: bool,
    deletions: &mut HashMap<String, StagedDeletion>,
    upsertions: &mut HashMap<String, StagedUpsertion>,
    errors: &mut HashMap<String, AlterationError>,
) {
    let prior = prior_state(
        &upsertion.name,
        deletions.contains_key(&upsertion.name),
        upsertions.contains_key(&upsertion.name),
        errors,
    );
    let decision = decide_upsertion(&upsertion, authorized, prior);
    match decision {
        ScramAlterationDecision::KeepPriorError => {}
        ScramAlterationDecision::Duplicate => {
            deletions.remove(&upsertion.name);
            upsertions.remove(&upsertion.name);
            let error = decision_error(decision).expect("duplicate has an error");
            errors.insert(upsertion.name, error);
        }
        ScramAlterationDecision::AcceptSha256 | ScramAlterationDecision::AcceptSha512 => {
            let mechanism =
                decision_mechanism(decision).expect("accepted decision has a mechanism");
            upsertions.insert(upsertion.name.clone(), StagedUpsertion {
                upsertion,
                mechanism,
            });
        }
        _ => {
            let error = decision_error(decision).expect("rejected decision has an error");
            errors.insert(upsertion.name, error);
        }
    }
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            codes,
            handlers::alter_user_scram_credentials::{
                answer,
                test_support::{
                    KAFKA_DUPLICATE_RESOURCE, expected_result, mixed_mechanisms, start_broker,
                    test_context, valid_upsertion, valid_upsertion_for_mechanism,
                },
            },
        },
        assert2::assert,
        krabka_metadata::ScramCredentialRecord,
        krabka_security::scram::MIN_SCRAM_ITERATIONS,
    };

    /// Check the response and unchanged credential image for a refused request.
    macro_rules! check_refusal {
        (($response:ident, $expected:ident, $image:ident), $broker:ident, $request:ident, $context:ident,
            $code:expr, $message:expr, [$($mechanism:expr => $state:ident),+ $(,)?]) => {
            let $response = answer(&$broker, $request, &$context).await;
            let $expected = crate::handlers::alter_user_scram_credentials::test_support::expected_response(vec![
                expected_result("alice", $code, Some($message)),
            ]);
            assert!($response == $expected);
            let $image = $broker.controller.current_image();
            $(assert!($image.scram_credential("alice", $mechanism).$state());)+
        };
    }

    #[tokio::test]
    async fn plan_emits_one_record_per_success_and_is_retry_stable() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker] allow_all;
        };
        let mut overflow = valid_upsertion("overflow");
        overflow.iterations = i32::MAX;
        let req = AlterUserScramCredentialsRequest {
            upsertions: vec![
                valid_upsertion("accepted"),
                overflow,
                valid_upsertion_for_mechanism("duplicate", 1, SaslMechanism::ScramSha256),
                valid_upsertion_for_mechanism("duplicate", 2, SaslMechanism::ScramSha512),
            ],
            ..Default::default()
        };

        let first = plan_alterations(&broker, req.clone(), true);
        let retry = plan_alterations(&broker, req, true);

        assert!(first.user_results.len() == 3);
        assert!(first.user_results[0].error_code == 0);
        assert!(first.user_results[1].error_code == codes::UNACCEPTABLE_CREDENTIAL);
        assert!(first.user_results[2].error_code == codes::DUPLICATE_RESOURCE);
        assert!(first.records.len() == 1);
        let MetadataRecord::V1ScramCredential(record) = &first.records[0] else {
            panic!("accepted upsertion must emit a SCRAM credential record");
        };
        assert!(record.user == "accepted");
        assert!(retry.user_results == first.user_results);
        assert!(retry.records == first.records);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_across_upsertion_mechanisms_returns_one_error_row() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            ready controller_leader;
            user ["admin"];
        };
        let req = AlterUserScramCredentialsRequest {
            upsertions: vec![
                valid_upsertion_for_mechanism("alice", 1, SaslMechanism::ScramSha256),
                valid_upsertion_for_mechanism("alice", 2, SaslMechanism::ScramSha512),
            ],
            ..Default::default()
        };

        check_refusal!((resp, expected, image), broker, req, ctx, KAFKA_DUPLICATE_RESOURCE, "A user credential cannot be altered twice in the same request", [SaslMechanism::ScramSha256 => is_none, SaslMechanism::ScramSha512 => is_none]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_between_deletion_and_upsertion_returns_one_error_row() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker);
            boot [start_broker] allow_all;
            ready controller_leader;
        };
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1ScramCredential(
                ScramCredentialRecord {
                    user: "alice".into(),
                    mechanism: SaslMechanism::ScramSha512,
                    salt: b"salt".to_vec(),
                    stored_key: vec![1; 64],
                    server_key: vec![2; 64],
                    iterations: u32::try_from(MIN_SCRAM_ITERATIONS).expect("min fits"),
                },
            )])
            .await
            .expect("seed alice SCRAM credential");
        broker_handle
            .wait_for_image(|image| {
                image
                    .scram_credential("alice", SaslMechanism::ScramSha512)
                    .is_some()
            })
            .await;
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["admin"];
        };
        let req = mixed_mechanisms("alice");

        check_refusal!((resp, expected, image), broker, req, ctx, KAFKA_DUPLICATE_RESOURCE, "A user credential cannot be altered twice in the same request", [SaslMechanism::ScramSha512 => is_some, SaslMechanism::ScramSha256 => is_none]);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_duplicate_username_after_missing_deletion_preserves_resource_not_found() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] allow_all;
            ready controller_leader;
            user ["admin"];
        };
        let req = mixed_mechanisms("alice");

        check_refusal!((resp, expected, image), broker, req, ctx, codes::RESOURCE_NOT_FOUND, "Attempt to delete a user credential that does not exist", [SaslMechanism::ScramSha256 => is_none, SaslMechanism::ScramSha512 => is_none]);
        broker_handle.shutdown().await;
    }
}
