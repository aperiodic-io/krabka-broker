//! Request builders, the broker harness, and the single-row helpers that the
//! `AlterUserScramCredentials` tests share.
//!
//! The validation, record, response, and end-to-end tests build the same rows
//! and assert on the same result shape, so the fixtures live in one module
//! rather than being duplicated per test file.

use {
    super::{
        records::{delete_record, upsertion_record},
        response::{err_result, ok_result},
        validation::{validate_deletion, validate_upsertion},
    },
    crate::{
        authorizer::Authorizer,
        broker::{Broker, BrokerHandle},
        test_support::start_broker_with,
    },
    bytes::Bytes,
    krabka_metadata::MetadataRecord,
    krabka_protocol::owned::{
        alter_user_scram_credentials_request::{ScramCredentialDeletion, ScramCredentialUpsertion},
        alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
    },
    krabka_security::{SaslMechanism, scram::MIN_SCRAM_ITERATIONS},
    std::sync::Arc,
};

pub(super) const KAFKA_DUPLICATE_RESOURCE: i16 = 92;
pub(super) const KAFKA_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
pub(super) const KAFKA_UNACCEPTABLE_CREDENTIAL: i16 = 93;
pub(super) const KAFKA_MAX_SCRAM_ITERATIONS: i32 = 16_384;

pub(super) fn valid_upsertion(name: &str) -> ScramCredentialUpsertion {
    valid_upsertion_for_mechanism(name, 1, SaslMechanism::ScramSha256)
}

pub(super) fn valid_upsertion_for_mechanism(
    name: &str,
    wire_mechanism: i8,
    mechanism: SaslMechanism,
) -> ScramCredentialUpsertion {
    ScramCredentialUpsertion {
        name: name.into(),
        mechanism: wire_mechanism,
        iterations: MIN_SCRAM_ITERATIONS,
        salt: Bytes::from_static(b"salt"),
        salted_password: Bytes::from(vec![7; krabka_security::scram_hash_len(mechanism)]),
        ..Default::default()
    }
}

pub(super) fn deletion(name: &str) -> ScramCredentialDeletion {
    ScramCredentialDeletion {
        name: name.into(),
        mechanism: 1,
        ..Default::default()
    }
}

pub(super) fn mixed_mechanisms(name: &str) -> super::AlterUserScramCredentialsRequest {
    super::AlterUserScramCredentialsRequest {
        deletions: vec![ScramCredentialDeletion {
            name: name.into(),
            mechanism: 2,
            ..Default::default()
        }],
        upsertions: vec![valid_upsertion_for_mechanism(
            name,
            1,
            SaslMechanism::ScramSha256,
        )],
        ..Default::default()
    }
}

/// A fully-pinned per-user result row, as the handler renders it.
pub(super) fn expected_result(
    user: &str,
    error_code: i16,
    error_message: Option<&str>,
) -> AlterUserScramCredentialsResult {
    tagged_wire!(AlterUserScramCredentialsResult {
        user: user.into(),
        error_code,
        error_message: error_message.map(Into::into),
    })
}

pub(super) fn expected_response(
    results: Vec<AlterUserScramCredentialsResult>,
) -> super::AlterUserScramCredentialsResponse {
    unthrottled_wire!(super::AlterUserScramCredentialsResponse { results })
}

pub(super) async fn low_metadata_version(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: crate::features::METADATA_VERSION.to_string(),
                level: krabka_metadata::metadata_version::SCRAM_MIN_LEVEL - 1,
            },
        )])
        .await
        .expect("seed low metadata.version");
}

crate::test_support::context_helper!(pub(super) client_id = "admin-client");

pub(super) async fn start_broker(
    authorizer: Arc<dyn Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.authorizer = authorizer;
    })
    .await
}

/// Validates one deletion and accepts it if it is valid. It returns the
/// per-user result row for the response, and on accept it pushes the metadata
/// record to `records`.
pub(super) fn process_deletion(
    broker: &Broker,
    d: ScramCredentialDeletion,
    authorized: bool,
    records: &mut Vec<MetadataRecord>,
) -> AlterUserScramCredentialsResult {
    let mech = match validate_deletion(broker, &d, authorized) {
        Ok(mech) => mech,
        Err(error) => return err_result(d.name, error.code, error.message),
    };
    records.push(delete_record(&d, mech));
    ok_result(d.name)
}

/// Validates one upsertion and accepts it if it is valid. It returns the
/// per-user result row for the response, and on accept it pushes the metadata
/// record to `records`.
pub(super) fn process_upsertion(
    u: ScramCredentialUpsertion,
    authorized: bool,
    records: &mut Vec<MetadataRecord>,
) -> AlterUserScramCredentialsResult {
    let mech = match validate_upsertion(&u, authorized) {
        Ok(mech) => mech,
        Err(error) => return err_result(u.name, error.code, error.message),
    };
    records.push(upsertion_record(&u, mech));
    ok_result(u.name)
}
