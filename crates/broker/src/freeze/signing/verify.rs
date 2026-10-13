//! The six rules that a detached operator signature passes.
//!
//! The rules run in one order and stop at the first failure: the trust set
//! knows the `key_id`, the key speaks for the author the record claims, the
//! connection authenticated that same author, `set_at_ms` sits inside the skew
//! window, `set_at_ms` is newer than the entry the record replaces, and the
//! signature verifies over the canonical bytes.

use {
    super::{SignatureRefusal, freeze_signing_bytes},
    crate::operator_keys::OperatorKeys,
    krabka_metadata::TopicFreezeRecord,
    krabka_units::{Time, convert::TimeExt as _},
    krabka_verified::{
        FreezeIdentityState, FreezeSignatureDecision, FreezeSignatureFacts,
        freeze_signature_decision,
    },
};

/// Everything outside the record that a signature check reads.
pub(crate) struct FreezeSignatureCheck<'a> {
    /// The operator key trust set from `[[operator_keys]]`.
    pub keys: &'a OperatorKeys,
    /// The cluster this broker belongs to, in string form.
    pub cluster_id: &'a str,
    /// The principal the broker authenticated on the connection.
    pub connection_principal: &'a str,
    /// How far `set_at_ms` may sit from `now_ms`, from
    /// `freeze.signature_max_skew`.
    pub max_skew: Time,
    /// This broker's clock, in milliseconds since the Unix epoch.
    pub now_ms: i64,
    /// The live registry entry that the incoming record replaces, when there
    /// is one. Its `set_at_ms` is the floor that the incoming timestamp must
    /// pass.
    pub replaces: Option<&'a TopicFreezeRecord>,
}

/// Verify the detached operator signature that `record` carries.
///
/// The function is the one place that holds all six rules. It checks that the
/// trust set knows the `key_id`, that the claimed author is the principal
/// bound to that key, that the same author is the principal on the connection,
/// that `set_at_ms` sits inside the skew window, that `set_at_ms` is newer than
/// the entry the record replaces, and that the signature verifies over
/// [`freeze_signing_bytes`].
///
/// # Errors
///
/// Returns the [`SignatureRefusal`] of the first rule that fails. Every one of
/// them carries `OPERATOR_SIGNATURE_INVALID` (1009).
pub(crate) fn verify_freeze_signature(
    check: &FreezeSignatureCheck<'_>,
    record: &TopicFreezeRecord,
) -> Result<(), SignatureRefusal> {
    let identity = match check.keys.get(&record.key_id) {
        None => FreezeIdentityState::UnknownKey,
        Some(key) if key.principal() != record.set_by => FreezeIdentityState::WrongKeyPrincipal,
        Some(_) if record.set_by != check.connection_principal => {
            FreezeIdentityState::WrongConnectionPrincipal
        }
        Some(_) => FreezeIdentityState::Bound,
    };
    let mut facts = FreezeSignatureFacts {
        identity,
        set_at_ms: record.set_at_ms,
        now_ms: check.now_ms,
        max_skew_ms: check.max_skew.millis_i64(),
        replaces: check.replaces.is_some(),
        replaced_set_at_ms: check.replaces.map_or(0, |replaced| replaced.set_at_ms),
        // The first pass stops before crypto when any earlier rule fails.
        signature_valid: false,
    };
    let precheck = freeze_signature_decision(facts);
    if precheck != FreezeSignatureDecision::SignatureInvalid {
        return decision_result(precheck);
    }

    let message = freeze_signing_bytes(check.cluster_id, record);
    facts.signature_valid =
        check
            .keys
            .verify(&record.key_id, &record.set_by, &message, &record.signature);
    decision_result(freeze_signature_decision(facts))
}

fn decision_result(decision: FreezeSignatureDecision) -> Result<(), SignatureRefusal> {
    match decision {
        FreezeSignatureDecision::UnknownKey => Err(SignatureRefusal::UnknownKeyId),
        FreezeSignatureDecision::AuthorIsNotKeyPrincipal => {
            Err(SignatureRefusal::AuthorIsNotTheKeyPrincipal)
        }
        FreezeSignatureDecision::AuthorIsNotConnectionPrincipal => {
            Err(SignatureRefusal::AuthorIsNotTheConnectionPrincipal)
        }
        FreezeSignatureDecision::TimestampOutsideSkewWindow => {
            Err(SignatureRefusal::TimestampOutsideSkewWindow)
        }
        FreezeSignatureDecision::TimestampNotNewer => {
            Err(SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces)
        }
        FreezeSignatureDecision::SignatureInvalid => Err(SignatureRefusal::SignatureDidNotVerify),
        FreezeSignatureDecision::Admit => Ok(()),
    }
}
