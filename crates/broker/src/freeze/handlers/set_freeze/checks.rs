//! The rules a `SetTopicFreeze` request passes before it becomes records.
//!
//! Each rule is a pure function of the configuration, the metadata image and
//! the connection, so the order of the refusals is the order this module
//! applies them: the cluster `Alter` gate, the shape of the pattern type, the
//! scope, the registry ceiling, the operator signature, and the break-glass
//! approval that a thaw needs.

use krabka_metadata::{
    BreakGlassAction, MetadataImage, MetadataRecord, PatternType, TopicFreezeRecord,
};
use krabka_protocol::krabka::freeze::SetTopicFreezeRequest;
use uuid::Uuid;

use super::outcome::{Accepted, Refusal};
pub(super) use crate::break_glass::gate::consumed_proposal_id;
use crate::{
    break_glass::{gate, handlers::principal_name},
    cluster_id::encode,
    codes,
    config::BrokerConfig,
    freeze::{
        freeze_target,
        handlers::pattern_type_concrete,
        scope_covers_internal_topic,
        signing::{FreezeSignatureCheck, verify_freeze_signature},
    },
    handlers::{RequestContext, cluster_alter_denied},
    time_util::now_ms,
};

/// Everything the checks read, and nothing they write.
///
/// The checks take this rather than the whole broker, so each rule is a pure
/// function of the configuration, the metadata image, and the connection.
pub(super) struct FreezeEnv<'a> {
    pub(super) config: &'a BrokerConfig,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
}

/// Check the request against every rule, and build the records it becomes.
///
/// # Errors
///
/// Returns the [`Refusal`] of the first rule that fails, in this order: the
/// cluster `Alter` gate, the shape of the pattern type, the scope, the registry
/// ceiling, the signature, and then the break-glass approval that a thaw needs.
pub(super) fn prepare(
    env: &FreezeEnv<'_>,
    req: &SetTopicFreezeRequest,
) -> Result<Accepted, Refusal> {
    if cluster_alter_denied(env.config.authorizer.as_ref(), env.image, env.ctx) {
        return Err(Refusal::new(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "set-topic-freeze denied",
        ));
    }
    let pattern_type = pattern_type_concrete(req.pattern_type).ok_or_else(|| {
        Refusal::new(
            codes::INVALID_REQUEST,
            format!(
                "pattern_type {} names no freeze scope kind; 3 is literal and 4 is prefixed",
                req.pattern_type
            ),
        )
    })?;
    check_scope(pattern_type, &req.scope)?;

    let replaces = live_entry(env.image, pattern_type, &req.scope).cloned();
    if req.frozen && replaces.is_none() {
        check_limit(env.image, env.config.freeze.max_entries)?;
    }

    // The Kafka form, not the bare session name. `[[operator_keys]]` is one
    // trust set for both features, and the break-glass path spells the same
    // person `User:alice`. A bare `alice` here would make one entry verify a
    // break-glass approval and refuse every freeze signature by that person.
    let author = principal_name(env.ctx);
    let record = record_of(req, pattern_type, &author);
    let signature_verified = check_signature(env, &record, replaces.as_ref())?;
    let consumed_proposal = check_approval(env, &record)?;

    Ok(Accepted {
        record,
        consumed_proposal,
        signature_verified,
    })
}

/// Refuse a scope that no freeze may cover.
pub(super) fn check_scope(pattern_type: PatternType, scope: &str) -> Result<(), Refusal> {
    if scope.is_empty() {
        return Err(Refusal::new(
            codes::FREEZE_SCOPE_INVALID,
            "a freeze scope is empty",
        ));
    }
    if scope_covers_internal_topic(pattern_type, scope) {
        return Err(Refusal::new(
            codes::FREEZE_SCOPE_INVALID,
            format!("freeze scope {scope:?} reaches an internal topic, which is never freezable"),
        ));
    }
    Ok(())
}

/// Refuse a freeze that would take the registry past `freeze.max_entries`.
///
/// The ceiling bounds the reverse walk that a prefix-scoped resolve does, and
/// that resolve is one hop from the produce path.
pub(super) fn check_limit(image: &MetadataImage, max_entries: usize) -> Result<(), Refusal> {
    let live = image.topic_freezes().count();
    if live >= max_entries {
        return Err(Refusal::new(
            codes::FREEZE_LIMIT_EXCEEDED,
            format!("the freeze registry already holds its ceiling of {max_entries} entries"),
        ));
    }
    Ok(())
}

/// Check the signature rules, and report whether a signature verified.
///
/// A thaw needs a signature whatever `freeze.require_signature` says, because a
/// thaw is the dangerous direction. A freeze needs one when
/// `freeze.require_signature` is on. A signature that the request carries is
/// verified whether or not it was needed, so a broken one never passes as an
/// unsigned request.
pub(super) fn check_signature(
    env: &FreezeEnv<'_>,
    record: &TopicFreezeRecord,
    replaces: Option<&TopicFreezeRecord>,
) -> Result<bool, Refusal> {
    if !is_signed(&record.key_id, &record.signature) {
        if !record.frozen {
            return Err(Refusal::new(
                codes::OPERATOR_SIGNATURE_REQUIRED,
                "a thaw needs a detached operator signature",
            ));
        }
        if env.config.freeze.require_signature {
            return Err(Refusal::new(
                codes::OPERATOR_SIGNATURE_REQUIRED,
                "freeze.require_signature is on, so a freeze needs a detached operator signature",
            ));
        }
        return Ok(false);
    }

    // `krabka-guard` reads the cluster id from `DescribeCluster`, which
    // reports Kafka's base64 `Uuid` form (#1082), and signs that exact
    // string. The broker must build the same string to verify.
    let cluster_id = encode(env.image.cluster_id());
    let check = FreezeSignatureCheck {
        keys: &env.config.operator_keys,
        cluster_id: &cluster_id,
        connection_principal: &principal_name(env.ctx),
        max_skew: env.config.freeze.signature_max_skew,
        now_ms: now_ms(),
        replaces,
    };
    verify_freeze_signature(&check, record).map_err(|refusal| {
        let (code, message) = refusal.wire();
        Refusal::new(code, message)
    })?;
    Ok(true)
}

/// Find the break-glass approval that a thaw needs, and stamp it consumed.
///
/// A freeze needs none and returns `None`. A thaw names its proposal on the
/// request, which is a field a krabka-private message can carry and a Kafka
/// message cannot. The gate then finds the approval, and the caller puts the
/// record the gate returns in the same raft append as the thaw.
pub(super) fn check_approval(
    env: &FreezeEnv<'_>,
    record: &TopicFreezeRecord,
) -> Result<Option<MetadataRecord>, Refusal> {
    if record.frozen {
        return Ok(None);
    }
    if record.proposal_id.is_nil() {
        return Err(Refusal::new(
            codes::BREAK_GLASS_APPROVAL_REQUIRED,
            "a thaw names the break-glass proposal that approved it",
        ));
    }
    let target = freeze_target(record.pattern_type, &record.scope);
    let consumed = gate::authorize(
        env.image,
        &env.config.break_glass,
        BreakGlassAction::ThawTopicFreeze,
        &target,
        now_ms(),
    )
    .map_err(|denial| Refusal::new(codes::BREAK_GLASS_APPROVAL_REQUIRED, denial.to_string()))?;
    if consumed_proposal_id(&consumed) != Some(record.proposal_id) {
        return Err(Refusal::new(
            codes::BREAK_GLASS_APPROVAL_REQUIRED,
            format!(
                "no approved proposal {} covers a thaw of {target}",
                record.proposal_id
            ),
        ));
    }
    Ok(Some(consumed))
}

/// Whether the request carries a signature at all.
///
/// A half-filled pair is signed for this test, so a request that names a key
/// and no signature reaches the verifier and is refused there rather than
/// passing as an unsigned request.
pub(super) fn is_signed(key_id: &str, signature: &[u8]) -> bool {
    !key_id.is_empty() || !signature.is_empty()
}

/// The registry record that the request becomes.
///
/// `set_by` is the principal the broker authenticated on the connection, and
/// never a field of the request. A client cannot claim another author, and the
/// signature covers the name the broker fills in.
pub(super) fn record_of(
    req: &SetTopicFreezeRequest,
    pattern_type: PatternType,
    set_by: &str,
) -> TopicFreezeRecord {
    TopicFreezeRecord {
        scope: req.scope.clone(),
        pattern_type,
        frozen: req.frozen,
        reason: req.reason.clone(),
        set_by: set_by.to_owned(),
        set_at_ms: if is_signed(&req.key_id, &req.signature) {
            req.set_at_ms
        } else {
            now_ms()
        },
        proposal_id: Uuid::from_bytes(req.proposal_id.0),
        key_id: req.key_id.clone(),
        signature: req.signature.clone(),
    }
}

/// The live registry entry at exactly this scope and pattern type.
///
/// This is an exact key lookup and not a topic resolve: a freeze on
/// `prefixed:tenant-a.` replaces the prefixed entry alone, and never the
/// literal entry of the same name.
pub(super) fn live_entry<'a>(
    image: &'a MetadataImage,
    pattern_type: PatternType,
    scope: &str,
) -> Option<&'a TopicFreezeRecord> {
    image
        .topic_freezes()
        .find(|entry| entry.pattern_type == pattern_type && entry.scope == scope)
}
