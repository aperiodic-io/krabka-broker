//! The audit log as evidence in its own right.
//!
//! KFC-9's load-bearing claim is that the audit log *alone* answers who did
//! what. An auditor holding this topic and the operator public keys has to be
//! able to say who froze a topic, who approved the thaw, and which approval
//! authorized which transition — with no metadata image to read and no broker
//! to take anybody's word for. The unit tests cannot reach that claim: they
//! observe the event on its way into the log rather than on its way back out
//! of it.
//!
//! Each case here drives the whole workflow in [`crate::freeze_workflow`] over
//! the wire and then reads the answer back off `__krabka_audit`.

use assert2::{assert, check};
use krabka_broker::coordinator::AUDIT_TOPIC;

use crate::{
    freeze_signing::{FreezeBytes, freeze_signing_bytes, pattern_type_byte},
    freeze_workflow::{ALICE, ALICE_KEY_ID, BOB, CAROL, run_thaw_workflow},
    support,
};

/// The `PrivilegedAction` records whose `api.operation` is `operation`.
///
/// The OCSF body spells that field `"<action>.<phase>"`, so one string selects
/// both the transition and the step of the workflow.
fn privileged<'a>(records: &'a [serde_json::Value], operation: &str) -> Vec<&'a serde_json::Value> {
    records
        .iter()
        .filter(|j| j["class_uid"] == 6003 && j["api"]["operation"] == operation)
        .collect()
}

/// The one `PrivilegedAction` record for `operation`.
fn one_privileged<'a>(records: &'a [serde_json::Value], operation: &str) -> &'a serde_json::Value {
    let rows = privileged(records, operation);
    assert!(
        rows.len() == 1,
        "expected exactly one {operation} record, got {}",
        rows.len()
    );
    rows[0]
}

/// Verifies that every KFC-9 phase reaches the audit topic, and that the
/// hash chain still runs unbroken across all of them.
///
/// The unit tests prove that each handler hands a `PrivilegedAction` event to
/// the audit log. They cannot prove that the event survives the sink, the
/// chain, and the produce into `__krabka_audit`, and a phase that reaches no
/// audit topic is a phase an auditor cannot count.
///
/// The chain assertion is the second half. `krabka-audit`'s offline verifier
/// is the reader an auditor would actually run: it walks the segment files and
/// recomputes `SHA256(prev ‖ seq ‖ value)` for each record, with no broker in
/// the loop. The broker is shut down first, so what the verifier reads is the
/// durable log rather than a live writer's buffer.
#[tokio::test]
async fn every_kfc9_phase_writes_one_chained_audit_record() {
    let w = run_thaw_workflow().await;
    let records = support::wait_for_audit_record(&w.alice, "the withdrawal", |j| {
        j["api"]["operation"] == "thaw_topic_freeze.consumed"
    })
    .await;

    // One row per phase the workflow produced. `status_id` is 1 for a success,
    // and every phase here succeeded: a refusal would be 2.
    for (label, operation) in [
        ("a freeze", "set_topic_freeze.applied"),
        ("a proposal", "thaw_topic_freeze.proposed"),
        ("an approval", "thaw_topic_freeze.approved"),
        ("a thaw", "thaw_topic_freeze.applied"),
        ("a consumed proposal", "thaw_topic_freeze.consumed"),
    ] {
        let rows = privileged(&records, operation);
        check!(
            !rows.is_empty(),
            "case {label}: no {operation} on the topic"
        );
        check!(
            rows.iter().all(|j| j["status_id"] == 1),
            "case {label}: {operation} did not record a success"
        );
    }

    // The seqs the topic carries are contiguous and never repeat, so no phase
    // slipped in without taking a chain slot.
    let seqs = support::audit_record_seqs(&w.alice).await;
    check!(
        seqs.len() >= 5,
        "five phases wrote fewer than five chained records: {seqs:?}"
    );
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    sorted.dedup();
    check!(sorted.len() == seqs.len(), "a seq repeats: {seqs:?}");
    let contiguous: Vec<u64> =
        (0..u64::try_from(seqs.len()).expect("a test-sized chain")).collect();
    check!(sorted == contiguous, "the chain has a hole: {seqs:?}");

    w.cluster.broker.shutdown().await;

    let partition = krabka_log::name::partition_dir(&w.cluster.log_dir, AUDIT_TOPIC, 0);
    let report =
        krabka_audit::verify_partition_dir(&partition, &krabka_audit::TrustedKeys::default())
            .expect("read the audit partition off disk");
    check!(
        report.ok,
        "the audit hash chain broke: {:?}",
        report.first_break
    );
    check!(
        report.records.0 >= u64::try_from(seqs.len()).expect("a test-sized chain"),
        "the offline reader saw fewer records than the fetch did"
    );
}

/// Verifies the join an auditor actually runs: take the proposal id off the
/// transition row, and it selects exactly the approvals that authorized it.
///
/// Presence is not the property that matters here. A log holding an approve
/// row and a transition row proves nothing unless the two can be tied
/// together, and the proposal id is the only thing that ties them. This case
/// starts from the transition, follows the id, and asserts that the principals
/// it reaches are the two people who actually approved.
///
/// It also asserts the shape of an approval row. A two-person rule whose
/// record names one person is not a two-person rule, so carol's approval has
/// to name carol as the actor and bob among the counterparties.
#[tokio::test]
async fn the_proposal_id_joins_each_approval_to_the_transition_it_authorized() {
    let w = run_thaw_workflow().await;
    let records = support::wait_for_audit_record(&w.alice, "the thaw", |j| {
        j["api"]["operation"] == "thaw_topic_freeze.applied"
    })
    .await;

    // Start where an auditor starts: the row that says a thaw happened.
    let thaw = one_privileged(&records, "thaw_topic_freeze.applied");
    let proposal = thaw["privileged_action"]["proposal_id"]
        .as_str()
        .expect("the transition names the proposal it spent")
        .to_owned();
    check!(proposal == w.proposal_id.to_string());

    // Follow that id into the approvals. Two proposals exist on this cluster,
    // so a join that matched on anything looser would pull in the wrong rows.
    let mut approvers: Vec<&str> = privileged(&records, "thaw_topic_freeze.approved")
        .into_iter()
        .filter(|j| j["privileged_action"]["proposal_id"] == serde_json::json!(proposal))
        .filter_map(|j| j["actor"]["user"]["name"].as_str())
        .collect();
    approvers.sort_unstable();
    check!(
        approvers == vec![BOB, CAROL],
        "the proposal id did not reach both approvers"
    );

    // And into the proposal, which names the person who asked for the thaw.
    let target_prop_json = serde_json::json!(proposal);
    let proposed: Vec<&str> = privileged(&records, "thaw_topic_freeze.proposed")
        .iter()
        .filter(|row| row["privileged_action"]["proposal_id"] == target_prop_json)
        .filter_map(|row| row["actor"]["user"]["name"].as_str())
        .collect();
    check!(proposed == vec![ALICE]);

    // The approval row itself names both people. `counterparties` is the
    // approval list as it stood after this approval landed, so carol's row
    // carries bob as well as carol; the whole array is compared rather than a
    // membership test, so a lost or reordered name fails here.
    let carols = privileged(&records, "thaw_topic_freeze.approved")
        .into_iter()
        .find(|j| j["actor"]["user"]["name"] == serde_json::json!(CAROL))
        .expect("carol's approval reached the audit topic");
    check!(
        carols["privileged_action"]["counterparties"]
            == serde_json::json!([
                { "name": BOB, "type": "" },
                { "name": CAROL, "type": "" },
            ]),
        "carol's approval does not name bob"
    );

    w.cluster.broker.shutdown().await;
}

/// Verifies the claim the whole feature rests on: a signed freeze re-verifies
/// from the audit topic alone, against the operator's public key, with no
/// metadata image read and no broker asked.
///
/// This is the case that makes the audit log independent evidence rather than
/// a second copy of the broker's opinion. The record in the metadata log
/// carries the same signature, but reading it means trusting the broker that
/// serves it. Here the event is fetched off `__krabka_audit`, the signed bytes
/// are rebuilt out of that event's own fields, and the signature is checked
/// against the public key file — by an Ed25519 verifier this test drives
/// directly, so no broker code decides the answer.
///
/// # What the event does not carry
///
/// One of the eight signed fields does not come out of the event: the cluster
/// id, which is a property of the cluster whose log the auditor is holding and
/// so is theirs to know. Every other field, `set_at_ms` included, is read here
/// out of the record itself.
///
/// `set_at_ms` needs its own field because the event's `time` is the moment the
/// broker emitted the record, not the moment the operator signed, and it is the
/// signed instant that is inside the preimage. An earlier version of this case
/// took it from the operator's own note of what they had signed and said so,
/// because the event did not carry it -- which meant the audit topic alone was
/// not enough, contradicting what KFC-9 claims for it.
///
/// The tampering check is what keeps the verification from being vacuous: the
/// same signature over the same record with `frozen` flipped must fail, which
/// is the replay-a-freeze-as-a-thaw attack that KFC-9 signs that byte to stop.
#[tokio::test]
async fn a_signed_freeze_reverifies_from_the_audit_topic_with_no_metadata_image() {
    let w = run_thaw_workflow().await;
    let records = support::wait_for_audit_record(&w.alice, "the freeze", |j| {
        j["api"]["operation"] == "set_topic_freeze.applied"
    })
    .await;
    let freeze = one_privileged(&records, "set_topic_freeze.applied");
    let action = &freeze["privileged_action"];

    // Everything below this line comes out of the event.
    let target = action["target"]
        .as_str()
        .expect("the event names its scope");
    let (pattern, scope) = target
        .split_once(':')
        .expect("a freeze target is \"<pattern>:<scope>\"");
    let set_by = freeze["actor"]["user"]["name"]
        .as_str()
        .expect("the event names its actor");
    let reason = freeze["status_detail"]
        .as_str()
        .expect("the event carries the operator's reason");
    let proposal_id = uuid::Uuid::parse_str(
        action["proposal_id"]
            .as_str()
            .expect("the event carries a proposal id"),
    )
    .expect("a uuid");
    let key_id = action["key_id"].as_str().expect("the event names the key");
    let set_at_ms = action["signed_at_ms"]
        .as_i64()
        .expect("the event carries the stamp the signature covers");
    let signature = hex::decode(
        action["signature"]
            .as_str()
            .expect("the event carries the raw signature"),
    )
    .expect("the signature is lowercase hex");

    let signed = FreezeBytes {
        cluster_id: &w.cluster_id,
        pattern_type: pattern_type_byte(pattern),
        scope,
        // `set_topic_freeze` is the freeze direction; `thaw_topic_freeze` is
        // the other one. The action name is what tells the two apart.
        frozen: true,
        reason,
        set_by,
        set_at_ms,
        proposal_id,
    };

    check!(key_id == ALICE_KEY_ID);
    check!(action["signature_verified"] == true);
    // The stamp the event carries is the one the operator signed, and it is a
    // different instant from the one the broker logged. Asserting both halves
    // is what stops a future change from quietly setting `signed_at_ms` to the
    // emit time, which would verify here and be wrong.
    check!(set_at_ms == w.freeze_set_at_ms);
    check!(freeze["time"].as_i64() != Some(set_at_ms));
    let public = std::fs::read(&w.cluster.alice_key.public_path).expect("the operator public key");
    let key = ring::signature::UnparsedPublicKey::new(&ring::signature::ED25519, &public);
    check!(
        key.verify(&freeze_signing_bytes(&signed), &signature)
            .is_ok(),
        "the freeze on the audit topic does not verify under alice's key"
    );

    // The same signature must not carry a thaw. One byte separates the two
    // records, and it is inside the signed bytes precisely so that a captured
    // freeze signature cannot be replayed in the dangerous direction.
    let as_thaw = FreezeBytes {
        frozen: false,
        ..signed
    };
    check!(
        key.verify(&freeze_signing_bytes(&as_thaw), &signature)
            .is_err(),
        "a freeze signature verified as a thaw"
    );

    // The join the fix to the freeze principal made possible: the freeze names
    // its author in the same Kafka form the break-glass events use, so an
    // auditor can tie a freeze to the proposal its author later opened. Two
    // spellings of one person would break this and nothing else would notice.
    let proposed = privileged(&records, "thaw_topic_freeze.proposed");
    check!(set_by == ALICE);
    check!(
        proposed
            .iter()
            .any(|j| j["actor"]["user"]["name"] == serde_json::json!(set_by)),
        "the freeze and the proposal spell their author differently"
    );

    w.cluster.broker.shutdown().await;
}
