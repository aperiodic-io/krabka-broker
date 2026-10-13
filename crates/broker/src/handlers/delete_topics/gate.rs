//! KFC-9: the break-glass two-person rule over a topic deletion.
//!
//! A deletion destroys every record the topic holds, so it is gated. This
//! module builds the record list one deletion appends -- the consumed proposal
//! ahead of the delete record, so a single raft append carries both -- and
//! reads back the proposal that list spends. The freeze check that answers
//! ahead of this gate stays in the module root.

pub(super) use crate::break_glass::gate::consumed_proposal_id;
use {
    crate::break_glass::gate::{
        self, BreakGlassAction, BreakGlassConfig, BreakGlassDenial, MetadataImage, MetadataRecord,
    },
    krabka_metadata::DeleteTopicRecord,
};

/// The records one topic deletion appends.
///
/// The consumed break-glass proposal goes first, and the delete record follows
/// it, so one raft append carries both. That single append is what stops an
/// approval from being spent twice across a crash: a broker that committed the
/// deletion has committed the consume with it.
///
/// A broker whose `[break_glass]` names no approver gates nothing, and the
/// answer is then the delete record alone.
///
/// # Errors
///
/// Returns the [`BreakGlassDenial`] when no approved proposal covers this
/// topic. The caller answers `POLICY_VIOLATION (44)` on that topic's row.
pub(super) fn delete_topic_records(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    name: &str,
    now_ms: i64,
) -> Result<Vec<MetadataRecord>, BreakGlassDenial> {
    let record = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
        name: name.to_owned(),
    });
    gate::records_with_consume(
        image,
        config,
        BreakGlassAction::DeleteTopic,
        name,
        now_ms,
        record,
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            break_glass::gate::tests::{approved_proposal, image_of},
            handlers::delete_topics::test_support::{DOOMED, gated_config},
        },
        assert2::{assert, check},
    };

    const NOW_MS: i64 = 60_000;

    fn deleted() -> MetadataRecord {
        MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: DOOMED.to_owned(),
        })
    }

    #[test]
    fn a_deletion_with_no_proposal_appends_nothing() {
        let denial = delete_topic_records(&image_of(&[]), &gated_config(), DOOMED, NOW_MS)
            .expect_err("no proposal covers the topic");

        check!(denial.action == BreakGlassAction::DeleteTopic);
        check!(
            denial.to_string()
                == "break-glass refused delete_topic on doomed: no approved proposal covers the request"
        );
    }

    #[test]
    fn an_approved_deletion_appends_the_consume_beside_the_delete() {
        let proposal = approved_proposal(BreakGlassAction::DeleteTopic, DOOMED);
        let image = image_of(std::slice::from_ref(&proposal));

        let records = delete_topic_records(&image, &gated_config(), DOOMED, NOW_MS)
            .expect("the proposal authorizes the deletion");

        let expected = vec![
            MetadataRecord::V1BreakGlassProposal(krabka_metadata::BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS,
                ..proposal
            }),
            deleted(),
        ];
        assert!(records == expected);
    }

    #[test]
    fn a_topic_scoped_proposal_covers_no_other_topic() {
        // `delete_topic` names no partition, so `doomed` never covers
        // `doomed-2024`, which reads as partition 2024 of topic `doomed`.
        let image = image_of(&[approved_proposal(BreakGlassAction::DeleteTopic, DOOMED)]);

        let denial = delete_topic_records(&image, &gated_config(), "doomed-2024", NOW_MS)
            .expect_err("a proposal for one topic authorizes nothing about another");

        check!(denial.proposal_id() == None);
    }

    #[test]
    fn a_broker_with_no_approver_set_gates_nothing() {
        let records =
            delete_topic_records(&image_of(&[]), &BreakGlassConfig::default(), DOOMED, NOW_MS)
                .expect("an ungated broker deletes with no proposal");

        assert!(records == vec![deleted()]);
    }
}
