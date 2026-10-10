//! The bridge from one modelled step to the code the broker really runs.
//!
//! [`CrossSpendModel`] holds the scenario that stays fixed across a run. Its
//! methods project a [`Universe`] back into the stored records a gated handler
//! would read, hand those records to
//! [`approve::decide`](crate::break_glass::handlers::approve::decide) and
//! [`gate::authorize`](crate::break_glass::gate::authorize), and fold the
//! answer into the next state. No rule the broker owns is restated here, so a
//! rule this model checks is the rule the broker runs.

use krabka_metadata::{BreakGlassProposalRecord, MetadataImage, MetadataRecord};
use uuid::Uuid;

use super::universe::{EXPIRES_AT, PROPOSALS, ProposalSpec, Request, Universe};
use crate::{
    break_glass::{
        config::BreakGlassPolicy,
        gate,
        state_model::{distinct, model_approval_principals, model_approvals, model_settle},
    },
    config::BreakGlassConfig,
};

pub(super) struct CrossSpendModel {
    pub(super) config: BreakGlassConfig,
    pub(super) proposals: [ProposalSpec; PROPOSALS],
    pub(super) requests: Vec<Request>,
    /// Every principal that can send a request, inside the approver set and
    /// outside it.
    pub(super) principals: Vec<&'static str>,
}

impl CrossSpendModel {
    fn policy(&self) -> BreakGlassPolicy<'_> {
        BreakGlassPolicy::new(&self.config)
    }

    /// The stored record that one proposal's model state stands for.
    fn record(&self, index: usize, state: &Universe) -> BreakGlassProposalRecord {
        let spec = self.proposals[index];
        let proposal = &state.proposals[index];
        BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(spec.id),
            action: spec.action,
            target: spec.target.to_owned(),
            proposer: spec.proposer.to_owned(),
            reason: "incident 42".to_owned(),
            created_at_ms: 0,
            expires_at_ms: EXPIRES_AT,
            approvals: model_approvals(&proposal.approvals),
            // `0` is the unconsumed sentinel.
            consumed_at_ms: i64::from(proposal.consumed),
            withdrawn: proposal.withdrawn,
        }
    }

    /// The image a gated handler reads, carrying both proposals at once.
    fn image_of(&self, state: &Universe) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for index in 0..PROPOSALS {
            image.apply(&MetadataRecord::V1BreakGlassProposal(
                self.record(index, state),
            ));
        }
        image
    }

    /// Which proposal a returned record names.
    fn index_of(&self, id: Uuid) -> Option<usize> {
        self.proposals
            .iter()
            .position(|spec| Uuid::from_u128(spec.id) == id)
    }

    /// Apply one approval or one withdrawal through the real handler decision.
    pub(super) fn settle(
        &self,
        state: &mut Universe,
        index: usize,
        principal: &'static str,
        withdraw: bool,
    ) {
        let stored = self.record(index, state);
        if let Ok(updated) = model_settle(self.policy(), &stored, principal, withdraw, state.now_ms)
        {
            let proposal = &mut state.proposals[index];
            proposal.withdrawn = updated.withdrawn;
            proposal.approvals = model_approval_principals(&self.principals, &updated.approvals);
        }
    }

    /// Ask the real gate to authorize one request against both proposals.
    pub(super) fn consume(&self, state: &mut Universe, request_index: usize) {
        let request = self.requests[request_index];
        let image = self.image_of(state);
        let Ok(MetadataRecord::V1BreakGlassProposal(spent)) = gate::authorize(
            &image,
            &self.config,
            request.action,
            request.target,
            state.now_ms,
        ) else {
            return;
        };
        let index = self
            .index_of(spent.proposal_id)
            .expect("the gate spent a proposal that the model put in the image");

        state.spends[index] = state.spends[index].saturating_add(1);
        if !request.covered_by[index] {
            state.cross_spent = true;
        }
        if distinct(&state.proposals[index].approvals) < self.policy().required_approvals() {
            state.under_approved = true;
        }
        state.proposals[index].consumed = true;
    }
}
