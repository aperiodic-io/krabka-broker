//! The background reaper for break-glass proposals.
//!
//! A proposal stays in the metadata image after it expires, after a transition
//! consumes it, and after an operator withdraws it. This sweep tombstones the
//! old ones with `V1DeleteBreakGlassProposal`, the way the delegation-token
//! sweep tombstones an expired token.
//!
//! # Correctness does not depend on this task
//!
//! The sweep bounds the size of the image, and nothing else. Every gated
//! handler already refuses an expired proposal against its own clock, and
//! [`gate::authorize`](crate::break_glass::gate::authorize) refuses a consumed
//! one and a withdrawn one. A broker that never runs this task is safe and
//! keeps a growing image. A broker that runs it twice tombstones the same
//! proposal twice, and the second tombstone is a no-op on the apply path,
//! because the entry is already gone. Every broker can therefore run the sweep,
//! which is Kafka's own "every broker sweeps, idempotent" pattern from KIP-48.
//!
//! # A tombstone comes well after the expiry
//!
//! [`PROPOSAL_RETENTION`] holds a settled proposal for a day past its expiry.
//! `DescribeBreakGlass` can then still show why an incident stalled: an
//! operator who finds a refused transition in the morning can read the proposal
//! that ran out of time overnight, and see who approved it and who did not. A
//! sweep that removed a proposal the moment it expired would delete the answer
//! to the question the operator is about to ask.
//!
//! The audit log carries the full history either way, and it is the record that
//! outlives the image.

use {
    crate::{task_util::run_every, time_util::now_ms},
    async_trait::async_trait,
    krabka_metadata::{MetadataImage, MetadataRecord},
    krabka_units::{Time, convert::TimeExt as _, hours, minutes},
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
    tracing::{debug, info, warn},
    uuid::Uuid,
};

/// How long a proposal stays in the image after it expires.
pub(crate) const PROPOSAL_RETENTION: Time = hours(24);

/// How often the sweep runs.
///
/// The interval is far below [`PROPOSAL_RETENTION`], so a proposal is
/// tombstoned close to the moment it falls out of retention. It is far above
/// the cost of one image scan, so an idle cluster pays almost nothing.
pub(crate) const SWEEP_INTERVAL: Time = minutes(5);

/// The controller surface the sweep needs. [`crate::broker`] adapts the real
/// [`krabka_raft::ControllerHandle`], and tests inject a mock.
#[async_trait]
pub(crate) trait BreakGlassController: Send + Sync {
    fn current_image(&self) -> Arc<MetadataImage>;
    async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String>;
}

/// The spawned task. It returns when `shutdown` is cancelled.
pub(crate) async fn run(
    controller: Arc<dyn BreakGlassController>,
    interval: Time,
    retention: Time,
    shutdown: CancellationToken,
) {
    let tick = tokio::time::interval(interval.to_std());
    run_every(tick, &shutdown, || sweep(&*controller, retention)).await;
    info!("break-glass proposal sweep shutting down");
}

/// Tombstone every proposal that fell out of retention.
pub(crate) async fn sweep(controller: &dyn BreakGlassController, retention: Time) {
    let now_ms = now_ms();
    let reapable: Vec<Uuid> = reapable_ids(&controller.current_image(), retention, now_ms);
    if reapable.is_empty() {
        return;
    }
    let records: Vec<MetadataRecord> = reapable
        .iter()
        .map(|id| MetadataRecord::V1DeleteBreakGlassProposal(*id))
        .collect();
    let count = reapable.len();
    if let Err(error) = controller.submit_change(records).await {
        warn!(
            error = %error,
            count,
            "failed to submit the break-glass tombstone batch"
        );
    } else {
        for id in &reapable {
            debug!(proposal_id = %id, "break-glass proposal reaped");
        }
    }
}

/// The proposals that the sweep tombstones now.
///
/// A proposal is reapable once the clock passes its expiry plus `retention`.
/// The rule reads the expiry alone, so a consumed proposal and a withdrawn one
/// stay as long as an unused one, and an operator sees the whole incident in
/// one `DescribeBreakGlass` answer.
fn reapable_ids(image: &MetadataImage, retention: Time, now_ms: i64) -> Vec<Uuid> {
    let retention_ms = retention.millis_i64();
    image
        .break_glass_proposals()
        .filter(|proposal| now_ms >= proposal.expires_at_ms.saturating_add(retention_ms))
        .map(|proposal| proposal.proposal_id)
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::break_glass::gate::tests::{image_of, proposal},
        assert2::check,
        krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord},
        krabka_units::{days, millis},
        std::{sync::Mutex, time::Duration},
    };

    struct MockController {
        image: Mutex<Arc<MetadataImage>>,
        submitted: Mutex<Vec<MetadataRecord>>,
    }

    impl MockController {
        fn holding(proposals: &[BreakGlassProposalRecord]) -> Arc<Self> {
            Arc::new(Self {
                image: Mutex::new(Arc::new(image_of(proposals))),
                submitted: Mutex::new(Vec::new()),
            })
        }

        fn tombstoned(&self) -> Vec<Uuid> {
            let mut ids: Vec<Uuid> = self
                .submitted
                .lock()
                .expect("the submitted record list")
                .iter()
                .map(|record| match record {
                    MetadataRecord::V1DeleteBreakGlassProposal(id) => *id,
                    other => panic!("unexpected record {other:?}"),
                })
                .collect();
            ids.sort();
            ids
        }
    }

    #[async_trait]
    impl BreakGlassController for MockController {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.lock().expect("the held image").clone()
        }

        async fn submit_change(&self, records: Vec<MetadataRecord>) -> Result<(), String> {
            let mut image: MetadataImage = (**self.image.lock().expect("the held image")).clone();
            for record in &records {
                image.apply(record);
            }
            *self.image.lock().expect("the held image") = Arc::new(image);
            self.submitted
                .lock()
                .expect("the submitted record list")
                .extend(records);
            Ok(())
        }
    }

    // `sweep` reads the wall clock, so a test picks expiry times far in the
    // past and far in the future rather than pinning the clock.
    fn long_ago(id: u128) -> BreakGlassProposalRecord {
        BreakGlassProposalRecord {
            created_at_ms: 1_000,
            expires_at_ms: 2_000,
            ..proposal(id, BreakGlassAction::DeleteTopic, "doomed")
        }
    }

    fn far_ahead(id: u128) -> BreakGlassProposalRecord {
        BreakGlassProposalRecord {
            created_at_ms: 1_000,
            expires_at_ms: i64::MAX,
            ..proposal(id, BreakGlassAction::DeleteTopic, "doomed")
        }
    }

    #[tokio::test]
    async fn the_sweep_tombstones_only_the_proposals_out_of_retention() {
        let controller = MockController::holding(&[long_ago(1), long_ago(2), far_ahead(3)]);

        sweep(&*controller, PROPOSAL_RETENTION).await;

        check!(controller.tombstoned() == [Uuid::from_u128(1), Uuid::from_u128(2)]);
        check!(controller.current_image().break_glass_proposals().count() == 1);
        check!(
            controller
                .current_image()
                .break_glass_proposal(Uuid::from_u128(3))
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_sweep_with_nothing_to_reap_submits_no_record() {
        let controller = MockController::holding(&[far_ahead(1)]);

        sweep(&*controller, PROPOSAL_RETENTION).await;

        check!(controller.tombstoned().is_empty());
        check!(controller.current_image().break_glass_proposals().count() == 1);
    }

    #[tokio::test]
    async fn a_second_sweep_over_the_same_image_is_a_no_op() {
        let controller = MockController::holding(&[long_ago(1)]);

        sweep(&*controller, PROPOSAL_RETENTION).await;
        sweep(&*controller, PROPOSAL_RETENTION).await;

        check!(controller.tombstoned() == [Uuid::from_u128(1)]);
    }

    #[tokio::test]
    async fn a_settled_proposal_waits_out_the_same_retention_as_an_unused_one() {
        let consumed = BreakGlassProposalRecord {
            consumed_at_ms: 1_500,
            ..far_ahead(1)
        };
        let withdrawn = BreakGlassProposalRecord {
            withdrawn: true,
            ..far_ahead(2)
        };
        let controller = MockController::holding(&[consumed, withdrawn]);

        sweep(&*controller, PROPOSAL_RETENTION).await;

        check!(controller.tombstoned().is_empty());
    }

    #[test]
    fn a_proposal_is_reapable_one_retention_after_it_expires() {
        let image = image_of(&[BreakGlassProposalRecord {
            expires_at_ms: 1_000,
            ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
        }]);
        let retention = PROPOSAL_RETENTION.millis_i64();
        let cases = [
            ("at the expiry", 1_000_i64, false),
            (
                "one millisecond before the retention runs out",
                1_000 + retention - 1,
                false,
            ),
            (
                "exactly at the end of the retention",
                1_000 + retention,
                true,
            ),
            ("well after the retention", 1_000 + retention * 2, true),
        ];
        for (label, now_ms, expected) in cases {
            check!(
                (reapable_ids(&image, PROPOSAL_RETENTION, now_ms) == vec![Uuid::from_u128(1)])
                    == expected,
                "case {label}"
            );
        }
    }

    #[test]
    fn an_expiry_at_the_end_of_time_saturates_instead_of_wrapping() {
        let image = image_of(&[BreakGlassProposalRecord {
            expires_at_ms: i64::MAX,
            ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
        }]);
        // Without the saturating add the sum wraps negative, and every clock
        // reading is then past it, so the sweep reaps a live proposal at once.
        let cases = [
            ("a clock below the end of time", i64::MAX - 1, false),
            ("a clock at the end of time", i64::MAX, true),
        ];
        for (label, now_ms, expected) in cases {
            check!(
                !reapable_ids(&image, PROPOSAL_RETENTION, now_ms).is_empty() == expected,
                "case {label}"
            );
        }
    }

    #[test]
    fn the_sweep_runs_far_more_often_than_it_reaps() {
        check!(SWEEP_INTERVAL < PROPOSAL_RETENTION);
        check!(PROPOSAL_RETENTION == days(1));
    }

    #[tokio::test]
    async fn the_task_sweeps_on_its_first_tick_and_waits_for_the_shutdown() {
        let controller = MockController::holding(&[long_ago(1)]);
        let shutdown = CancellationToken::new();
        let mut task = tokio::spawn(run(
            controller.clone(),
            millis(50),
            PROPOSAL_RETENTION,
            shutdown.clone(),
        ));

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if !controller.tombstoned().is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the sweep runs on its first tick");

        check!(
            tokio::time::timeout(Duration::from_millis(25), &mut task)
                .await
                .is_err(),
            "the sweep loop stopped before the shutdown"
        );

        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("the sweep task sees the shutdown")
            .expect("the sweep task does not panic");
    }
}
