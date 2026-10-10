//! The observer's serve loop: it picks a controller, fetches from it, records
//! the applied offset and the leader the answer names, and parks on the poll
//! interval whenever it is caught up or the controller is unreachable.
//!
//! The controllers it picks from are the voters and bootstrap servers that
//! [`quorum_targets`] lists, read again before every fetch. A node in a KIP-853
//! dynamic quorum starts with bootstrap servers alone and learns the voters,
//! and the endpoint of the leader, from the records it applies.
//!
//! The loop starts from what this node last checkpointed rather than from
//! offset 0, and checkpoints again as it advances, so a restart does not replay
//! the whole metadata log — and does not ask a pruned controller for records it
//! no longer holds.

use std::sync::{Arc, atomic::Ordering};

use krabka_raft::NodeId;
use krabka_units::convert::TimeExt as _;
use tokio_util::sync::CancellationToken;

use super::{MetadataObserver, ObserverConfig, fetch::fetch_once, store::ObserverStore};
use crate::{controller_endpoint::quorum_targets, time_util};

/// Names this loop in the timer-failure logs that [`time_util::arm`] and
/// [`time_util::fired`] emit.
const TASK: &str = "metadata observer";

/// Parks for the configured poll interval on the injected timer.
///
/// Reports whether the timer held: `false` means the observer's only pacing
/// mechanism is gone, and the loop must stop rather than spin at full speed
/// through a poll that no longer waits.
async fn park(config: &ObserverConfig) -> bool {
    let Some(tick) = time_util::arm(&*config.timer, config.poll_interval.to_std(), TASK) else {
        return false;
    };
    time_util::fired(tick.await, TASK)
}

/// Which controller the loop fetches from next, and what the last answers
/// said.
///
/// The cursor names controllers by node id, never by their position in the
/// target list, because the loop reads that list again before every fetch and
/// the voter set at its head can change between two fetches.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Cursor {
    /// The controller that answered last, or the one to try after a failure.
    current: Option<NodeId>,
    /// The leader the last answer named.
    leader: Option<NodeId>,
    /// The controller whose fetch failed most recently, until one answers
    /// naming a different leader, or this one answers again.
    failed: Option<NodeId>,
}

impl Cursor {
    /// The controller to fetch from next, out of a non-empty `targets`.
    ///
    /// Any controller serves the fetch, but only the leader records an
    /// observer's progress for `DescribeQuorum`, so the loop goes to the leader
    /// the last answer named, as a Kafka broker only ever fetches from the
    /// leader. It does not go back to a leader that has just failed: a leader
    /// this node cannot reach would otherwise cost a failed dial on every
    /// second poll, for as long as a follower keeps naming it. Short of the
    /// leader, the loop stays on the controller it is on, and short of that it
    /// takes the first target.
    fn pick<'a>(&self, targets: &'a [(NodeId, String)]) -> &'a (NodeId, String) {
        let leader = self.leader.filter(|leader| Some(*leader) != self.failed);
        [leader, self.current]
            .into_iter()
            .flatten()
            .find_map(|id| targets.iter().find(|(target, _)| *target == id))
            .unwrap_or(&targets[0])
    }

    /// `target`, one of `targets`, answered and named `leader`.
    ///
    /// An answer that names no leader comes from a controller that knows of
    /// none. The quorum can be in an election, or the controller can have left
    /// the voter set and not found its successor yet. So the loop moves on to
    /// the next target, as a Kafka observer looks for the leader elsewhere
    /// when its fetch names none.
    fn answered(&mut self, targets: &[(NodeId, String)], target: NodeId, leader: Option<NodeId>) {
        if self.failed == Some(target) || leader.is_some_and(|leader| Some(leader) != self.failed) {
            self.failed = None;
        }
        self.current = if leader.is_some() {
            Some(target)
        } else {
            target_after(targets, target)
        };
        self.leader = leader;
    }

    /// `target`, one of `targets`, did not answer: the loop moves on to the
    /// target after it.
    fn missed(&mut self, targets: &[(NodeId, String)], target: NodeId) {
        self.failed = Some(target);
        self.current = target_after(targets, target);
    }
}

/// The target after `target` in `targets`, wrapping round to the first. A
/// target that has left the list is followed by the first.
fn target_after(targets: &[(NodeId, String)], target: NodeId) -> Option<NodeId> {
    let next = targets
        .iter()
        .position(|(id, _)| *id == target)
        .map_or(0, |index| (index + 1) % targets.len());
    targets.get(next).map(|(id, _)| *id)
}

/// Successive fetches that leave the image empty while the quorum has
/// committed records before the loop warns about it. One such fetch is
/// ordinary — a node that has just started has an empty image and no records
/// yet; a second one means no progress is being made at all.
const EMPTY_IMAGE_POLLS_BEFORE_WARNING: u32 = 2;

/// Watches for the stall where every fetch is answered and nothing is ever
/// applied: the quorum has committed records, and this node's image is still
/// empty a poll interval later.
///
/// Left silent, that state is only visible as a readiness lag the node can
/// never close. The warning is raised once per stall and re-armed when the
/// observer starts making progress again, so a node that is merely slow to see
/// its first record does not warn, and a node that is genuinely stuck does not
/// repeat itself on every poll.
#[derive(Default)]
struct EmptyImageWatch {
    polls: u32,
    warned: bool,
}

impl EmptyImageWatch {
    /// Record one answered fetch. Reports whether this is the moment to warn.
    fn observe(&mut self, image_is_empty: bool, quorum_high_watermark: i64) -> bool {
        if !image_is_empty || quorum_high_watermark <= 0 {
            *self = Self::default();
            return false;
        }
        self.polls = self.polls.saturating_add(1);
        let warn_now = self.polls >= EMPTY_IMAGE_POLLS_BEFORE_WARNING && !self.warned;
        self.warned |= warn_now;
        warn_now
    }
}

/// Restore the image this node last checkpointed and report the offset to
/// fetch from next. A node with no checkpoint starts at the log start.
fn resume(config: &ObserverConfig, store: &mut ObserverStore, observer: &MetadataObserver) -> u64 {
    let Some((image, fetch_offset)) = store.resume(config.cluster_id) else {
        return 0;
    };
    let _ = observer.image.send_replace(Arc::new(image));
    // The offset convention is one-past-the-last, so the last applied record
    // sits one below the offset the next fetch asks for.
    observer.metadata_offset.store(
        i64::try_from(fetch_offset).unwrap_or(i64::MAX) - 1,
        Ordering::Release,
    );
    observer
        .last_applied_record_timestamp_ms
        .store(store.last_applied_record_timestamp_ms(), Ordering::Release);
    fetch_offset
}

pub(super) async fn run_loop(
    config: ObserverConfig,
    observer: Arc<MetadataObserver>,
    shutdown: CancellationToken,
) {
    let mut store = ObserverStore::open(&config.data_dir, config.snapshot_interval_records);
    let mut fetch_offset: u64 = resume(&config, &mut store, &observer);
    let mut cursor = Cursor::default();
    let mut empty_image = EmptyImageWatch::default();
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let targets = quorum_targets(
            &observer.current_image(),
            &config.voters,
            &config.bootstrap_servers,
        );
        if targets.is_empty() {
            if !park(&config).await {
                return;
            }
            continue;
        }
        let (target, addr) = cursor.pick(&targets).clone();
        let result = tokio::select! {
            () = shutdown.cancelled() => return,
            r = fetch_once(&config, &addr, target, fetch_offset, &observer.image, &mut store) => r,
        };
        if let Some(outcome) = result {
            // A record this node could not decode was skipped, with the rest
            // of the response: Kafka's non-fatal "metadata loading" fault on
            // a node without the controller role.
            observer
                .load_errors
                .fetch_add(outcome.load_errors, Ordering::AcqRel);
            // An invalid `KRaft` control record halts a Kafka node of any role.
            // The loop ends here, and the broker stops over the fault.
            if let Some(fault) = &outcome.fatal {
                observer.fatal.send_replace(Some(fault.clone()));
                return;
            }
            let new_offset = outcome.next_fetch_offset;
            if let Some(timestamp_ms) = outcome.last_applied_record_timestamp_ms {
                observer
                    .last_applied_record_timestamp_ms
                    .store(timestamp_ms, Ordering::Release);
                store.record_applied_timestamp(timestamp_ms);
            }
            if new_offset > fetch_offset
                && let Some(processing_us) = outcome.pending_record_processing_time_us
            {
                observer.record_pending_record_processing_time_us(processing_us);
            }
            observer.metadata_offset.store(
                i64::try_from(new_offset).unwrap_or(i64::MAX) - 1,
                Ordering::Release,
            );
            // The quorum's high watermark counts committed *records*; the
            // offset convention here is one-past-the-last, so the last
            // committed offset is one below it, matching `metadata_offset`.
            // It is the quorum's and not the responder's: the voter this loop
            // picked may itself be a follower that is behind.
            observer.quorum_committed_offset.store(
                outcome.quorum_high_watermark.saturating_sub(1),
                Ordering::Release,
            );
            // The leader is the one the answer names, which a bootstrap
            // server knows by node id even though this node knows the server
            // only by its address. An answer that names none, from a quorum in
            // the middle of an election, leaves the last one standing.
            if let Some(leader) = outcome.leader_hint {
                let _ = observer.leader.send_replace(Some(leader));
            }
            cursor.answered(&targets, target, outcome.leader_hint);
            // Every poll is answered and nothing is ever applied: the stall
            // shows up as a readiness lag this node can never close, with
            // nothing saying why. The responder's log start is what separates
            // "still catching up" from "asking for records that were pruned
            // away".
            if empty_image.observe(
                observer.metadata_offset.load(Ordering::Acquire) < 0,
                outcome.quorum_high_watermark,
            ) {
                tracing::warn!(
                    fetch_offset,
                    log_start_offset = outcome.log_start_offset,
                    quorum_high_watermark = outcome.quorum_high_watermark,
                    voter = target.0,
                    %addr,
                    "observer metadata image is still empty while the quorum has committed \
                     records; this node is not making progress"
                );
            }
            if new_offset == fetch_offset {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    held = park(&config) => if !held { return; },
                }
            } else {
                fetch_offset = new_offset;
                store.maybe_checkpoint(&observer.current_image(), fetch_offset);
            }
        } else {
            cursor.missed(&targets, target);
            tokio::select! {
                () = shutdown.cancelled() => return,
                held = park(&config) => if !held { return; },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

    use assert2::assert;
    use bytes::Bytes;
    use krabka_protocol::owned::api_versions_request;
    use krabka_raft::OutboundDialer;
    use krabka_units::millis;
    use qubit_clock::{ManualMonotonicClock, MonotonicClock as _, Timer};
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata_observer::test_support::{api_versions_response_v0, observer_config},
        test_support::{BrokenTimer, TimerFailure},
    };

    /// A write side for an `ObserverSource` built only to read offsets
    /// through: readiness never submits anything.
    struct NoWrites;

    #[async_trait::async_trait]
    impl crate::metadata_source::MetadataWriter for NoWrites {
        async fn submit_change(
            &self,
            _records: Vec<krabka_metadata::MetadataRecord>,
        ) -> Result<krabka_raft::SubmitChangeResult, krabka_raft::RaftError> {
            unreachable!("readiness reads offsets, it never writes")
        }
    }

    #[derive(Clone)]
    struct CountingDialer {
        dial_count: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl OutboundDialer for CountingDialer {
        async fn dial(
            &self,
            target: NodeId,
            addr: &str,
            options: krabka_client_core::ConnectionOptions,
        ) -> Result<krabka_client_core::Connection, krabka_client_core::ClientError> {
            self.dial_count.fetch_add(1, Ordering::SeqCst);
            krabka_raft::PlaintextDialer
                .dial(target, addr, options)
                .await
        }
    }

    /// `high_watermark` is what the responder itself has committed and
    /// `quorum_high_watermark` is what the quorum has: they differ whenever
    /// the controller that answered is a follower still catching up.
    fn metadata_fetch_response_body(
        records: Bytes,
        high_watermark: i64,
        quorum_high_watermark: i64,
    ) -> Vec<u8> {
        metadata_fetch_response_naming(1, records, high_watermark, quorum_high_watermark)
    }

    /// [`metadata_fetch_response_body`] from a controller that believes
    /// `leader_hint` leads.
    fn metadata_fetch_response_naming(
        leader_hint: i64,
        records: Bytes,
        high_watermark: i64,
        quorum_high_watermark: i64,
    ) -> Vec<u8> {
        let mut out = vec![0u8]; // flexible ResponseHeader v1 tagged-fields
        krabka_raft::KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint,
            leader_epoch: 3,
            log_start_offset: 0,
            high_watermark,
            quorum_high_watermark,
            snapshot_id: None,
            records,
        }
        .encode_v0(&mut out)
        .unwrap();
        out
    }

    /// The stall warning fires once, after more than one answered fetch has
    /// left the image empty, and re-arms only after the observer has made
    /// progress. A node that has simply not seen its first record yet, or one
    /// whose quorum has committed nothing, must stay silent.
    #[test]
    fn the_empty_image_warning_fires_once_per_stall_and_re_arms_after_progress() {
        let mut watch = EmptyImageWatch::default();

        // A quorum with nothing committed says nothing about this node.
        assert!(!watch.observe(true, 0));
        // The first answered fetch on a fresh node is ordinary.
        assert!(!watch.observe(true, 10_000));
        // The second one is the stall, and it is reported exactly once.
        assert!(watch.observe(true, 10_000));
        assert!(!watch.observe(true, 10_000));

        // Progress clears it, and a later stall is reported again.
        assert!(!watch.observe(false, 10_000));
        assert!(!watch.observe(true, 10_000));
        assert!(watch.observe(true, 10_000));
    }

    fn targets(ids: &[u64]) -> Vec<(NodeId, String)> {
        ids.iter()
            .map(|&id| (NodeId(id), format!("controller-{id}:9093")))
            .collect()
    }

    /// A controller that does not answer hands over to the next target, and
    /// the last one wraps round to the first. A target that has left the list
    /// since it was picked hands over to the first.
    #[test]
    fn a_missed_controller_hands_over_to_the_next_target() {
        let listed = targets(&[1, 2, 3]);
        let node = NodeId;
        // (target that missed, cursor afterwards)
        let cases = [(1, 2), (2, 3), (3, 1), (9, 1)].map(|(missed, next)| {
            (
                missed,
                Cursor {
                    current: Some(node(next)),
                    leader: Some(node(2)),
                    failed: Some(node(missed)),
                },
            )
        });
        for (missed, expected) in cases {
            let mut cursor = Cursor {
                current: Some(node(missed)),
                leader: Some(node(2)),
                failed: None,
            };
            cursor.missed(&listed, node(missed));
            assert!(cursor == expected, "missed {missed}");
        }
    }

    /// The loop moves to the leader an answer names, so that the leader sees
    /// this node fetch, and does not go back to a controller that has just
    /// failed. A bootstrap server is known only by its address, so the leader
    /// it names is reached once the voter set read from the log lists it.
    #[test]
    fn the_loop_follows_a_named_leader_unless_it_just_failed() {
        let node = NodeId;
        let voters = targets(&[1, 2, 3]);
        let before_the_voter_set = targets(&[u64::MAX, u64::MAX - 1]);
        let after_the_voter_set = targets(&[3001, u64::MAX - 1]);
        let failed = |id| Cursor {
            failed: Some(node(id)),
            ..Cursor::default()
        };
        // (label, cursor before the answer, target that answered, leader it
        // named, targets of the answer and of the next poll, target the next
        // poll picks)
        let cases = [
            (
                "a follower names the leader",
                Cursor::default(),
                1,
                Some(3),
                &voters,
                3,
            ),
            (
                "the target is the leader",
                Cursor::default(),
                3,
                Some(3),
                &voters,
                3,
            ),
            (
                "an answer that names no leader moves on",
                Cursor::default(),
                1,
                None,
                &voters,
                2,
            ),
            (
                "the last target names no leader",
                Cursor::default(),
                3,
                None,
                &voters,
                1,
            ),
            (
                "the leader is not a target",
                Cursor::default(),
                1,
                Some(9),
                &voters,
                1,
            ),
            (
                "the named leader just failed",
                failed(3),
                1,
                Some(3),
                &voters,
                1,
            ),
            (
                "a leader other than the one that failed",
                failed(3),
                1,
                Some(2),
                &voters,
                2,
            ),
            (
                "a bootstrap server names a leader the voter set does not list yet",
                Cursor::default(),
                u64::MAX,
                Some(3001),
                &before_the_voter_set,
                u64::MAX,
            ),
            (
                "the voter set lists the leader a bootstrap server named",
                Cursor::default(),
                u64::MAX,
                Some(3001),
                &after_the_voter_set,
                3001,
            ),
        ];
        let picked: Vec<(&str, NodeId)> = cases
            .iter()
            .map(|&(label, cursor, target, leader, next_targets, _)| {
                let mut cursor = cursor;
                cursor.answered(next_targets, node(target), leader.map(node));
                (label, cursor.pick(next_targets).0)
            })
            .collect();
        let expected: Vec<(&str, NodeId)> = cases
            .iter()
            .map(|&(label, .., next)| (label, node(next)))
            .collect();
        assert!(picked == expected);
    }

    #[tokio::test]
    async fn run_loop_sleeps_after_empty_fetch() {
        let fetches = Arc::new(AtomicUsize::new(0));
        let fetches_for_mock = fetches.clone();
        let mock = crate::test_support::mock_request_broker(
            krabka_raft::API_KEY_METADATA_FETCH,
            move || {
                fetches_for_mock.fetch_add(1, Ordering::SeqCst);
                metadata_fetch_response_body(Bytes::new(), 0, 0)
            },
            api_versions_response_v0,
        )
        .await;
        let dial_count = Arc::new(AtomicUsize::new(0));
        let clock = ManualMonotonicClock::new_shared();
        let dir = tempfile::tempdir().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(krabka_raft::NodeId(1), mock.addr.to_string())],
            dialer: Arc::new(CountingDialer {
                dial_count: dial_count.clone(),
            }),
            client_id: "sleep-test".into(),
            poll_interval: millis(250),
            timer: clock.new_timer(),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
        });

        // Await (not sleep) for the first fetch to land. The fetch is real
        // loopback network I/O through the mock broker, which is not time-gated
        // — drive the executor with `yield_now` until the counter moves.
        let mut saw_first_fetch = false;
        for _ in 0..100_000 {
            if fetches.load(Ordering::SeqCst) >= 1 {
                saw_first_fetch = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(saw_first_fetch, "observer should issue the first fetch");
        let after_first_fetch = fetches.load(Ordering::SeqCst);

        // The empty fetch left the observer caught up, so it must now be parked
        // on a timer armed for `poll_interval`. That park is the loop's only
        // registration on this clock, so confirming one waiter confirms the
        // park (blocking thread — never stalls the current-thread runtime that
        // drives the observer to its park). Parked on a manual timeline that we
        // never advance, the observer cannot re-fetch, so the counts are
        // deterministically frozen at their first-fetch values.
        let waiters = clock.clone();
        let parked = tokio::task::spawn_blocking(move || {
            waiters.wait_for_waiters(1, Duration::from_secs(5))
        })
        .await
        .unwrap();
        assert!(
            parked,
            "observer should park on the poll-interval timer after an empty fetch",
        );

        assert!(fetches.load(Ordering::SeqCst) == after_first_fetch);
        assert!(dial_count.load(Ordering::SeqCst) == after_first_fetch);

        observer.cancel().await;
        mock.stop();
    }

    /// Runs the serve loop over `config` until it returns on its own, and
    /// hands back the observer it wrote to.
    ///
    /// The shutdown token is never cancelled, so a loop that returns here
    /// returned because it gave up on its timer. A hang-guard keeps a loop
    /// that does not give up from wedging the whole suite.
    async fn run_until_it_stops(config: ObserverConfig) -> Arc<MetadataObserver> {
        let shutdown = CancellationToken::new();
        let observer = MetadataObserver::new(config.cluster_id, shutdown.clone());
        tokio::time::timeout(
            Duration::from_secs(10),
            run_loop(config, Arc::clone(&observer), shutdown),
        )
        .await
        .expect("the serve loop stops once its timer is gone");
        observer
    }

    /// An `ObserverConfig` over `voters`, driven by `timer` and storing its
    /// checkpoints under `data_dir`.
    fn config_on(
        voters: Vec<(NodeId, String)>,
        timer: Arc<dyn Timer>,
        data_dir: &std::path::Path,
    ) -> ObserverConfig {
        ObserverConfig {
            voters,
            client_id: "dead-timer-test".into(),
            poll_interval: millis(250),
            timer,
            ..observer_config(Uuid::nil(), data_dir.to_path_buf())
        }
    }

    /// The loopback port nothing listens on, so a dial fails at once rather
    /// than on a connect timeout.
    const UNREACHABLE: &str = "127.0.0.1:1";

    /// A restarted observer comes up on what it last checkpointed rather than on
    /// an empty image at offset 0.
    ///
    /// The loop publishes the restored image and its applied offset before it
    /// contacts anybody, so a node that was caught up before the restart serves
    /// metadata immediately instead of replaying the log — or, once the
    /// controller has pruned, instead of asking for records that are gone.
    #[tokio::test]
    async fn the_loop_resumes_from_the_checkpoint_this_node_last_wrote() {
        let dir = tempfile::tempdir().unwrap();
        let mut image = krabka_metadata::MetadataImage::new(Uuid::nil());
        image.apply(&krabka_metadata::MetadataRecord::V1Topic(
            crate::test_support::single_partition_topic("survives-the-restart", Uuid::new_v4()),
        ));
        ObserverStore::open(dir.path(), 1).maybe_checkpoint(&image, 12);

        // No voters and a dead timer, so the loop resumes and then stops
        // without ever reaching a controller: what it holds came off disk.
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable(), dir.path())).await;

        assert!(
            observer
                .current_image()
                .topic("survives-the-restart")
                .is_some()
        );
        // The checkpoint covers records below offset 12, so the last applied
        // one is 11 — the offset this node reports in its `BrokerHeartbeat`.
        assert!(observer.current_metadata_offset() == 11);
    }

    #[tokio::test]
    async fn the_loop_stops_when_a_voterless_park_cannot_be_armed() {
        // No voters: the loop's only move is to park, and the park cannot be
        // armed, so it stops instead of spinning through a poll that no longer
        // waits.
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable(), dir.path())).await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(observer.watch_leader().borrow().is_none());
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_a_voterless_park_is_armed_but_never_completes() {
        // The other half of the park: the deadline registers and then fails,
        // which ends the loop the same way a refused registration does.
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Completion);
        let observer = run_until_it_stops(config_on(vec![], timer.injectable(), dir.path())).await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_the_back_off_after_an_unreachable_voter_cannot_be_armed() {
        // The fetch fails, so the loop rotates to the next voter and backs off
        // — and the back-off is the park that cannot be armed. It leaves no
        // leader hint, because no voter answered.
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), UNREACHABLE.to_string())],
            timer.injectable(),
            dir.path(),
        ))
        .await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(observer.watch_leader().borrow().is_none());
        assert!(timer.registrations() == 1);
    }

    #[tokio::test]
    async fn the_loop_stops_when_the_caught_up_park_cannot_be_armed() {
        // A fetch that returns no records leaves the observer caught up, which
        // is the third place the loop parks. The record of that answered fetch
        // — the applied offset and the leader hint — survives the shutdown,
        // which is what separates this exit from the unreachable-voter one.
        let mock = crate::test_support::mock_request_broker(
            krabka_raft::API_KEY_METADATA_FETCH,
            move || metadata_fetch_response_body(Bytes::new(), 0, 0),
            api_versions_response_v0,
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), mock.addr.to_string())],
            timer.injectable(),
            dir.path(),
        ))
        .await;

        assert!(observer.current_metadata_offset() == -1);
        assert!(*observer.watch_leader().borrow() == Some(NodeId(1)));
        assert!(timer.registrations() == 1);
        mock.stop();
    }

    /// The serve loop over a controller whose first answer carries
    /// `records` and whose later answers carry none, driven by a timer that
    /// cannot be armed, so the loop stops at its first park. Returns the
    /// observer, read through the `ObserverSource` the broker reads it
    /// through, and the number of parks it tried.
    async fn serve_once(records: Bytes) -> (crate::metadata_source::ObserverSource, usize) {
        let served = Arc::new(AtomicUsize::new(0));
        let mock = {
            let served = Arc::clone(&served);
            krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                    let first = served.fetch_add(1, Ordering::SeqCst) == 0;
                    let body = if first { records.clone() } else { Bytes::new() };
                    return Some(metadata_fetch_response_body(body, 10, 10));
                }
                None
            })
            .await
        };
        let dir = tempfile::tempdir().unwrap();
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), mock.addr.to_string())],
            timer.injectable(),
            dir.path(),
        ))
        .await;
        mock.stop();
        (
            crate::metadata_source::ObserverSource::new(observer, Arc::new(NoWrites)),
            timer.registrations(),
        )
    }

    /// One row per outcome of a fetched response. A record that does not
    /// decode is counted as Kafka's `metadata-load-error-count` and the broker
    /// keeps going: the loop fetches again past the response and parks once
    /// caught up. An invalid `KRaft` control record publishes the fatal fault
    /// that stops the broker, and the loop ends without parking.
    #[tokio::test]
    async fn the_loop_counts_load_errors_and_stops_on_an_invalid_control_record() {
        use crate::{
            metadata_observer::test_support::{
                encode_batches, negative_kraft_version_batch, patched_topic_value, topic_value,
                values_batch,
            },
            metadata_source::MetadataSource as _,
        };

        let control = negative_kraft_version_batch(1);
        let fault = krabka_raft::control_batch_image_records(&control)
            .expect_err("an invalid control record")
            .to_string();
        let cases = [
            (
                "a record that does not decode",
                encode_batches(&[
                    values_batch(0, &[topic_value("before", 1)]),
                    values_batch(1, &[patched_topic_value(1, 99), topic_value("after", 2)]),
                ]),
                (1, None, 2, 1),
            ),
            (
                "an invalid control record",
                encode_batches(&[values_batch(0, &[topic_value("before", 1)]), control]),
                (0, Some(fault), -1, 0),
            ),
        ];
        for (case, records, want) in cases {
            let (source, parks) = serve_once(records).await;

            let got = (
                source.metadata_load_error_count(),
                source.watch_fatal().borrow().clone(),
                source.current_metadata_offset(),
                parks,
            );
            assert!(got == want, "{case}");
            assert!(source.current_image().topic("before").is_some(), "{case}");
            assert!(source.current_image().topic("after").is_none(), "{case}");
        }
    }

    /// A response with a corrupt batch leaves the fetch offset where it was,
    /// and the loop waits out its poll interval before it asks again, rather
    /// than asking the same voter for the same offset in a tight loop. Kafka
    /// asks again on its next poll too; nothing faults and nothing counts.
    #[tokio::test]
    async fn a_corrupt_fetch_waits_for_the_next_poll_rather_than_spinning() {
        use crate::metadata_observer::test_support::{encode_batches, topic_value, values_batch};

        let mut corrupt =
            encode_batches(&[values_batch(0, &[topic_value("never-applied", 1)])]).to_vec();
        *corrupt.last_mut().expect("a batch has bytes") ^= 0xff;
        let corrupt = Bytes::from(corrupt);
        let fetches = Arc::new(AtomicUsize::new(0));
        let mock = {
            let fetches = Arc::clone(&fetches);
            krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, _body| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    return Some(metadata_fetch_response_body(corrupt.clone(), 1, 1));
                }
                None
            })
            .await
        };
        let dir = tempfile::tempdir().unwrap();
        // The park after the corrupt answer is the loop's only wait; a timer
        // that cannot be armed turns it into the loop's exit, so a loop that
        // fetched again without parking would show more than one fetch.
        let timer = BrokenTimer::dead(TimerFailure::Registration);
        let observer = run_until_it_stops(config_on(
            vec![(NodeId(1), mock.addr.to_string())],
            timer.injectable(),
            dir.path(),
        ))
        .await;
        mock.stop();

        assert!(
            (
                fetches.load(Ordering::SeqCst),
                timer.registrations(),
                observer.current_metadata_offset(),
                observer.metadata_load_error_count(),
                observer.watch_fatal().borrow().clone(),
                observer.current_image().topic("never-applied").is_some(),
            ) == (1, 1, -1, 0, None, false)
        );
    }

    /// A broker-only node that landed on a follower moves to the leader that
    /// follower names, because only the leader records the observers it sees
    /// for `DescribeQuorum`, and it identifies itself to that leader in every
    /// fetch.
    #[tokio::test]
    async fn the_loop_moves_to_the_leader_a_follower_names_and_names_itself_there() {
        use std::sync::Mutex;

        // The fetches each controller was sent, as (node id, directory id).
        type Seen = Arc<Mutex<Vec<(i32, Uuid)>>>;
        let serve = |seen: Seen| {
            move |api_key: i16, _version: i16, _corr_id: i32, body: &[u8]| {
                if api_key == api_versions_request::API_KEY {
                    return Some(api_versions_response_v0());
                }
                if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                    // The request header precedes the fixed 32-byte payload.
                    let payload = &body[body.len() - 32..];
                    let request =
                        krabka_raft::KrabkaMetadataFetchRequest::decode_v0(&mut &payload[..])
                            .expect("a metadata fetch request");
                    seen.lock()
                        .unwrap()
                        .push((request.replica_id, request.replica_directory_id));
                    // Both controllers name node 2 as the leader.
                    return Some(metadata_fetch_response_naming(2, Bytes::new(), 5, 5));
                }
                None
            }
        };
        let (follower_seen, leader_seen) = (Seen::default(), Seen::default());
        let follower =
            krabka_client_core::MockBroker::start(serve(Arc::clone(&follower_seen))).await;
        let leader = krabka_client_core::MockBroker::start(serve(Arc::clone(&leader_seen))).await;
        let dir = tempfile::tempdir().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![
                (krabka_raft::NodeId(1), follower.addr.to_string()),
                (krabka_raft::NodeId(2), leader.addr.to_string()),
            ],
            client_id: "follow-test".into(),
            // Real time, so the parked poll wakes and the loop goes on to the
            // leader.
            poll_interval: millis(10),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
        });

        // The first fetch goes to the first voter. From then on the loop stays
        // on the leader it names.
        for _ in 0..100_000 {
            if !leader_seen.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        observer.cancel().await;
        follower.stop();
        leader.stop();

        let identity = (2, Uuid::from_u128(2));
        let (follower_seen, leader_seen) = (
            follower_seen.lock().unwrap().clone(),
            leader_seen.lock().unwrap().clone(),
        );
        assert!(
            follower_seen == vec![identity],
            "follower {follower_seen:?}, leader {leader_seen:?}"
        );
        assert!(leader_seen.first() == Some(&identity));
    }

    /// The observer records the *quorum's* committed offset, not the watermark
    /// of whichever controller answered.
    ///
    /// Every controller serves `MetadataFetch`, and the loop keeps whichever
    /// voter responds, so the responder can be a follower whose own watermark
    /// is clamped to a log end far below what the quorum has committed. An
    /// observer that read that value would draw level with the lagging
    /// follower and report zero readiness lag against a quorum thousands of
    /// records ahead.
    #[tokio::test]
    async fn the_quorums_committed_offset_is_read_past_a_lagging_responder() {
        let mock = crate::test_support::mock_request_broker(
            krabka_raft::API_KEY_METADATA_FETCH,
            move || {
                // A follower holding 5 of the quorum's 10 000 records.
                metadata_fetch_response_body(Bytes::new(), 5, 10_000)
            },
            api_versions_response_v0,
        )
        .await;
        let dir = tempfile::tempdir().unwrap();
        let observer = MetadataObserver::start(ObserverConfig {
            voters: vec![(krabka_raft::NodeId(1), mock.addr.to_string())],
            client_id: "lag-test".into(),
            poll_interval: millis(250),
            timer: ManualMonotonicClock::new_shared().new_timer(),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
        });

        // The fetch is real loopback I/O, so drive the executor until the
        // first successful round trip has published its offsets.
        let mut recorded = -1;
        for _ in 0..100_000 {
            recorded = observer.quorum_committed_offset();
            if recorded != -1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        // 10 000 committed records is offsets 0..=9_999.
        assert!(recorded == 9_999);
        assert!(observer.current_metadata_offset() == -1);

        // The readiness probe reads these two offsets through
        // `health::metadata_progress`, and this is the only fixture where they
        // differ -- so it is where the mapping is pinned. Swapped, or both
        // taken from one side, a node 10 000 records behind would report no
        // lag at all and `/readyz` would answer 200 for it.
        let source = Arc::new(crate::metadata_source::ObserverSource::new(
            observer.clone(),
            Arc::new(NoWrites),
        ));
        let progress = crate::health::metadata_progress(source);
        assert!(progress.node_metadata_offset() == -1);
        assert!(progress.quorum_committed_offset() == 9_999);

        observer.cancel().await;
        mock.stop();
    }
}
