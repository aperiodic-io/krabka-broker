//! The model configuration and its stateright implementation: the initial
//! state, the enabled actions, the transition, the properties and the search
//! boundary.
//!
//! A `Model` implementation is one indivisible unit — the action generator,
//! the transition and the properties only make sense against each other — so
//! it stays whole in this file, and every transition it takes calls out to the
//! seam modules that wrap the real broker cores.

use {
    super::{
        bounds::{MAX_EPOCH, MAX_LEN, NB, NB_U8, has, model_index, model_offset},
        election::do_failover,
        elr,
        hwm::{consistent_leo, real_hwm, real_wal_hwm},
        state::{
            Act, AssignedRange, DpState, ELR_BEAT_LONGER_LOG, ELR_DROPPED_COMMITTED, ELR_ELECTED,
            isr_eligible,
        },
        truncation::real_truncation_offset,
    },
    crate::handlers::fetch::{FetchWatermarks, compute_visibility_window},
    krabka_log::Offset,
    krabka_metadata::{MetadataImage, MetadataRecord, PartitionOffsetAdvanceRecord},
    stateright::{Model, Property},
    std::time::Instant,
};

pub(super) struct DpModel {
    pub(super) base: Instant,
    pub(super) unclean: bool,  // false in DPC-1/2 (clean), true in DPC-3
    pub(super) diskless: bool, // true drives the WAL durability path instead of ISR-HWM
    /// The metadata image the real controller rules read. It carries the
    /// topic's `min.insync.replicas`, which is what decides both when the ELR
    /// rule clears the set and when the leader's high watermark stops.
    image: MetadataImage,
    /// `min.insync.replicas` as [`effective_min_insync_replicas`] resolves it
    /// out of [`Self::image`], not a second copy of the number.
    ///
    /// [`effective_min_insync_replicas`]: crate::config_keys::effective_min_insync_replicas
    min_isr: usize,
    /// The longest log this configuration lets a broker reach, at most
    /// [`MAX_LEN`]. Each extra record multiplies the reachable states, and the
    /// ELR configuration carries more per-state than the others, so it buys
    /// its ELR bookkeeping back out of its log length.
    max_len: usize,
}

/// The base offset a reservation hands out, and the next offset the image holds
/// afterwards.
struct Reservation {
    base: i64,
    next: i64,
}

impl DpModel {
    /// A configuration of the modelled cluster. `min_isr` is the topic's
    /// `min.insync.replicas`: at 1 no partition can ever have a non-empty ELR,
    /// because the rule clears the set as soon as the ISR meets the threshold
    /// and an ISR that reached zero has no partition record left to reach it
    /// with, so only a configuration above 1 exercises the ELR rule.
    pub(super) fn config(
        base: Instant,
        unclean: bool,
        diskless: bool,
        min_isr: usize,
        max_len: usize,
    ) -> Self {
        assert2::assert!(
            max_len <= MAX_LEN,
            "the cast helpers are bounded by MAX_LEN"
        );
        assert2::assert!(
            min_isr == 1 || (unclean && !diskless),
            "only the unclean replicated configuration is configured with an ELR"
        );
        let image = elr::image(min_isr);
        let min_isr = elr::min_insync_replicas(&image);
        Self {
            base,
            unclean,
            diskless,
            image,
            min_isr,
            max_len,
        }
    }

    /// Whether this configuration maintains and checks KIP-966 ELR state.
    ///
    /// At Kafka's default `min.insync.replicas` of 1 the rule clears the set
    /// on every change a live partition can make, so the other configurations
    /// would carry an always-empty set through every state they enumerate --
    /// state identity they pay for in the search and get nothing back from.
    /// They leave it empty instead, and the ELR properties are stated only
    /// here, where a `sometimes` property has states that can witness it.
    fn tracks_elr(&self) -> bool { self.min_isr > 1 }

    /// Reserve `count` diskless offsets the way the controller does, over a
    /// partition whose committed next offset is `committed_next`.
    ///
    /// The controller's `submit_change` takes the base from the image's
    /// `partition_next_offset`, reserves the range with the proved
    /// [`reserve_offsets`](krabka_verified::reserve_offsets), and commits a
    /// `PartitionOffsetAdvance` whose replay bumps the image's next offset.
    /// This runs those three production pieces in that order and returns the
    /// reserved base and the next offset the image holds afterwards, so
    /// `offsets_contiguous_and_unique` checks what they compute rather than
    /// what the model wrote down.
    ///
    /// The image the state stands for is rebuilt from its one number: every
    /// advance adds to the same counter, so a single advance of
    /// `committed_next` replays to the same image as the history that reached
    /// it.
    fn reserve(&self, committed_next: i64, count: i64) -> Reservation {
        let advance = |count| {
            MetadataRecord::V1PartitionOffsetAdvance(PartitionOffsetAdvanceRecord {
                topic: elr::TOPIC.to_string(),
                partition: elr::PARTITION,
                count,
            })
        };
        let mut image = self.image.clone();
        if committed_next > 0 {
            image.apply(&advance(committed_next));
        }
        let next_offset = image
            .partition_next_offset(elr::TOPIC, elr::PARTITION)
            .unwrap_or(0);
        let (base, _) = krabka_verified::reserve_offsets(next_offset, count)
            .expect("a bounded model reservation is representable");
        image.apply(&advance(count));
        let next = image
            .partition_next_offset(elr::TOPIC, elr::PARTITION)
            .expect("the advance just applied");
        Reservation { base, next }
    }
}

impl Model for DpModel {
    type Action = Act;
    type State = DpState;

    krabka_macros::model_transition!(last, a, s; {
            match a {
                Act::Produce => {
                    s.log[usize::from(s.leader)].push(s.leader_epoch);
                }
                Act::Assign(count) => {
                    let Reservation { base, next } = self.reserve(s.seq_next, i64::from(count));
                    s.assigned.push(AssignedRange {
                        start: base,
                        end: base + i64::from(count),
                    });
                    s.seq_next = next;
                }
                Act::Replicate(b) => {
                    let leader_log = s.log[usize::from(s.leader)].clone();
                    let trunc =
                        model_index(real_truncation_offset(&s.log[usize::from(b)], &leader_log));
                    s.log[usize::from(b)].truncate(trunc);
                    if s.log[usize::from(b)].len() < leader_log.len() {
                        let off = s.log[usize::from(b)].len();
                        s.log[usize::from(b)].push(leader_log[off]);
                    }
                }
                Act::AdvanceHwm => {
                    // The real core: frozen while the ISR is under min ISR (Kafka's
                    // `Partition.maybeIncrementLeaderHW`), otherwise the minimum ISR
                    // LEO if that is higher. It never falls within one leadership;
                    // only an election that drops records lowers it, in
                    // `apply_elect`. Every record it passes therefore reached the
                    // HWM with at least min ISR replicas holding it, which is the
                    // obligation KIP-966's eligible-leader set is a claim about.
                    s.hwm = real_hwm(&s, self.base, self.min_isr);
                    let leader_log = &s.log[usize::from(s.leader)];
                    append_committed_prefix(&mut s.committed, leader_log, s.hwm);
                }
                Act::WalSync => {
                    // fsync makes the leader's appended prefix durable and releases
                    // it through the same HW seam the broker's diskless path uses.
                    let leader_log = &s.log[s.leader as usize];
                    while s.wal_acked.len() < leader_log.len() {
                        let off = s.wal_acked.len();
                        s.wal_acked.push(leader_log[off]);
                    }
                    s.hwm = real_wal_hwm(s.leader, model_offset(s.wal_acked.len()), self.base);
                    append_committed_prefix(&mut s.committed, leader_log, s.hwm);
                }
                Act::ConsumerFetch {
                    read_committed,
                    fetch_offset,
                } => {
                    let leader_log_len = s.leader_leo();
                    let vw = compute_visibility_window(
                        false, // consumer, not follower
                        read_committed,
                        FetchWatermarks {
                            log_start: Offset(0),
                            hw: Offset(s.hwm),
                            lso: Offset(s.hwm), // lso = hwm (no txns in v1)
                            log_end: Offset(leader_log_len),
                            // This model's topic delivers immediately.
                            deliverable: Offset(s.hwm),
                        },
                        Offset(fetch_offset),
                    );
                    assert2::assert!(
                        vw.limit_offset <= s.hwm,
                        "consumer limit {} exceeds HWM {}",
                        vw.limit_offset,
                        s.hwm
                    );
                    assert2::assert!(vw.response_hw == s.hwm, "response_hw drift");
                }
                Act::Die(b) => {
                    s.live &= !(1 << b);
                }
                Act::Revive(b) => {
                    s.live |= 1 << b;
                }
                Act::ExpandIsr(b) => {
                    let previous = elr::partition_record(s.leader, s.isr, s.leader_epoch);
                    s.isr |= 1 << b;
                    // An ISR change is a partition change, and every controller
                    // path that submits one runs the ELR publisher over it. An
                    // expansion back to min ISR is how the set empties again.
                    if self.tracks_elr() {
                        elr::maintain(&self.image, &mut s, &previous);
                    }
                }
                Act::Failover(dead) => do_failover(
                    self.tracks_elr().then_some(&self.image),
                    &mut s,
                    dead,
                    self.unclean,
                ),
            }
            Some(s)

    });

    fn init_states(&self) -> Vec<Self::State> {
        vec![DpState {
            log: [vec![], vec![], vec![]],
            hwm: 0,
            leader: 0,
            leader_epoch: 1,
            // The diskless runtime foundation is a single-node local-fsync WAL. Keep the RF=3
            // model for classic clean/unclean checks, but constrain diskless to
            // the leader broker so WAL durability is not incorrectly invalidated
            // by electing a different replica that never fsynced the record.
            isr: if self.diskless { 0b001 } else { 0b111 },
            live: if self.diskless { 0b001 } else { 0b111 },
            // A partition whose ISR meets min ISR has no eligible-leader set,
            // and every configuration starts with a full ISR.
            elr: 0,
            committed: vec![],
            wal_acked: vec![],
            seq_next: 0,
            assigned: vec![],
            lost: false,
            elr_trace: 0,
        }]
    }

    fn actions(&self, s: &Self::State, acts: &mut Vec<Self::Action>) {
        let leader_live = has(s.live, s.leader);
        // Data-path actions require a live leader.
        if leader_live {
            if s.log[usize::from(s.leader)].len() < self.max_len && s.leader_epoch <= MAX_EPOCH {
                acts.push(Act::Produce);
                if self.diskless && s.assigned.len() < 3 {
                    acts.push(Act::Assign(1));
                    acts.push(Act::Assign(2));
                }
            }
            if self.diskless && s.wal_acked.len() < s.log[s.leader as usize].len() {
                acts.push(Act::WalSync);
            }
            if !self.diskless {
                for b in 0..NB_U8 {
                    if b != s.leader
                        && has(s.live, b)
                        && model_offset(s.log[usize::from(b)].len()) < s.leader_leo()
                    {
                        acts.push(Act::Replicate(b));
                    }
                }
                acts.push(Act::AdvanceHwm);
            }
            for fo in 0..=s.leader_leo() {
                acts.push(Act::ConsumerFetch {
                    read_committed: false,
                    fetch_offset: fo,
                });
                acts.push(Act::ConsumerFetch {
                    read_committed: true,
                    fetch_offset: fo,
                });
            }
        }
        // Liveness + failover.
        let live_count = u32::from(s.live).count_ones();
        for b in 0..NB_U8 {
            if self.diskless && b != s.leader {
                continue;
            }
            if has(s.live, b) && (live_count > 1 || self.diskless) {
                acts.push(Act::Die(b));
            }
            if !has(s.live, b) {
                acts.push(Act::Revive(b));
                // Controller failover: elect (dead leader, epoch headroom) or
                // shrink the ISR (dead non-leader ISR member).
                if !self.diskless
                    && ((b == s.leader && s.leader_epoch < MAX_EPOCH)
                        || (b != s.leader && has(s.isr, b)))
                {
                    acts.push(Act::Failover(b));
                }
            }
            // Re-admit a follower to the ISR only once the leader's real
            // expansion rule admits it: an epoch-consistent prefix of the
            // leader's log (it has truncated + replicated any divergence via
            // the real protocol) whose log end reaches the HWM and the start of
            // the leader's epoch. Checking LEO alone would admit a stale,
            // divergent follower that hasn't reconciled — which is unreachable
            // in real Kafka, where the follower fetch/OffsetForLeaderEpoch loop
            // truncates before its reported progress can make it eligible.
            if !self.diskless
                && has(s.live, b)
                && b != s.leader
                && !has(s.isr, b)
                && isr_eligible(s, b)
            {
                acts.push(Act::ExpandIsr(b));
            }
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut props = vec![
            Property::always("committed_durable", |_, s: &DpState| {
                let lg = &s.log[usize::from(s.leader)];
                s.committed
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("wal_acked_durable", |_, s: &DpState| {
                let lg = &s.log[s.leader as usize];
                s.wal_acked
                    .iter()
                    .enumerate()
                    .all(|(off, &e)| lg.get(off) == Some(&e))
            }),
            Property::always("hwm_within_leader_log", |_, s: &DpState| {
                s.hwm <= s.leader_leo()
            }),
        ];
        if self.diskless {
            props.extend([
                Property::always("diskless_hw_released_by_wal_sync", |_, s: &DpState| {
                    s.hwm == model_offset(s.wal_acked.len())
                }),
                Property::sometimes("wal_acked_progress", |_, s: &DpState| {
                    !s.wal_acked.is_empty()
                }),
                Property::sometimes("wal_acked_survives_broker_down", |_, s: &DpState| {
                    !has(s.live, s.leader) && !s.wal_acked.is_empty()
                }),
                Property::always("offsets_contiguous_and_unique", |_, s: &DpState| {
                    let mut next = 0;
                    for &AssignedRange { start, end } in &s.assigned {
                        if start != next || end <= start {
                            return false;
                        }
                        next = end;
                    }
                    next == s.seq_next
                }),
            ]);
        } else {
            props.extend([
                Property::sometimes("committed_progress", |_, s: &DpState| {
                    !s.committed.is_empty()
                }),
                Property::sometimes("full_replication", |_, s: &DpState| {
                    s.hwm == s.leader_leo() && s.hwm > 0
                }),
                // A leader change occurred.
                Property::sometimes("leader_changed", |_, s: &DpState| s.leader_epoch >= 2),
                // The ISR shrank below the full replica set.
                Property::sometimes("isr_shrunk", |_, s: &DpState| {
                    u32::from(s.isr).count_ones() < u32::from(NB_U8)
                }),
                // Two brokers hold different epochs at one offset — truncation
                // territory (a follower must truncate to reconcile).
                Property::sometimes("divergence_present", |_, s: &DpState| {
                    (0..MAX_LEN).any(|off| {
                        let mut seen: Option<u8> = None;
                        for b in 0..NB {
                            if let Some(&e) = s.log[b].get(off) {
                                match seen {
                                    None => seen = Some(e),
                                    Some(x) if x != e => return true,
                                    _ => {}
                                }
                            }
                        }
                        false
                    })
                }),
            ]);
        }
        if self.tracks_elr() {
            props.extend([
                // THE CLAIM. `failover_one` and `select_leader` elect a
                // surviving eligible leader replica ahead of a longer log and
                // report that election as losing nothing -- the
                // unclean-election counter does not count it, the audit reason
                // says no committed record is lost, and KFC-9's `require` gate
                // lets it through. All of that rests on the published set
                // naming only replicas that hold every committed record, which
                // KIP-966 gets from the leader's high watermark standing still
                // while the ISR is under min ISR. This is that, stated over a
                // set the model did not choose but computed with the real
                // maintenance rule, and over a watermark the real core moved.
                Property::always("elr_holds_every_committed_record", |_, s: &DpState| {
                    (0..NB_U8).filter(|&b| has(s.elr, b)).all(|b| {
                        let log = &s.log[usize::from(b)];
                        s.committed
                            .iter()
                            .enumerate()
                            .all(|(off, &e)| log.get(off) == Some(&e))
                    })
                }),
                // The same claim at the moment it is cashed in: the election
                // that took the ELR rule did not drop a committed record.
                Property::always(
                    "elr_election_keeps_every_committed_record",
                    |_, s: &DpState| s.elr_trace & ELR_DROPPED_COMMITTED == 0,
                ),
                // Anti-vacuity for the rule the claim rests on: a state in
                // which every ISR member holds a record past the HWM, so a
                // watermark that ignored min ISR would have committed it, but
                // the ISR is under min ISR and the watermark stayed.
                Property::sometimes("hwm_held_by_min_isr", |m: &DpModel, s: &DpState| {
                    let leader_log = &s.log[usize::from(s.leader)];
                    has(s.isr, s.leader)
                        && usize::try_from(u32::from(s.isr).count_ones())
                            .expect("a bitmask over three brokers counts low")
                            < m.min_isr
                        && (0..NB_U8)
                            .filter(|&b| has(s.isr, b))
                            .all(|b| consistent_leo(&s.log[usize::from(b)], leader_log) > s.hwm)
                }),
                // Anti-vacuity. Without these three the two `always`
                // properties above would pass on a model that never publishes
                // an ELR, never elects out of one, or only ever elects the
                // replica the fallback would have picked anyway.
                Property::sometimes("elr_published", |_, s: &DpState| s.elr != 0),
                Property::sometimes("elr_election_taken", |_, s: &DpState| {
                    s.elr_trace & ELR_ELECTED != 0
                }),
                Property::sometimes("elr_election_beat_a_longer_log", |_, s: &DpState| {
                    s.elr_trace & ELR_BEAT_LONGER_LOG != 0
                }),
            ]);
        }
        if self.unclean {
            // Loss characterization: an unclean-election data loss is reachable
            // (and `committed_durable` above still holds — `committed` is the LIVE
            // durability obligation, truncated when an unclean election drops it).
            props.push(Property::sometimes("unclean_loss", |_, s: &DpState| s.lost));
        } else {
            // Clean config: NO committed-data loss ever occurs.
            props.push(Property::always("no_loss_when_clean", |_, s: &DpState| {
                !s.lost
            }));
        }
        props
    }

    fn within_boundary(&self, s: &Self::State) -> bool {
        s.log.iter().all(|l| l.len() <= self.max_len) && s.leader_epoch <= MAX_EPOCH + 1
    }
}

fn append_committed_prefix<T: Copy>(committed: &mut Vec<T>, leader_log: &[T], hwm: i64) {
    while model_offset(committed.len()) < hwm {
        let offset = committed.len();
        committed.push(leader_log[offset]);
    }
}
