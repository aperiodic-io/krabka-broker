//! Exhaustive stateright interleaving model of the `krabka_throttle::TokenBucket`
//! concurrency, stepped one shared-memory access at a time.
//!
//! # What is modeled
//!
//! The state is the bucket's shared group `{rate, burst, available,
//! last_refill}`, the fast-path rate mirror, the lock, a clock, a dedicated
//! resetter thread, and a program counter per consumer thread. Each action is
//! one access to shared memory, in the order production makes it; values a
//! thread read earlier travel in its program counter. A `Tick` advances the
//! clock by one unit at any point, so a consumer's clock reading can be stale
//! by the time it acts on it.
//!
//! Two algorithms are modeled:
//!
//! * [`Algorithm::Locked`] follows production locking with whole-credit clocks.
//!   A consumer reads the rate mirror
//!   without the lock and grants a rate-0 request there. Otherwise it takes the
//!   lock, reads the clock, stores the claimed `last_refill`, stores
//!   `available`, and releases. A reset takes the lock, reads the clock, refills
//!   the old configuration to now, keeps that balance under the new burst,
//!   stores the group, publishes the mirror, and releases. A bucket with no old
//!   or no new rate starts full instead.
//! * [`Algorithm::SeqlockCas`] is the lock-free design production used before.
//!   A consumer reads the rate, then the seqlock generation (spinning while it
//!   is odd), the rate and the burst, the clock, and `last_refill`, then claims
//!   the refill with a compare-and-exchange on `last_refill`, reads
//!   `available`, re-checks the generation, and commits `available` with a
//!   second compare-and-exchange. Any failure restarts from the generation
//!   read. A reset makes the generation odd, stores rate, burst, `available`,
//!   and `last_refill` one at a time, and makes the generation even again.
//!   The runs over it are RED witnesses: they must find its two bugs.
//!
//! # Driven and modeled
//!
//! DRIVEN: the production cap-and-grant arithmetic,
//! [`krabka_verified::throttle::plan_consume`], and the whole-token request
//! cut, [`krabka_throttle::whole_token_request`], at every commit.
//!
//! MODELED: one clock unit is one second and every quantity is in the
//! bucket's storage unit, `units_per_token` to a token; production stores
//! micro-tokens. Every rate is a whole number of storage units per second, so
//! a refill is `elapsed * rate` and a consume claims the whole elapsed gap.
//! Production's part-unit remainder is covered by the unit tests in
//! `src/runtime/consume.rs`. A consume asks for whole tokens and is granted
//! whole tokens, so with `units_per_token > 1` a rate or a burst of a
//! fraction of a token leaves part tokens in the bucket, as production does
//! under a fractional quota rate. Memory is sequentially consistent: the
//! model checks the protocol, not the atomic orderings, and `Locked` needs no
//! ordering beyond what the lock gives.
//!
//! # Properties
//!
//! The ghost state records, for the configuration in force since the last
//! reset, the reset's clock reading `t0`, the balance the reset started with,
//! the tokens granted, and the tokens the burst capped away. Both safety properties are checked whenever no reset
//! is part-way through its stores; a seqlock reader never acts on such a
//! state, and under the lock nobody can see one.
//!
//! * `available_within_burst`: `available <= burst`.
//! * `grants_whole_tokens`: every grant in the configuration is a whole
//!   number of tokens, even when the bucket holds a part token.
//! * `claimed_refill_conserved`: `available + granted + capped + in_flight ==
//!   base + rate * (last_refill - t0)`. The right side is every token the
//!   configuration has made available: the balance it started with plus the
//!   refill for all the time consumers have claimed. `in_flight` is the refill a consumer
//!   has claimed time for in this configuration but not committed yet. The
//!   equation fails if a claimed refill vanishes (tokens lost) or if a commit
//!   adds tokens the configuration never made (tokens over-granted).
//!
//! Reachability witnesses show that consumers overlap, a reset overlaps a
//! consume, a claimed refill is in flight, a refill is granted and another is
//! capped, a reset shrinks the burst, the bucket drains, and a reset keeps a
//! balance under the new burst. Debt is outside the model: `try_consume`
//! never creates it, and `record` differs from a consume only in what it does
//! with the part it cannot grant, which it keeps as debt, at most what the
//! refill repays in `max_wait` for `record_bounded`. The unit tests in
//! `src/runtime/consume.rs` cover both. Fractional micro-token numerators and
//! nanosecond clock claims are also outside this bounded model; the production
//! adapter tests and the verified refill-partition composition cover them.
//!
//! # Runs
//!
//! `bucket_basic`, `bucket_wide` and `bucket_fractional` check `Locked`
//! exhaustively and pin their unique-state counts. `bucket_fractional` runs
//! at two storage units to a token, so its rates and bursts are half tokens. The RED runs check `SeqlockCas` in the smallest search
//! that shows each bug, and `locked_holds_where_seqlock_cas_fails` checks
//! `Locked` in the same searches. Two scripted schedules replay each bug step
//! by step, and show that the lock disables the same interleaving.
//!
//! An earlier version of this model made the generation check and the
//! `available` write one atomic step, tied the burst to the rate, blocked a
//! consume from starting while a reset ran, and claimed the refill with a swap
//! on an abstract `pending` counter. It could not reach either bug, and
//! production shipped both.

use {
    krabka_throttle::whole_token_request,
    krabka_verified::throttle::{
        AvailableTokens, BurstCapacity, RefillTokens, RequestedTokens, plan_consume,
    },
    stateright::{Checker, Model, Path, Property},
};

const TARGET_STATE_COUNT: usize = 4_000_000;
const MAX_DEPTH: usize = 80;

// The exact unique-state count of the exhaustive BFS over each config below.
// `unique_state_count()` is deterministic for a fixed model, so pinning it
// turns any change to the reachable set -- a dropped action, a `next_state` arm
// that starts returning `None`, a derived `Hash`/`PartialEq` that stops
// considering a field -- into a failure instead of a silently smaller search
// that still passes the upper bound. The *generated* count is deliberately not
// pinned: it depends on dedupe timing across the BFS worker threads.
//
// These pins replaced 1,352 and 7,635 when the model was rewritten to step
// like production: separate shared accesses per step, a burst apart from the
// rate, a clock and `last_refill` instead of a `pending` counter, a dedicated
// resetter over several configurations, the rate mirror and the lock, and the
// conservation ghosts. They then moved from 41,601, 222,236 and 25,984 when a
// reset began to keep the balance: a reset now lands in states with any
// carried balance, and the ghost `base` tells configurations apart by it.
const PINNED_UNIQUE_STATES_BASIC: usize = 56_259;
const PINNED_UNIQUE_STATES_WIDE: usize = 372_204;
const PINNED_UNIQUE_STATES_FRACTIONAL: usize = 33_964;

/// Which consume and reset protocol the model steps through.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Algorithm {
    /// Production: one lock around each consume and each reset.
    Locked,
    /// The former lock-free seqlock with two compare-and-exchanges.
    SeqlockCas,
}

/// One `(rate, burst)` configuration a reset can install.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Config {
    rate: u64,
    burst: u64,
}

/// Refill a consumer has claimed time for and not yet committed.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
struct Claim {
    refill: u64,
    /// The ghost configuration epoch the claim moved `last_refill` in.
    epoch: u64,
}

/// A consumer's program counter. Each variant names the next shared access.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Consumer {
    Idle,
    /// Both algorithms: read the rate without the lock or the generation.
    FastRate {
        req: u64,
    },

    /// `Locked`: take the lock.
    Acquire {
        req: u64,
    },
    /// `Locked`, lock held: read the clock.
    LockedClock {
        req: u64,
    },
    /// `Locked`, lock held: store the claimed `last_refill`.
    LockedClaim {
        req: u64,
        now: u64,
    },
    /// `Locked`, lock held: store `available`.
    LockedCommit {
        req: u64,
        claim: Claim,
    },
    /// `Locked`, lock held: release it.
    Release,

    /// `SeqlockCas`: the next access, and what this attempt has read so far.
    Seqlock {
        next: SeqStep,
        read: SeqRead,
    },
}

/// The next shared access of a `SeqlockCas` consume attempt.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum SeqStep {
    /// Read the generation; spin while it is odd.
    LoadGen,
    /// Read the rate.
    LoadRate,
    /// Read the burst.
    LoadBurst,
    /// Zero burst: re-read the generation before granting 0.
    ZeroBurstCheck,
    /// Read the clock.
    Clock,
    /// Read `last_refill`.
    LoadLast,
    /// Compare-and-exchange `last_refill` from `last` to `last + claimed`.
    ClaimCas,
    /// Read `available`.
    LoadAvail,
    /// Re-read the generation.
    GenCheck,
    /// Compare-and-exchange `available` from `cur`.
    AvailCas,
}

/// The values a `SeqlockCas` consume attempt has read or computed. A restart
/// clears every field but `req`, so a claim held by a failed attempt is gone.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Default)]
struct SeqRead {
    req: u64,
    generation: u64,
    rate: u64,
    burst: u64,
    now: u64,
    last: u64,
    claimed: u64,
    refill: u64,
    cur: u64,
    claim: Option<Claim>,
}

impl SeqRead {
    /// A fresh attempt: the program counter of a (re)start.
    fn restart(req: u64) -> Consumer {
        Consumer::Seqlock {
            next: SeqStep::LoadGen,
            read: Self {
                req,
                ..Self::default()
            },
        }
    }
}

impl Consumer {
    /// The claim this consumer holds, if it holds one.
    fn claim(self) -> Option<Claim> {
        match self {
            Self::LockedCommit { claim, .. } => Some(claim),
            Self::Seqlock { read, .. } => read.claim,
            _ => None,
        }
    }
}

/// The resetter's program counter. Each variant names the next shared access.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Resetter {
    Idle,
    /// `Locked`: take the lock.
    Acquire {
        config: Config,
    },
    /// `Locked`, lock held: read the clock and store the whole group.
    Write {
        config: Config,
    },
    /// `Locked`, lock held: publish the rate mirror.
    Publish {
        config: Config,
    },
    /// `Locked`, lock held: release it.
    Release,
    /// `SeqlockCas`: make the generation odd.
    Enter {
        config: Config,
    },
    /// `SeqlockCas`: store the rate.
    StoreRate {
        config: Config,
    },
    /// `SeqlockCas`: store the burst.
    StoreBurst {
        config: Config,
    },
    /// `SeqlockCas`: store `available`.
    StoreAvail {
        config: Config,
    },
    /// `SeqlockCas`: read the clock and store `last_refill`.
    StoreLast,
    /// `SeqlockCas`: make the generation even.
    Leave,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct BucketState {
    rate: u64,
    burst: u64,
    available: u64,
    last_refill: u64,
    /// `Locked`: the lock-free copy of `rate` the fast path reads.
    mirror: u64,
    /// `Locked`: whether a thread holds the lock.
    locked: bool,
    /// `SeqlockCas`: the seqlock generation.
    generation: u64,
    now: u64,
    resets: usize,
    consumers: Vec<Consumer>,
    resetter: Resetter,
    // Ghost state for the configuration in force since the last reset.
    epoch: u64,
    t0: u64,
    /// The balance the configuration started with: the burst under the first
    /// configuration and under the seqlock's resets, the balance the reset
    /// carried over under production's.
    base: u64,
    granted: u64,
    capped: u64,
}

impl BucketState {
    /// A reset has begun storing the group and has not finished.
    fn resetting(&self) -> bool {
        matches!(
            self.resetter,
            Resetter::StoreRate { .. }
                | Resetter::StoreBurst { .. }
                | Resetter::StoreAvail { .. }
                | Resetter::StoreLast
                | Resetter::Leave
        )
    }

    /// Refill claimed in the current configuration and not yet committed.
    fn in_flight(&self) -> u64 {
        self.consumers
            .iter()
            .filter_map(|c| c.claim())
            .filter(|claim| claim.epoch == self.epoch)
            .map(|claim| claim.refill)
            .sum()
    }

    /// Commits the planned consume of `req` whole tokens from `cur` plus
    /// `refill` under `burst`, in storage units, `units_per_token` to a token,
    /// recording the grant and the capped remainder in the ghosts.
    ///
    /// Production's whole-token consume: cap the refill, cut the request to
    /// the whole tokens the capped balance holds, then grant.
    fn commit(&mut self, cur: u64, refill: u64, burst: u64, req: u64, units_per_token: u64) {
        let (_, total) = plan_consume(
            AvailableTokens(cur),
            RefillTokens(refill),
            BurstCapacity(burst),
            RequestedTokens(0),
        );
        let (grant, new) = plan_consume(
            AvailableTokens(total.0),
            RefillTokens(0),
            BurstCapacity(burst),
            RequestedTokens(whole_token_request(req, total.0, units_per_token)),
        );
        self.available = new.0;
        self.granted += grant.0;
        self.capped += cur + refill - total.0;
    }

    /// Starts a new ghost configuration after a reset stored the group.
    fn start_epoch(&mut self) {
        self.epoch += 1;
        self.granted = 0;
        self.capped = 0;
    }
}

fn available_within_burst(s: &BucketState) -> bool { s.resetting() || s.available <= s.burst }

fn claimed_refill_conserved(s: &BucketState) -> bool {
    s.resetting()
        || s.available + s.granted + s.capped + s.in_flight()
            == s.base + s.rate * (s.last_refill - s.t0)
}

struct BucketModel {
    algorithm: Algorithm,
    /// Storage units to a token: `1` meters whole tokens, `2` half tokens.
    units_per_token: u64,
    consumers: usize,
    /// `configs[0]` is installed at start; a reset installs any of them.
    configs: Vec<Config>,
    max_resets: usize,
    max_time: u64,
    max_req: u64,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Act {
    Tick,
    StartConsume { consumer: usize, req: u64 },
    StartReset { config: usize },
    StepConsumer(usize),
    StepResetter,
}

impl BucketModel {
    fn step_consumer(&self, s: &mut BucketState, t: usize) -> Option<()> {
        s.consumers[t] = match s.consumers[t] {
            Consumer::Idle => return None,
            Consumer::FastRate { req } => match self.algorithm {
                Algorithm::Locked if s.mirror == 0 => Consumer::Idle,
                Algorithm::Locked => Consumer::Acquire { req },
                Algorithm::SeqlockCas if s.rate == 0 => Consumer::Idle,
                Algorithm::SeqlockCas => SeqRead::restart(req),
            },
            Consumer::Acquire { req } => {
                if s.locked {
                    return None;
                }
                s.locked = true;
                Consumer::LockedClock { req }
            }
            Consumer::LockedClock { req } => Consumer::LockedClaim { req, now: s.now },
            Consumer::LockedClaim { req, now } => {
                if s.rate == 0 {
                    Consumer::Release
                } else {
                    let claimed = now.saturating_sub(s.last_refill);
                    s.last_refill += claimed;
                    let claim = Claim {
                        refill: claimed * s.rate,
                        epoch: s.epoch,
                    };
                    Consumer::LockedCommit { req, claim }
                }
            }
            Consumer::LockedCommit { req, claim } => {
                s.commit(
                    s.available,
                    claim.refill,
                    s.burst,
                    req,
                    self.units_per_token,
                );
                Consumer::Release
            }
            Consumer::Release => {
                s.locked = false;
                Consumer::Idle
            }
            Consumer::Seqlock { next, read } => Self::step_seqlock(s, next, read)?,
        };
        Some(())
    }

    /// One shared access of a `SeqlockCas` consume attempt, mirroring the
    /// former `try_consume` loop line by line. `None` means the access spins.
    fn step_seqlock(s: &mut BucketState, next: SeqStep, mut read: SeqRead) -> Option<Consumer> {
        let restart = SeqRead::restart(read.req);
        let next = match next {
            SeqStep::LoadGen => {
                if !s.generation.is_multiple_of(2) {
                    return None;
                }
                read.generation = s.generation;
                SeqStep::LoadRate
            }
            SeqStep::LoadRate => {
                read.rate = s.rate;
                if read.rate == 0 {
                    return Some(Consumer::Idle);
                }
                SeqStep::LoadBurst
            }
            SeqStep::LoadBurst => {
                read.burst = s.burst;
                if read.burst == 0 {
                    SeqStep::ZeroBurstCheck
                } else {
                    SeqStep::Clock
                }
            }
            SeqStep::ZeroBurstCheck => {
                return Some(if s.generation == read.generation {
                    Consumer::Idle
                } else {
                    restart
                });
            }
            SeqStep::Clock => {
                read.now = s.now;
                SeqStep::LoadLast
            }
            SeqStep::LoadLast => {
                read.last = s.last_refill;
                read.claimed = read.now.saturating_sub(read.last);
                read.refill = read.claimed * read.rate;
                if read.claimed == 0 {
                    SeqStep::LoadAvail
                } else {
                    SeqStep::ClaimCas
                }
            }
            SeqStep::ClaimCas => {
                if s.last_refill != read.last {
                    return Some(restart);
                }
                s.last_refill = read.last + read.claimed;
                read.claim = Some(Claim {
                    refill: read.refill,
                    epoch: s.epoch,
                });
                SeqStep::LoadAvail
            }
            SeqStep::LoadAvail => {
                read.cur = s.available;
                SeqStep::GenCheck
            }
            SeqStep::GenCheck => {
                if s.generation != read.generation {
                    return Some(restart);
                }
                SeqStep::AvailCas
            }
            SeqStep::AvailCas => {
                if s.available != read.cur {
                    return Some(restart);
                }
                s.commit(read.cur, read.refill, read.burst, read.req, 1);
                return Some(Consumer::Idle);
            }
        };
        Some(Consumer::Seqlock { next, read })
    }

    fn step_resetter(s: &mut BucketState) -> Option<()> {
        s.resetter = match s.resetter {
            Resetter::Idle => return None,
            Resetter::Acquire { config } => {
                if s.locked {
                    return None;
                }
                s.locked = true;
                Resetter::Write { config }
            }
            Resetter::Write { config } => {
                // Production's reset keeps the balance: the time up to now is
                // refilled at the old rate and capped at the old burst, then
                // the balance is capped at the new burst. A bucket with no old
                // or no new rate starts full.
                s.available = if s.rate == 0 || config.rate == 0 {
                    config.burst
                } else {
                    let (_, refilled) = plan_consume(
                        AvailableTokens(s.available),
                        RefillTokens(s.now.saturating_sub(s.last_refill) * s.rate),
                        BurstCapacity(s.burst),
                        RequestedTokens(0),
                    );
                    refilled.0.min(config.burst)
                };
                s.rate = config.rate;
                s.burst = config.burst;
                s.last_refill = s.now;
                s.t0 = s.now;
                s.base = s.available;
                s.start_epoch();
                Resetter::Publish { config }
            }
            Resetter::Publish { config } => {
                s.mirror = config.rate;
                Resetter::Release
            }
            Resetter::Release => {
                s.locked = false;
                Resetter::Idle
            }
            Resetter::Enter { config } => {
                s.generation += 1;
                Resetter::StoreRate { config }
            }
            Resetter::StoreRate { config } => {
                s.rate = config.rate;
                Resetter::StoreBurst { config }
            }
            Resetter::StoreBurst { config } => {
                s.burst = config.burst;
                Resetter::StoreAvail { config }
            }
            Resetter::StoreAvail { config } => {
                s.available = config.burst;
                s.base = config.burst;
                Resetter::StoreLast
            }
            Resetter::StoreLast => {
                s.last_refill = s.now;
                s.t0 = s.now;
                Resetter::Leave
            }
            Resetter::Leave => {
                s.generation += 1;
                s.start_epoch();
                Resetter::Idle
            }
        };
        Some(())
    }
}

impl Model for BucketModel {
    type Action = Act;
    type State = BucketState;

    krabka_macros::model_transition!(last, action, s; {
        match action {
            Act::Tick => s.now += 1,
            Act::StartConsume { consumer, req } => {
                s.consumers[consumer] = Consumer::FastRate { req };
            }
            Act::StartReset { config } => {
                let config = self.configs[config];
                s.resets += 1;
                s.resetter = match self.algorithm {
                    Algorithm::Locked => Resetter::Acquire { config },
                    Algorithm::SeqlockCas => Resetter::Enter { config },
                };
            }
            Act::StepConsumer(t) => self.step_consumer(&mut s, t)?,
            Act::StepResetter => Self::step_resetter(&mut s)?,
        }
        Some(s)
    });

    fn init_states(&self) -> Vec<Self::State> {
        let Config { rate, burst } = self.configs[0];
        vec![BucketState {
            rate,
            burst,
            available: burst,
            last_refill: 0,
            mirror: rate,
            locked: false,
            generation: 0,
            now: 0,
            resets: 0,
            consumers: vec![Consumer::Idle; self.consumers],
            resetter: Resetter::Idle,
            epoch: 0,
            t0: 0,
            base: burst,
            granted: 0,
            capped: 0,
        }]
    }

    fn actions(&self, s: &Self::State, actions: &mut Vec<Self::Action>) {
        if s.now < self.max_time {
            actions.push(Act::Tick);
        }
        for (consumer, pc) in s.consumers.iter().enumerate() {
            if matches!(pc, Consumer::Idle) {
                for req in 0..=self.max_req {
                    actions.push(Act::StartConsume { consumer, req });
                }
            } else {
                actions.push(Act::StepConsumer(consumer));
            }
        }
        if matches!(s.resetter, Resetter::Idle) {
            if s.resets < self.max_resets {
                for config in 0..self.configs.len() {
                    actions.push(Act::StartReset { config });
                }
            }
        } else {
            actions.push(Act::StepResetter);
        }
    }

    fn properties(&self) -> Vec<Property<Self>> {
        let mut properties = vec![
            Property::always("available_within_burst", |_, s: &BucketState| {
                available_within_burst(s)
            }),
            Property::always("claimed_refill_conserved", |_, s: &BucketState| {
                claimed_refill_conserved(s)
            }),
            Property::always("grants_whole_tokens", |m: &BucketModel, s: &BucketState| {
                s.granted.is_multiple_of(m.units_per_token)
            }),
            Property::sometimes("consumers_overlap", |_, s: &BucketState| {
                s.consumers
                    .iter()
                    .filter(|c| !matches!(c, Consumer::Idle))
                    .count()
                    >= 2
            }),
            Property::sometimes("reset_overlaps_consume", |_, s: &BucketState| {
                !matches!(s.resetter, Resetter::Idle)
                    && s.consumers.iter().any(|c| !matches!(c, Consumer::Idle))
            }),
            Property::sometimes("refill_in_flight", |_, s: &BucketState| s.in_flight() > 0),
            Property::sometimes("refill_granted", |_, s: &BucketState| s.granted > s.burst),
            Property::sometimes("refill_capped", |_, s: &BucketState| s.capped > 0),
            Property::sometimes("burst_shrunk", |m: &BucketModel, s: &BucketState| {
                s.resets > 0 && s.burst < m.configs[0].burst
            }),
            Property::sometimes("bucket_drained", |_, s: &BucketState| {
                s.rate > 0 && s.available == 0
            }),
        ];
        // A reset that carries a balance under the new burst instead of
        // refilling to it. Whole-token grants at two storage units to a token
        // never leave a balance under the smaller burst of that search, so it
        // has no such reset to show.
        if self.units_per_token == 1 {
            properties.push(Property::sometimes(
                "reset_keeps_the_balance",
                |_, s: &BucketState| s.epoch > 0 && s.base < s.burst,
            ));
        }
        properties
    }
}

fn run(model: BucketModel) -> impl Checker<BucketModel> {
    model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(TARGET_STATE_COUNT)
        .spawn_bfs()
        .join()
}

krabka_macros::bounded_bfs!(bounded_bfs);

fn green_run(model: BucketModel, label: &str, pinned_unique_states: usize) {
    let checker = bounded_bfs(model, label, MAX_DEPTH, TARGET_STATE_COUNT);
    // Pin: a changed count is a changed model, not a retuning knob.
    assert2::assert!(
        checker.unique_state_count() == pinned_unique_states,
        "[{label}] unique-state count moved: the reachable set of this model changed"
    );
    checker.assert_properties();
}

#[test]
fn bucket_basic() {
    green_run(
        BucketModel {
            algorithm: Algorithm::Locked,
            units_per_token: 1,
            consumers: 2,
            configs: vec![Config { rate: 1, burst: 2 }, Config { rate: 1, burst: 1 }],
            max_resets: 1,
            max_time: 2,
            max_req: 2,
        },
        "bucket_basic",
        PINNED_UNIQUE_STATES_BASIC,
    );
}

#[test]
fn bucket_wide() {
    green_run(
        BucketModel {
            algorithm: Algorithm::Locked,
            units_per_token: 1,
            consumers: 2,
            configs: vec![
                Config { rate: 1, burst: 3 },
                Config { rate: 2, burst: 1 },
                Config { rate: 0, burst: 0 },
                Config { rate: 1, burst: 0 },
            ],
            max_resets: 2,
            max_time: 2,
            max_req: 2,
        },
        "bucket_wide",
        PINNED_UNIQUE_STATES_WIDE,
    );
}

/// Half-token rates and bursts, as a fractional quota rate gives production:
/// at two storage units to a token, `rate: 1` is half a token per second and
/// `burst: 1` holds half a token, so no whole token is ever granted under it.
/// The bucket still conserves every claimed refill, stays within its burst,
/// and grants only whole tokens.
#[test]
fn bucket_fractional() {
    green_run(
        BucketModel {
            algorithm: Algorithm::Locked,
            units_per_token: 2,
            consumers: 2,
            configs: vec![Config { rate: 1, burst: 3 }, Config { rate: 1, burst: 1 }],
            max_resets: 1,
            max_time: 2,
            max_req: 2,
        },
        "bucket_fractional",
        PINNED_UNIQUE_STATES_FRACTIONAL,
    );
}

/// The smallest search in which the seqlock lets a straddled reset raise
/// `available` past the new burst: one consumer, one shrinking reset.
fn straddle_config(algorithm: Algorithm) -> BucketModel {
    BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 1,
        configs: vec![Config { rate: 1, burst: 2 }, Config { rate: 1, burst: 1 }],
        max_resets: 1,
        max_time: 1,
        max_req: 1,
    }
}

/// The smallest search in which the seqlock drops a claimed refill: two
/// consumers and no reset at all.
fn contention_config(algorithm: Algorithm) -> BucketModel {
    BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 2,
        configs: vec![Config { rate: 1, burst: 3 }],
        max_resets: 0,
        max_time: 1,
        max_req: 1,
    }
}

/// RED witness: the former seqlock lets a whole reset run between a
/// consumer's generation re-check and its `available` commit. The reset stores
/// the value the commit expects, so the stale commit succeeds and leaves
/// `available` above the new burst.
#[test]
fn seqlock_cas_lets_a_straddled_reset_exceed_burst() {
    let checker = run(straddle_config(Algorithm::SeqlockCas));
    let found = checker.assert_any_discovery("available_within_burst");
    eprintln!("straddled reset counterexample: {:?}", found.into_actions());
}

/// RED witness: the former seqlock claims a refill on `last_refill` before it
/// commits `available`, and a commit that loses its race restarts without the
/// refill it claimed. Without any reset, `available` stays within the burst,
/// yet the claimed tokens vanish.
#[test]
fn seqlock_cas_drops_a_claimed_refill() {
    let checker = run(contention_config(Algorithm::SeqlockCas));
    checker.assert_no_discovery("available_within_burst");
    let found = checker.assert_any_discovery("claimed_refill_conserved");
    eprintln!("dropped refill counterexample: {:?}", found.into_actions());
}

/// GREEN counterpart of both RED witnesses: production's lock holds both
/// properties in the very searches where the seqlock breaks them.
#[test]
fn locked_holds_where_seqlock_cas_fails() {
    for model in [
        straddle_config(Algorithm::Locked),
        contention_config(Algorithm::Locked),
    ] {
        let checker = run(model);
        checker.assert_no_discovery("available_within_burst");
        checker.assert_no_discovery("claimed_refill_conserved");
    }
}

/// Replays `schedule` from the initial state, or returns `None` if one of its
/// actions is not enabled where it is taken.
fn replay(model: &BucketModel, schedule: &[Vec<Act>]) -> Option<BucketState> {
    Path::from_actions(
        model,
        model.init_states().remove(0),
        schedule.concat().iter(),
    )
    .map(|path| path.last_state().clone())
}

/// Consumer `consumer` starts a consume of `req` and takes `steps` steps.
///
/// A consume that finds time to claim takes 10 steps under the seqlock (9 to
/// reach its `available` commit) and 6 under the lock, which it holds from
/// its second step to its last.
fn consume(consumer: usize, req: u64, steps: usize) -> Vec<Act> {
    let mut acts = vec![Act::StartConsume { consumer, req }];
    acts.extend(std::iter::repeat_n(Act::StepConsumer(consumer), steps));
    acts
}

/// A reset to `configs[1]` that takes `steps` steps: 6 under the seqlock and 4
/// under the lock.
fn reset(steps: usize) -> Vec<Act> {
    let mut acts = vec![Act::StartReset { config: 1 }];
    acts.extend(std::iter::repeat_n(Act::StepResetter, steps));
    acts
}

/// Replays the review's concrete straddle, `burst = 10`, `available = 3`, a
/// refill of 7, and a request of 1, against both algorithms.
///
/// Under the seqlock, the consume passes its generation re-check, a whole
/// reset to `(rate 3, burst 3)` runs and stores `available = 3`, and the
/// consume's commit, which expects 3, then stores 9 under a burst of 3. Under
/// the lock, the reset cannot take its first step while the consume holds the
/// lock across its claim and commit; it runs after, and the bucket ends at the
/// new burst.
#[test]
fn straddled_reset_schedule() {
    let model = |algorithm| BucketModel {
        algorithm,
        units_per_token: 1,
        consumers: 1,
        configs: vec![Config { rate: 1, burst: 10 }, Config { rate: 3, burst: 3 }],
        max_resets: 1,
        max_time: 7,
        max_req: 7,
    };
    let ticks = vec![Act::Tick; 7];

    // Drain 7 of 10 at no refill (9 steps), let 7 seconds pass, run the consume
    // of 1 up to its commit, run the whole reset, then commit.
    let last = replay(&model(Algorithm::SeqlockCas), &[
        consume(0, 7, 9),
        ticks.clone(),
        consume(0, 1, 9),
        reset(6),
        vec![Act::StepConsumer(0)],
    ])
    .expect("the straddle is enabled step by step under the seqlock");
    assert2::assert!((last.available, last.burst, available_within_burst(&last)) == (9, 3, false));

    let locked = model(Algorithm::Locked);
    let straddle = [consume(0, 7, 6), ticks.clone(), consume(0, 1, 3), reset(1)];
    assert2::assert!(replay(&locked, &straddle[..3]).is_some());
    assert2::assert!(
        replay(&locked, &straddle).is_none(),
        "the reset waits for the lock"
    );
    let last = replay(&locked, &[
        consume(0, 7, 6),
        ticks,
        consume(0, 1, 6),
        reset(4),
    ])
    .expect("the reset runs once the consume releases the lock");
    assert2::assert!((last.available, last.burst, available_within_burst(&last)) == (3, 3, true));
}

/// Replays a dropped refill whose loss a later consume can observe.
///
/// The bucket holds 2 of 3 tokens when one second passes. Consumer 0 claims
/// that second's token and reaches its commit expecting 2; consumer 1 then
/// takes a token, so consumer 0's commit fails and it restarts with nothing
/// left to claim. Both grants of 1 succeed, but the bucket ends empty, where a
/// serial run of the two consumes leaves `min(2 + 1, 3) - 2 = 1`. Under the
/// lock, consumer 1 cannot take its lock step while consumer 0 holds it, and
/// the bucket ends at 1.
#[test]
fn dropped_refill_schedule() {
    let tick = vec![Act::Tick];

    let last = replay(&contention_config(Algorithm::SeqlockCas), &[
        consume(0, 1, 9),
        tick.clone(),
        consume(0, 1, 9),
        consume(1, 1, 9),
        vec![Act::StepConsumer(0); 9],
    ])
    .expect("the contention is enabled step by step under the seqlock");
    assert2::assert!(
        (
            last.available,
            last.granted,
            claimed_refill_conserved(&last)
        ) == (0, 3, false)
    );

    let locked = contention_config(Algorithm::Locked);
    let contention = [
        consume(0, 1, 6),
        tick.clone(),
        consume(0, 1, 3),
        consume(1, 1, 2),
    ];
    assert2::assert!(replay(&locked, &contention[..3]).is_some());
    assert2::assert!(
        replay(&locked, &contention).is_none(),
        "consumer 1 waits for the lock"
    );
    let last = replay(&locked, &[
        consume(0, 1, 6),
        tick,
        consume(0, 1, 3),
        consume(1, 1, 1),
        vec![Act::StepConsumer(0); 3],
        vec![Act::StepConsumer(1); 5],
    ])
    .expect("consumer 1 runs once consumer 0 releases the lock");
    assert2::assert!(
        (
            last.available,
            last.granted,
            claimed_refill_conserved(&last)
        ) == (1, 3, true)
    );
}
