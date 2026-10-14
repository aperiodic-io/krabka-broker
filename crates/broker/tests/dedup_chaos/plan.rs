//! The fault plan a seed stands for, and the run size the environment asks
//! for.
//!
//! A plan is drawn from the seed alone, before the cluster boots, so the
//! same seed always plans the same faults at the same offsets. Which broker
//! "the leader of a partition" or "the controller leader" is gets decided
//! when the fault fires, and the run's timeline records the answer.

use {
    crate::{
        rng::Rng,
        topics::{LONG_WINDOW_MS, MUTABLE_TOPIC, TOPICS, targets},
    },
    std::{fmt, time::Duration},
};

/// The seeds of the default run. Fixed, so a CI failure replays as is.
const DEFAULT_SEEDS: [u64; 2] = [0x00C0_FFEE_D00D, 0x0000_5EED_CAFE];

/// The fault phase of a default run, per seed.
const DEFAULT_SECS: u64 = 40;

/// The lane of a seed the planner draws from. Producers use lanes from 1.
const PLAN_LANE: u64 = 0;

/// Which broker a stop takes down.
#[derive(Clone, Copy, Debug)]
pub enum Victim {
    Broker(u64),
    /// The leader of one partition of [`targets`], by index.
    LeaderOf(usize),
    ControllerLeader,
}

/// How a broker stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    /// `BrokerHandle::shutdown`: the clean-shutdown proof is written.
    Graceful,
    /// `BrokerHandle::crash_for_test`: no clean-shutdown proof and no
    /// producer-state snapshot, as after `SIGKILL`.
    Crash,
    /// A crash after which the first half of the last batch of one
    /// partition is written again behind it, as a write that power loss
    /// interrupted leaves it. Recovery cuts the partial copy, and every
    /// complete batch survives.
    CrashTornTail(usize),
    /// A crash after which the last batch of one partition is cut in half,
    /// so that this replica loses it. The run allows it only while both
    /// other replicas run and are in the partition's ISR, so they hold
    /// everything that was acknowledged; otherwise it becomes a
    /// [`Stop::CrashTornTail`].
    CrashCutTail(usize),
}

/// A change of the mutable topic's config.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigChange {
    Window(i64),
    /// Deduplicate by the dedup header (`true`) or by the key (`false`).
    HeaderRule(bool),
    /// Turn deduplication on, with the long window, or off.
    Enabled(bool),
}

#[derive(Clone, Copy, Debug)]
pub enum Fault {
    /// Stop a broker and start it again on its own directory `hold` later.
    /// A graceful stop held under a second is a pause.
    Down {
        victim: Victim,
        stop: Stop,
        hold: Duration,
    },
    /// A preferred-leader election of every partition.
    ElectPreferred,
    /// Make `to` the preferred replica of a partition and elect it.
    MoveLeader {
        target: usize,
        to: u64,
    },
    Config(ConfigChange),
}

/// A fault and when it fires, measured from the start of the fault phase.
#[derive(Clone, Copy, Debug)]
pub struct Scheduled {
    pub at: Duration,
    pub fault: Fault,
}

/// Everything one seed stands for.
#[derive(Clone, Debug)]
pub struct Plan {
    pub seed: u64,
    pub fault_time: Duration,
    pub faults: Vec<Scheduled>,
}

impl Plan {
    /// The faults of `seed` over a fault phase of `fault_time`. Faults start
    /// one to five seconds apart and a broker stays down for up to eight, so
    /// they overlap often.
    pub fn draw(seed: u64, fault_time: Duration) -> Self {
        let mut rng = Rng::lane(seed, PLAN_LANE);
        let partitions = targets().len();
        let mut faults = Vec::new();
        let mut at = rng.millis(1_000, 3_000);
        while at < fault_time {
            let fault = draw_fault(&mut rng, partitions);
            faults.push(Scheduled { at, fault });
            at += rng.millis(1_000, 5_000);
        }
        Self {
            seed,
            fault_time,
            faults,
        }
    }
}

fn draw_fault(rng: &mut Rng, partitions: usize) -> Fault {
    // Graceful stop, crash, torn tail, cut tail, pause, preferred election,
    // leader move, config change.
    let kind = rng.weighted(&[14, 14, 8, 8, 10, 7, 9, 14]);
    let down = |stop: Stop, hold: Duration, rng: &mut Rng| Fault::Down {
        victim: draw_victim(rng, partitions),
        stop,
        hold,
    };
    match kind {
        0 => down(Stop::Graceful, rng.millis(1_500, 8_000), rng),
        1 => down(Stop::Crash, rng.millis(1_500, 8_000), rng),
        2 => {
            let target = rng.index(partitions);
            down(Stop::CrashTornTail(target), rng.millis(1_500, 6_000), rng)
        }
        3 => {
            let target = rng.index(partitions);
            down(Stop::CrashCutTail(target), rng.millis(1_500, 6_000), rng)
        }
        4 => down(Stop::Graceful, rng.millis(100, 900), rng),
        5 => Fault::ElectPreferred,
        6 => Fault::MoveLeader {
            target: rng.index(partitions),
            to: rng.between(1, 3),
        },
        _ => Fault::Config(draw_change(rng)),
    }
}

fn draw_victim(rng: &mut Rng, partitions: usize) -> Victim {
    match rng.weighted(&[40, 40, 20]) {
        0 => Victim::Broker(rng.between(1, 3)),
        1 => Victim::LeaderOf(rng.index(partitions)),
        _ => Victim::ControllerLeader,
    }
}

fn draw_change(rng: &mut Rng) -> ConfigChange {
    match rng.weighted(&[2, 1, 1, 1, 1]) {
        0 => ConfigChange::Window([LONG_WINDOW_MS, 120_000, 20_000][rng.index(3)]),
        1 => ConfigChange::HeaderRule(true),
        2 => ConfigChange::HeaderRule(false),
        3 => ConfigChange::Enabled(false),
        _ => ConfigChange::Enabled(true),
    }
}

impl fmt::Display for Victim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Broker(node) => write!(f, "broker {node}"),
            Self::LeaderOf(target) => write!(f, "the leader of {}", targets()[*target]),
            Self::ControllerLeader => f.write_str("the controller leader"),
        }
    }
}

impl fmt::Display for Stop {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Graceful => f.write_str("graceful"),
            Self::Crash => f.write_str("crash"),
            Self::CrashTornTail(target) => write!(f, "crash, torn tail of {}", targets()[*target]),
            Self::CrashCutTail(target) => write!(f, "crash, cut tail of {}", targets()[*target]),
        }
    }
}

impl fmt::Display for ConfigChange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let topic = TOPICS[MUTABLE_TOPIC].name;
        match self {
            Self::Window(ms) => write!(f, "{topic}: dedup.window.ms={ms}"),
            Self::HeaderRule(true) => write!(f, "{topic}: dedup by header"),
            Self::HeaderRule(false) => write!(f, "{topic}: dedup by key"),
            Self::Enabled(on) => write!(f, "{topic}: dedup {}", if *on { "on" } else { "off" }),
        }
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Down { victim, stop, hold } => {
                write!(f, "stop {victim} ({stop}) for {}ms", hold.as_millis())
            }
            Self::ElectPreferred => f.write_str("preferred election"),
            Self::MoveLeader { target, to } => {
                write!(
                    f,
                    "move the leader of {} to broker {to}",
                    targets()[*target]
                )
            }
            Self::Config(change) => write!(f, "config {change}"),
        }
    }
}

impl fmt::Display for Plan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "seed {} (replay: DEDUP_CHAOS_SEED={} DEDUP_CHAOS_SECS={}), {} faults:",
            self.seed,
            self.seed,
            self.fault_time.as_secs(),
            self.faults.len()
        )?;
        for scheduled in &self.faults {
            writeln!(
                f,
                "  +{:>6}ms  {}",
                scheduled.at.as_millis(),
                scheduled.fault
            )?;
        }
        Ok(())
    }
}

/// The seeds and the fault phase a run uses.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunSize {
    pub seeds: Vec<u64>,
    pub fault_time: Duration,
}

impl RunSize {
    /// `DEDUP_CHAOS_SEED=<n>` replays one seed, `DEDUP_CHAOS_SEEDS=<count>`
    /// runs that many seeds drawn from the clock, and `DEDUP_CHAOS_SECS=<s>`
    /// sets the fault phase of each. Without them, the two default seeds for
    /// [`DEFAULT_SECS`] each.
    pub fn from_env() -> Self {
        let read = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
        let clock = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        Self::parse(
            read("DEDUP_CHAOS_SEED").as_deref(),
            read("DEDUP_CHAOS_SEEDS").as_deref(),
            read("DEDUP_CHAOS_SECS").as_deref(),
            clock,
        )
    }

    /// What the three variables ask for, with `clock` as the base of drawn
    /// seeds.
    pub fn parse(seed: Option<&str>, seeds: Option<&str>, secs: Option<&str>, clock: u64) -> Self {
        let seeds = if let Some(seed) = seed {
            vec![parse_number(seed, "DEDUP_CHAOS_SEED")]
        } else if let Some(count) = seeds {
            let count = parse_number(count, "DEDUP_CHAOS_SEEDS");
            let mut rng = Rng::lane(clock, 0);
            (0..count).map(|_| rng.next_u64() >> 16).collect()
        } else {
            DEFAULT_SEEDS.to_vec()
        };
        let secs = secs.map_or(DEFAULT_SECS, |secs| {
            parse_number(secs.trim_end_matches('s'), "DEDUP_CHAOS_SECS")
        });
        Self {
            seeds,
            fault_time: Duration::from_secs(secs),
        }
    }
}

/// A decimal or `0x` hexadecimal number.
fn parse_number(text: &str, name: &str) -> u64 {
    let text = text.trim();
    let parsed = match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => text.parse(),
    };
    parsed.unwrap_or_else(|error| panic!("{name}={text}: {error}"))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn the_environment_picks_the_seeds_and_the_fault_phase() {
        let cases = [
            ((None, None, None), RunSize {
                seeds: DEFAULT_SEEDS.to_vec(),
                fault_time: Duration::from_secs(DEFAULT_SECS),
            }),
            ((Some("0x10"), Some("7"), Some("60s")), RunSize {
                seeds: vec![16],
                fault_time: Duration::from_secs(60),
            }),
            ((Some("42"), None, Some("5")), RunSize {
                seeds: vec![42],
                fault_time: Duration::from_secs(5),
            }),
        ];
        for ((seed, seeds, secs), expected) in cases {
            assert!(RunSize::parse(seed, seeds, secs, 1) == expected);
        }
        let drawn = RunSize::parse(None, Some("3"), None, 1);
        assert!(drawn.seeds.len() == 3);
        assert!(drawn == RunSize::parse(None, Some("3"), None, 1));
    }

    #[test]
    fn a_seed_always_plans_the_same_faults() {
        let plan = |seed| Plan::draw(seed, Duration::from_secs(60)).to_string();
        assert!(plan(7) == plan(7));
        assert!(plan(7) != plan(8));
    }
}
