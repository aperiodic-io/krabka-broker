//! One seed, start to end: boot the cluster, start the producers and the
//! gauge sampler, play the fault plan, bring every broker back, stop the
//! producers, probe every partition, and check the invariants.

use {
    crate::{
        check::{
            Findings, ReplicaLog, await_converged, check_acked_offsets, check_agreement,
            check_durability, check_first_wins, describe, read_replica,
        },
        cluster::{ChaosCluster, dial},
        gauges::Sampler,
        plan::{Fault, Plan, Stop, Victim},
        topics::{MUTABLE_TOPIC, TOPICS, Target, targets},
        workload::{Attempt, History, Outcome, Sent, Workload, plain_batch, produce_to},
    },
    std::{
        fmt,
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// How long the cluster has to become whole again once the faults stop.
const HEAL_LIMIT: Duration = Duration::from_secs(90);

/// How long a fresh produce to a partition may take once the cluster is
/// whole.
const PROBE_LIMIT: Duration = Duration::from_secs(60);

/// The longest the whole run may go without any acknowledgement while the
/// faults play. Two brokers down at once stop every partition until one
/// returns, which takes a stop's hold plus a restart.
const STALL_LIMIT: Duration = Duration::from_secs(30);

/// How many brokers may be down at once. The third stop would leave the
/// controller quorum and every partition without a majority.
const MAX_DOWN: usize = 2;

/// The producer number the liveness probes record under.
const PROBE_PRODUCER: u64 = 1_000;

/// What happened during the run, as it happened.
pub struct Timeline {
    start: Instant,
    seed: u64,
    lines: Vec<String>,
}

impl Timeline {
    fn note(&mut self, what: impl fmt::Display) {
        let line = format!("+{:>6}ms  {what}", self.start.elapsed().as_millis());
        eprintln!("dedup-chaos seed {}: {line}", self.seed);
        self.lines.push(line);
    }
}

/// Everything a seed's run produced.
pub struct SeedOutcome {
    pub plan: Plan,
    pub timeline: Vec<String>,
    pub findings: Findings,
    pub elapsed: Duration,
}

impl SeedOutcome {
    /// The verdicts, notes, plan and timeline: what a failure prints, so
    /// that the seed can be replayed and the faults read next to it.
    pub fn report(&self) -> String {
        format!(
            "seed {} in {:.1}s: {}\n{}\nexecuted:\n  {}\n",
            self.plan.seed,
            self.elapsed.as_secs_f64(),
            self.findings,
            self.plan,
            self.timeline.join("\n  ")
        )
    }
}

pub async fn run_seed(seed: u64, fault_time: Duration) -> SeedOutcome {
    let began = Instant::now();
    let plan = Plan::draw(seed, fault_time);
    eprintln!("dedup-chaos: {plan}");
    let mut cluster = ChaosCluster::boot().await;
    let sampler = Sampler::start(cluster.meters.clone());
    let start = Instant::now();
    let workload = Workload::start(seed, &cluster.addrs(), &cluster.topic_ids, start);
    let mut timeline = Timeline {
        start,
        seed,
        lines: Vec::new(),
    };
    let changed = play(&mut cluster, &plan, &mut timeline).await;
    let fault_end = start.elapsed();

    let mut findings = Findings::default();
    for node in cluster.down() {
        cluster.restart(node).await;
        timeline.note(format_args!("broker {node} restarted after the faults"));
    }
    match cluster.await_whole(HEAL_LIMIT).await {
        Ok(took) => timeline.note(format_args!("whole again after {}ms", took.as_millis())),
        Err(why) => findings.violate("liveness", why),
    }
    let mut history = workload.finish().await;
    timeline.note(format_args!(
        "producers stopped: {} requests",
        history.attempts.len()
    ));
    check_progress(&mut findings, &history, fault_end);
    probe(&cluster, &mut history, &mut findings, start).await;

    for target in targets() {
        verify(&cluster, &history, target, changed, &mut findings).await;
    }
    let gauges = sampler.finish().await;
    for violation in gauges.violations {
        findings.violate("memory-bound", violation);
    }
    findings.notes.push(format!(
        "{} gauge samples; highest partition gauge {} ({}), highest broker gauge {} (broker {})",
        gauges.samples,
        gauges.max_partition.0,
        gauges.max_partition.1,
        gauges.max_broker.0,
        gauges.max_broker.1
    ));
    findings.notes.push(format!(
        "{} idempotent producer restarts after a sequence error{}",
        history.restarts.len(),
        if history.restarts.is_empty() {
            String::new()
        } else {
            format!(": {:?}", history.restarts)
        }
    ));
    if changed {
        findings.notes.push(format!(
            "{} left out of first-wins: its config changed",
            TOPICS[MUTABLE_TOPIC].name
        ));
    }
    cluster.shutdown().await;
    SeedOutcome {
        plan,
        timeline: timeline.lines,
        findings,
        elapsed: began.elapsed(),
    }
}

/// Play the faults of `plan` and restart each stopped broker when its hold
/// ends. Returns whether a config change may have reached the mutable
/// topic.
async fn play(cluster: &mut ChaosCluster, plan: &Plan, timeline: &mut Timeline) -> bool {
    let mut returns: Vec<(Instant, u64)> = Vec::new();
    let mut changed = false;
    for scheduled in &plan.faults {
        let due = timeline.start + scheduled.at;
        restart_until(cluster, &mut returns, due, timeline).await;
        tokio::time::sleep_until(due.into()).await;
        let fault = scheduled.fault;
        match fault {
            Fault::Down { victim, stop, hold } => {
                if let Some(node) = take_down(cluster, victim, stop, timeline).await {
                    returns.push((Instant::now() + hold, node));
                }
            }
            Fault::ElectPreferred => {
                let outcome = cluster.elect_preferred(None).await;
                timeline.note(format_args!("{fault}: {outcome:?}"));
            }
            Fault::MoveLeader { target, to } if cluster.is_live(to) => {
                let outcome = cluster.move_leader(targets()[target], to).await;
                timeline.note(format_args!("{fault}: {outcome:?}"));
            }
            Fault::MoveLeader { .. } => {
                timeline.note(format_args!("skip {fault}: the broker is down"));
            }
            Fault::Config(change) => {
                let outcome = cluster.change_config(change).await;
                // Only an error code proves the change did not apply; a lost
                // answer may hide an applied one.
                changed |=
                    !matches!(&outcome, Err(why) if why.starts_with("IncrementalAlterConfigs"));
                timeline.note(format_args!("{fault}: {outcome:?}"));
            }
        }
    }
    let end = timeline.start + plan.fault_time;
    restart_until(cluster, &mut returns, end, timeline).await;
    tokio::time::sleep_until(end.into()).await;
    timeline.note("faults end");
    changed
}

/// Restart every stopped broker whose hold ends by `until`, in order.
async fn restart_until(
    cluster: &mut ChaosCluster,
    returns: &mut Vec<(Instant, u64)>,
    until: Instant,
    timeline: &mut Timeline,
) {
    returns.sort_unstable();
    while let Some(&(due, node)) = returns.first() {
        if due > until {
            return;
        }
        tokio::time::sleep_until(due.into()).await;
        returns.remove(0);
        let restarting = Instant::now();
        cluster.restart(node).await;
        timeline.note(format_args!(
            "broker {node} restarted in {}ms",
            restarting.elapsed().as_millis()
        ));
    }
}

/// Stop the broker `victim` names, if it runs and no more than
/// [`MAX_DOWN`] would then be down. Returns the broker stopped.
async fn take_down(
    cluster: &mut ChaosCluster,
    victim: Victim,
    stop: Stop,
    timeline: &mut Timeline,
) -> Option<u64> {
    let node = match victim {
        Victim::Broker(node) => Some(node).filter(|node| cluster.is_live(*node)),
        Victim::LeaderOf(target) => cluster.leader_of(targets()[target]),
        Victim::ControllerLeader => cluster.controller_leader(),
    };
    let Some(node) = node else {
        timeline.note(format_args!(
            "skip stop of {victim}: not running or unknown"
        ));
        return None;
    };
    if cluster.down().len() >= MAX_DOWN {
        timeline.note(format_args!(
            "skip stop of {victim} (broker {node}): {:?} are down already",
            cluster.down()
        ));
        return None;
    }
    let stop = match stop {
        Stop::CrashCutTail(target) if !cluster.others_hold(targets()[target], node) => {
            timeline.note(format_args!(
                "a cut tail of {} needs both other replicas up and in its ISR {:?}; tearing \
                 instead",
                targets()[target],
                cluster.isr_of(targets()[target])
            ));
            Stop::CrashTornTail(target)
        }
        other => other,
    };
    let damage = cluster.stop(node, stop).await;
    timeline.note(format_args!(
        "stopped broker {node}, {victim} ({stop}) {damage}"
    ));
    Some(node)
}

/// Liveness while the faults played: no stretch without an
/// acknowledgement longer than [`STALL_LIMIT`], and no batch given up on.
fn check_progress(findings: &mut Findings, history: &History, fault_end: Duration) {
    let acks: Vec<Duration> = history
        .attempts
        .iter()
        .filter(|attempt| attempt.acked().is_some())
        .map(|attempt| attempt.at)
        .filter(|at| *at <= fault_end)
        .collect();
    let mut previous = Duration::ZERO;
    let mut longest = (Duration::ZERO, Duration::ZERO);
    for at in acks.iter().copied().chain([fault_end]) {
        let quiet = at.saturating_sub(previous);
        if quiet > longest.1 {
            longest = (previous, quiet);
        }
        previous = at;
    }
    if longest.1 > STALL_LIMIT {
        findings.violate(
            "liveness",
            format!(
                "no acknowledgement for {}ms from +{}ms",
                longest.1.as_millis(),
                longest.0.as_millis()
            ),
        );
    }
    for abandoned in &history.abandoned {
        findings.violate("liveness", abandoned.clone());
    }
    findings.notes.push(format!(
        "{} acknowledgements while the faults played; longest stretch without one {}ms from +{}ms",
        acks.len(),
        longest.1.as_millis(),
        longest.0.as_millis()
    ));
}

/// Liveness after the faults: one fresh record to every partition is
/// acknowledged within [`PROBE_LIMIT`]. The probes join the history.
async fn probe(
    cluster: &ChaosCluster,
    history: &mut History,
    findings: &mut Findings,
    start: Instant,
) {
    for target in targets() {
        let records: Arc<[Sent]> = vec![Sent {
            key: format!("probe-{target}"),
            value: format!("probe-of-{target}"),
            header: Some(format!("probe-{target}")),
        }]
        .into();
        let attempt = deliver(cluster, target, records, PROBE_PRODUCER, start).await;
        if let Outcome::Failed(why) = &attempt.outcome {
            findings.violate(
                "liveness",
                format!("{target}: a fresh produce failed for {PROBE_LIMIT:?}: {why}"),
            );
        }
        history.attempts.push(attempt);
    }
}

/// Send `records` to the current leader of `target` as one plain batch
/// until a leader acknowledges it or [`PROBE_LIMIT`] passes, and record it
/// as batch 0 of `producer`.
pub async fn deliver(
    cluster: &ChaosCluster,
    target: Target,
    records: Arc<[Sent]>,
    producer: u64,
    start: Instant,
) -> Attempt {
    let batch = plain_batch(&records);
    let began = Instant::now();
    let outcome = loop {
        let answer = match cluster.leader_of(target) {
            Some(leader) => match dial(cluster.addr(leader), "dedup-chaos-probe").await {
                Ok(conn) => {
                    let topic_id = cluster.topic_ids[target.topic];
                    produce_to(&conn, target, topic_id, batch.clone()).await
                }
                Err(why) => Err(why),
            },
            None => Err("no leader".to_owned()),
        };
        let failure = match answer {
            Ok(Ok(base)) => break Outcome::Acked(base),
            Ok(Err(code)) => format!("error {code}"),
            Err(why) => why,
        };
        if began.elapsed() > PROBE_LIMIT {
            break Outcome::Failed(failure);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    Attempt {
        producer,
        target,
        batch: 0,
        replay: false,
        records,
        at: start.elapsed(),
        outcome,
    }
}

/// Wait until the replicas of `target` agree on its end, read each of them,
/// and check durability, exact acknowledgements, first-wins and agreement.
pub async fn verify(
    cluster: &ChaosCluster,
    history: &History,
    target: Target,
    changed: bool,
    findings: &mut Findings,
) {
    if let Err(why) = await_converged(cluster, target, HEAL_LIMIT).await {
        findings.violate("liveness", why);
    }
    let Some(leader) = cluster.leader_of(target) else {
        findings.violate("liveness", format!("{target}: no leader to read"));
        return;
    };
    let mut logs: Vec<ReplicaLog> = Vec::new();
    // The leader first, so every comparison is against it.
    let mut order = cluster.live();
    order.sort_by_key(|node| *node != leader);
    for node in order {
        match read_replica(cluster, node, target, cluster.topic_ids[target.topic]).await {
            Ok(log) => logs.push(log),
            Err(why) => findings.violate("replica-agreement", format!("{target}: {why}")),
        }
    }
    for log in &logs {
        check_durability(findings, history, target, log);
    }
    let Some(leader_log) = logs.first() else {
        return;
    };
    check_acked_offsets(findings, history, target, leader_log);
    check_agreement(findings, target, &logs);
    let unique = target.spec().unique && !(target.spec().mutable && changed);
    let checked = if unique {
        format!(
            ", {} identities first-wins checked",
            check_first_wins(findings, target, leader_log)
        )
    } else {
        String::new()
    };
    findings.notes.push(format!(
        "{}{checked}",
        describe(history, target, leader_log)
    ));
}
