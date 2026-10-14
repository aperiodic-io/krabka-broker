//! One soak run: boot, cycle until the configured time is up, tear down,
//! judge.
//!
//! A cycle is a load, an idle drain and a churn step:
//!
//! 1. **Load.** [`Settings::producers`] producers send the steady mix of
//!    [`workload`] for [`Settings::steady_load`], then one hot partition and
//!    then every partition is flooded past its limit.
//! 2. **Drain.** Nothing is produced until every index on every broker is
//!    empty again, by the broker gauge and every partition gauge, and at least
//!    [`Settings::min_idle`] has passed. Only the broker's ticker can empty
//!    them: it is the drain the soak checks. The process is then sampled at a
//!    point that is the same in every cycle.
//! 3. **Churn.** One topic is replaced, toggled, given a new window or a new
//!    identity rule, and every few cycles a broker restarts.

use {
    crate::{
        cluster::SoakCluster,
        judge::judge,
        process::{self, ProcessSample},
        record::{CycleRow, LatencySummary, Phase, Recorder, Sample},
        settings::{RESTARTED_NODE, SLOTS, Settings},
        topics::{Change, SoakTopic},
        workload::{self, LatencySink, SteadyPlan, Tally, Target},
    },
    assert2::assert,
    std::time::{Duration, Instant},
    tokio::task::JoinSet,
};

/// How often a wait loop checks whether a sample is due.
const POLL: Duration = Duration::from_millis(250);

/// How long a deleted topic's KFC-10 series may outlive its partitions.
///
/// The broker evicts a partition's series when the image drops the
/// partition, and again on a pass every 30 seconds, which takes away a series
/// that a report racing the deletion set again. A series that survives that
/// pass is kept for good.
const EVICTION_TIMEOUT: Duration = Duration::from_secs(45);

pub async fn soak(settings: Settings) {
    let sink = LatencySink::default();
    let mut cluster = Box::pin(SoakCluster::boot(&settings)).await;
    let mut topics: Vec<SoakTopic> = (0..SLOTS).map(SoakTopic::first).collect();
    for topic in &mut topics {
        cluster.create(topic).await;
    }
    let mut recorder = Recorder::new(&settings, sink.clone());
    let started = Instant::now();
    let mut warm = None;
    let mut cycle = 0;
    while cycle < settings.min_cycles || started.elapsed() < settings.soak {
        let (tally, latency) =
            load(&settings, &cluster, &topics, &mut recorder, &sink, cycle).await;
        let drained_in = drain(&settings, &cluster, &mut recorder, cycle).await;
        let drained = recorder.sample(&cluster, cycle, Phase::Drained).await;
        check_residual(&mut recorder, &drained, cycle);
        if cycle + 1 == settings.warmup_cycles {
            process::reset_peak();
            warm = Some(drained.process);
        }
        let churn = churn(&settings, &mut cluster, &mut topics, &mut recorder, cycle).await;
        recorder.push_cycle(CycleRow {
            cycle,
            churn,
            drained_in,
            drained: drained.process,
            drained_heap: drained.heap_sum(),
            drained_series: drained.registry_series(),
            tally,
            latency,
        });
        cycle += 1;
    }
    let last = teardown(&cluster, &topics, &mut recorder, cycle).await;
    cluster.shutdown().await;
    let warm: ProcessSample = warm.expect("the warm-up ends before the last cycle");
    let failures = judge(&settings, &recorder, warm, last.process);
    assert!(
        failures.is_empty(),
        "the dedup soak failed {} checks:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

/// The steady load, then the saturation. Returns what was sent and its
/// latencies.
async fn load(
    settings: &Settings,
    cluster: &SoakCluster,
    topics: &[SoakTopic],
    recorder: &mut Recorder,
    sink: &LatencySink,
    cycle: usize,
) -> (Tally, LatencySummary) {
    let targets = cluster.targets(topics);
    let mut producers = JoinSet::new();
    for producer in 0..settings.producers.min(targets.len()) {
        let plan = SteadyPlan {
            cycle,
            producer,
            duration: settings.steady_load,
            requests_per_second: settings.requests_per_second,
            records_per_batch: settings.records_per_batch,
        };
        let mine: Vec<Target> = targets
            .iter()
            .skip(producer)
            .step_by(settings.producers)
            .cloned()
            .collect();
        producers.spawn(workload::steady(plan, mine, sink.clone()));
    }
    let mut tally = Tally::default();
    while !producers.is_empty() {
        if let Some(joined) = producers.try_join_next() {
            tally.absorb(joined.expect("a steady producer"));
            continue;
        }
        recorder.sample_if_due(cluster, cycle, Phase::Steady).await;
        tokio::time::sleep(POLL).await;
    }
    let hot = hot_target(cluster, topics, cycle);
    let max_per_partition = settings.max_entries_per_partition;
    tally.absorb(workload::saturate_partition(cycle, max_per_partition, &hot, sink).await);
    recorder.sample(cluster, cycle, Phase::HotPartition).await;
    let max_entries = settings.max_entries;
    tally.absorb(workload::saturate_brokers(cycle, max_entries, &targets, sink).await);
    recorder.sample(cluster, cycle, Phase::Saturated).await;
    (tally, recorder.take_cycle_latency())
}

/// Partition 0 of the first topic with dedup on, starting at a slot that
/// moves every cycle.
fn hot_target(cluster: &SoakCluster, topics: &[SoakTopic], cycle: usize) -> Target {
    let topic = (0..SLOTS)
        .map(|offset| &topics[(cycle + offset) % SLOTS])
        .find(|topic| topic.dedup_on)
        .expect("a topic with dedup on");
    cluster.targets(std::slice::from_ref(topic)).swap_remove(0)
}

/// Produce nothing until every index is empty and the idle period has
/// outlasted every window. Returns how long the indexes took to empty.
async fn drain(
    settings: &Settings,
    cluster: &SoakCluster,
    recorder: &mut Recorder,
    cycle: usize,
) -> Duration {
    let started = Instant::now();
    let mut emptied = None;
    loop {
        let gauges = cluster.gauges().await;
        if emptied.is_none() && gauges.iter().all(|(_, gauges)| gauges.is_empty()) {
            emptied = Some(started.elapsed());
        }
        if let Some(emptied) = emptied.filter(|_| started.elapsed() >= Settings::min_idle()) {
            return emptied;
        }
        if started.elapsed() > settings.drain_timeout() {
            let occupied: Vec<String> = gauges
                .iter()
                .filter(|(_, gauges)| !gauges.is_empty())
                .map(|(node, gauges)| {
                    format!(
                        "broker {node} holds {} entries: {:?}",
                        gauges.broker_entries,
                        gauges.occupied()
                    )
                })
                .collect();
            recorder.violation(format!(
                "cycle {cycle}: the indexes did not drain within {:?}: {}",
                settings.drain_timeout(),
                occupied.join("; ")
            ));
            return started.elapsed();
        }
        recorder.sample_if_due(cluster, cycle, Phase::Drain).await;
        tokio::time::sleep(POLL).await;
    }
}

/// Every drained index is back to its residual heap: each segment table at
/// its shrink floor or below.
fn check_residual(recorder: &mut Recorder, drained: &Sample, cycle: usize) {
    for (node, gauges) in &drained.brokers {
        let partitions = u64::try_from(gauges.heap_bytes.len()).expect("a partition count");
        let ceiling = Settings::residual_heap_ceiling(partitions);
        if gauges.heap_sum() > ceiling {
            recorder.violation(format!(
                "cycle {cycle}: broker {node}'s drained indexes report {} heap bytes over \
                 {partitions} partitions, above their residual of {ceiling}",
                gauges.heap_sum()
            ));
        }
    }
}

/// Make this cycle's change, and restart a broker when one is due. Returns
/// what was done, for the cycle's row.
async fn churn(
    settings: &Settings,
    cluster: &mut SoakCluster,
    topics: &mut [SoakTopic],
    recorder: &mut Recorder,
    cycle: usize,
) -> String {
    let (change, slot) = Change::after(cycle);
    if let Some(edit) = change.edit(&mut topics[slot]) {
        cluster.edit(&topics[slot], &edit).await;
    } else {
        let old = topics[slot].clone();
        cluster.delete(&old).await;
        await_evicted(cluster, recorder, &old.name(), cycle).await;
        let mut next = old.successor();
        cluster.create(&mut next).await;
        topics[slot] = next;
    }
    let mut done = format!("{change}:{slot}");
    if settings.restarts_after(cycle) {
        cluster.restart(RESTARTED_NODE, topics).await;
        done.push_str("+restart");
    }
    done
}

/// Wait until no broker has a KFC-10 series of the deleted `topic`.
async fn await_evicted(cluster: &SoakCluster, recorder: &mut Recorder, topic: &str, cycle: usize) {
    let deadline = Instant::now() + EVICTION_TIMEOUT;
    loop {
        let holders: Vec<u64> = cluster
            .gauges()
            .await
            .into_iter()
            .filter(|(_, gauges)| gauges.topics.contains(topic))
            .map(|(node, _)| node)
            .collect();
        if holders.is_empty() {
            return;
        }
        if Instant::now() > deadline {
            recorder.violation(format!(
                "cycle {cycle}: brokers {holders:?} still report KFC-10 series of the deleted \
                 topic {topic} after {EVICTION_TIMEOUT:?}"
            ));
            return;
        }
        tokio::time::sleep(POLL).await;
    }
}

/// Delete every topic and check that every broker is back to no index at
/// all: no entries, no heap, no series. Returns the last sample.
async fn teardown(
    cluster: &SoakCluster,
    topics: &[SoakTopic],
    recorder: &mut Recorder,
    cycle: usize,
) -> Sample {
    for topic in topics {
        cluster.delete(topic).await;
    }
    for topic in topics {
        await_evicted(cluster, recorder, &topic.name(), cycle).await;
    }
    let last = recorder.sample(cluster, cycle, Phase::Final).await;
    for (node, gauges) in &last.brokers {
        if gauges.broker_entries != 0 || gauges.entries_sum() != 0 || gauges.heap_sum() != 0 {
            recorder.violation(format!(
                "broker {node} still reports an index once every dedup topic is deleted: {} \
                 broker entries, {} partition entries, {} heap bytes",
                gauges.broker_entries,
                gauges.entries_sum(),
                gauges.heap_sum()
            ));
        }
    }
    last
}
