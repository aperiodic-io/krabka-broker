//! The invariants a quiesced cluster must satisfy, checked against the
//! history the producers recorded and the logs each replica serves.
//!
//! Every replica is read with consumer Fetch requests sent to that replica
//! itself: from v11 on a follower serves a consumer up to its own high
//! watermark, as Kafka's `fetchOnlyLeader` allows, so the bytes compared are
//! what each replica would hand a consumer.

use {
    crate::{
        cluster::{ChaosCluster, bounded, dial},
        support::{
            dedup::DEDUP_HEADER,
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
        },
        topics::{Rule, Target},
        workload::{History, Sent},
    },
    krabka_protocol::{
        primitives::uuid::Uuid,
        records::{Record, RecordBatch, RecordsPayload},
    },
    std::{
        collections::{BTreeMap, BTreeSet},
        fmt,
        time::{Duration, Instant},
    },
};

/// The invariants, in the order a report lists them.
pub const INVARIANTS: [&str; 6] = [
    "durability",
    "first-wins",
    "acked-offsets",
    "replica-agreement",
    "memory-bound",
    "liveness",
];

/// How many violations of one invariant a report prints in full.
const SHOWN: usize = 12;

/// What the checks found.
#[derive(Debug, Default)]
pub struct Findings {
    violations: BTreeMap<&'static str, Vec<String>>,
    /// What each check covered, for the report.
    pub notes: Vec<String>,
}

impl Findings {
    pub fn violate(&mut self, invariant: &'static str, what: String) {
        self.violations.entry(invariant).or_default().push(what);
    }

    pub fn is_clean(&self) -> bool { self.violations.values().all(Vec::is_empty) }

    /// One line per invariant: `ok`, or how many times it broke.
    pub fn verdicts(&self) -> String {
        INVARIANTS
            .iter()
            .map(|invariant| {
                let count = self.violations.get(invariant).map_or(0, Vec::len);
                if count == 0 {
                    format!("{invariant}: ok")
                } else {
                    format!("{invariant}: {count} violations")
                }
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

impl fmt::Display for Findings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.verdicts())?;
        for (invariant, violations) in &self.violations {
            for violation in violations.iter().take(SHOWN) {
                writeln!(f, "  {invariant}: {violation}")?;
            }
            if violations.len() > SHOWN {
                writeln!(f, "  {invariant}: ... {} more", violations.len() - SHOWN)?;
            }
        }
        for note in &self.notes {
            writeln!(f, "  note: {note}")?;
        }
        Ok(())
    }
}

/// One record as a replica serves it.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Stored {
    key: Option<String>,
    value: Option<String>,
    header: Option<String>,
}

impl Stored {
    fn of(record: &Record) -> Self {
        let text =
            |bytes: Option<&[u8]>| bytes.map(|bytes| String::from_utf8_lossy(bytes).into_owned());
        Self {
            key: text(record.key.as_deref()),
            value: text(record.value.as_deref()),
            header: record
                .headers
                .iter()
                .find(|header| header.key == DEDUP_HEADER)
                .and_then(|header| text(header.value.as_deref())),
        }
    }

    fn is(&self, sent: &Sent) -> bool {
        self.key.as_deref() == Some(sent.key.as_str())
            && self.value.as_deref() == Some(sent.value.as_str())
            && self.header == sent.header
    }

    fn identity(&self, rule: Rule) -> Option<&str> {
        match rule {
            Rule::Key => self.key.as_deref(),
            Rule::Header => self.header.as_deref(),
        }
    }
}

fn identity_of(sent: &Sent, rule: Rule) -> Option<&str> {
    match rule {
        Rule::Key => Some(sent.key.as_str()),
        Rule::Header => sent.header.as_deref(),
    }
}

/// The committed log of one partition on one replica.
pub struct ReplicaLog {
    pub node: u64,
    pub batches: Vec<RecordBatch>,
    pub high_watermark: i64,
}

/// The offset after the last record a batch spans.
fn end_of(batch: &RecordBatch) -> i64 { batch.base_offset + i64::from(batch.last_offset_delta) + 1 }

/// Every batch broker `node` serves a consumer of `target`, read from
/// offset 0 to its high watermark.
pub async fn read_replica(
    cluster: &ChaosCluster,
    node: u64,
    target: Target,
    topic_id: Uuid,
) -> Result<ReplicaLog, String> {
    let conn = dial(cluster.addr(node), "dedup-chaos-reader").await?;
    let mut batches: Vec<RecordBatch> = Vec::new();
    loop {
        let from = batches.last().map_or(0, end_of);
        let request = single_partition_fetch()
            .topic(target.spec().name.to_owned())
            .topic_id(topic_id)
            .partition(
                fetch_partition()
                    .partition(target.partition)
                    .offset(from)
                    .call(),
            )
            .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
            .call();
        let response = bounded(conn.send(request))
            .await?
            .map_err(|error| format!("Fetch from broker {node}: {error}"))?;
        let row = response
            .responses
            .first()
            .and_then(|topic| topic.partitions.first())
            .ok_or_else(|| format!("broker {node} answered Fetch without {target}"))?;
        if row.error_code != 0 {
            return Err(format!("broker {node}: Fetch error {}", row.error_code));
        }
        let before = batches.len();
        batches.extend(
            row.records
                .as_ref()
                .and_then(RecordsPayload::as_v2)
                .unwrap_or_default()
                .iter()
                .filter(|batch| end_of(batch) > from)
                .cloned(),
        );
        if batches.len() == before {
            return Ok(ReplicaLog {
                node,
                batches,
                high_watermark: row.high_watermark,
            });
        }
    }
}

/// Wait until every replica of `target` holds the leader's whole log and
/// has a high watermark at its end, for at most `limit`.
pub async fn await_converged(
    cluster: &ChaosCluster,
    target: Target,
    limit: Duration,
) -> Result<(), String> {
    let started = Instant::now();
    loop {
        let mut frontier = Vec::new();
        for node in cluster.live() {
            let handle = cluster.handle(node).expect("a live broker");
            let (topic, partition) = (target.spec().name, target.partition);
            let end = handle
                .local_batches_for_test(topic, partition)
                .map(|batches| batches.last().map_or(0, end_of));
            let high_watermark = handle.high_watermark_for_test(topic, partition).await;
            frontier.push((node, end, high_watermark));
        }
        let ends: BTreeSet<Option<i64>> = frontier.iter().map(|(_, end, _)| *end).collect();
        let settled = frontier.len() == 3
            && cluster.leader_of(target).is_some()
            && ends.len() == 1
            && frontier
                .iter()
                .all(|(_, end, hw)| end.is_some() && end == hw);
        if settled {
            return Ok(());
        }
        if started.elapsed() > limit {
            return Err(format!(
                "{target}: replicas did not converge within {limit:?}: (broker, log end, high \
                 watermark) {frontier:?}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One replica's log, as offsets and spans.
struct LogView {
    records: BTreeMap<i64, Stored>,
    /// Each batch's last offset, by base offset.
    spans: BTreeMap<i64, i64>,
    /// The first offset of each identity, under each rule.
    first: BTreeMap<(Rule, String), i64>,
}

impl LogView {
    fn of(log: &ReplicaLog) -> Self {
        let mut view = Self {
            records: BTreeMap::new(),
            spans: BTreeMap::new(),
            first: BTreeMap::new(),
        };
        for batch in &log.batches {
            view.spans.insert(batch.base_offset, end_of(batch) - 1);
            for record in &batch.records {
                let offset = batch.base_offset + i64::from(record.offset_delta);
                let stored = Stored::of(record);
                for rule in [Rule::Key, Rule::Header] {
                    if let Some(identity) = stored.identity(rule) {
                        view.first
                            .entry((rule, identity.to_owned()))
                            .or_insert(offset);
                    }
                }
                view.records.insert(offset, stored);
            }
        }
        view
    }

    /// Whether some batch spans `offset`.
    fn spans(&self, offset: i64) -> bool {
        self.spans
            .range(..=offset)
            .next_back()
            .is_some_and(|(_, last)| *last >= offset)
    }

    /// Whether this log still holds `sent`, acknowledged at `offset`: the
    /// record is there, or a batch spans the gap a dropped duplicate leaves
    /// and an earlier copy of its identity exists.
    fn accounts_for(&self, sent: &Sent, offset: i64, rules: &[Rule]) -> bool {
        match self.records.get(&offset) {
            Some(stored) => stored.is(sent),
            None => self.spans(offset) && self.has_earlier_copy(sent, offset, rules),
        }
    }

    /// Whether a record before `offset` has the identity of `sent` under one
    /// of `rules`: the copy that made `sent` a duplicate.
    fn has_earlier_copy(&self, sent: &Sent, offset: i64, rules: &[Rule]) -> bool {
        rules.iter().any(|rule| {
            identity_of(sent, *rule).is_some_and(|identity| {
                self.first
                    .get(&(*rule, identity.to_owned()))
                    .is_some_and(|first| *first < offset)
            })
        })
    }
}

/// The rules a dropped record of `target` may have been a duplicate under:
/// the topic's rule, or either rule for the topic whose rule changes.
fn rules_of(target: Target) -> Vec<Rule> {
    if target.spec().mutable {
        vec![Rule::Key, Rule::Header]
    } else {
        vec![target.spec().rule]
    }
}

/// Every acknowledged record of `target`: the attempt's description, the
/// record, and the offset its acknowledgement gives it.
fn acked_records(history: &History, target: Target) -> Vec<(String, &Sent, i64, (i64, i64))> {
    history
        .attempts
        .iter()
        .filter(|attempt| attempt.target == target)
        .filter_map(|attempt| attempt.acked().map(|base| (attempt, base)))
        .flat_map(|(attempt, base)| {
            let last = base + i64::try_from(attempt.records.len()).expect("a batch size") - 1;
            attempt
                .records
                .iter()
                .zip(base..)
                .map(move |(sent, offset)| (attempt.to_string(), sent, offset, (base, last)))
        })
        .collect()
}

/// Durability: every acknowledged record is on this replica, at its offset
/// or, when it was dropped as a duplicate, as an earlier copy of its
/// identity in a batch whose span still holds its offset.
pub fn check_durability(
    findings: &mut Findings,
    history: &History,
    target: Target,
    log: &ReplicaLog,
) {
    let view = LogView::of(log);
    let rules = rules_of(target);
    for (attempt, sent, offset, _) in acked_records(history, target) {
        if !view.accounts_for(sent, offset, &rules) {
            findings.violate(
                "durability",
                format!(
                    "{target} broker {}: acknowledged {sent:?} at offset {offset} is lost, the \
                     offset holds {:?} (high watermark {}); {attempt}",
                    log.node,
                    view.records.get(&offset),
                    log.high_watermark
                ),
            );
        }
    }
}

/// Exact acknowledgements, on the leader's log: each acknowledged request
/// spans exactly one stored batch; each acknowledged offset holds the
/// record sent, or nothing when the record was a duplicate of an earlier
/// one; and every stored record is one some producer sent.
pub fn check_acked_offsets(
    findings: &mut Findings,
    history: &History,
    target: Target,
    leader: &ReplicaLog,
) {
    let view = LogView::of(leader);
    let rules = rules_of(target);
    let mut reported_spans = BTreeSet::new();
    for (attempt, sent, offset, (base, last)) in acked_records(history, target) {
        if view.spans.get(&base) != Some(&last) && reported_spans.insert(base) {
            findings.violate(
                "acked-offsets",
                format!(
                    "{target}: acknowledged span {base}..={last} is no stored batch (stored: \
                     {:?}); {attempt}",
                    view.spans.range(..=base).next_back()
                ),
            );
        }
        match view.records.get(&offset) {
            Some(stored) if !stored.is(sent) => findings.violate(
                "acked-offsets",
                format!("{target}: offset {offset} acknowledged for {sent:?} holds {stored:?}; {attempt}"),
            ),
            None if !view.has_earlier_copy(sent, offset, &rules) => findings.violate(
                "acked-offsets",
                format!(
                    "{target}: {sent:?} was dropped at offset {offset}, but no earlier record has \
                     its identity; {attempt}"
                ),
            ),
            _ => {}
        }
    }
    let sent: BTreeSet<(&str, &str)> = history
        .attempts
        .iter()
        .filter(|attempt| attempt.target == target)
        .flat_map(|attempt| attempt.records.iter())
        .map(|sent| (sent.key.as_str(), sent.value.as_str()))
        .collect();
    for (offset, stored) in &view.records {
        let pair = (
            stored.key.as_deref().unwrap_or_default(),
            stored.value.as_deref().unwrap_or_default(),
        );
        if !sent.contains(&pair) {
            findings.violate(
                "acked-offsets",
                format!("{target}: offset {offset} holds {stored:?}, which no producer sent"),
            );
        }
    }
}

/// First wins: within the window each identity is in the log at most once.
pub fn check_first_wins(findings: &mut Findings, target: Target, leader: &ReplicaLog) -> usize {
    let view = LogView::of(leader);
    let mut copies: BTreeMap<&str, Vec<(i64, &str)>> = BTreeMap::new();
    for (offset, stored) in &view.records {
        if let Some(identity) = stored.identity(target.spec().rule) {
            copies
                .entry(identity)
                .or_default()
                .push((*offset, stored.value.as_deref().unwrap_or_default()));
        }
    }
    for (identity, offsets) in &copies {
        if offsets.len() > 1 {
            findings.violate(
                "first-wins",
                format!(
                    "{target}: {identity} is in the log {} times: {offsets:?}",
                    offsets.len()
                ),
            );
        }
    }
    copies.len()
}

/// Replica agreement: every replica serves the same batches, byte for byte,
/// up to the same high watermark.
pub fn check_agreement(findings: &mut Findings, target: Target, logs: &[ReplicaLog]) {
    let encode = |log: &ReplicaLog| -> Vec<bytes::Bytes> {
        log.batches
            .iter()
            .map(|batch| batch.encode_to_bytes().expect("encode a fetched batch"))
            .collect()
    };
    let Some((reference, others)) = logs.split_first() else {
        return;
    };
    let expected = encode(reference);
    for log in others {
        let actual = encode(log);
        let first_difference = expected
            .iter()
            .zip(&actual)
            .position(|(left, right)| left != right);
        if first_difference.is_some()
            || expected.len() != actual.len()
            || reference.high_watermark != log.high_watermark
        {
            findings.violate(
                "replica-agreement",
                format!(
                    "{target}: broker {} serves {} batches up to {}, broker {} serves {} up to {}; \
                     first differing batch: {} | {}",
                    reference.node,
                    expected.len(),
                    reference.high_watermark,
                    log.node,
                    actual.len(),
                    log.high_watermark,
                    summary(reference, first_difference),
                    summary(log, first_difference),
                ),
            );
        }
    }
}

/// The batch at `index` of `log`: its header fields and its records.
fn summary(log: &ReplicaLog, index: Option<usize>) -> String {
    let Some(batch) = index.and_then(|index| log.batches.get(index)) else {
        return format!("broker {}: none", log.node);
    };
    let records: Vec<String> = batch
        .records
        .iter()
        .map(|record| {
            let stored = Stored::of(record);
            format!(
                "+{} {}={}",
                record.offset_delta,
                stored.key.unwrap_or_default(),
                stored.value.unwrap_or_default()
            )
        })
        .collect();
    format!(
        "broker {}: base {} last delta {} leader epoch {} producer {}/{} seq {} {records:?}",
        log.node,
        batch.base_offset,
        batch.last_offset_delta,
        batch.partition_leader_epoch,
        batch.producer_id,
        batch.producer_epoch,
        batch.base_sequence
    )
}

/// Counts of what one partition's log and history hold, for the report.
pub fn describe(history: &History, target: Target, leader: &ReplicaLog) -> String {
    let attempts = history
        .attempts
        .iter()
        .filter(|attempt| attempt.target == target);
    let (mut sent, mut acked) = (0, 0);
    for attempt in attempts {
        sent += 1;
        acked += usize::from(attempt.acked().is_some());
    }
    let records: usize = leader.batches.iter().map(|batch| batch.records.len()).sum();
    let empty = leader
        .batches
        .iter()
        .filter(|batch| batch.records.is_empty())
        .count();
    format!(
        "{target}: {sent} requests, {acked} acknowledged; {} batches ({empty} empty), {records} \
         records, {} offsets dropped, high watermark {}",
        leader.batches.len(),
        usize::try_from(leader.high_watermark)
            .unwrap_or_default()
            .saturating_sub(records),
        leader.high_watermark
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            support::dedup::{Rec, keyed_batch},
            workload::{Attempt, Outcome},
        },
        assert2::assert,
    };

    const KEYED: Target = Target {
        topic: 0,
        partition: 0,
    };

    /// A stored batch at `base` spanning `span` offsets, holding a record
    /// `(offset delta, key, value)` for each of `records`.
    fn stored(base: i64, span: i32, records: &[(i32, &str, &str)]) -> RecordBatch {
        let recs: Vec<Rec> = records
            .iter()
            .map(|(_, key, value)| Rec::key(key).value(value))
            .collect();
        let mut batch = keyed_batch().records(&recs).call();
        batch.base_offset = base;
        batch.last_offset_delta = span - 1;
        for (record, (delta, _, _)) in batch.records.iter_mut().zip(records) {
            record.offset_delta = *delta;
        }
        batch
    }

    fn log(node: u64, batches: Vec<RecordBatch>) -> ReplicaLog {
        let high_watermark = batches.last().map_or(0, end_of);
        ReplicaLog {
            node,
            batches,
            high_watermark,
        }
    }

    /// `a` and `b` acknowledged at 0, then a second `a` acknowledged at 2.
    fn history() -> History {
        let acked = |base: i64, records: &[(&str, &str)]| Attempt {
            producer: 0,
            target: KEYED,
            batch: 0,
            replay: false,
            records: records
                .iter()
                .map(|(key, value)| Sent {
                    key: (*key).to_owned(),
                    value: (*value).to_owned(),
                    header: None,
                })
                .collect::<Vec<_>>()
                .into(),
            at: Duration::ZERO,
            outcome: Outcome::Acked(base),
        };
        History {
            attempts: vec![
                acked(0, &[("a", "v1"), ("b", "v2")]),
                acked(2, &[("a", "v3")]),
            ],
            ..History::default()
        }
    }

    /// Every check of one partition over `logs`, the first the leader's.
    fn verdicts(logs: &[ReplicaLog]) -> String {
        let history = history();
        let mut findings = Findings::default();
        for log in logs {
            check_durability(&mut findings, &history, KEYED, log);
        }
        check_acked_offsets(&mut findings, &history, KEYED, &logs[0]);
        check_first_wins(&mut findings, KEYED, &logs[0]);
        check_agreement(&mut findings, KEYED, logs);
        findings.verdicts()
    }

    #[test]
    fn each_check_catches_the_log_that_breaks_its_invariant() {
        let first = || stored(0, 2, &[(0, "a", "v1"), (1, "b", "v2")]);
        let verdict = |broken: &[(&str, usize)]| {
            INVARIANTS
                .iter()
                .map(
                    |invariant| match broken.iter().find(|(name, _)| name == invariant) {
                        Some((_, count)) => format!("{invariant}: {count} violations"),
                        None => format!("{invariant}: ok"),
                    },
                )
                .collect::<Vec<_>>()
                .join(", ")
        };
        let cases = [
            (
                "the second a dropped into an empty batch",
                vec![log(1, vec![first(), stored(2, 1, &[])])],
                verdict(&[]),
            ),
            (
                "the second a admitted",
                vec![log(1, vec![first(), stored(2, 1, &[(0, "a", "v3")])])],
                verdict(&[("first-wins", 1)]),
            ),
            (
                "the acknowledged offset holds another record",
                vec![log(1, vec![first(), stored(2, 1, &[(0, "c", "v9")])])],
                // The record differs from the one sent, and no producer sent it.
                verdict(&[("durability", 1), ("acked-offsets", 2)]),
            ),
            (
                "the acknowledged gap is gone",
                vec![log(1, vec![first()])],
                verdict(&[("durability", 1), ("acked-offsets", 1)]),
            ),
            (
                "a follower lost the gap",
                vec![
                    log(1, vec![first(), stored(2, 1, &[])]),
                    log(2, vec![first()]),
                ],
                verdict(&[("durability", 1), ("replica-agreement", 1)]),
            ),
        ];
        for (case, logs, expected) in cases {
            assert!(verdicts(&logs) == expected, "{case}");
        }
    }

    #[test]
    fn a_dropped_record_needs_an_earlier_copy_of_its_identity() {
        let view = LogView::of(&log(1, vec![stored(0, 3, &[
            (0, "a", "v1"),
            (2, "b", "v2"),
        ])]));
        let sent = |key: &str| Sent {
            key: key.to_owned(),
            value: "v".to_owned(),
            header: None,
        };
        assert!(view.accounts_for(&sent("a"), 1, &[Rule::Key]));
        assert!(!view.accounts_for(&sent("b"), 1, &[Rule::Key]));
        assert!(!view.accounts_for(&sent("a"), 3, &[Rule::Key]));
        assert!(!view.accounts_for(&sent("a"), 1, &[Rule::Header]));
    }
}
