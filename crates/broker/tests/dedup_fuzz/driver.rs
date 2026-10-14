//! The fuzz loop: one broker, four dedup topics with a twin each, and one
//! generated case per iteration.
//!
//! Each case goes to one dedup topic, and to its twin without dedup, as the
//! whole records field of a Produce request, under a timeout. The loop then
//! checks the answers:
//!
//! - the row code is one [`ALLOWED_CODES`] lists, and the request neither
//!   timed out nor lost the connection;
//! - no thread of the process panicked;
//! - the dedup topic answers as its twin does, except where header mode
//!   refuses the records that lack the header, and then with exactly those
//!   records in `record_errors`;
//! - an accepted batch is one v2 batch that the protocol crate's validating
//!   parser reads too, and Fetch returns its records with only the dropped
//!   ones left out;
//! - an accepted plain batch, sent again, is dropped record for record:
//!   every record with an identity inside the window.
//!
//! Every [`HEALTH_EVERY`] iterations, and at the end, an ordinary produce and
//! a fetch of what it wrote must succeed.

use {
    crate::{
        child::proc_status_kib,
        mutate::{Bombs, Case, DEDUP_HEADER, Mode, generate},
        oracle::{
            ALLOWED_CODES, Accepted, SentRecord, WINDOW_MS, codec_id, is_subsequence,
            missing_header, parse, same_answer_but_dedup,
        },
        rng::Rng,
        support::{
            InProcess,
            client::create_topic_with,
            dedup::{Rec, create_dedup_topic, keyed_batch, partition_metric, produce},
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
            produce::{
                ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
                single_partition_produce,
            },
            records::now_ms,
            start, topic_id_for,
            topics::CreateTopicSetup,
        },
    },
    assert2::assert,
    bytes::Bytes,
    krabka_broker::codes,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse,
        primitives::uuid::Uuid,
        records::{RecordBatch, RecordsPayload},
    },
    std::{
        collections::BTreeMap,
        sync::Mutex,
        time::{Duration, Instant},
    },
};

/// How long one request may take before the broker counts as hung. A record
/// bomb takes a few seconds in an unoptimized build.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Iterations between two health checks.
pub const HEALTH_EVERY: u64 = 25;

/// Every panic message of this process since [`watch_panics`].
static PANICS: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Record every panic of every thread, then let the default hook print it.
/// A broker task that panics is caught by its runtime and leaves no other
/// trace a client can see.
pub fn watch_panics() {
    let default = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if let Ok(mut panics) = PANICS.lock() {
            panics.push(info.to_string());
        }
        default(info);
    }));
}

fn panics() -> Vec<String> { PANICS.lock().map(|p| p.clone()).unwrap_or_default() }

/// The run: its seed, its first iteration, and how many iterations it runs.
#[derive(Clone, Copy, Debug)]
pub struct Run {
    pub seed: u64,
    pub start: u64,
    pub iterations: u64,
}

/// What a run saw.
#[derive(Debug, Default)]
pub struct Summary {
    /// Row codes and how often each came back.
    pub codes: BTreeMap<i16, u64>,
    /// Accepted batches.
    pub accepted: u64,
    /// Accepted batches whose second send was checked.
    pub resends: u64,
    /// Accepted batches with a codec id that names no codec. Kafka refuses
    /// these; see `refuses_a_batch_with_an_unknown_codec_id` for the bug.
    pub unknown_codec_accepted: u64,
    /// The case that raised the peak resident size most: by how many KiB,
    /// its iteration, and its mutations.
    pub largest_peak_step: (u64, u64, String),
}

/// One fuzz topic.
#[derive(Clone, Copy)]
pub struct Topic {
    pub name: &'static str,
    pub id: Uuid,
    pub mode: Mode,
}

/// One dedup topic and its twin: the same configs, without dedup.
#[derive(Clone, Copy)]
struct Pair {
    dedup: Topic,
    plain: Topic,
}

/// The names, identity mode and extra configs of one [`Pair`].
struct Shape {
    dedup: &'static str,
    plain: &'static str,
    mode: Mode,
    configs: &'static [(&'static str, &'static str)],
}

/// Key mode and header mode as the producer compresses, key mode
/// recompressed to zstd so every other codec takes the owned decode, and
/// header mode on a compacted topic.
const SHAPES: [Shape; 4] = [
    Shape {
        dedup: "fuzz-key",
        plain: "plain-key",
        mode: Mode::Key,
        configs: &[],
    },
    Shape {
        dedup: "fuzz-header",
        plain: "plain-header",
        mode: Mode::Header,
        configs: &[],
    },
    Shape {
        dedup: "fuzz-recompressed",
        plain: "plain-recompressed",
        mode: Mode::Key,
        configs: &[("compression.type", "zstd")],
    },
    Shape {
        dedup: "fuzz-compacted",
        plain: "plain-compacted",
        mode: Mode::Header,
        configs: &[("cleanup.policy", "compact")],
    },
];

async fn pairs(p: &InProcess) -> Vec<Pair> {
    let mut pairs = Vec::new();
    for shape in SHAPES {
        let id = create_dedup_topic()
            .broker(&p.broker)
            .client(&p.client)
            .topic(shape.dedup)
            .window_ms(WINDOW_MS)
            .maybe_key_header((shape.mode == Mode::Header).then_some(DEDUP_HEADER))
            .extra_configs(shape.configs)
            .call()
            .await;
        create_topic_with(&p.client, CreateTopicSetup {
            topic: shape.plain,
            configs: shape.configs,
            ..Default::default()
        })
        .await;
        p.broker.wait_until_partition_present(shape.plain, 0).await;
        pairs.push(Pair {
            dedup: Topic {
                name: shape.dedup,
                id,
                mode: shape.mode,
            },
            plain: Topic {
                name: shape.plain,
                id: topic_id_for(&p.client, shape.plain).await,
                mode: shape.mode,
            },
        });
    }
    pairs
}

/// Produce `records` as the raw records field, `acks=all`, under
/// [`REQUEST_TIMEOUT`].
pub async fn send(
    p: &InProcess,
    topic: Topic,
    records: &[u8],
) -> Result<PartitionProduceResponse, String> {
    let request = single_partition_produce(SinglePartitionProduceSetup {
        topic: topic.name.to_owned(),
        topic_id: topic.id,
        partition: krabka_ids::PartitionIndex(0),
        acknowledgements: ProduceAcknowledgements::AllReplicas,
        timeout: ProduceTimeoutMillis(30_000),
        records: Some(RecordsPayload::Raw(Bytes::copy_from_slice(records))),
    });
    match tokio::time::timeout(REQUEST_TIMEOUT, p.client.send(request)).await {
        Err(_) => Err(format!("no answer within {REQUEST_TIMEOUT:?}")),
        Ok(Err(error)) => Err(format!("the request failed: {error}")),
        Ok(Ok(response)) => response
            .responses
            .first()
            .and_then(|topic| topic.partition_responses.first())
            .cloned()
            .ok_or_else(|| format!("no partition row in {response:?}")),
    }
}

/// The stored batch that starts at `base_offset`, read with Fetch.
async fn stored_batch(
    p: &InProcess,
    topic: Topic,
    base_offset: i64,
) -> Result<RecordBatch, String> {
    let request = single_partition_fetch()
        .topic(topic.name.to_owned())
        .topic_id(topic.id)
        .partition(fetch_partition().offset(base_offset).call())
        .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
        .call();
    let response = tokio::time::timeout(REQUEST_TIMEOUT, p.client.send(request))
        .await
        .map_err(|_| "fetch timed out".to_owned())?
        .map_err(|error| format!("fetch failed: {error}"))?;
    let row = &response.responses[0].partitions[0];
    if row.error_code != 0 {
        return Err(format!("fetch error {}", row.error_code));
    }
    row.records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .and_then(|batches| batches.iter().find(|b| b.base_offset == base_offset))
        .cloned()
        .ok_or_else(|| format!("no stored batch at offset {base_offset}"))
}

/// A stored record in the shape the oracle compares.
fn as_sent(record: &krabka_protocol::records::Record) -> SentRecord {
    SentRecord {
        key: record.key.as_ref().map(|key| key.to_vec()),
        value: record.value.as_ref().map(|value| value.to_vec()),
        headers: record
            .headers
            .iter()
            .map(|header| {
                (
                    header.key.clone(),
                    header.value.as_ref().map(|v| v.to_vec()),
                )
            })
            .collect(),
        timestamp_ms: 0,
        identity: false,
    }
}

/// Whether the first batch's codec id names no codec. Kafka's append throws
/// `IllegalArgumentException` from `CompressionType.forId` on such a batch
/// and answers `UNKNOWN_SERVER_ERROR`, so that answer is Kafka's.
fn names_no_codec(records: &[u8]) -> bool {
    records
        .get(22)
        .is_some_and(|attributes_low| attributes_low & 0x07 > 4)
}

async fn dropped_total(p: &InProcess, topic: Topic) -> u64 {
    let value = partition_metric(&p.broker, "dedup_records_dropped_total", topic.name, 0).await;
    value.to_string().parse().unwrap_or(0)
}

/// One iteration's context, for every failure message.
struct Context<'a> {
    run: Run,
    iteration: u64,
    topic: Topic,
    case: &'a Case,
}

impl Context<'_> {
    fn describe(&self) -> String {
        format!(
            "replay with DEDUP_FUZZ_SEED={} DEDUP_FUZZ_START={} DEDUP_FUZZ_ITERS=1\ntopic {} \
             ({:?} mode)\n{}",
            self.run.seed,
            self.iteration,
            self.topic.name,
            self.topic.mode,
            self.case.describe()
        )
    }
}

/// Check what the broker did with a batch it accepted.
async fn check_accepted(
    p: &InProcess,
    ctx: &Context<'_>,
    row: &PartitionProduceResponse,
    dropped_before: u64,
    summary: &mut Summary,
) {
    summary.accepted += 1;
    let bytes = &ctx.case.bytes;
    if bytes.get(16).is_some_and(|magic| *magic < 2) {
        // A legacy message set, up-converted: nothing below reads it.
        return;
    }
    let accepted: Accepted = match parse(bytes, ctx.topic.mode) {
        Ok(accepted) => accepted,
        Err(error) => {
            panic!(
                "the broker accepted a records field the protocol crate refuses ({error}): {row:?}\n{}",
                ctx.describe()
            )
        }
    };
    if codec_id(accepted.attributes) > 4 {
        summary.unknown_codec_accepted += 1;
    }
    let dropped_first = dropped_total(p, ctx.topic).await - dropped_before;
    let stored = stored_batch(p, ctx.topic, row.base_offset).await;
    let stored = stored.unwrap_or_else(|error| panic!("{error}\n{}", ctx.describe()));
    let stored: Vec<SentRecord> = stored.records.iter().map(as_sent).collect();
    let sent = u64::try_from(accepted.records.len()).unwrap_or(u64::MAX);
    let kept = u64::try_from(stored.len()).unwrap_or(u64::MAX);
    assert!(
        kept + dropped_first == sent,
        "stored {kept} and dropped {dropped_first} of {sent} records\n{}",
        ctx.describe()
    );
    assert!(
        is_subsequence(&stored, &accepted.records),
        "the stored records are not the sent ones less the dropped ones: {stored:?}\n{}",
        ctx.describe()
    );
    if !accepted.resend_is_deduplicated() {
        return;
    }
    let before = dropped_total(p, ctx.topic).await;
    let again = send(p, ctx.topic, bytes).await;
    let again = again.unwrap_or_else(|error| panic!("resend: {error}\n{}", ctx.describe()));
    let now = now_ms();
    if again.error_code != 0 && accepted.near_future_edge(now) {
        return;
    }
    assert!(
        again.error_code == 0,
        "the same bytes were accepted once and refused the second time: {again:?}\n{}",
        ctx.describe()
    );
    let dropped = dropped_total(p, ctx.topic).await - before;
    let (at_least, at_most) = accepted.resend_drops(now);
    assert!(
        (at_least..=at_most).contains(&dropped),
        "the second send dropped {dropped} records, expected {at_least}..={at_most}\n{}",
        ctx.describe()
    );
    summary.resends += 1;
}

/// An ordinary produce to `topic`, and a fetch of what it wrote.
async fn check_health(p: &InProcess, topic: Topic, iteration: u64) {
    let key = format!("health-{iteration}-{}", now_ms());
    let row = tokio::time::timeout(
        REQUEST_TIMEOUT,
        produce()
            .client(&p.client)
            .topic(topic.name)
            .topic_id(topic.id)
            .batch(
                keyed_batch()
                    .records(&[Rec::key(&key).dedup_id(&key)])
                    .call(),
            )
            .call(),
    )
    .await
    .unwrap_or_else(|_| panic!("health produce timed out after iteration {iteration}"));
    assert!(
        row.error_code == 0,
        "health produce after iteration {iteration}: {row:?}"
    );
    let stored = stored_batch(p, topic, row.base_offset).await;
    let stored = stored.unwrap_or_else(|error| panic!("health fetch after {iteration}: {error}"));
    assert!(
        stored.records.first().and_then(|r| r.key.as_deref()) == Some(key.as_bytes()),
        "health fetch after iteration {iteration} read {stored:?}"
    );
}

/// Run the fuzz loop against a fresh broker.
pub async fn fuzz(run: Run) -> Summary {
    let p = start().await;
    let pairs = pairs(&p).await;
    let bombs = Bombs::shared();
    let mut summary = Summary::default();
    let started = Instant::now();
    for iteration in run.start..run.start.saturating_add(run.iterations) {
        let mut rng = Rng::for_iteration(run.seed, iteration);
        let pair = pairs[rng.below(pairs.len())];
        let topic = pair.dedup;
        let case = generate(&mut rng, topic.mode, now_ms(), bombs);
        let ctx = Context {
            run,
            iteration,
            topic,
            case: &case,
        };
        let peak_before = proc_status_kib("VmHWM");
        let dropped_before = dropped_total(&p, topic).await;
        let row = send(&p, topic, &case.bytes).await;
        let row = row
            .unwrap_or_else(|error| panic!("{error}; panics: {:?}\n{}", panics(), ctx.describe()));
        *summary.codes.entry(row.error_code).or_default() += 1;
        let plain = send(&p, pair.plain, &case.bytes).await;
        let plain = plain.unwrap_or_else(|error| panic!("plain twin: {error}\n{}", ctx.describe()));
        let parsed = parse(&case.bytes, topic.mode).ok();
        if let Err(difference) = same_answer_but_dedup(
            missing_header(topic.mode, parsed.as_ref()).as_deref(),
            &row,
            &plain,
        ) {
            panic!("{difference}: {row:?} {plain:?}\n{}", ctx.describe());
        }
        assert!(
            ALLOWED_CODES.contains(&row.error_code)
                || (row.error_code == codes::UNKNOWN_SERVER_ERROR && names_no_codec(&case.bytes)),
            "row code {} is not one a malformed produce may get: {row:?}\n{}",
            row.error_code,
            ctx.describe()
        );
        assert!(
            panics().is_empty(),
            "the broker panicked: {:?}\n{}",
            panics(),
            ctx.describe()
        );
        if row.error_code == 0 {
            check_accepted(&p, &ctx, &row, dropped_before, &mut summary).await;
        }
        let step = proc_status_kib("VmHWM").saturating_sub(peak_before);
        if step > summary.largest_peak_step.0 {
            summary.largest_peak_step = (step, iteration, case.notes.join(", "));
        }
        if (iteration + 1) % HEALTH_EVERY == 0 {
            check_health(&p, pairs[0].dedup, iteration).await;
        }
    }
    for pair in &pairs {
        check_health(&p, pair.dedup, run.start.saturating_add(run.iterations)).await;
        check_health(&p, pair.plain, run.start.saturating_add(run.iterations)).await;
    }
    assert!(panics().is_empty(), "the broker panicked: {:?}", panics());
    eprintln!(
        "dedup_fuzz: seed {} iterations {}..{} in {:?}: {summary:?}",
        run.seed,
        run.start,
        run.start.saturating_add(run.iterations),
        started.elapsed()
    );
    p.broker.shutdown().await;
    summary
}
