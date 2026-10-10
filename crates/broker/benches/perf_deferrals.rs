//! `CodSpeed` microbenchmarks for the two costs the broker documents as
//! deliberately deferred, so that the decision to keep or fix each one rests
//! on a measured payoff and not only on an estimated price.
//!
//! **Response framing.** `encode_response` copies a handler's body to prepend
//! the 4- or 5-byte response header, and `KafkaCodec::encode` copies
//! the result again into the codec's write buffer. The deferred alternative is
//! a chained `bytes::Buf` written with one vectored write, which copies the
//! body zero times but needs a custom `Encoder<impl Buf>` and reaches every
//! `framed.send` call site in `dispatch.rs`. `bench_response_framing` runs both
//! over 1 KiB, 64 KiB and 1 MiB bodies.
//!
//! **Replicated-batch sizing.** The replicator calls
//! `RecordBatch::encoded_len` for the replication-bytes metric, and the append
//! path behind `replicate_batch` walks the same records again. The deferred
//! alternative threads one computation through the writer API.
//! `bench_replication` times the walk against the `replicate_batch` it sits in
//! front of, because the walk only matters as a fraction of that.
//!
//! `bench_ratio` prints both comparisons as a table, so a run answers the
//! keep-or-fix question without reading the criterion report.
//!
//! Both framing paths write to a sink that keeps no bytes, so the socket write
//! is excluded from both sides. What is left is the userspace copying the PERF
//! note is about, which makes the reported saving an upper bound on what the
//! refactor could deliver.
//!
//! The numbers this suite produced, and the keep-or-fix call each one settled,
//! are recorded at the two PERF notes themselves: `encode_response` in
//! `network::dispatch::response`, and the `encoded_len` walk in
//! `replicator::response`. Both came out "keep".

use std::{
    future::Future,
    hint::black_box,
    io::IoSlice,
    pin::Pin,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use assert2::assert;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures_util::SinkExt as _;
use krabka_broker::{
    replicate_hot_path::ReplicaSeam,
    response_framing::{self, KafkaCodec},
};
use krabka_protocol::{api_key::ApiKey, records::RecordBatch};
use tokio::{io::AsyncWrite, runtime::Runtime};
use tokio_util::codec::{Encoder as _, Framed};

/// Kafka's default `socket.request.max.bytes`, the request limit the broker
/// builds its codec with.
const MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

/// The response shape both framing paths carry.
///
/// Metadata is the largest response that still goes through the generic
/// `framed.send` path: Fetch, the one API whose bodies are unbounded, already
/// has its own zero-copy writer (see `network::fetch_writer`). Its response is
/// flexible from v9, so the header here is the 5-byte v1 shape, which is the
/// one with the tagged-fields byte to carry along.
const API_KEY: i16 = ApiKey::Metadata as i16;
const BODY_FLEXIBLE: bool = true;
const CORRELATION_ID: i32 = 0x0102_0304;

/// Response-body sizes. 1 KiB is the order of a real Metadata or admin
/// response; 64 KiB and 1 MiB are there to show where the copy starts to
/// dominate, and whether any traffic on this path actually reaches them.
const BODY_SIZES: [(&str, usize); 3] =
    [("1KiB", 1024), ("64KiB", 64 * 1024), ("1MiB", 1024 * 1024)];

mod support;
use support::{LEADER_EPOCH, LOG_BUDGET, SHAPES, WARMUP};

// ---------------------------------------------------------------------------
// Deferral 1: response framing.
// ---------------------------------------------------------------------------

/// An `AsyncWrite` that accepts every byte and keeps none.
///
/// Both framing paths write to one of these, so the kernel is out of the
/// comparison on both sides and what separates them is only the userspace
/// copying each does first. It reports vectored support, because a chained
/// `Buf` that could not be written as segments would not be the prototype
/// under discussion.
#[derive(Default)]
struct NullSink {
    written: usize,
}

impl AsyncWrite for NullSink {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        self.get_mut().written += buf.len();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let total: usize = bufs.iter().map(|slice| slice.len()).sum();
        self.get_mut().written += total;
        Poll::Ready(Ok(total))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Poll `future` to completion on the calling thread.
///
/// [`NullSink`] is always ready, so neither framing path ever parks and no
/// executor is needed. Driving them by hand keeps a runtime's per-call
/// bookkeeping out of a measurement whose whole subject is a `memcpy`.
fn drive<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut cx = Context::from_waker(Waker::noop());
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("a write to NullSink never parks"),
    }
}

// A response body of `len` bytes.
//
// The pattern is non-uniform so that nothing downstream can shortcut it, and
// so a compressed variant of this suite would not measure an unrealistically
// compressible payload.
krabka_macros::patterned_bytes_fixture!(body);

krabka_macros::frame_prefix_fixture!(wire_frame_prefix);

/// The chained-`Buf` prototype's leading segment: the codec's 4-byte frame
/// length followed by the response header the copy path prepends.
///
/// This is the whole of what the prototype has to build. Everything after it
/// is the handler's body, handed to the socket as its own segment.
fn frame_prefix(body_len: usize) -> Bytes {
    let header_len = response_framing::response_header_len(API_KEY, BODY_FLEXIBLE);
    wire_frame_prefix(ResponsePrefixSetup {
        header_len: ResponseByteCount(header_len),
        correlation_id: ResponseCorrelationId(CORRELATION_ID),
        header: ResponseHeaderEncoding::from_wire(response_framing::response_header_v1(
            API_KEY,
            BODY_FLEXIBLE,
        )),
        body_len: ResponseByteCount(body_len),
        capacity: ResponseByteCount(4 + header_len),
        context: "a bench body fits in a frame",
    })
    .freeze()
}

/// Frame one response the way the dispatch loop does and hand it to `framed`.
fn copy_send(framed: &mut Framed<NullSink, KafkaCodec>, payload: &Bytes) -> Duration {
    let start = Instant::now();
    let response =
        response_framing::encode_response(API_KEY, CORRELATION_ID, BODY_FLEXIBLE, payload)
            .expect("a bench body is well under the frame maximum");
    // `Framed::send` is `start_send` plus `poll_flush`, which is the pair the
    // dispatch loop drives per response.
    drive(framed.send(response)).expect("NullSink never fails");
    start.elapsed()
}

/// Frame the same response as a chained `Buf` and hand it to `sink` as
/// segments, which is the prototype the PERF note defers.
fn chained_send(sink: &mut NullSink, payload: &Bytes) -> Duration {
    let start = Instant::now();
    let mut plan = frame_prefix(payload.len()).chain(payload.clone());
    while plan.has_remaining() {
        let mut slices = [IoSlice::new(&[]); 2];
        let filled = plan.chunks_vectored(&mut slices);
        let written = drive(write_vectored(sink, &slices[..filled]));
        plan.advance(written);
    }
    start.elapsed()
}

/// One vectored write against `sink`, as a future the prototype can await.
async fn write_vectored(sink: &mut NullSink, slices: &[IoSlice<'_>]) -> usize {
    std::future::poll_fn(|cx| Pin::new(&mut *sink).poll_write_vectored(cx, slices))
        .await
        .expect("NullSink never fails")
}

/// The bytes the copy path puts on the wire, taken from the real codec.
fn copy_path_wire(payload: &Bytes) -> BytesMut {
    let response =
        response_framing::encode_response(API_KEY, CORRELATION_ID, BODY_FLEXIBLE, payload)
            .expect("a bench body is well under the frame maximum");
    let mut wire = BytesMut::new();
    response_framing::codec(MAX_FRAME_BYTES)
        .encode(response, &mut wire)
        .expect("a bench body is well under the frame maximum");
    wire
}

/// The prototype is only a fair comparison while it is byte-identical to the
/// path it would replace. Were the header shape or the frame length to drift
/// apart, the table would compare two different amounts of work and report a
/// saving that does not exist. This fails the run instead.
fn assert_prototype_is_wire_identical(payload: &Bytes) {
    let mut prototype = BytesMut::from(&frame_prefix(payload.len())[..]);
    prototype.put_slice(payload);
    assert!(
        copy_path_wire(payload) == prototype,
        "the chained prototype must put the same bytes on the wire as the copy path"
    );
}

/// The two framing paths a body is measured on, in report order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Framing {
    /// `encode_response` plus `Framed::send`: two copies of the body.
    Copy,
    /// The chained `Buf` written as segments: no copy of the body.
    Chained,
}

/// Frame and send `payload` `iters` times, returning only the time the framing
/// and the sends took.
fn timed_sends(payload: &Bytes, framing: Framing, iters: u64) -> Duration {
    assert_prototype_is_wire_identical(payload);
    let mut framed = Framed::new(
        NullSink::default(),
        response_framing::codec(MAX_FRAME_BYTES),
    );
    let mut one = |framing| match framing {
        Framing::Copy => copy_send(&mut framed, payload),
        Framing::Chained => chained_send(framed.get_mut(), payload),
    };
    for _ in 0..WARMUP {
        one(framing);
    }
    let mut elapsed = Duration::ZERO;
    for _ in 0..iters {
        elapsed += one(framing);
    }
    // The sink counts what it was handed. Reading it keeps the writes from
    // being anything the optimizer can discard.
    black_box(framed.get_ref().written);
    elapsed
}

// ---------------------------------------------------------------------------
// Deferral 2: the replicator's `encoded_len` walk.
// ---------------------------------------------------------------------------

// Explicit epochs retain the original unclamped last-offset delta.
krabka_macros::record_batch_fixture!(make_batch, LEADER_EPOCH);

/// A follower partition that starts over once its log has taken
/// [`LOG_BUDGET`] bytes.
struct BoundedReplica {
    _dir: tempfile::TempDir,
    seam: ReplicaSeam,
    written: usize,
}

impl BoundedReplica {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let seam = ReplicaSeam::spawn(dir.path()).expect("open follower log");
        Self {
            _dir: dir,
            seam,
            written: 0,
        }
    }

    /// Replace the partition when its log is full. Call this outside the timed
    /// region.
    fn rotate_if_full(&mut self, incoming: usize) {
        if self.written + incoming > LOG_BUDGET {
            *self = Self::new();
        }
        self.written += incoming;
    }
}

/// Hand one leader-assigned batch to the follower's writer, returning only the
/// time `replicate_batch` took.
///
/// The clone and the offset stamp sit outside the timer: the replicator's
/// batches arrive owned out of the Fetch response and already carry the
/// leader's offsets.
async fn replicate_once(replica: &mut BoundedReplica, template: &RecordBatch) -> Duration {
    replica.rotate_if_full(template.encoded_len());
    let mut batch = template.clone();
    batch.base_offset = replica.seam.next_offset().0;
    let start = Instant::now();
    replica
        .seam
        .replicate(batch)
        .await
        .expect("the bench shapes are all valid replicated batches");
    start.elapsed()
}

/// Replicate `template` `iters` times, returning only the time the appends
/// took.
fn timed_replicates(runtime: &Runtime, template: &RecordBatch, iters: u64) -> Duration {
    runtime.block_on(async {
        let mut replica = BoundedReplica::new();
        for _ in 0..WARMUP {
            replicate_once(&mut replica, template).await;
        }
        let mut elapsed = Duration::ZERO;
        for _ in 0..iters {
            elapsed += replicate_once(&mut replica, template).await;
        }
        elapsed
    })
}

/// Walk `template` for its encoded length `iters` times, which is the one
/// extra call the replicator makes per batch.
fn timed_encoded_len(template: &RecordBatch, iters: u64) -> Duration {
    let start = Instant::now();
    let mut total: usize = 0;
    for _ in 0..iters {
        total = total.wrapping_add(black_box(template).encoded_len());
    }
    let elapsed = start.elapsed();
    black_box(total);
    elapsed
}

/// A runtime shaped like the broker's: the writer actor is a spawned task and
/// its append runs on a blocking thread, so a current-thread runtime would
/// measure a different hand-off than production does.
fn replication_runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("build the replication runtime")
}

// ---------------------------------------------------------------------------
// Criterion entry points.
// ---------------------------------------------------------------------------

fn bench_response_framing(c: &mut Criterion) {
    let mut group = c.benchmark_group("broker/response_framing");

    for (label, size) in BODY_SIZES {
        let payload = body(PatternedPayloadLength(size));
        group.throughput(Throughput::Bytes(size as u64));
        for (case, framing) in [("copy", Framing::Copy), ("chained", Framing::Chained)] {
            group.bench_function(format!("{label}/{case}"), |b| {
                b.iter_custom(|iters| timed_sends(&payload, framing, iters));
            });
        }
    }

    group.finish();
}

fn bench_replication(c: &mut Criterion) {
    let runtime = replication_runtime();
    let mut group = c.benchmark_group("broker/replication");

    for (shape, records, payload) in SHAPES {
        let template = make_batch(records, payload);
        group.throughput(Throughput::Bytes(template.encoded_len() as u64));
        group.bench_function(format!("{shape}/encoded_len"), |b| {
            b.iter_custom(|iters| timed_encoded_len(&template, iters));
        });
        group.bench_function(format!("{shape}/replicate_batch"), |b| {
            b.iter_custom(|iters| timed_replicates(&runtime, &template, iters));
        });
    }

    group.finish();
}

/// Appends or sends per measurement run.
const SAMPLES: u32 = 100;

/// Measurement runs per case, of which the fastest is the one reported.
///
/// A single run is noise-dominated on a machine that is doing anything else.
/// Interference only ever adds time, so the minimum over a few runs is the
/// estimator it cannot pull the wrong way.
const RUNS: u32 = 5;

/// The fastest per-iteration nanosecond figure `measure` reports over [`RUNS`]
/// runs of [`SAMPLES`] iterations.
fn fastest(measure: impl Fn(u64) -> Duration) -> f64 {
    (0..RUNS)
        .map(|_| measure(u64::from(SAMPLES)).as_secs_f64() * 1e9 / f64::from(SAMPLES))
        .fold(f64::INFINITY, f64::min)
}

/// Print both deferrals as a table.
///
/// Criterion compares a benchmark against its own previous run, not against a
/// sibling, so the two numbers this suite exists for — what the chained `Buf`
/// would save, and what the extra `encoded_len` walk costs — have to be
/// computed here.
fn bench_ratio(_c: &mut Criterion) {
    println!(
        "\nbroker/response_framing — nanoseconds per response (fastest of {RUNS} runs).\n\
         The sink keeps no bytes, so `saved` is an upper bound on what a chained-Buf\n\
         encoder could remove from this path."
    );
    println!(
        "{:<10} {:>12} {:>12} {:>12} {:>10}",
        "body", "copy", "chained", "saved", "saved %"
    );
    for (label, size) in BODY_SIZES {
        let payload = body(PatternedPayloadLength(size));
        let copy = fastest(|iters| timed_sends(&payload, Framing::Copy, iters));
        let chained = fastest(|iters| timed_sends(&payload, Framing::Chained, iters));
        println!(
            "{label:<10} {copy:>12.0} {chained:>12.0} {:>12.0} {:>9.1}%",
            copy - chained,
            100.0 * (copy - chained) / copy
        );
    }

    let runtime = replication_runtime();
    println!(
        "\nbroker/replication — nanoseconds per batch (fastest of {RUNS} runs).\n\
         `encoded_len` is the walk the replicator adds for the metric; the last column\n\
         is what that walk costs as a fraction of the append it precedes."
    );
    println!(
        "{:<14} {:>14} {:>18} {:>22}",
        "shape", "encoded_len", "replicate_batch", "encoded_len/replicate"
    );
    for (shape, records, payload) in SHAPES {
        let template = make_batch(records, payload);
        let walk = fastest(|iters| timed_encoded_len(&template, iters));
        let replicate = fastest(|iters| timed_replicates(&runtime, &template, iters));
        println!(
            "{shape:<14} {walk:>14.0} {replicate:>18.0} {:>21.2}%",
            100.0 * walk / replicate
        );
    }
    println!();
}

criterion_group!(
    benches,
    bench_response_framing,
    bench_replication,
    bench_ratio
);
criterion_main!(benches);
