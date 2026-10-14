//! KFC-10 produce-time deduplication under a seeded, structure-aware fuzzer.
//!
//! One in-process broker serves four dedup topics: key mode and header mode
//! as the producer compresses them, key mode recompressed so the owned decode
//! runs, and header mode on a compacted topic. Each iteration starts from a
//! valid record batch and damages it (see [`mutate`]): lengths and counts that
//! lie, overlong and unterminated varints, flipped and missing bytes,
//! duplicate, null and invalid-UTF-8 header names, compressed payloads that
//! decode to garbage or to far more than they are, and the transactional,
//! control and timestamp flags. The CRC is usually recomputed afterwards, so
//! the bytes reach the record walk and the dedup classification.
//!
//! [`driver`] lists what each answer must satisfy. The loop runs in a child
//! process under `prlimit --as`, so that an allocation sized by a lying count
//! fails the test instead of killing the runner, and the child reports its
//! peak resident size.
//!
//! The run is seeded. `DEDUP_FUZZ_SEED` picks the seed, `DEDUP_FUZZ_ITERS` the
//! number of iterations, and `DEDUP_FUZZ_START` the first one; each iteration
//! draws from its own generator, so a failure message's
//! `DEDUP_FUZZ_SEED=.. DEDUP_FUZZ_START=.. DEDUP_FUZZ_ITERS=1` replays exactly
//! that case. The failure message also prints the mutations and the input as
//! hex. For example:
//!
//! ```text
//! DEDUP_FUZZ_SEED=7 DEDUP_FUZZ_ITERS=20000 \
//!     cargo test -p krabka-broker --test dedup_fuzz -- --nocapture
//! ```
//!
//! Cases marked `product bug` fail on the current broker and stay ignored
//! until the bug they name is fixed.

mod support;

#[path = "dedup_fuzz/child.rs"]
mod child;
#[path = "dedup_fuzz/driver.rs"]
mod driver;
#[path = "dedup_fuzz/mutate.rs"]
mod mutate;
#[path = "dedup_fuzz/oracle.rs"]
mod oracle;
#[path = "dedup_fuzz/regressions.rs"]
mod regressions;
#[path = "dedup_fuzz/rng.rs"]
mod rng;
#[path = "dedup_fuzz/wire.rs"]
mod wire;

use {
    assert2::assert,
    driver::Run,
    mutate::{Bombs, Mode, generate},
    oracle::parse,
    rng::Rng,
};

/// The seed a run uses unless `DEDUP_FUZZ_SEED` names another.
const DEFAULT_SEED: u64 = 0x6B72_6162_6B61;

/// The iterations a run makes unless `DEDUP_FUZZ_ITERS` says otherwise:
/// under a minute on a loaded four-core machine. Each iteration is two to
/// five requests.
const DEFAULT_ITERATIONS: u64 = 1_000;

/// The address space the child may use: about four times the 1 GiB a run
/// reaches, and far below the 64 GiB that one allocation sized by a lying
/// `i32::MAX` count of 32-byte entries asks for.
const ADDRESS_SPACE: u64 = 4 << 30;

/// The peak resident size the child may reach. A run reaches about 700 MiB:
/// the broker, eight topics, and glibc keeping the freed 16 MiB buffers that
/// decompression bombs filled.
const PEAK_RSS_KIB: u64 = 1536 << 10;

/// How far one request may raise the peak resident size. A record bomb of
/// 100,000 records, a few KiB on the wire, raises it by about 90 MiB.
const PEAK_STEP_KIB: u64 = 384 << 10;

/// A `u64` from the environment, in decimal or `0x` hex.
fn env_u64(name: &str, default: u64) -> u64 {
    let Ok(text) = std::env::var(name) else {
        return default;
    };
    let parsed = match text.strip_prefix("0x") {
        Some(hex) => u64::from_str_radix(hex, 16),
        None => text.parse(),
    };
    parsed.unwrap_or_else(|_| panic!("{name}={text} is not a number"))
}

fn run_from_env() -> Run {
    Run {
        seed: env_u64("DEDUP_FUZZ_SEED", DEFAULT_SEED),
        start: env_u64("DEDUP_FUZZ_START", 0),
        iterations: env_u64("DEDUP_FUZZ_ITERS", DEFAULT_ITERATIONS),
    }
}

/// The last `limit` bytes of `text`, on a character boundary.
fn tail(text: &str, limit: usize) -> &str {
    let mut from = text.len().saturating_sub(limit);
    while !text.is_char_boundary(from) {
        from += 1;
    }
    &text[from..]
}

#[test]
fn malformed_produces_leave_the_broker_healthy_and_dedup_consistent() {
    let run = run_from_env();
    eprintln!(
        "dedup_fuzz: DEDUP_FUZZ_SEED={:#x} DEDUP_FUZZ_START={} DEDUP_FUZZ_ITERS={}",
        run.seed, run.start, run.iterations
    );
    let outcome = child::run("fuzz_child", ADDRESS_SPACE);
    eprintln!("{}", tail(&outcome.stderr, 64 << 10));
    eprintln!("dedup_fuzz child: {} {:?}", outcome.status, outcome.numbers);
    assert!(
        outcome.success,
        "the fuzz child exited {}; its output is above",
        outcome.status
    );
    let peak = outcome.numbers.get("peak_rss_kib").copied();
    assert!(
        peak.is_some_and(|kib| kib < PEAK_RSS_KIB),
        "peak resident {peak:?} KiB, limit {PEAK_RSS_KIB} KiB: {:?}",
        outcome.numbers
    );
    let step = outcome.numbers.get("largest_peak_step_kib").copied();
    assert!(
        step.is_some_and(|kib| kib < PEAK_STEP_KIB),
        "one request raised the peak resident size by {step:?} KiB, limit {PEAK_STEP_KIB} KiB"
    );
}

/// The child side of the test above: the fuzz loop itself. It does nothing
/// unless [`child::CHILD_ENV`] is set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fuzz_child() {
    if !child::is_child() {
        return;
    }
    driver::watch_panics();
    let summary = driver::fuzz(run_from_env()).await;
    child::report("peak_rss_kib", child::proc_status_kib("VmHWM"));
    child::report("peak_vm_kib", child::proc_status_kib("VmPeak"));
    child::report("rss_anon_kib", child::proc_status_kib("RssAnon"));
    child::report("rss_file_kib", child::proc_status_kib("RssFile"));
    child::report("accepted", summary.accepted);
    child::report("resends", summary.resends);
    child::report("unknown_codec_accepted", summary.unknown_codec_accepted);
    child::report("largest_peak_step_kib", summary.largest_peak_step.0);
}

/// The generator's untampered batches are valid: the protocol crate reads
/// each one back as the records it was built from. Without this, a mutation
/// that "lies" could be lying about a batch that was never right.
#[test]
fn untampered_batches_parse_back_as_built() {
    for iteration in 0..300 {
        let mut rng = Rng::for_iteration(DEFAULT_SEED, iteration);
        let mode = if iteration % 2 == 0 {
            Mode::Key
        } else {
            Mode::Header
        };
        let (case, records) = mutate::valid_case(&mut rng, mode, 1_700_000_000_000);
        let parsed = parse(&case.bytes, mode);
        assert!(
            parsed.is_ok_and(|accepted| accepted.records.len() == records),
            "iteration {iteration}: {}",
            case.describe()
        );
    }
}

/// The same seed and iteration give the same bytes.
#[test]
fn a_seed_and_iteration_replay_the_same_case() {
    let bombs = Bombs::shared();
    for iteration in [0, 1, 99] {
        let first = generate(
            &mut Rng::for_iteration(7, iteration),
            Mode::Header,
            1_700_000_000_000,
            bombs,
        );
        let again = generate(
            &mut Rng::for_iteration(7, iteration),
            Mode::Header,
            1_700_000_000_000,
            bombs,
        );
        assert!(first.bytes == again.bytes);
        assert!(first.notes == again.notes);
    }
}
