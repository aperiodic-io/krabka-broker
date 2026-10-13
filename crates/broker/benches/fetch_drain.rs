//! `CodSpeed` microbenchmarks for the broker's zero-copy fetch drain.
//!
//! A fetch response's records region reaches the socket one of two ways. The
//! vectored path (Increment C) `pread`s the region out of the segment file
//! into a buffer and writes the buffer; the sendfile path (Increments D + E)
//! hands the region's file descriptor to the kernel, which moves the pages to
//! the NIC without a userspace copy. Both produce the same wire bytes, so
//! nothing but a measurement says which is faster — and `sendfile(2)` is not
//! free: below some response size its syscall overhead costs more than the
//! copy it avoids.
//!
//! `sendfile_min` is the broker's guess at where that crossover is, and every
//! fetch is routed by it. This suite measures it. Each size drains a whole
//! response — the frame length, a response envelope, and one records region —
//! over a real loopback TCP socket with a reader draining the other end, once
//! through each path.
//!
//! Both cases pay their real per-response cost inside the timed region: the
//! vectored case runs `resolve_records_inline`, which is the `pread` the
//! broker's read path does for a run below the threshold, and the sendfile
//! case runs `resolve_records_sendfile`, which only clones the region
//! descriptor. The segment file is freshly written and so is warm in the page
//! cache, which is the state a tail read finds it in.
//!
//! What the sweep leaves out is the read planning ahead of the drain: the
//! batch-header walk (`Log::read_raw_desc`) that builds the file regions. That
//! omission does not tilt the table toward sendfile, because the walk is not
//! the sendfile path's alone. `handlers::fetch::read` runs it on every
//! sendfile-capable connection *before* it consults `sendfile_min`, so a
//! response the threshold rejects pays the walk and then pays `read_raw` on
//! top of it. Excluding the walk from both columns therefore understates the
//! vectored column's real cost, not sendfile's.
//!
//! `sendfile_min` still sits at the smallest swept size rather than below it:
//! the sweep says nothing about a response under 4 KiB, and a threshold is
//! only as good as the measurement under it.
//!
//! `bench_crossover` prints ns per response for both paths at every size, the
//! sendfile-over-vectored ratio, and the smallest swept size at which sendfile
//! wins — the number `sendfile_min`'s default has to match.

use criterion::{criterion_group, criterion_main};

/// Every response size the sweep covers, and the label each is reported under.
#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
const SIZES: [(&str, usize); 5] = [
    ("4KiB", 4 * 1024),
    ("16KiB", 16 * 1024),
    ("32KiB", 32 * 1024),
    ("64KiB", 64 * 1024),
    ("256KiB", 256 * 1024),
];

/// The sweep, on the platforms that have a file-to-socket `sendfile(2)`:
/// Linux, the Apple targets, and FreeBSD/DragonFly.
#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
mod sweep {
    use {
        super::SIZES,
        assert2::assert,
        bytes::{BufMut, Bytes, BytesMut},
        criterion::{Criterion, Throughput},
        krabka_broker::{
            fetch_drain::{
                WriteOp, resolve_records_inline, resolve_records_sendfile, write_fetch_plan,
            },
            metrics::{BrokerMetrics, FetchDrainPath, FetchDrainPathLabel},
        },
        krabka_protocol::records::{FileRegion, RecordsPayload},
        std::{
            io::Write as _,
            sync::Arc,
            time::{Duration, Instant},
        },
        tokio::{
            io::AsyncReadExt as _,
            net::{TcpListener, TcpStream},
            runtime::Runtime,
        },
    };

    /// Untimed drains run before the measured ones, over the same socket.
    ///
    /// The first write on a fresh connection pays for the socket's send-buffer
    /// growth and for the reader task's first scheduling. Criterion amortizes
    /// that over a large `iters`, but [`bench_crossover`]'s short runs do not.
    const WARMUP: u64 = 8;

    /// Response bytes the writer emits ahead of the records: the correlation
    /// header and the partition metadata.
    ///
    /// The exact size does not matter to the comparison — it is identical on
    /// both paths — but its presence does, because a real drain always writes
    /// an inline op before it reaches the records.
    const ENVELOPE: usize = 64;

    /// Send and receive buffers for both ends of the loopback socket.
    ///
    /// It is the broker's own `socket_send_buffer` default, and it is larger
    /// than the largest response in the sweep, so no case is measured against
    /// a different amount of flow-control back-pressure than another.
    const SOCKET_BUFFER: usize = 1024 * 1024;

    /// The two ways a records region reaches the socket.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(crate) enum Case {
        /// `pread` the region into a buffer, then write the buffer.
        Vectored,
        /// Hand the region's descriptor to the kernel.
        Sendfile,
    }

    impl Case {
        /// The label its drain must be counted under. A case that took the
        /// other path would compare two measurements of the same code.
        fn expected(self) -> FetchDrainPath {
            match self {
                Self::Vectored => FetchDrainPath::Vectored,
                Self::Sendfile => FetchDrainPath::Sendfile,
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::Vectored => "vectored",
                Self::Sendfile => "sendfile",
            }
        }
    }

    /// One response's worth of on-disk records, plus the envelope bytes that
    /// precede them on the wire.
    struct Response {
        /// Kept so the file outlives the regions that point into it.
        _file: tempfile::NamedTempFile,
        payload: RecordsPayload,
        /// The frame length and a response header, as the writer emits them.
        header: Bytes,
        records_bytes: usize,
    }

    impl Response {
        /// A response whose records region is `records_bytes` of segment file.
        fn new(records_bytes: usize) -> Self {
            // A repeating non-uniform pattern rather than zeros: an all-zero
            // file is not what a segment looks like, and a filesystem is free
            // to store it as a hole.
            let records: Vec<u8> = (0..records_bytes)
                .map(|i| u8::try_from(i % 251).expect("i % 251 fits in a byte"))
                .collect();
            let mut file = tempfile::NamedTempFile::new().expect("temp file");
            file.write_all(&records).expect("write the segment bytes");
            file.flush().expect("flush the segment bytes");
            let handle = Arc::new(file.reopen().expect("reopen the segment file"));

            let mut header = BytesMut::with_capacity(4 + ENVELOPE);
            header.put_u32(u32::try_from(ENVELOPE + records_bytes).expect("frame length fits"));
            header.put_bytes(0, ENVELOPE);

            Self {
                _file: file,
                payload: RecordsPayload::FileRegions(vec![FileRegion {
                    file: handle,
                    offset: 0,
                    len: records_bytes,
                }]),
                header: header.freeze(),
                records_bytes,
            }
        }

        /// The write plan for one drain of this response, in `case`.
        ///
        /// Building it is inside the timed region because that is where the
        /// paths differ: the vectored resolver reads every records byte into a
        /// fresh buffer, and the sendfile resolver clones a descriptor.
        fn plan(&self, case: Case) -> Vec<WriteOp> {
            let mut ops = vec![WriteOp::Inline(self.header.clone())];
            let records = match case {
                Case::Vectored => resolve_records_inline(&self.payload),
                Case::Sendfile => resolve_records_sendfile(&self.payload),
            }
            .expect("the sweep's payloads all resolve");
            ops.extend(records);
            ops
        }
    }

    /// Set both buffers on one end of the loopback pair, so a large response
    /// does not measure the reader's scheduling instead of the write.
    fn size_buffers(stream: &TcpStream) {
        let socket = socket2::SockRef::from(stream);
        let _ = socket.set_send_buffer_size(SOCKET_BUFFER);
        let _ = socket.set_recv_buffer_size(SOCKET_BUFFER);
    }

    /// The count `path` carries on the drain counter.
    fn drained(metrics: &BrokerMetrics, path: FetchDrainPath) -> u64 {
        metrics
            .fetch_response_drain
            .get_or_create(&FetchDrainPathLabel { path })
            .get()
    }

    /// Drain `response` `iters` times in `case`, returning the time the drains
    /// took.
    ///
    /// The socket pair, the reader task and the runtime are built once per
    /// call and are outside the timer. The path assertion afterwards is what
    /// keeps the comparison meaningful: were a change to route the sendfile
    /// case onto the copy path, the two columns would converge and the table
    /// would report `sendfile(2)` as costing nothing. This fails the run
    /// instead.
    fn timed_drains(response: &Response, case: Case, iters: u64) -> Duration {
        let runtime = Runtime::new().expect("build a bench runtime");
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("local addr");
            let reader = tokio::spawn(async move {
                let mut client = TcpStream::connect(addr).await.expect("connect");
                size_buffers(&client);
                let mut sink = vec![0u8; SOCKET_BUFFER];
                while client.read(&mut sink).await.unwrap_or(0) > 0 {}
            });
            let (mut server, _) = listener.accept().await.expect("accept");
            size_buffers(&server);
            let metrics = BrokerMetrics::new();

            for _ in 0..WARMUP {
                write_fetch_plan(&mut server, response.plan(case), &metrics)
                    .await
                    .expect("drain the warmup response");
            }
            let start = Instant::now();
            for _ in 0..iters {
                write_fetch_plan(&mut server, response.plan(case), &metrics)
                    .await
                    .expect("drain the response");
            }
            let elapsed = start.elapsed();

            drop(server); // EOF for the reader
            reader.await.expect("the reader task drains to EOF");

            let counts = FetchDrainPath::ALL.map(|p| drained(&metrics, p));
            let mut expected = [0u64; 3];
            let index = FetchDrainPath::ALL
                .iter()
                .position(|p| *p == case.expected())
                .expect("every case's path is one of the three");
            expected[index] = iters + WARMUP;
            assert!(
                counts == expected,
                "the {} case must drain every response on its own path",
                case.name()
            );
            elapsed
        })
    }

    /// The criterion sweep: every size, on both paths.
    pub(crate) fn bench_fetch_drain(c: &mut Criterion) {
        let mut group = c.benchmark_group("broker/fetch_drain");

        for (label, records_bytes) in SIZES {
            let response = Response::new(records_bytes);
            group.throughput(Throughput::Bytes(response.records_bytes as u64));
            for case in [Case::Vectored, Case::Sendfile] {
                group.bench_function(format!("{label}/{}", case.name()), |b| {
                    b.iter_custom(|iters| timed_drains(&response, case, iters));
                });
            }
        }

        group.finish();
    }

    /// Print both paths' cost at every size, and the crossover between them.
    ///
    /// Criterion compares a benchmark against its own previous run, not
    /// against a sibling, so the number this suite exists for — the response
    /// size at which `sendfile(2)` starts beating the copy, which is what
    /// `sendfile_min` has to be set to — has to be computed here.
    pub(crate) fn bench_crossover(_c: &mut Criterion) {
        /// Drains per measurement run.
        const SAMPLES: u32 = 200;
        /// Measurement runs per case, of which the fastest is reported.
        ///
        /// A single run is noise-dominated on a machine that is doing anything
        /// else. Interference only ever adds time, so the minimum over a few
        /// runs is the estimator it cannot pull the wrong way.
        const RUNS: u32 = 5;

        println!(
            "\nbroker/fetch_drain — nanoseconds per response (fastest of {RUNS} runs), and where sendfile starts winning"
        );
        println!(
            "{:<10} {:>12} {:>12} {:>20}",
            "size", "vectored", "sendfile", "sendfile/vectored"
        );
        let mut crossover: Option<&str> = None;
        for (label, records_bytes) in SIZES {
            let response = Response::new(records_bytes);
            let cost = |case| {
                (0..RUNS)
                    .map(|_| {
                        timed_drains(&response, case, u64::from(SAMPLES)).as_secs_f64() * 1.0e9
                            / f64::from(SAMPLES)
                    })
                    .fold(f64::INFINITY, f64::min)
            };
            let vectored = cost(Case::Vectored);
            let sendfile = cost(Case::Sendfile);
            if sendfile < vectored && crossover.is_none() {
                crossover = Some(label);
            }
            println!(
                "{label:<10} {vectored:>12.0} {sendfile:>12.0} {:>19.2}x",
                sendfile / vectored
            );
        }
        match crossover {
            Some(size) => println!(
                "\nsendfile first wins at {size}: that is the response size sendfile_min should carry."
            ),
            None => println!(
                "\nsendfile won at no swept size: sendfile_min should sit above the largest of them."
            ),
        }
        println!();
    }
}

/// The sweep has nothing to measure where the platform has no file-to-socket
/// `sendfile(2)`: every fetch drains through the vectored path, and
/// `sendfile_min` routes nothing.
#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
)))]
mod sweep {
    use criterion::Criterion;

    pub(crate) fn bench_fetch_drain(_c: &mut Criterion) {
        println!(
            "broker/fetch_drain: this target has no file→socket sendfile, so every fetch drains \
             through the vectored path and there is no crossover to sweep"
        );
    }

    pub(crate) fn bench_crossover(_c: &mut Criterion) {}
}

criterion_group!(benches, sweep::bench_fetch_drain, sweep::bench_crossover);
criterion_main!(benches);
