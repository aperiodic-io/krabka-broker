# krabka-broker

The [krabka](https://github.com/krabka-io) broker: an Apache Kafka-compatible
broker with a KRaft consensus engine, tiered storage, and its supporting
subsystems.

It builds on two sibling repositories and nothing else in the stack —
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for the wire
layer and [`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs)
for the Kafka client the broker and its tests use.

## AI co-development and verification

A human maintainer and AI coding agents codeveloped krabka-broker. AI output is
a proposed change, not evidence that the code is correct. The repository checks
changes against executable tests, proof obligations, model checks, and observed
Kafka behavior.

The checks use several independent methods:

* Unit, property, integration, and crash-recovery tests check behavior at
  different boundaries.
* Mutation testing changes production code and checks whether the test suite
  detects each change.
* Creusot proves contracts for small executable kernels. Stateright enumerates
  all reachable states within each model's stated bounds. Pull request CI reruns
  every registered proof and model check.
* Differential and JVM acceptance suites start digest-pinned Apache Kafka and
  Confluent Platform brokers. They compare krabka with live reference processes
  and run real Kafka clients and admin tools against krabka.

Stateright covers event orderings that example-based tests can miss. It explores
every reachable state within explicit bounds for crashes, retries, failover,
replay, and concurrent operations. Some models drive production decision
functions directly. Other models compose those functions with a small
environment.

Each entry in the [Stateright inventory](docs/verification.md#stateright-model-check-tier)
records its bounds, properties, reached state and transition counts, truncation
guards, and modeled assumptions. Pull request CI runs every model-bearing target
even when impact analysis would skip it.

A proved kernel can still have a bad adapter. Mutation testing can expose weak
assertions, while live Kafka comparisons can reveal undocumented or
version-specific behavior.

This process is not a whole-system formal proof. The
[verification catalog](docs/verification.md) lists each proof, each model's
bounds, caller preconditions, and the I/O or orchestration outside its scope.

## Crates

| Crate | What it is |
| --- | --- |
| `krabka-broker` | The broker: request handlers, coordinators, replication, quotas, the server binary. |
| `krabka-raft` | KRaft consensus engine. |
| `krabka-kraft-core` | Shared KRaft types and the quorum state machine. |
| `krabka-log` | The on-disk log: segments, indexes, compaction, retention. |
| `krabka-records-legacy` | v0/v1 message-set codecs for down-conversion. |
| `krabka-remote-storage` | Tiered-storage backends and the remote log reader. |
| `krabka-remote-storage-topic` | The `__remote_log_metadata` topic-backed RLMM. |
| `krabka-object-store` | Object-store abstraction shared by the tiered paths. |
| `krabka-authz` | Authorizer traits, ACL evaluation, and the OPA client. |
| `krabka-audit` | Audit event model, OCSF serialization, and the `krabka-audit verify` CLI. |
| `krabka-barrier` | Operator CLI for barrier groups: define a group, trigger and list cuts, verify a cut against the log. |
| `krabka-guard` | Operator CLI for topic write freezes and break-glass proposals: freeze, thaw, propose, approve. |
| `krabka-backup` | Operator CLI for restore inputs: capture the RLMM and metadata snapshots and group offsets, verify a capture, put offsets back. |
| `krabka-format` | Formats a fresh log directory: Kafka's `meta.properties`, bootstrap records, the singleton `VotersRecord`. |
| `krabka-restore` | Offline point-in-time restore of a bootable log directory from a KIP-405 archive. |
| `krabka-throttle` | Quota token buckets (Creusot-verified). |
| `krabka-dedup` | The windowed first-wins index behind produce-time record deduplication (KFC-10). |
| `krabka-verified` | Formally verified pure kernels shared by consensus and log. |
| `krabka-telemetry` | OTLP pipeline, metrics registry, and the debug/pprof routes. |
| `krabka-logfmt` | logfmt encoder for structured logs. |
| `krabka-parse-benches` | Parses Criterion benchmark output into structured JSON summaries. |
| `krabka-docgen` | Renders the broker reference pages from the broker's own in-process data. |
| `krabka-macros` | Derive macros for the broker: `RegisterMetrics` builds and registers `BrokerMetrics` from its field attributes. |
| `krabka-bench-driver` | Load driver and report aggregator for the cluster benchmark harness under `bench/`. |
| `krabka-log-iobench` | Bench-only crate: measures the log read path against mmap. |

Rustdoc for every crate is published at
[krabka-io.github.io/krabka-broker](https://krabka-io.github.io/krabka-broker/)
on each push to `main`.

- [KIP compatibility matrix](docs/KIP_MATRIX.md): generated per-KIP status,
  owner, tests, Kafka image and client evidence. Regenerate with
  `aspect generate-kip-matrix`; CI diffs it.
- [Persisted formats](docs/persisted_formats.md): every on-disk and
  object-store format, its version marker, and the 1.x compatibility contract.

## Configuration

`krabka-broker --config-file PATH` reads one TOML document. The
[configuration reference](docs/config-reference.md) lists every key with its
type, default, units, and description. It is generated from the schema that
`krabka-broker --print-config-schema` prints, and CI regenerates and diffs it.
Two annotated examples, a [single node](docs/examples/broker-single-node.toml)
and a [three-node quorum](docs/examples/broker-three-node-quorum.toml), are
parsed and applied by the broker test suite. The
[krabka-docgen contract](docs/docgen-contract.md) records what
[`crates/docgen`](crates/docgen/README.md) reads from this crate, and where CI
renders it.

## Design documents

Each subsystem with invariants that are hard to reconstruct from the code
carries a design document inside its crate, in the shape the
[design-doc style guide](docs/style_guides/design_doc_style_guide.md) defines:

- [Diskless WAL](crates/broker/docs/diskless-wal-design.md)
- [KRaft consensus and the controller](crates/raft/docs/design.md)
- [Replication and ISR](crates/broker/docs/replication-isr-design.md)
- [Log and segment format](crates/log/docs/design.md)
- [Transaction coordinator](crates/broker/docs/transaction-coordinator-design.md)

## Build

Bazel is the build and test path. Cargo stays the dependency source of truth:
[`rules_rs`](https://github.com/hermeticbuild/rules_rs) reads the same
`Cargo.toml` and `Cargo.lock` that Cargo does, so there is no second dependency
set to keep in sync.

```
bazel test //...
```

Run the broker:

```
bazel run //:broker_bin -- --help
```

Build and load a native Linux ARM64 container image (for example, on an Apple
Silicon Mac running Docker Desktop):

```
bazel run -c opt --platforms=//:linux_arm64 //packaging:image_arm64_load
```

The loaded image is tagged `krabka-io/krabka-broker:dev-arm64`.
Release tags and `latest` on GHCR contain both Linux AMD64 and ARM64 images.

`cargo` works the same way it always did:

```
cargo nextest run --workspace
```

Both run the same workspace test set, and CI runs both -- Bazel in the `ci` and
`coverage` jobs, Cargo in the `cargo` job. Bazel additionally runs the rustdoc
examples, which `cargo nextest` cannot; `cargo test --workspace --doc` covers
those.

`cargo bench -p krabka-broker` (and `-p krabka-log` for the storage suite) measures
performance on your own machine. Bazel's `crate_bench` macro also compiles and
lints these suites; `bazel run //crates/log:log_bench` runs the storage suite.
The other way is to read one somebody else already
took: the `bench` job of the `ci` workflow runs every criterion suite on the
nightly schedule. It alternates four runs of a reference commit and the
candidate on one host, then publishes raw output, exact commits, host resources
and a variance-calibrated verdict with a 10% materiality floor in the
`benchmark-regression-evidence` artifact. A performance figure quoted in a
comment names that run.

Those are microbenchmarks. [`bench/`](bench/README.md) is the other kind: a
cluster harness that runs `krabka-bench-driver` as a Kubernetes Job against a
Krabka cluster and a Strimzi cluster in turn, and aggregates the per-run JSON
into one report. It needs a live cluster, so no CI job runs it.

### Everything CI does, locally

The [Aspect CLI](https://github.com/aspect-build/aspect-cli) narrows each task to
what a change actually touched, which for a repository this size is the
difference between a minute and most of an hour. Every one has a plain-Bazel
equivalent, so the CLI is a convenience rather than a requirement:

| | Aspect CLI | Plain Bazel |
| --- | --- | --- |
| Build | `aspect build //...` | `bazel build //...` |
| Test\* | `aspect test //...` | `bazel test //...` |
| Lint | `aspect lint` | `bazel build --config=lint //...` |
| Format | `aspect format` | `bazel run //tools/format` |
| Coverage | `aspect test --coverage` | `bazel coverage //crates/...` |
| Docs | — | `bazel build //crates/audit:audit_doc` |
| Delivery | `aspect delivery` | `bazel run -c opt //packaging:push -- --tag dev` |

\* The `ci` job runs this set in two steps. `daemon-free tests` runs
`bazel test --test_tag_filters=-docker,-timing-sensitive //crates/...`.
`test` runs the timing-sensitive suites, plus
`//packaging:image_binaries_test`, which sits outside `//crates`, in an
invocation of their own. Together they are what `bazel test //...` runs
locally. `coverage` runs the first set again under instrumentation, for the
report.

Formatting and linting are Bazel targets rather than a separate `cargo fmt`
pass, so they see exactly the files and crates the build sees. A file in a
target cannot drift unnoticed, and clippy resolves the same features the build
resolves. The aspect only sees Bazel targets, though, so CI also runs
`cargo clippy --workspace --all-targets -- -D warnings` in its `cargo` job: the
benches under `crates/broker/benches` and `crates/log/benches` also have Bazel
targets, but Cargo checks their Cargo dependency graph, `aspect lint` narrows
to what a change touched, and
`krabka-protocol`'s build script only ever runs under Cargo. Its `wasm` job
runs the same Clippy for `wasm32-wasip1` over the broker library and every
workspace crate in its graph.

Two details worth knowing:

* **rustfmt runs on a pinned nightly.** `rustfmt.toml` uses
  `format_code_in_doc_comments`, `group_imports` and `imports_granularity`, all
  still nightly-gated; stable rustfmt warns and silently skips them. The nightly
  is pinned in `MODULE.bazel`, so formatting is reproducible rather than a
  function of whichever nightly is installed.
* **`rustfmt.toml` states its edition.** `cargo fmt` passes `--edition` from
  `Cargo.toml`; rustfmt invoked directly defaults to 2015 and sorts `use` lists
  differently. Stating it makes formatting a property of the repository rather
  than of how rustfmt was launched.

## Container suites

The JVM acceptance, tiered-storage and Kerberos suites drive real Kafka
containers. They are `#[ignore]`d under Cargo, and under Bazel they are separate
targets tagged `docker`, which the default `bazel test` filters out:

```
bazel test --config=docker //crates/...
```

The images are **pinned by digest in `MODULE.bazel` and fetched by Bazel**, not
pulled by testcontainers mid-test. `//bazel/images` turns each into a
`docker load`-able tarball, and `//bazel:docker_test.sh` loads them before the
suite runs. So the bytes a suite runs against are the bytes the build pinned, the
network is out of the test itself, and the repository cache carries the images
between CI runs. The JDK that `jvm_tiered_storage`'s probe needs is downloaded by
Bazel too, rather than resolved from the machine — under Cargo that suite simply
fails on a host with no JDK.

The Docker daemon is the one thing Bazel cannot own, so it is the one thing left
undeclared: those targets are tagged `no-sandbox` for the socket and `external`
so a pass is never cached against inputs that do not describe the daemon's state.

They run nightly rather than per-PR — the images alone are a few gigabytes.

`describe_groups_jvm` is *not* among them. It records a real-Kafka fixture into
`tests/fixtures/` rather than reading one, which makes it a recording tool: a
Bazel test cannot write to the source tree, and should not want to. It stays
`#[ignore]`d under Cargo, which is where fixtures get regenerated.

### Ports and concurrency

These suites used to hard-code `9092`/`9093`, so two of them could not run at
once: the second to start lost the bind and reported `Address already in use` as
a test failure. Each test process allocates its own ports now, so any two suites
can run side by side.

The tests reach the broker over loopback; only the containers use the advertised
`host.docker.internal` name, which they resolve through
`--add-host=host.docker.internal:host-gateway`. CI additionally maps that name to
`127.0.0.1` in `/etc/hosts`, because a few host-side Rust clients bootstrap
through the advertised name rather than over loopback.

`jvm_acceptance` was the long pole at 552s: 48 tests in one binary, serialised
by that process's single port allocation. It is now eight `jvm_acceptance_*`
targets grouped by the cluster each one boots -- CLI round-trips, legacy 0.10
clients, durability, SASL, TLS, reassignment, quotas and tiered storage -- over
one harness in `tests/jvm_acceptance/mod.rs`. Each is its own process, so each
gets its own ports for free.

Splitting alone made CI *slower*, which is worth recording because the reasoning
looks sound until you measure it. The critical path did fall, 552s to 150s, but
the job went 852s to 1059s: these suites saturate a runner, so Bazel got about
1.2x concurrency both before and after, while eight binaries paid the image-load
and broker-boot cost eight times instead of once. Target granularity was never
the limit -- the box was.

The shard is therefore across runners, not within one. `docker-select` computes
the suite set once and emits a matrix, and each suite gets its own job, so the
wall clock is the slowest suite rather than the sum: 1179s to 355s end to end,
for about 2.7x the runner-minutes. Within a single binary the tests still share
that process's ports and run one at a time; per-test ports would lift that too.

### Docker Desktop hosts

The mixed-cluster suites advertise the bridge gateway that `docker network
inspect bridge` reports, so the host-run broker and the containers share one
address. That holds on native Linux Docker, where the gateway *is* the host.
Under Docker Desktop the containers sit inside a VM: `172.17.0.1` is the VM's
bridge, a host-bound port is unreachable at it, and only `host.docker.internal`
-- a different address again -- resolves. So `jvm_acceptance_*`, `jvm_features`
and `jvm_kip320_divergence` fail on such a host, with the JVM CLI reporting
`UnsupportedVersionException: The broker does not support CREATE_TOPICS`, which
is what it says when it cannot reach a broker at all.

To tell that apart from a real break: bind a host port, then from a container run
`nc -z 172.17.0.1 <port>`. Unreachable means Docker Desktop semantics. CI runs
native Linux Docker, so treat a local failure in these suites as environmental
until it reproduces there.

## Delivery

```
bazel run -c opt //packaging:image_load    # load into the local daemon
bazel run -c opt //packaging:push -- --tag dev  # push to ghcr.io
```

apko builds the broker base from locked Wolfi packages, including glibc and CA
certificates. rules_img adds the Bazel-built binary, loads it locally, and
pushes it. The image runs as `nonroot` (65532). The build uses no Dockerfile or
host package manager. `aspect delivery` skips the push when the image output did
not change.

A second layer carries the operator tools — `krabka-format`, `krabka-audit`,
`krabka-barrier`, `krabka-guard`, `krabka-worm-verify`, `krabka-backup` and
`krabka-restore` —
beside the broker under `/usr/bin`. The base has no shell, so a tool that is not
in the image cannot be run in a container at all, and `krabka-format` has to run
against the log directory before the broker will boot:

```
mkdir -p krabka-data && chown 65532:65532 krabka-data
docker run --rm -v "${PWD}/krabka-data:/var/lib/krabka" \
    --entrypoint /usr/bin/krabka-format krabka-io/krabka-broker:dev \
    --log-dir /var/lib/krabka --standalone --node-id 1 \
    --controller-listener 127.0.0.1:9093
```

The `chown` is not incidental. Both the formatter and the broker run as
`nonroot`. The apko base does create `/var/lib/krabka` owned by 65532, but a
bind mount replaces that directory with the host's, and a freshly created host
directory belongs to root — so the format step fails with `Permission denied`
before it writes `meta.properties`.

`//packaging:image_binaries_test` asserts what those layers carry and that each
binary answers `--help`; it needs no daemon and runs in `bazel test //...`.
`bazel test --config=docker //packaging:image_docker_test` repeats the check
against a loaded image through Docker.

## Kubernetes

`packaging/k8s/` holds a reference deployment of that image: a three-node
StatefulSet, a headless Service plus a bootstrap Service, and a
PodDisruptionBudget. They are a starting point to read and adapt, not a chart.
Set the image tag, the storage class and size, and above all the two identities
the manifests ship placeholders for -- `KRABKA_CLUSTER_ID` and the seed
directory ids in `KRABKA_INITIAL_CONTROLLERS` -- before you apply them. CI
applies the same manifests to a three-worker kind cluster and exercises a
rolling restart.

```
kubectl apply -f packaging/k8s/
```

Three parts of them carry the design, not only the wiring:

* **The format step is an init container on the same image.** The tools ride
  beside the broker, so there is no second image to keep in step with it and no
  volume to share between two pods. `krabka-format --ignore-formatted`, the same
  flag Kafka's `kafka-storage.sh format` carries, makes the step a no-op on the
  second and every later boot of a pod that keeps its volume. Without that flag
  the init container fails every restart after the first, because the image has
  no shell to test the directory with first.
* **The two probes answer different questions.** `/healthz` says the process is
  up. It stays green through log-dir recovery and through metadata catch-up, so
  the kubelet cannot kill a node partway through a recovery. `/readyz` says the
  node can serve a client: log dirs recovered, listeners bound, and its
  `__cluster_metadata` offset within `--readiness-max-metadata-lag` records of
  the quorum's committed offset. A 503 names the condition that failed, so
  `kubectl describe pod` reports which one. The rolling update waits on
  readiness, and that is what stops a restart from running ahead of the quorum.
* **`minAvailable: 2` is the majority of three.** A drain that evicted two of
  these pods costs the metadata quorum its majority. Scale the StatefulSet and
  the budget has to move with it. The budget also keeps the default
  `unhealthyPodEvictionPolicy`: a krabka pod that fails `/readyz` is usually
  still a voter, because the controller joins the quorum before log-dir
  recovery runs, so `AlwaysAllow` would let a drain take a second voter out
  while the budget was already unmet.

## Operations

The operator guide lives in [docs/operations/](docs/operations/README.md):
[deploy and rolling upgrade](docs/operations/deploy.md),
[capacity](docs/operations/capacity.md), the
[metrics contract](docs/operations/metrics.md) with the JMX names each series
replaces, a reference Grafana dashboard, Prometheus alert rules, and one
runbook per alert. `aspect check-metrics-contract` and the
`metrics_contract` suite fail the build when the dashboard or the rules name
a series the registry does not export.

## Mutation testing

Mutation sweeps run through
[`rules_rs_mutants`](https://github.com/robot-head/rules_rs_mutants), which lets
`cargo-mutants` enumerate mutants and lets Bazel build and test them:

```
bazel test //crates/raft:raft_mutants
```

They are tagged `manual`, so `bazel test //...` skips them. Run sweeps locally
with the commands in [`CONTRIBUTING.md`](CONTRIBUTING.md). The scheduled
`mutants` workflow has been removed. A non-excluded survivor fails its shard.
[`docs/mutants-baseline.md`](docs/mutants-baseline.md) keeps the historical
results from scheduled runs. Two things to know about the results:

* The target runs unit tests and ordinary `tests/*.rs` integration tests. It
  excludes manual and Docker-backed suites because the sweep reruns the tests
  for each mutant.
* The `cargo-mutants` version is pinned by `tools/mutants/Cargo.lock`, not by
  whatever is on `PATH`.

### Skipping a mutant

A skip removes code from the tier that measures the tests, so it needs a reason
that survives review. Two reasons are accepted for the function-level
`#[cfg_attr(test, mutants::skip)]` attribute:

* **An I/O-only wrapper with no in-process signal.** The function dials a
  socket, fsyncs a file, or spawns a task, and nothing it does is observable
  from inside the test process — so every mutant of it survives no matter how
  good the tests are.
* **Orchestration whose every branch is a call into a separately
  mutation-tested kernel.** The decisions live elsewhere and are measured
  there; what is left is the loop or the match that dispatches to them.

Whichever it is, write it down. A `// cargo-mutants: <reason>` line goes
directly above the attribute, naming the reason as it applies to *this*
function — only other attributes, doc comments and blank lines may sit between
the two. `aspect check-mutants-skip` fails on a skip with no such line, and CI
runs it in the `checks` job.

**"Integration-tested" is not an accepted reason.** The sweep runs unit tests
and ordinary `tests/*.rs` only; it excludes the manual and Docker-backed
suites. A skip justified by integration coverage therefore removes exactly the
code the mutation tier never sees, which is the opposite of what it should do.
Either the behavior can be asserted in process — write that test and drop the
skip — or the function is one of the two cases above and the reason should say
so.

Prefer a [`.cargo/mutants.toml`](.cargo/mutants.toml) `exclude_re` entry when
what survives is an **equivalent mutant of one expression**: a comparison whose
two branches agree at the boundary the mutation moves, or a field whose written
value is already its `Default`. The attribute is function-wide and would take
the function's other, killable mutants out of the sweep with it; the regex
names just the one. The trade is that a regex keyed to a path or a line number
rots when the code moves, which `aspect check-mutants-config` catches — so the
attribute is still the better choice when the equivalent mutant is the *only*
one the function generates.

The sibling rule is `aspect check-creusot-skip`, which makes the attribute
mandatory on every `#[cfg(creusot)]` function in `crates/verified`: those are
never compiled in a normal build, so cargo-mutants would report each as an
unexplained survivor.

## Storage formatting

A KRaft node must have its log directory formatted before the broker will boot —
that step seeds Kafka's `meta.properties` and the singleton `VotersRecord`, and
the broker treats an unformatted directory as operator error. `--node-id` is
required: `meta.properties` records it, and the broker refuses to start with a
`--broker-id` other than the one the directory records.

```
bazel run //:format_bin -- --log-dir /var/lib/krabka --standalone \
    --node-id 1 --controller-listener 0.0.0.0:9093
```

The operator CLI spells this `krabka format`, as a subcommand.
That CLI also drives the gres layer, so it could not follow the broker out; the
command needs only the metadata and security crates, so it lives here as
`krabka-format` — a library as well as a binary, so `krabka-cli` can call it
rather than carry a second copy.

## Releases

A release of this repository is an annotated `vX.Y.Z` tag on `main`. Pushing
that tag builds the image, signs it, attests its SBOM and provenance, and
creates the GitHub release. [`CHANGELOG.md`](CHANGELOG.md) records what each tag
contains, and [releasing](docs/releasing.md) gives the commands that cut one.

The workspace is versioned and tagged as one unit, so no crate has a release of
its own and no crate changelog records one. `aspect check-crate-changelogs`
holds that rule in CI. Each crate changelog keeps an `[Unreleased]` heading and
points at robot-head/crabka for the history before the extraction.

## Publishing

A `vX.Y.Z` tag on `main` publishes `krabka-log` and `krabka-verified` to
crates.io, with `krabka-macros`, which `krabka-log` needs. Every other crate,
the broker included, sets `publish = false`; consumers of those pin this
repository by git revision. [Releasing](docs/releasing.md#cratesio) gives the
procedure.

## Roadmap

[docs/roadmap.md](docs/roadmap.md) gives the milestone sequence and the reason
for it. Each milestone asks one question about a claim the project makes, and
the [GitHub milestones](https://github.com/krabka-io/krabka-broker/milestones)
carry the full statement of the gap with the citations behind it.

## Contributing

Read [CONTRIBUTING.md](CONTRIBUTING.md) before you open a pull request. Report
a vulnerability as [SECURITY.md](SECURITY.md) describes, not in a public issue.
