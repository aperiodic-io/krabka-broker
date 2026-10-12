# krabka-verified

Formally verified pure kernels, proved with Creusot, that the other krabka
crates call.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

The kernels are synchronous and allocation-light. Creusot proves their functional contracts under the stated preconditions. Normal Rust builds do not enforce the proof preconditions; invalid inputs can panic. A host crate maps its state into
the kernel's inputs, calls the kernel, and applies the decision. There is no
second copy of any decision in a host crate.

The kernels cover the safety-critical decisions of consensus, storage,
security, and protocol handling: KRaft vote admission and quorum size, ISR and
leader failover, KIP-534 compaction retention, quota precedence and the token
bucket refill, ACL matching, audit chain steps, break-glass admission, and
more. Each subject keeps its public types and re-exports in a small module
file; large subjects put their kernels and tests in a matching subdirectory.
The [composition proofs](src/composition/) are grouped by the safety property
they connect, such as quota accounting, snapshot replay, and WAL recovery.

The contract attributes beside each function are the source of truth. The
[verification catalog](../../docs/verification.md) lists every kernel, its
host caller, its proof session under `verif/`, and the preconditions the caller
has to establish. Pull request CI discharges every proof again.

## Usage

A host crate calls a kernel like any other function. The proof holds only when
the caller establishes the listed preconditions.

```rust
use krabka_verified::consensus::{election_has_quorum, majority_size};

let voter_count = 5;
assert2::assert!(majority_size(voter_count) == 3);
assert2::assert!(election_has_quorum(voter_count, 3));
```

## Features

- The crate builds on stable Rust. `cargo creusot` compiles it with
  `--cfg creusot`, which the build script registers so the workspace Clippy
  gate stays quiet.
- No I/O, no allocation in the hot paths, and no dependency on the broker.

## Documentation

- [Verification catalog](../../docs/verification.md)
- [Proof sessions](../../verif/)
- [API documentation](https://krabka-io.github.io/krabka-broker/krabka-verified/)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see
[NOTICE](../../NOTICE).
