# krabka-broker — project-specific guidance

## Compatibility

**From 1.0.0 on, krabka is backwards compatible on disk.** Any 1.x broker reads
every artifact that an earlier 1.x broker wrote, and a rolling upgrade from 1.x
to 1.y works. [`docs/persisted_formats.md`](docs/persisted_formats.md) lists
every persisted format, states the contract, and records the known gaps. The
contract covers this repository and the persisted crates of `krabka-protocol`.
Data written before 1.0.0 gets no promise: a 0.x data directory is reformatted.

A persisted format is anything a broker writes to disk or to an object store:
partition logs and their sidecars, metadata log segments and snapshots,
`quorum-state`, `meta.properties`, the bootstrap files, internal-topic records,
the krabka-private wincode records inside `NoOpRecord` tags, tiered segments,
WORM manifests, diskless WAL objects, and backup captures. The
controller-forwarding RPCs (`SubmitChange`, `MetadataFetch` and
`DelegationTokenMutation`) carry krabka-private records, wincode
`MetadataRecord` values among them, between nodes of two versions during a
rolling upgrade, so they follow the same rules. For every one of them:

- Never reorder or remove a variant of a persisted wincode enum such as
  `MetadataRecord`, and never insert one before an existing variant. Add a new
  variant at the end only. wincode encodes a variant by its index.
- Never change the fields of a persisted wincode type. wincode is positional and
  carries no field names, so `#[serde(default)]` does not help it. To change a
  record's shape, add a new variant at the end (for example `V2Topic`) and keep
  the old variant readable.
- Never reuse a `NoOpRecord` private tag. 1001 and 1003 to 1006 are assigned,
  and 1002 is burned. A new private record takes a new tag.
- Give a new or changed format a version marker, and a reader for every earlier
  1.x version of it.
- Gate new writer behavior on a feature level. The broker keeps writing the old
  format until the operator finalizes the level that introduces the new one, as
  in Kafka's KIP-584 and KIP-778. Use a `metadata.version` level only where
  Kafka defines one. Never add a level that Kafka's `MetadataVersion` does not
  have.
  A krabka-only change takes a new `krabka.version` level (krabka-protocol
  `krabka_metadata::krabka_version`). Levels 0 and 1 are the 1.0.0 formats, so
  the first new format takes level 2. A new version of a private controller RPC
  (1003-1005) is a row in that crate's `private_rpc_version` table at the new
  level.
- Add a golden-bytes fixture test for each persisted format you add or change.
  The test decodes bytes that an earlier release wrote and compares the decoded
  value. It does not compare source text.
- Where they keep 1.x data readable, `#[serde(default)]` on a JSON field, a kept
  `V1` variant beside its `V2`, and a reader for an older version are required,
  not forbidden.

During development, deleting local raft logs and data directories is still fine
for a format that no release has shipped.

Everything that is not persisted keeps the greenfield rule: in-memory types,
internal APIs, configuration, and CLI flags. For those, do not write
backwards-compatibility shims:

- No feature flags that gate new non-persisted behavior behind a default-off
  switch
- No deprecated-but-kept API surfaces

When a non-persisted schema, enum, or interface changes, change it. The Rust API
is not under a stability promise.

**Kafka compatibility is the constraint that matters.** Always keep:

- Apache Kafka wire-protocol byte exactness for request and response shapes,
  field order, error codes, and version negotiation
- KIP semantics for the feature that you implement
- Behavior that the JVM admin tools rely on, such as `kafka-topics`,
  `kafka-acls`, `kafka-leader-election`, and `kafka-reassign-partitions`

When in doubt, match Kafka. If Kafka's behavior is undocumented or
version-dependent, check the behavior of the latest released cp-kafka image. Do
not rely on the wiki.

## Build

Bazel is the build and test path; Cargo is the dependency source of truth.
`rules_rs` reads the same `Cargo.toml` / `Cargo.lock` Cargo does.

```
bazel test //...          # everything CI gates on
cargo nextest run --workspace
```

Per-crate BUILD files stay small on purpose: `//bazel:defs.bzl` reads crate
name, edition, feature set and dependency labels out of the `@crates` repo that
`crate.from_cargo` generates, so a manifest change does not need a matching
BUILD edit. Add a new workspace member by writing its `Cargo.toml` and a
four-line `BUILD.bazel` that calls `crate_library` and `crate_tests`.

Suites that cannot run hermetically are tagged `manual` at their `crate_tests`
call, with a comment saying why. Add to that list rather than deleting a test.

Three sibling repositories sit below this one:
[`krabka-protocol`](https://github.com/krabka-io/krabka-protocol) for the wire
layer (`krabka-protocol`, `krabka-compression`, `krabka-ids`,
`krabka-metadata`, `krabka-security`, `krabka-trace-context`, `krabka-units`
and `krabka-voters`),
[`krabka-client-rs`](https://github.com/krabka-io/krabka-client-rs) for the
Kafka client (`krabka-client-core`, `krabka-client-admin`,
`krabka-client-consumer` and `krabka-client-producer`), and
[`krabka-schema-registry`](https://github.com/krabka-io/krabka-schema-registry)
for schema handling (`krabka-schema-serde`). All three are pinned by revision
in one place -- the `[patch.crates-io]` block at the bottom of the root
`Cargo.toml`. Member manifests declare those crates as ordinary
`krabka-x = "0.4.0"` requirements; the patch is what redirects them at the git
checkouts. To move to a newer sibling, change the revision there, re-run
`cargo generate-lockfile`, and commit both files.

`MODULE.bazel` additionally names each sibling crate's directory. rules_rs finds
a git crate's path by matching the crate name against the workspace `members`
list, and every sibling's members is the glob `crates/*`, which it skips.

`krabka format` lives here as the `krabka-format` crate, extracted from
`krabka-cli` because that crate also drives the gres layer and could not follow
the broker out. It is a library as well as a binary: broker tests that
need a formatted log directory call `krabka_format::run_from_args` in process
rather than spawning it, because a subprocess needs a Cargo working tree and a
Bazel test sandbox has none.

## Repository tasks

Every check and generator that runs on a developer's or a runner's machine is an
Aspect CLI task under `.aspect/`, written in AXL, with unit tests beside it.

```
aspect --help            # the whole task surface
aspect axl-tests         # every task's unit tests
aspect check-scripts     # the ratchet below
```

A task is `.aspect/<name>.axl`: the pure functions the check is made of, and a
thin `task(...)` that does the IO around them. Its tests are
`.aspect/<name>_test.axl`, exporting a suite that `.aspect/axl_tests.axl` runs
and an `aspect tests <name>` command of its own. `.aspect/repo.axl` holds the
guarded filesystem walk `ctx.std.fs` does not provide; `.aspect/testing.axl`
holds the assertions, the suite type and the runner.

**Do not add a shell script or a Python script.** `aspect check-scripts` fails
on a new `.sh` or `.py`, and its `ALLOWED` table is not a list of exceptions to
taste: every row names a file that runs inside a Bazel action, a Bazel test
sandbox, or a container image built from an apko base -- somewhere the Aspect
CLI is not, and a task cannot reach. A new file that genuinely runs in one of
those gets a row saying which. Everything else is a task.

AXL is Starlark: no regular expressions, no `while`, no recursion. A port from
`re` or `awk` becomes explicit string scanning, and the unit tests are what say
it still means the same thing.

The `rustdoc-site` task comes from the krabka-io/tooling AXL module that
`MODULE.aspect` pins. Do not add a local copy of it. To change it, change
krabka-io/tooling. Then bump the `axl_archive_dep` revision and integrity in
`MODULE.aspect`.

### Rust Analyzer

This Bazel-first workspace needs a generated project for Rust Analyzer to
resolve crate paths. From the workspace root, generate it for the Rust targets
you are editing:

```sh
source /workspace/.krabka-env/activate.sh
bazel run @rules_rust//tools/rust_analyzer:gen_rust_project -- //crates/broker:broker
```

Pass additional Bazel Rust targets after `--` to include their crates and
dependencies. The generator writes `rust-project.json` and a local cache under
`.rules_rust_analyzer/`; both are generated files and should stay untracked.
Restart Rust Analyzer or reload the workspace after generation, and keep
procedural macro expansion enabled. Test helpers generated by macros otherwise
remain unresolved. Its **Replace qualified path with use** code action then
resolves paths and adds a module-scope import. Prefer this assist for shortening
qualified references; it preserves name resolution and handles import conflicts.
Avoid `crate::...` paths in code: import the item or its containing module at
module scope and refer to that name. CI checks this with
`aspect check-imported-paths`.

## Code & Documentation Style

Follow the style guides in [`docs/style_guides/`](docs/style_guides/README.md):
[code](docs/style_guides/code_style_guide.md),
[rustdoc](docs/style_guides/rustdoc_style_guide.md),
[README](docs/style_guides/readme_style_guide.md),
[design docs](docs/style_guides/design_doc_style_guide.md), and
[coverage reports](docs/style_guides/coverage_report_style_guide.md). Examples
are the pinned stable toolchain, `cargo +nightly fmt`, forbidden `unsafe`, and
`clippy::pedantic`.

Do not make style-only sweeps across untouched files. Bring a file into line
with the guides only when you already edit it. Keep the tidy-up proportionate to
the change.

### Builders

Use `bon` builders for factory and test-fixture functions that take a setup
struct, especially when callers override a few values on top of defaults. Do
this for production helpers and test helpers alike; do not make callers build a
`Setup { field: value, ..Default::default() }` just to pass it to a factory.
Give defaultable builder fields explicit defaults and call the builder with
named setters followed by `.call()` (or `.build()` for a derived struct
builder).

For builder fields that use scalar newtypes, make the setter accept the raw
scalar with bon's `#[builder(into)]`. Derive the needed `From` conversions on
the newtype with `derive_more` instead of writing repetitive conversion
implementations by hand.

### Assertions and Clippy

- Never add `#[allow(clippy::...)]` or any equivalent Clippy suppression. Fix
  every Clippy warning in the code, regardless of the effort required.
- Never use Rust's plain `assert!`, `assert_eq!`, or `assert_ne!` macros. Use
  the `assert2` crate's `assert!` macro instead. Use it also for equality and
  inequality comparisons.

Clippy is a Cargo-side gate. `bazel build` applies `-Funsafe_code` (the one
`[workspace.lints]` entry whose guarantee must not lapse under a second build
system) but does not run Clippy, so run `cargo clippy --workspace --all-targets
-- -D warnings` before you push.

## Execution

When you execute an implementation plan, always use **subagent-driven
development in parallel batches** where the per-task file sets do not overlap.
Dispatch all tasks in a batch concurrently, in one message with multiple Agent
calls. Then wait for the batch to complete, review it, and move to the next.

A "conflict" between parallel implementers occurs only when both edit the same
file. When in doubt, list the file set that each task touches before you decide.

**Never discard working-tree state while parallel implementers run.**
`git checkout -- <path>`, `git restore`, `git stash`, and `git clean` all
destroy *every* uncommitted change in the files they touch, not only yours. To
undo your own edit, reverse it directly.

Tests must exercise behavior, not source text. Do not read source files in tests
and assert against their contents. `include_str!` and `fs::read_to_string` are
examples of such reads. If a behavior is hard to test, add a narrow helper or
seam. Then test that behavior directly.

When you check generated protocol records or other structured values in tests,
compare the whole expected struct. This is better than long chains of
field-by-field assertions. Use table-driven or parameterized tests for repeated
scenarios that differ only by inputs, protocol version, or expected request
shape.

## Releases

A `vX.Y.Z` tag on `main` starts two independent workflows.
`.github/workflows/release.yml` signs and promotes the container image and
creates the GitHub release. `.github/workflows/publish.yml` publishes
`krabka-log` and `krabka-verified` to crates.io, with `krabka-macros`, which
`krabka-log` needs as a normal dependency.

`publish.yml` publishes every member crate without `publish = false`. Every
other member, the broker included, sets `publish = false`, and a new crate that
is not a library for other repositories sets it too. Every `krabka-*` normal
dependency of a published crate carries a `version` as well as its `path`,
because `cargo publish` needs one. A path or git dependency that is not on
crates.io may appear only as a dev-dependency, and without a `version`, so that
cargo drops it from the published manifest. `cargo publish` ignores
`[patch.crates-io]`, so the krabka-protocol release a published crate requires
must already be on crates.io.

[`docs/releasing.md`](docs/releasing.md) is the procedure, including the token
bootstrap for a new crate name and the switch to trusted publishing.
