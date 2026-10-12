# The krabka-docgen contract

`krabka-docgen` is the reference-page generator in
[`crates/docgen`](../crates/docgen/README.md). It links this broker as a
library and renders four pages from values it builds in process. It does not
spawn the broker binary and it does not read Rust source. This document names
what it reads, so a change here that breaks the tool fails a test in this
repository first.

The tool came from [`robot-head/crabka`](https://github.com/robot-head/crabka),
where it was pinned by revision and the crates were still named `crabka-*`.
It is a workspace member now, so there is no pin to move: a rename in the
broker breaks the tool's own build.

The operator CRD pages are not rendered here. The operator crate lives in
[`krabka-io/krabka-operator`](https://github.com/krabka-io/krabka-operator), so
its reference pages are rendered in that repository.

## What the tool reads

### The `FileConfig` JSON Schema

`crates/docgen/src/broker.rs` calls `krabka_broker::file_config::config_schema`
and hands the value to `render_sectioned_field_table` in `schema_md.rs`. That
renderer depends on this shape:

- The root is an object with `properties`, one per top-level TOML key, and a
  `$defs` table that every `$ref` points into as `#/$defs/<Name>`.
- An `Option<Struct>` field is `anyOf: [{$ref}, {type: "null"}]`. The renderer
  collapses it to the referenced struct.
- Each field carries its `///` comment as `description`, and a
  `#[serde(default)]` field carries `default`.
- A struct's `required` list names the fields with no default.

The same value is what `krabka-broker --print-config-schema` prints and what
[`config-schema.json`](config-schema.json) holds. The `format` annotations
`duration`, `byte-size`, and `ratio` on string fields are an addition the
renderer ignores; [`config-reference.md`](config-reference.md) uses them for
its units column.

### `api_catalog::supported_apis`

`protocol_apis_md` calls
`krabka_broker::api_catalog::supported_apis(ListenerKind::Client,
ClientMetricsReceiver::Configured)`, sorts the result by `api_key`, and prints
one row per entry with the name `krabka_protocol::ApiKey::from_i16` gives it.
That argument pair is the widest client-facing set: it leaves out the
inter-broker-only keys, which no Kafka client sends, and it keeps the two
KIP-714 telemetry keys. The tool expects a non-empty list with no repeated key,
and a name for every key.

### `topic_config_docs`

`topic_configs_md` calls `krabka_broker::topic_config_docs()` and prints
`key`, `value_type`, `default`, `kip`, and `description` for each entry.

### `krabka_raft::scenarios`

`crates/docgen/src/scenarios.rs` runs `krabka_raft::scenarios::scenarios()`
and renders each `ScenarioTrace` into a Mermaid sequence diagram. It reads
`id`, `title`, `summary`, `invariant`, `nodes`, `steps`, and `outcome`, and
matches every `TraceAction` variant: `Deliver`, `Partition`, `Heal`, `Timeout`,
`Elected`, `Append`, and `Drop`. It special-cases the scenario whose `id` is
`split_brain_prevented`.

The module is `krabka_kraft_core::sim`, re-exported by `krabka-raft` under the
`scenarios` feature. No production build enables that feature. Two consumers
turn it on: `crates/docgen/Cargo.toml` names it on its own dependency, and
`crates/broker/Cargo.toml` turns it on through a dev-dependency on
`krabka-raft`, so the broker's test build links the simulator and
`docgen_contract.rs` destructures every field and matches every variant above.
A rename fails to compile there before it fails in the tool.
`.cargo/mutants.toml` excludes `crates/kraft-core/src/sim.rs` from the mutation
sweep on the strength of this consumer.

## Where the contract is tested

`crates/broker/tests/docgen_contract.rs` asserts the schema shape, the
`supported_apis` invariants, the `topic_config_docs` columns, and the
`ScenarioTrace` shape, including that `scenarios()` returns
`split_brain_prevented`.
`crates/broker/tests/config_reference.rs` asserts that
[`config-schema.json`](config-schema.json) equals the schema the crate
generates and that [`config-reference.md`](config-reference.md) carries a row
for every key. `crates/broker/tests/example_broker_toml.rs` parses the example
configs and checks every key they set against the schema. The docs CI job
regenerates the reference page with `aspect generate-config-reference` and
diffs it.

## Where the rendering is checked

The `checks` job in [`ci.yml`](../.github/workflows/ci.yml) runs the tool
itself, in two steps.

The first renders the whole tree into a temporary directory. That proves every
entry point above still resolves and every renderer still produces a page. A
signature change that the contract test does not reach fails here.

The second runs `krabka-docgen snippets` in place over both `docs/` and `crates/`
and then `git diff --exit-code -- docs crates`. The `snippets` command rewrites each fenced
block that a page opens with an HTML comment naming a source path and an
anchor, and closes with the matching end comment. The source region carries
`docs:begin <anchor>` and `docs:end <anchor>` on two comment lines.
[`crates/docgen/README.md`](../crates/docgen/README.md) shows the exact
spelling of both. The command is idempotent, so a tree that is in sync does not
change and the step passes. A source edit that moves a quoted region leaves the
page stale, the rewrite changes the file, and the step fails.

The marker spelling in the README is split on purpose. The scan reads raw
text, including Markdown fences, so a complete opening marker opts into a
snippet even inside a documentation example. Both documentation pages and
crate READMEs are checked against their source regions.

The rendered reference tree itself is not checked in. Its consumer is the Zola
site in
[`krabka-io/krabka-io.github.io`](https://github.com/krabka-io/krabka-io.github.io),
which pulls the pages at build time; a second copy here would be one more
generated tree to keep in step. `docs/config-reference.md` is the page this
repository does keep, and `aspect generate-config-reference` is what regenerates
it.
