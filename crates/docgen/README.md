# krabka-docgen

Renders the broker reference pages from the broker's own in-process data.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

This tool links `krabka-broker` and `krabka-raft` as libraries. It reads four
values and turns each into one Zola markdown page:

- the `FileConfig` JSON schema, from `krabka_broker::file_config::config_schema`,
- the topic-config whitelist, from `krabka_broker::topic_config_docs`,
- the protocol API catalog, from `krabka_broker::api_catalog::supported_apis`,
- the recorded `KRaft` failure traces, from `krabka_raft::scenarios`.

Reference generation does not spawn the broker binary or read Rust source. The `snippets` command reads the source regions that Markdown pages name.
[`docs/docgen-contract.md`](../../docs/docgen-contract.md) names every entry
point above, and `crates/broker/tests/docgen_contract.rs` holds the broker to
that shape. A rename in the broker fails that test before it fails here.

The operator CRD pages are not rendered here. The operator crate lives in
[`krabka-io/krabka-operator`](https://github.com/krabka-io/krabka-operator), so
its reference pages are rendered in that repository.

## Usage

Write the reference tree:

```sh
bazel run //crates/docgen:krabka-docgen -- all --out /tmp/reference
```

Rewrite the fenced code blocks of the checked-in markdown from the source
regions they name:

```sh
bazel run //crates/docgen:krabka-docgen -- \
    snippets --content "${PWD}/docs" --crates "${PWD}/crates"
bazel run //crates/docgen:krabka-docgen -- \
    snippets --content "${PWD}/crates" --crates "${PWD}/crates"
```

`bazel run` puts the binary's own runfiles tree on the working directory, so
both paths are absolute.

A page opts into a snippet with a pair of HTML comments, and the source region
carries the matching pair of `docs:begin` / `docs:end` markers:

The opening comment is `<!--` followed by
`snippet: raft/examples/quorum_state.rs#quorum_state -->`; the closing comment
is `<!--` followed by `/snippet -->`. The spelling is split here because the
scanner also reads examples inside Markdown fences.

The `snippets` command is idempotent. CI runs it in place and fails when the
working tree changes, so a source edit that a page quotes cannot land on its
own.

## Documentation

- [The docgen contract](../../docs/docgen-contract.md)
- [Configuration reference](../../docs/config-reference.md)
