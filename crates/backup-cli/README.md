# krabka-backup

Captures the inputs a point-in-time restore needs, and puts committed
consumer-group offsets back after one.

Part of [krabka-broker](../../README.md), an Apache Kafka-compatible broker
written in Rust.

## Overview

`krabka restore` rebuilds a log directory out of a KIP-405 archive, and it needs
three things the archive does not hold:

- `<log.dir>/remote-log-metadata/snapshot`, the RLMM snapshot. Without it a
  segment the old cluster had already released is indistinguishable from a live
  one, and the restore includes it. Restore also seeds its chain receipts into
  the recovered broker with fresh metadata-topic cursors, so WORM archival can
  continue without an unauthenticated epoch restart.
- The controller's newest `<end-offset>-<epoch>.checkpoint`. Topic
  configuration, ACLs, client quotas, SCRAM credentials and finalized feature
  levels live there and nowhere else.
- The committed offset of every consumer group. `__consumer_offsets` is
  compacted and internal, so it is never tiered and no archive holds it.

The disk that holds the first two is the disk a disaster destroys, and the third
dies with the cluster. This tool makes the copy that has to exist before that
day, and checks it.

The container image is built from an apko base and has no shell, no `cp` and no
`tar`, so `kubectl exec` and `kubectl cp` cannot take a file off a running
broker. This binary is the supported copy. It reads the two files through a
read-only mount of the broker's volume, and it reads group offsets over the
ordinary Kafka wire protocol with `ListGroups` and `OffsetFetch`. It speaks no
krabka-private API key, so the same capture runs against a cluster that is not
krabka.

The crate is a library as well as a binary. Tests call
`krabka_backup::run_from_args` in process, because a test that spawns the binary
needs a Cargo working tree and a Bazel test sandbox has none.

## Usage

The binary is named `krabka-backup`, so `krabka backup` reaches it: the `krabka`
operator CLI resolves an unknown subcommand to `krabka-<name>` on `PATH`, the
way git resolves `git foo` to `git-foo`.

Every subcommand takes the same `--archive-*` flags `krabka restore` takes, and
a capture is written into the archive the restore reads. Capture from a node and
a cluster at once:

```sh
krabka-backup capture \
  --log-dir /var/lib/krabka \
  --bootstrap-server broker-1:9092 \
  --command-config /etc/krabka/backup-client.properties \
  --archive-s3-bucket krabka-tier \
  --archive-s3-region eu-west-1 \
  --archive-prefix prod/
```

Read the captures back, and prove the newest one is whole:

```sh
krabka-backup list   --archive-s3-bucket krabka-tier --archive-prefix prod/
krabka-backup verify --archive-s3-bucket krabka-tier --archive-prefix prod/
```

After a restore, put the group positions back:

```sh
krabka-backup restore-offsets \
  -b restored-broker:9092 \
  --command-config /etc/krabka/backup-client.properties \
  --worm-key-id backup --worm-public-key /etc/krabka/backup.pub \
  --worm-expect-head <independently-recorded-diskless-capture-head> \
  --archive-s3-bucket krabka-tier --archive-prefix prod/
```

## Subcommands

| Subcommand | Flags | What it does |
| --- | --- | --- |
| `capture` | `--log-dir <dir>`, `--bootstrap-server <host:port>` (`-b`), `--command-config <file>`, `--worm-signing-key-id <id> --worm-signing-key <path>` | Copies the RLMM snapshot, newest metadata checkpoint, committed group offsets, and committed diskless-WAL projection into `restore-inputs/<capture-id>/`, with a `manifest.json`. The signing pair authenticates the diskless capture boundary, both snapshot digests, and every referenced WAL object. Each source is optional; at least one has to give something. |
| `list` | none | Names every capture in the archive, oldest first, with the artifacts it holds. |
| `verify` | `--capture <id\|latest>` | Re-reads each artifact and checks its size and SHA-256 against the manifest. |
| `restore-offsets` | `--capture <id\|latest>`, `-b <host:port>`, `--command-config <file>`, `--dry-run`, `--worm-key-id <id> --worm-public-key <path> --worm-expect-head <hex>` | Commits the captured offsets into a restored cluster. A signed diskless capture requires the trust triple and binds the offsets to that boundary. |

A capture id has a zero-padded epoch millisecond followed by a random UUID suffix. Concurrent captures and repeated clock values receive different identities, and capture writes refuse to overwrite existing objects. `latest` is still a listing and a maximum; older timestamp-only captures remain discoverable. Captures within the same millisecond have no time order beyond their common timestamp.

Archive reads enforce independent limits before buffering: 8 MiB for manifests, 64 MiB for group offsets, 512 MiB for metadata/RLMM snapshots, and 256 MiB for diskless capture documents and other artifacts. Manifest-declared sizes cannot raise these limits.

## What a capture holds

```
restore-inputs/0001762000000000/
  manifest.json               capture id, time, sources, and one row per artifact
  rlmm-snapshot               <log.dir>/remote-log-metadata/snapshot, byte for byte
  cluster-metadata.checkpoint the newest <end-offset>-<epoch>.checkpoint
  group-offsets.json          every group with a committed offset
```

The manifest records each artifact's size and SHA-256, which is what makes
`verify` possible. A backup nobody checks is a backup nobody has.

`capture` looks for the metadata checkpoint in `__cluster_metadata-0` and in
`__cluster_metadata-0/observer` under `--log-dir`, and takes the newest of the
two. A node that runs a controller has the first. A broker-only node has the
second, because the metadata observer keeps its checkpoints in that
subdirectory and never directly in `__cluster_metadata-0`.

Both directories are in the metadata log directory, Kafka's `metadata.log.dir`.
That is the broker's first log directory unless its `metadata_log_dir` names
another one. For a node with a separate metadata log directory, point
`--log-dir` at the metadata log directory to capture the checkpoint. The RLMM
snapshot stays in the broker's `log_dir`, so take it in a second `capture` run
with `--log-dir` set to that directory.

## Group offsets

`restore-offsets` commits as a simple consumer: empty `member_id`, generation
`-1`. That is what `kafka-consumer-groups --reset-offsets --execute` sends, and
the coordinator accepts it for a group with no live members. A group that
already has members is fenced, and that refusal is correct: an offset must not
move under a consumer that reads from it. Start the restored cluster, put the
offsets back, then start the consumers.

The offsets a capture holds are as old as the capture. A group that committed
after the last capture resumes at the older position and reads some records a
second time, which is the at-least-once behaviour every Kafka consumer already
has to be correct under.

When capture signing is enabled, the signed diskless boundary includes the
group-offset digest. `restore-offsets` then requires the trusted key and the
independently retained capture head before it will commit anything.

## Secured clusters

`capture` and `restore-offsets` accept the same Kafka client-properties file as
the JVM tools' `--command-config`. The backup tool reads `security.protocol`
(`PLAINTEXT`, `SSL`, `SASL_PLAINTEXT` or `SASL_SSL`), `sasl.mechanism`
(`PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`) and `sasl.jaas.config`.
TLS trust and client identity use PEM files:

```properties
security.protocol=SASL_SSL
ssl.truststore.type=PEM
ssl.truststore.location=/etc/krabka/ca.pem
ssl.server.name=broker.example
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="backup" password="secret";
```

For mutual TLS, set `ssl.keystore.type=PEM`,
`ssl.keystore.location=<client certificate PEM>` and
`ssl.key.location=<client private-key PEM>`. The properties file is never
copied into the archive or rendered in command output; restrict its filesystem
permissions because it contains the SASL password and private-key path.

The backup principal needs `Describe` on every captured group and `Read` on its
topics. A diskless capture also needs `Read`, `Write`, and `Describe` on
`__diskless_wal_index` plus `DescribeConfigs` on the captured topics so empty
diskless partitions remain in the recovery topology. Restoring offsets needs `Describe` and `Read` on each
captured group plus `Describe` and `Read` on each captured topic. Grant those
operations only on the group and topic prefixes covered by the backup policy.

## Exit codes

| Code | Meaning |
| :--- | :--- |
| 0 | Success. |
| 2 | A bad argument, or a flag whose backend was not selected. |
| 4 | The archive, the log directory, or the named capture cannot be read. |
| 5 | An integrity failure: an artifact does not match its recorded digest. |
| 6 | The cluster could not be reached, or it refused a request. |

`2`, `4` and `5` mean what they mean in `krabka restore`, because one runbook
branches on both tools.

## Documentation

- [Backup and restore](../../docs/operations/backup-restore.md): what to copy,
  how often, and how to check a copy.
- [restore-from-archive](../../docs/operations/runbooks/restore-from-archive.md):
  the runbook for the day it is needed.
- [KFC-3](../../docs/KFCs/KFC-3-point-in-time-restore.md): the restore this
  tool feeds.

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
