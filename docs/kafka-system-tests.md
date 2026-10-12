# Apache Kafka system tests

Apache Kafka keeps a [ducktape](https://github.com/confluentinc/ducktape)
system test suite in [`tests/`](https://github.com/apache/kafka/tree/trunk/tests).
Kafka's nightly runs use it. Each test starts its own cluster in containers,
then drives that cluster with Kafka's clients, admin tools, Connect workers and
Streams applications. Many tests also bounce or kill brokers while they run.

`aspect kafka-system-tests` runs that suite against krabka. The broker in every
cluster is `krabka-broker`. Everything else is Kafka's: the producers, the
consumers, the command-line tools, and the checks that decide whether a test
passed.

## Run the tests

```sh
aspect kafka-system-tests
```

The task needs Docker, Bazel, and a JDK 17 or later for Kafka's Gradle build.
It brings up 14 ducker-ak containers with a 2 GB memory limit each. The first
run downloads about 3 GB of Kafka release archives into the container image.

The task does these steps:

1. It checks out the Apache Kafka revision that the manifest pins, in
   `target/kafka-system-tests/kafka-<revision>`.
2. It builds `krabka-broker` and `krabka-format` with `bazel build -c opt` and
   copies them into the checkout. ducker-ak mounts the checkout into every
   container.
3. It installs the adapter, `tools/kafkatest/krabka.py`, into the checkout.
4. It builds Kafka's test libraries with `./gradlew systemTestLibs`.
5. It replaces any ducker-ak cluster on the machine with a new one, and runs
   the selected tests with ducktape.
6. It compares each result with the manifest and writes
   `target/kafka-system-tests/report.md`.

To run part of the manifest, give one or more `--test` prefixes:

```sh
aspect kafka-system-tests --test kafkatest/sanity_checks/
aspect kafka-system-tests --test kafkatest/tests/core/replication_test.py
```

`--parallel` sets how many tests ducktape runs at the same time. The default is
3. Use `--parallel 1` to examine a failure that can be a timeout under load.
`--keep-nodes` leaves the containers up after the run, so that you can use
`tests/docker/ducker-ak ssh` in the checkout.

ducktape writes its own results under
`target/kafka-system-tests/kafka-<revision>/results/krabka-<time>/`. The
`report.html` file there links the logs of each test. Each broker writes its log
to `server-start-stdout-stderr.log`. The test log `test_log.info` shows the
`kafka.properties` file of each node and the `krabka.toml` file made from it.

## How the adapter works

The adapter defines `KrabkaService`, a subclass of Kafka's `KafkaService`. The
task adds one import to `kafkatest/services/kafka/__init__.py`. That import
replaces `KafkaService` with `KrabkaService` in the package and in its `kafka`
module. Thus every test, and the isolated controller quorum that
`KafkaService` builds for itself, starts krabka. No test file changes.

`KrabkaService.start_node` makes the same `kafka.properties` file that Kafka
uses for the node, and translates it:

| Kafka property | krabka setting |
| :--- | :--- |
| `node.id` | `broker_id` |
| `process.roles` | `[process] roles` |
| `controller.quorum.voters` | `controller_quorum_voters` |
| `controller.quorum.bootstrap.servers` | `bootstrap_servers` |
| `listeners`, `advertised.listeners`, `listener.security.protocol.map` | `[[listeners]]` |
| the controller listener in `listeners` | `--controller-listen-addr` |
| `inter.broker.listener.name` | `inter_broker_listener_name` |
| `broker.rack` | `rack` |
| `replica.selector.class` | `replica_selector`, for Kafka's two selector classes |
| `remote.log.storage.system.enable` with Kafka's `LocalTieredStorage` | `[remote_storage] storage_dir` |
| `rlmm.config.remote.log.metadata.topic.*` | `[remote_storage.kafka_metadata]` |
| `log.dirs` | `log_dir` and `extra_log_dirs` |
| `metadata.log.dir` | `metadata_log_dir`, and `--metadata-log-dir` for `krabka-format` |
| keys in `RUNTIME_KEYS` | the `[runtime]` field with the same meaning |
| every key | `[server_properties]` |

Kafka's `KafkaConfig` always sets `metadata.log.dir` to
`/mnt/kafka/kafka-metadata-logs`, so every krabka node in the harness keeps its
metadata log apart from its data directories, as a Kafka node does. The
metadata log is in `__cluster_metadata-0` under that directory, with Kafka's
segment and snapshot file names, so `snapshot_test.py` finds the files it
checks. The `metadata.log.segment.*`, `metadata.max.retention.*` and
`metadata.max.idle.interval.ms` keys map to `[runtime]` keys. Kafka reads a
negative retention as no limit, which has no `[runtime]` form, so the adapter
refuses it.

The tests come from Kafka trunk, and they drive trunk clients. So the adapter
starts each node of the development branch in krabka's trunk mode: it sets
`unstable.api.versions.enable` and `unstable.feature.versions.enable` to
`true`, and it formats with `--unstable-feature-versions-enable`. In that mode
krabka serves the API versions and the feature levels that it implements from
Kafka trunk past the latest release, for example `share.version` 2 for
KIP-1191. A test that sets one of these properties keeps its own value.

A test can pin a node to a Kafka release. The adapter then keeps krabka's
default mode, which serves the API versions and the feature levels of the
latest release that krabka implements, 4.3.1. For example,
`StreamsTopologyDescriptionPluginTest.test_describe_topology_fails_against_pre_kip_1331_broker`
starts a 4.3 broker and expects `StreamsGroupDescribe` version 0 only. The
format follows the node's own `unstable.feature.versions.enable`.

The adapter then formats the log directories with `krabka-format`, with the
same cluster id and `--feature` flags that `kafka-storage.sh format` gets. It
starts `krabka-broker` and waits for the `krabka-broker started` log line, as
Kafka's `KafkaService` waits for `Kafka Server started`. Every node logs this
line when its start is complete. The line names the listeners that the node
opened: `listen_addr` for the client listener of a node with the broker role,
and `controller_listen_addr` for the controller listener of a node with the
controller role. A controller-only node opens no client listener, as Kafka's
`ControllerServer` opens only the listeners in `controller.listener.names`.
It finds the broker process with `pgrep -x krabka-broker`.

The adapter turns krabka's audit log off. Kafka has no audit log, and the
`__krabka_audit` partition would take a replica and a log directory slot that
the tests expect to find free.

Both the combined and the isolated KRaft modes work. The adapter supports
`PLAINTEXT` listeners only. A test that asks for TLS or SASL fails at broker
start with a message that says so.

## The manifest

[`tools/kafkatest/suite.json`](../tools/kafkatest/suite.json) pins the Kafka
revision and lists the tests. Each entry names one parametrization of one test,
in the form that ducktape uses:

```json
{"test": "kafkatest/tests/core/replication_test.py::ReplicationTest.test_replication_with_broker_failure",
 "parameters": {"failure_mode": "clean_bounce", "broker_type": "leader", "security_protocol": "PLAINTEXT", "enable_idempotence": true, "metadata_quorum": "ISOLATED_KRAFT"}}
```

The parameters must be equal to one parametrization of the test at the pinned
revision. Otherwise ducktape does not run the entry, and the task reports it as
`NOT RUN`.

An entry that krabka does not pass has a `known_failure` string that tells how
it fails. The task gives each entry one of four verdicts:

| Verdict | Expectation | Result |
| :--- | :--- | :--- |
| pass | pass | `PASS` |
| regression | pass | Any result other than `PASS` |
| regression | `known_failure` | Missing, skipped, aborted, or other non-executed results |
| known failure | `known_failure` | Observed `FAIL` |
| fixed | `known_failure` | `PASS` |

The task exits with status 1 if one or more entries is a regression. When an
entry is fixed, remove its `known_failure`.

The manifest holds one parametrization of each Kafka test function, for each
group protocol that the function tests. It holds every client version of the
client compatibility tests, and both transaction protocol versions of the
transactions test. The manifest does not hold these tests:

- Tests that run on ZooKeeper.
- Upgrade and downgrade tests, and tests that start a Kafka broker of an earlier
  release, other than the 4.3 test named above. A krabka cluster cannot
  upgrade from a Kafka broker, and krabka serves no release before 4.3.1.
- Tests that need TLS or SASL listeners, whether a parameter asks for them or
  the test sets them up itself: `SecurityTest`, `AuthorizerTest` and
  `DelegationTokenTest`, and the producer sanity tests of several security
  protocols.
- `QuotaTest`, which reads the broker's byte rates over JMX. krabka is not a
  JVM and serves no JMX. A test that reads a client's JMX, as
  `FetchFromFollowerTest` does, stays in.
- The kibosh and trogdor tool tests, which start no broker, and
  `NetworkDegradeTest`, which checks the `tc` network faults of the test
  environment and not the broker. Its `test_rate` fails against Apache Kafka
  in ducker-ak containers as well.
- `ReplicaVerificationToolTest`, which expects the follower to fall behind the
  leader while a producer sends 1000 records each second. In ducker-ak
  containers the follower of Apache Kafka keeps up, the tool reports a lag of
  0 each second, and the test fails against Apache Kafka as well.
- `ConnectPluginDiscoveryTest`, which takes a tuple parameter that JSON cannot
  express.

To run a test that the manifest does not hold, add its entry. To list the tests
at the pinned revision, run ducktape in collect mode in a running cluster:

```sh
tests/docker/ducker-ak ssh ducker01 "cd /opt/kafka-dev && ducktape --collect-only tests/kafkatest/tests"
```

## Move to a new Kafka revision

1. Change `kafka_revision` in the manifest.
2. Compare `KafkaService.start_node` at the new revision with
   `KrabkaService.start_node`. The adapter copies the steps before the format,
   and those steps must agree.
3. Collect the tests at the new revision. Update the manifest entries whose
   parametrizations changed.
4. Run the whole manifest, and update the `known_failure` entries.
