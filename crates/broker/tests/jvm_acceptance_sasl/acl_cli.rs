//! The `kafka-acls` administration round-trip: add a binding, list it, remove
//! it, and list again.
//!
//! This file covers the ACL *administration* surface -- `CreateAcls`,
//! `DescribeAcls` and `DeleteAcls` as the JVM CLI drives them -- and is
//! separate from the files that check what those bindings then permit or
//! deny on the data plane.

use {
    crate::{
        jvm_acceptance::{
            ADMIN, ADMIN_PASS, KAFKA_IMAGE_TXN, broker0_advertised,
            docker_run_kafka_tool_with_image_and_mount, nc_check_connectivity,
            plain_client_properties, start_sasl_plaintext_broker_with_super_user,
            write_plain_props,
        },
        oracle::secured_sides,
    },
    assert2::{assert, check},
};

/// JVM acceptance: `kafka-acls.sh` end-to-end provision flow.
///
/// The test drives the modern `kafka-acls.sh` flag set (cp-kafka:7.5.0,
/// Kafka 3.5+) against the Rust broker's `CreateAcls (30)`,
/// `DescribeAcls (29)`, and `DeleteAcls (31)` handlers. Admin authenticates
/// as PLAIN super-user, so the super-user short-circuit in `authorize()`
/// bypasses its `Cluster Alter` and `Cluster Describe` checks.
///
/// Sequence:
/// 1. `--add` an Allow Read on `Topic LITERAL "foo"` for `User:alice`.
/// 2. `--list --topic foo` must show that binding.
/// 3. `--remove --force` removes it. `--list --topic foo` must be empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn jvm_kafka_acls_provision_via_cli() {
    let (broker, _dir) =
        start_sasl_plaintext_broker_with_super_user(ADMIN, &[(ADMIN, ADMIN_PASS)]).await;
    nc_check_connectivity();

    let admin_props = write_plain_props(ADMIN, ADMIN_PASS);
    let mount = admin_props.mount_str();

    // 1. --add.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--add",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--topic",
        "foo",
    ]);

    // 2. --list --topic foo. Expect a line containing alice + READ + ALLOW.
    let list_out = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--list",
        "--topic",
        "foo",
    ]);
    let listed = String::from_utf8_lossy(&list_out.stdout);
    check!(
        listed.contains("User:alice"),
        "expected alice in --list output; got: {listed}"
    );
    check!(
        listed.to_ascii_uppercase().contains("READ"),
        "expected READ in --list output; got: {listed}"
    );
    check!(
        listed.to_ascii_uppercase().contains("ALLOW"),
        "expected ALLOW in --list output; got: {listed}"
    );

    // 3. --remove --force. Then re-list and assert alice is no longer present.
    docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--remove",
        "--force",
        "--allow-principal",
        "User:alice",
        "--operation",
        "Read",
        "--topic",
        "foo",
    ]);

    let list_out2 = docker_run_kafka_tool_with_image_and_mount(KAFKA_IMAGE_TXN, &mount, &[
        "kafka-acls",
        "--bootstrap-server",
        broker0_advertised(),
        "--command-config",
        "/client.properties",
        "--list",
        "--topic",
        "foo",
    ]);
    let listed2 = String::from_utf8_lossy(&list_out2.stdout);
    assert!(
        !listed2.contains("User:alice"),
        "alice should be gone after --remove; got: {listed2}"
    );

    broker.shutdown().await;
}

// ── the `kafka-acls` shorthands, against the stock broker ───────────────────
//
// `--producer`, `--consumer`, `--idempotent`, `--transactional-id`,
// `--deny-principal` and `--deny-host` are not requests: `AclCommand` expands
// each of them, client-side, into several bindings across several resource
// types, and sends those. So what a broker can be held to is not the flag but
// its consequence -- the set that `--list` reports afterwards -- and the only
// authority on what that set should be is Kafka itself.
//
// Every case below therefore runs the same `kafka-acls` binary, from the same
// `apache/kafka:4.3.1` image, against a stock broker of that release and
// against krabka, and compares the two sets. Each case also states the part of
// the expansion that is certain, so a pair of brokers that both expanded a
// shorthand into nothing at all cannot pass.
//
// Each case owns a principal, and `--list --principal` is what isolates its
// bindings from the ones the cases before it left behind. That flag is under
// test as much as it is a tool here: a broker whose `DescribeAcls` ignored the
// principal filter would answer every case with every case's bindings, and the
// set comparison would fail on the second one.

use {
    crate::{
        acl_output::{AclBinding, parse_acls},
        oracle::{CliRun, Oracle, Side, ToolFile},
    },
    std::collections::BTreeSet,
};

/// The topic every shorthand is scoped to.
const ACL_TOPIC: &str = "krabka-acl-shorthand-topic";
/// The group `--consumer` is scoped to.
const ACL_GROUP: &str = "krabka-acl-shorthand-group";
/// The transactional id `--transactional-id` is scoped to.
const ACL_TXN_ID: &str = "krabka-acl-shorthand-txn";
/// The host `--deny-host` names.
const DENIED_HOST: &str = "10.11.12.13";

/// Where the SASL client configuration is mounted for krabka's half.
const CONTAINER_PROPS: &str = "/tmp/client.properties";

/// One `kafka-acls` invocation on `side`.
///
/// The two halves differ by exactly one thing: krabka's authorizer has nothing
/// to say until a connection carries a principal, so its half authenticates as
/// a PLAIN super-user and the tool needs a client configuration; the oracle's
/// half runs inside the oracle over its own plaintext listener, as the
/// `User:ANONYMOUS` its `super.users` names. Nothing about the bindings
/// differs, which is what makes the two listings comparable.
fn acls(side: &Side<'_>, props: Option<&str>, args: &[&str]) -> CliRun {
    let mut full = vec!["--bootstrap-server", side.bootstrap()];
    full.extend_from_slice(args);
    let mut files = Vec::new();
    if let Some(props) = props {
        full.extend_from_slice(&["--command-config", CONTAINER_PROPS]);
        files.push(ToolFile::new(CONTAINER_PROPS, props));
    }
    side.run_with_files("kafka-acls", &full, &files, None)
}

/// One expected binding, in the four fields that identify it.
type Expected = (&'static str, &'static str, &'static str, &'static str);

/// One shorthand, the principal that owns its bindings, and the part of its
/// expansion that is not in doubt.
struct ShorthandCase {
    label: &'static str,
    /// `User:<name>`. Each case owns one, so `--list --principal` isolates it.
    principal: &'static str,
    /// The `--add` arguments after `--bootstrap-server`.
    add: &'static [&'static str],
    /// `(resource type, resource name, operation, permission)` for the
    /// bindings the expansion certainly contains. It need not be the whole
    /// expansion; the oracle settles the rest.
    must_contain: &'static [Expected],
    /// The host every binding of this case carries.
    host: &'static str,
}

/// Every `kafka-acls` shorthand the tool offers, each on its own principal.
const SHORTHAND_CASES: &[ShorthandCase] = &[
    ShorthandCase {
        label: "--producer",
        principal: "User:krabka-acl-producer",
        add: &[
            "--add",
            "--allow-principal",
            "User:krabka-acl-producer",
            "--producer",
            "--topic",
            ACL_TOPIC,
        ],
        must_contain: &[
            ("TOPIC", ACL_TOPIC, "WRITE", "ALLOW"),
            ("TOPIC", ACL_TOPIC, "DESCRIBE", "ALLOW"),
        ],
        host: "*",
    },
    ShorthandCase {
        label: "--producer --idempotent",
        principal: "User:krabka-acl-idempotent-producer",
        add: &[
            "--add",
            "--allow-principal",
            "User:krabka-acl-idempotent-producer",
            "--producer",
            "--idempotent",
            "--topic",
            ACL_TOPIC,
        ],
        // The cluster binding is the whole point of `--idempotent`: without it
        // the producer cannot claim a producer id, however complete its topic
        // rights are.
        must_contain: &[
            ("TOPIC", ACL_TOPIC, "WRITE", "ALLOW"),
            ("CLUSTER", "kafka-cluster", "IDEMPOTENT_WRITE", "ALLOW"),
        ],
        host: "*",
    },
    ShorthandCase {
        label: "--producer --transactional-id",
        principal: "User:krabka-acl-transactional-producer",
        add: &[
            "--add",
            "--allow-principal",
            "User:krabka-acl-transactional-producer",
            "--producer",
            "--topic",
            ACL_TOPIC,
            "--transactional-id",
            ACL_TXN_ID,
        ],
        must_contain: &[
            ("TOPIC", ACL_TOPIC, "WRITE", "ALLOW"),
            ("TRANSACTIONAL_ID", ACL_TXN_ID, "WRITE", "ALLOW"),
            ("TRANSACTIONAL_ID", ACL_TXN_ID, "DESCRIBE", "ALLOW"),
        ],
        host: "*",
    },
    ShorthandCase {
        label: "--consumer",
        principal: "User:krabka-acl-consumer",
        add: &[
            "--add",
            "--allow-principal",
            "User:krabka-acl-consumer",
            "--consumer",
            "--topic",
            ACL_TOPIC,
            "--group",
            ACL_GROUP,
        ],
        must_contain: &[
            ("TOPIC", ACL_TOPIC, "READ", "ALLOW"),
            ("TOPIC", ACL_TOPIC, "DESCRIBE", "ALLOW"),
            ("GROUP", ACL_GROUP, "READ", "ALLOW"),
        ],
        host: "*",
    },
    ShorthandCase {
        label: "--cluster",
        principal: "User:krabka-acl-cluster",
        add: &[
            "--add",
            "--allow-principal",
            "User:krabka-acl-cluster",
            "--operation",
            "ClusterAction",
            "--cluster",
        ],
        must_contain: &[("CLUSTER", "kafka-cluster", "CLUSTER_ACTION", "ALLOW")],
        host: "*",
    },
    ShorthandCase {
        label: "--deny-principal --deny-host",
        principal: "User:krabka-acl-denied",
        add: &[
            "--add",
            "--deny-principal",
            "User:krabka-acl-denied",
            "--deny-host",
            DENIED_HOST,
            "--operation",
            "Read",
            "--topic",
            ACL_TOPIC,
        ],
        // A deny binding that lost its host would deny that principal from
        // everywhere, which is a far larger outage than the one the operator
        // asked for -- so the host is asserted, not just the permission.
        must_contain: &[("TOPIC", ACL_TOPIC, "READ", "DENY")],
        host: DENIED_HOST,
    },
];

/// The bindings a case certainly produced, in the parser's own shape.
fn expected_bindings(case: &ShorthandCase) -> BTreeSet<AclBinding> {
    case.must_contain
        .iter()
        .map(
            |(resource_type, resource_name, operation, permission)| AclBinding {
                resource_type: (*resource_type).to_owned(),
                resource_name: (*resource_name).to_owned(),
                pattern_type: "LITERAL".to_owned(),
                principal: case.principal.to_owned(),
                host: case.host.to_owned(),
                operation: (*operation).to_owned(),
                permission: (*permission).to_owned(),
            },
        )
        .collect()
}

/// Every `kafka-acls` shorthand expands to the same binding set on krabka as
/// on Apache Kafka, and `--list --principal` reports exactly that set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn acl_shorthands_expand_as_apache_kafka_expands_them() {
    // Kafka first. Every expectation below is a claim about somebody else's
    // client, and this is where a wrong one is reported as such.
    let oracle = tokio::task::spawn_blocking(|| {
        Oracle::start_with_env("acls", &[
            "KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
            // The tool runs inside the container over the plaintext
            // listener, so it arrives as `ANONYMOUS`. Naming it a super
            // user is what lets it administer ACLs; it changes nothing
            // about the bindings it then creates.
            "KAFKA_SUPER_USERS=User:ANONYMOUS",
            "KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND=false",
        ])
    })
    .await
    .expect("oracle boot");
    let oracle_side = Side::Oracle(&oracle);

    let (broker, _dir) =
        start_sasl_plaintext_broker_with_super_user(ADMIN, &[(ADMIN, ADMIN_PASS)]).await;
    nc_check_connectivity();
    let advertised = broker0_advertised().to_owned();
    let krabka_side = Side::Krabka {
        bootstrap: &advertised,
    };
    let admin_props = plain_client_properties(ADMIN, ADMIN_PASS);

    for case in SHORTHAND_CASES {
        let mut listings: Vec<BTreeSet<AclBinding>> = Vec::new();
        for (side, props) in secured_sides(&oracle_side, &krabka_side, &admin_props) {
            let added = acls(side, props, case.add);
            assert!(
                added.succeeded(),
                "{}: {} was refused:\n{}",
                side.label(),
                case.label,
                added.text(),
            );

            let listed = acls(side, props, &["--list", "--principal", case.principal]);
            assert!(
                listed.succeeded(),
                "{}: --list --principal after {} failed:\n{}",
                side.label(),
                case.label,
                listed.text(),
            );
            let bindings = parse_acls(&listed.stdout);
            let expected = expected_bindings(case);
            assert!(
                expected.is_subset(&bindings),
                "{}: {} must expand to at least {expected:?}, got {bindings:?}\n{}",
                side.label(),
                case.label,
                listed.stdout,
            );
            assert!(
                bindings.iter().all(|b| b.principal == case.principal),
                "{}: --list --principal {} returned another principal's bindings: {bindings:?}",
                side.label(),
                case.principal,
            );
            listings.push(bindings);
        }
        assert!(
            listings[0] == listings[1],
            "{}: krabka and Apache Kafka expanded it differently: {listings:?}",
            case.label,
        );
    }

    // The removal half, so the suite does not certify a broker that accepts
    // every `--add` and forgets none of them. One case is enough: `--remove`
    // takes the same expansion the `--add` took, so a shorthand that removed
    // the wrong set would leave a difference the listing reports.
    let removing = SHORTHAND_CASES
        .iter()
        .find(|case| case.label == "--consumer")
        .expect("the --consumer case");
    let mut remaining: Vec<BTreeSet<AclBinding>> = Vec::new();
    for (side, props) in secured_sides(&oracle_side, &krabka_side, &admin_props) {
        let mut remove: Vec<&str> = vec!["--remove", "--force"];
        remove.extend(removing.add.iter().skip(1).copied());
        acls(side, props, &remove).expect_success();
        let listed =
            acls(side, props, &["--list", "--principal", removing.principal]).expect_success();
        remaining.push(parse_acls(&listed.stdout));
    }
    assert!(
        remaining[0].is_empty() && remaining[1] == remaining[0],
        "--remove must undo exactly what --consumer added: {remaining:?}",
    );

    broker.shutdown().await;
}
