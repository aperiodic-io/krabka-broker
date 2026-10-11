//! `kafka-acls --list` against a broker that has no authorizer.
//!
//! Kafka answers `DescribeAcls`, `CreateAcls` and `DeleteAcls` with
//! `SECURITY_DISABLED` (54) and "No Authorizer is configured on the broker"
//! whenever `authorizer.class.name` is unset, and krabka's `allow_all`
//! authorizer -- the
//! default, and not a decision point -- is that same state. An empty listing
//! would be the dangerous answer here: it tells an operator that the cluster
//! has no ACLs, when the truth is that it would never consult one.
//!
//! What the tool prints for that refusal is Kafka's business, so this case
//! states none of it. It runs the same `apache/kafka:4.3.1` `kafka-acls`
//! against a stock broker of that release with no authorizer configured and
//! against krabka with its default one, and holds the two renderings of the
//! error against each other. The one claim it makes on its own is the part
//! that cannot be in doubt: the refusal both sides print names
//! `SecurityDisabledException` and carries Kafka's message, so a pair of
//! brokers that both answered an empty listing cannot pass.
//!
//! Both brokers are plaintext and neither has a principal to authenticate, so
//! unlike the rest of this suite the invocation needs no client configuration.

use std::collections::BTreeSet;

use assert2::assert;

use crate::{
    acl_output::parse_acls,
    oracle::{CliRun, OracleComparison},
};

/// The package every Kafka client-facing exception is rendered under. The
/// tool's stack trace names the class by this path, on both sides.
const ERRORS_PACKAGE: &str = "org.apache.kafka.common.errors.";

/// The refusal both brokers owe an operator who administers ACLs on a cluster
/// that has no authorizer to administer them for.
const SECURITY_DISABLED: &str =
    "SecurityDisabledException: No Authorizer is configured on the broker";

/// Every Kafka exception one run rendered, as a set.
///
/// A rendering carries the bootstrap address, the tool's own timings and a
/// stack trace whose frames differ between a broker in a container and one on
/// the host, so the two sides' output is not comparable as text. The
/// exceptions it names are: each line is taken from the class name to its end,
/// which is the class and the message the broker sent. Stack frames are
/// dropped -- they say where the client was, not what the broker answered.
fn kafka_errors(run: &CliRun) -> BTreeSet<String> {
    run.text()
        .lines()
        .map(str::trim)
        .filter(|line| !line.starts_with("at "))
        .filter_map(|line| line.find(ERRORS_PACKAGE).map(|at| line[at..].to_owned()))
        .collect()
}

/// `kafka-acls --list` reports the same refusal against krabka with no
/// authorizer as against Apache Kafka with none.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires Docker"]
async fn acl_list_is_refused_as_apache_kafka_refuses_it_with_no_authorizer() {
    // No `KAFKA_AUTHORIZER_CLASS_NAME`: this oracle is the unsecured broker
    // the case is about, which is also the image's own default.
    let comparison = OracleComparison::start("acls-disabled").await;
    let [oracle_side, krabka_side] = comparison.sides();

    let mut refusals: Vec<BTreeSet<String>> = Vec::new();
    for side in [&oracle_side, &krabka_side] {
        let listed = side.run(
            "kafka-acls",
            &["--bootstrap-server", side.bootstrap(), "--list"],
        );
        assert!(
            !listed.succeeded(),
            "{}: --list must be refused when no authorizer is configured:\n{}",
            side.label(),
            listed.text(),
        );
        assert!(
            parse_acls(&listed.stdout).is_empty(),
            "{}: a refused --list must print no bindings:\n{}",
            side.label(),
            listed.stdout,
        );
        let errors = kafka_errors(&listed);
        assert!(
            errors
                .iter()
                .any(|error| error == &format!("{ERRORS_PACKAGE}{SECURITY_DISABLED}")),
            "{}: the refusal must name {SECURITY_DISABLED}, got {errors:?}\n{}",
            side.label(),
            listed.text(),
        );
        refusals.push(errors);
    }
    assert!(
        refusals[0] == refusals[1],
        "krabka and Apache Kafka refuse --list differently: {refusals:?}",
    );

    comparison.broker.shutdown().await;
}
