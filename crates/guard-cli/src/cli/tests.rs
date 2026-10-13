//! Tests for the command line: which lines the parser accepts, and what a flag
//! group resolves to once it has.

use {
    super::*,
    assert2::{assert, check},
    krabka_protocol::krabka::freeze::PATTERN_TYPE_ANY,
    krabka_units::convert::TimeExt as _,
};

krabka_macros::duration_parser_fixture!(
    {
        /// A time argument takes any unit the broker's own configuration takes, so
        /// an operator never has to convert to milliseconds by hand, and a number
        /// with no unit is refused rather than guessed at.
    },
    [
        ("30m", Some(1_800_000)),
        ("1h", Some(3_600_000)),
        ("1 hour", Some(3_600_000)),
        // A unit is required, so a number alone cannot be read as the
        // wrong scale. Zero is the one exemption, having no scale.
        ("30", None),
        ("0", Some(0)),
        ("banana", None),
        ("", None),
    ]
);

krabka_macros::bootstrap_parser_fixture!(["krabka-guard", "freeze", "list"], [
    "krabka-guard",
    "-b",
    "localhost:9092",
    "freeze",
    "list"
]);

/// `--topic` and `--prefix` name the two pattern types, and exactly one of
/// them is given. A freeze with neither would have no scope, and a freeze
/// with both would have two.
#[test]
fn a_freeze_names_exactly_one_scope() {
    let base = ["krabka-guard", "-b", "localhost:9092", "freeze", "set"];
    let reason = ["--reason", "DR cutover"];

    let literal = Cli::try_parse_from(
        base.iter()
            .chain(["--topic", "orders"].iter())
            .chain(reason.iter()),
    )
    .expect("a literal scope parses");
    check!(freeze_scope(literal) == Ok(scope("orders", PATTERN_TYPE_LITERAL)));

    let prefixed = Cli::try_parse_from(
        base.iter()
            .chain(["--prefix", "tenant-a."].iter())
            .chain(reason.iter()),
    )
    .expect("a prefixed scope parses");
    check!(freeze_scope(prefixed) == Ok(scope("tenant-a.", PATTERN_TYPE_PREFIXED)));

    assert!(
        Cli::try_parse_from(base.iter().chain(reason.iter())).is_err(),
        "a freeze with no scope is refused"
    );
    assert!(
        Cli::try_parse_from(
            base.iter()
                .chain(["--topic", "orders", "--prefix", "tenant-a."].iter())
                .chain(reason.iter())
        )
        .is_err(),
        "a freeze with two scopes is refused"
    );
}

/// The three signing flags travel together. A key file with no key id
/// would produce a signature the broker cannot look up, and a key id with
/// no key file would name a signature that was never made.
#[test]
fn the_signing_flags_travel_together() {
    let base = [
        "krabka-guard",
        "-b",
        "localhost:9092",
        "freeze",
        "set",
        "--topic",
        "orders",
        "--reason",
        "DR cutover",
    ];
    let cases: [(&'static str, Vec<&'static str>, bool); 5] = [
        ("no signature at all", vec![], true),
        (
            "all three flags",
            vec![
                "--sign-with",
                "key.pk8",
                "--key-id",
                "alice-yubi",
                "--principal",
                "User:alice",
            ],
            true,
        ),
        (
            "a key file with no key id",
            vec!["--sign-with", "key.pk8", "--principal", "User:alice"],
            false,
        ),
        (
            "a key file with no principal",
            vec!["--sign-with", "key.pk8", "--key-id", "alice-yubi"],
            false,
        ),
        (
            "a key id with no key file",
            vec!["--key-id", "alice-yubi"],
            false,
        ),
    ];
    for (case, extra, parses) in cases {
        let line = base.iter().copied().chain(extra);
        check!(Cli::try_parse_from(line).is_ok() == parses, "{case}");
    }
}

/// `--verify-signatures` with no key file would check the registry against
/// an empty trust set, which is a check that silently does nothing.
#[test]
fn verifying_signatures_needs_a_local_key_file() {
    let base = ["krabka-guard", "-b", "localhost:9092", "freeze", "list"];
    assert!(
        Cli::try_parse_from(base.iter().chain(["--verify-signatures"].iter())).is_err(),
        "verifying with no key file is refused"
    );
    assert!(
        Cli::try_parse_from(
            base.iter()
                .chain(["--verify-signatures", "--operator-keys", "keys.toml"].iter())
        )
        .is_ok(),
        "verifying with a key file parses"
    );
}

/// The break-glass action names reach the wire as the values the broker's
/// own action type carries, and no action takes the zero that a default
/// request holds.
#[test]
fn every_action_carries_its_own_wire_value_and_name() {
    let cases: [(&'static str, Action, i8, &'static str); 7] = [
        ("thaw", Action::ThawTopicFreeze, 1, "thaw_topic_freeze"),
        (
            "unclean election",
            Action::UncleanElectLeaders,
            2,
            "unclean_elect_leaders",
        ),
        (
            "unclean recovery",
            Action::UncleanRecovery,
            3,
            "unclean_recovery",
        ),
        (
            "unregister broker",
            Action::UnregisterBroker,
            4,
            "unregister_broker",
        ),
        (
            "cancel reassignment",
            Action::CancelReassignment,
            5,
            "cancel_reassignment",
        ),
        ("delete topic", Action::DeleteTopic, 6, "delete_topic"),
        ("delete records", Action::DeleteRecords, 7, "delete_records"),
    ];
    for (case, action, wire, name) in cases {
        check!(action.wire() == wire, "{case}");
        check!(action_name(wire) == name, "{case}");
    }
    check!(action_name(0) == "unknown");
    check!(action_name(8) == "unknown");
}

/// The command line spells an action with dashes, which is what the
/// documented runbook types.
#[test]
fn an_action_is_spelled_with_dashes_on_the_command_line() {
    let cli = Cli::try_parse_from([
        "krabka-guard",
        "-b",
        "localhost:9092",
        "break-glass",
        "propose",
        "--action",
        "delete-topic",
        "--target",
        "doomed",
        "--reason",
        "test data only",
        "--ttl",
        "30m",
    ])
    .expect("parses");
    let Command::BreakGlass {
        command: BreakGlassCommand::Propose { action, ttl, .. },
    } = cli.command
    else {
        panic!("expected a propose");
    };
    check!(action == Action::DeleteTopic);
    check!(ttl.map(Time::millis_i64) == Some(1_800_000));
}

#[test]
fn a_pattern_type_reads_as_a_word() {
    check!(pattern_name(PATTERN_TYPE_LITERAL) == "literal");
    check!(pattern_name(PATTERN_TYPE_PREFIXED) == "prefixed");
    check!(pattern_name(PATTERN_TYPE_ANY) == "unknown");
}

/// A scope with neither half is unreachable through the parser, and the
/// resolver still refuses it rather than falling back to an empty prefix,
/// which would name every topic in the cluster.
#[test]
fn a_scope_with_neither_half_is_refused_rather_than_defaulted() {
    let empty = ScopeArgs {
        topic: None,
        prefix: None,
    };
    check!(
        empty.resolve()
            == Err(Failure::Refused(
                "name exactly one of --topic and --prefix".to_owned()
            ))
    );
}

/// A key file with no key id is unreachable through the parser, and the
/// resolver still refuses it rather than signing under an empty key id.
#[test]
fn signing_material_with_a_missing_half_is_refused() {
    let half = FreezeSigningArgs {
        sign_with: Some(PathBuf::from("key.pk8")),
        key_id: None,
        principal: Some("User:alice".to_owned()),
    };
    check!(half.resolve().is_err());

    let none = FreezeSigningArgs {
        sign_with: None,
        key_id: None,
        principal: None,
    };
    check!(none.resolve() == Ok(None));
}

/// The scope a parsed `freeze set` names.
fn freeze_scope(cli: Cli) -> Result<Scope, Failure> {
    let Command::Freeze {
        command: FreezeCommand::Set { scope, .. },
    } = cli.command
    else {
        panic!("expected a freeze set");
    };
    scope.resolve()
}

fn scope(name: &str, pattern_type: i8) -> Scope {
    Scope {
        name: name.to_owned(),
        pattern_type,
    }
}
