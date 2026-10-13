//! Tests for the privileged-action sections: the TOML round trips of
//! `[[operator_keys]]`, `[freeze]` and `[break_glass]`, and the apply step's
//! per-section validation plus the two rules that cross those sections.

use {
    super::{FileBreakGlassConfig, FileFreezeConfig, FileOperatorKey},
    crate::{
        break_glass::{ALL_ACTIONS, action_name},
        config::{BackgroundUncleanRecovery, BrokerConfig},
        file_config::{FileConfig, FileConfigError, test_support::configured},
    },
    assert2::{assert, check},
    krabka_units::{hours, minutes, secs},
    tempfile::TempDir,
};

fn check_operator_key_refusal(file: FileConfig, cfg: &mut BrokerConfig, case: &str) {
    assert!(let Err(error) = file.apply_to(cfg), "case {case}");
    check!(
        matches!(error, FileConfigError::OperatorKeys(_)),
        "case {case}"
    );
}

// A well-formed 32-byte Ed25519 public key file plus the TOML
// `[[operator_keys]]` entry that points at it. The bytes never verify a
// signature here; only the length is checked at load.
fn operator_key_fixture(dir: &TempDir, key_id: &str, principal: &str) -> String {
    let path = dir.path().join(format!("{key_id}.pub"));
    std::fs::write(&path, [7u8; 32]).expect("write operator key file");
    format!(
        "[[operator_keys]]\nkey_id = {key_id:?}\nprincipal = {principal:?}\n\
         public_key_path = {:?}\n",
        path.display().to_string()
    )
}

#[test]
fn operator_keys_section_round_trips_every_key() {
    let toml = r#"
[[operator_keys]]
key_id = "alice-yubi"
principal = "User:alice"
public_key_path = "/etc/krabka/operator-keys/alice.pub"

[[operator_keys]]
key_id = "bob-yubi"
principal = "User:bob"
public_key_path = "/etc/krabka/operator-keys/bob.pub"
"#;
    let file: FileConfig = toml::from_str(toml).expect("parse operator_keys section");

    let expected = vec![
        FileOperatorKey {
            key_id: "alice-yubi".to_owned(),
            principal: "User:alice".to_owned(),
            public_key_path: "/etc/krabka/operator-keys/alice.pub".to_owned(),
        },
        FileOperatorKey {
            key_id: "bob-yubi".to_owned(),
            principal: "User:bob".to_owned(),
            public_key_path: "/etc/krabka/operator-keys/bob.pub".to_owned(),
        },
    ];
    assert!(file.operator_keys == expected);
}

#[test]
fn operator_keys_section_rejects_a_misspelled_key() {
    // `deny_unknown_fields`: an ignored `principal` typo would leave the
    // key bound to nobody, and the binding is what stops one operator's
    // key signing in another operator's name.
    let toml = r#"
[[operator_keys]]
key_id = "alice-yubi"
principle = "User:alice"
public_key_path = "/etc/krabka/operator-keys/alice.pub"
"#;
    assert!(toml::from_str::<FileConfig>(toml).is_err());
}

#[test]
fn freeze_section_round_trips_every_key() {
    let toml = r#"
[freeze]
max_entries = 1000
require_signature = false
signature_max_skew = "5m"
"#;
    let file: FileConfig = toml::from_str(toml).expect("parse freeze section");

    let expected = FileFreezeConfig {
        max_entries: Some(1_000),
        require_signature: Some(false),
        signature_max_skew: Some(minutes(5)),
    };
    assert!(file.freeze == Some(expected));
}

#[test]
fn freeze_section_rejects_a_misspelled_key() {
    let toml = r"
[freeze]
require_signatures = true
";
    assert!(toml::from_str::<FileConfig>(toml).is_err());
}

#[test]
fn freeze_signature_max_skew_rejects_a_bare_number() {
    // The human duration serde is what keeps `5` from meaning
    // five of whatever unit the reader assumed.
    let toml = r"
[freeze]
signature_max_skew = 300
";
    assert!(toml::from_str::<FileConfig>(toml).is_err());
}

#[test]
fn break_glass_section_round_trips_every_key() {
    let toml = r#"
[break_glass]
approvers = ["User:alice", "User:bob", "User:carol"]
required_approvals = 2
proposal_ttl = "30m"
signed_actions = ["unclean_elect_leaders", "unclean_recovery", "delete_topic"]
background_unclean_recovery = "audit-only"
"#;
    let file: FileConfig = toml::from_str(toml).expect("parse break_glass section");

    let expected = FileBreakGlassConfig {
        approvers: Some(vec![
            "User:alice".to_owned(),
            "User:bob".to_owned(),
            "User:carol".to_owned(),
        ]),
        required_approvals: Some(2),
        proposal_ttl: Some(minutes(30)),
        signed_actions: Some(vec![
            "unclean_elect_leaders".to_owned(),
            "unclean_recovery".to_owned(),
            "delete_topic".to_owned(),
        ]),
        background_unclean_recovery: Some(BackgroundUncleanRecovery::AuditOnly),
    };
    assert!(file.break_glass == Some(expected));
}

#[test]
fn break_glass_section_rejects_a_misspelled_key() {
    let toml = r#"
[break_glass]
approvers = ["User:alice"]
required_approval = 2
"#;
    assert!(toml::from_str::<FileConfig>(toml).is_err());
}

#[test]
fn background_unclean_recovery_accepts_exactly_three_spellings() {
    for (name, spelling, expected) in [
        ("off", "off", Some(BackgroundUncleanRecovery::Off)),
        (
            "audit-only",
            "audit-only",
            Some(BackgroundUncleanRecovery::AuditOnly),
        ),
        (
            "require",
            "require",
            Some(BackgroundUncleanRecovery::Require),
        ),
        ("a fourth spelling", "audit_only", None),
        ("a mode that does not exist", "warn", None),
    ] {
        let toml = format!(
            "[break_glass]\nsigned_actions = []\nbackground_unclean_recovery = {spelling:?}\n"
        );
        let parsed = toml::from_str::<FileConfig>(&toml)
            .ok()
            .and_then(|file| file.break_glass)
            .and_then(|section| section.background_unclean_recovery);
        check!(parsed == expected, "case {name}");
    }
}

/// Apply privileged-action sections beside the fixture's Alice operator key.
fn configured_privileged_actions(directory: &TempDir, sections: &str) -> BrokerConfig {
    let source = format!(
        "{}\n{sections}",
        operator_key_fixture(directory, "alice-yubi", "User:alice")
    );
    configured(&source, "parse config").expect("apply config")
}

#[test]
fn freeze_and_break_glass_sections_apply_their_documented_defaults() {
    let dir = TempDir::new().expect("tempdir");
    let cfg = configured_privileged_actions(
        &dir,
        "[freeze]\n[break_glass]\napprovers = [\"User:alice\", \"User:bob\"]\n",
    );

    check!(cfg.freeze == crate::config::FreezeConfig::default());
    check!(
        cfg.break_glass
            == crate::config::BreakGlassConfig {
                approvers: vec!["User:alice".to_owned(), "User:bob".to_owned()],
                signed_actions: vec![
                    "unclean_elect_leaders".to_owned(),
                    "unclean_recovery".to_owned(),
                    "delete_topic".to_owned(),
                ],
                ..crate::config::BreakGlassConfig::default()
            }
    );
    check!(cfg.operator_keys.len() == 1);
    check!(
        cfg.operator_keys
            .get("alice-yubi")
            .map(crate::operator_keys::OperatorKey::principal)
            == Some("User:alice")
    );
}

#[test]
fn absent_privileged_action_sections_retain_the_broker_defaults() {
    let cfg = configured("", "parse empty config").expect("apply empty config");

    check!(cfg.operator_keys.is_empty());
    check!(cfg.freeze == crate::config::FreezeConfig::default());
    check!(cfg.break_glass == crate::config::BreakGlassConfig::default());
    check!(cfg.break_glass.signed_actions.is_empty());
}

#[test]
fn freeze_and_break_glass_values_replace_the_broker_defaults() {
    let dir = TempDir::new().expect("tempdir");
    let cfg = configured_privileged_actions(
        &dir,
        "[freeze]\nmax_entries = 25\nrequire_signature = true\n\
         signature_max_skew = \"90s\"\n\
         [break_glass]\napprovers = [\"User:alice\", \"User:bob\", \"User:carol\"]\n\
         required_approvals = 3\nproposal_ttl = \"2h\"\n\
         signed_actions = [\"delete_topic\"]\nbackground_unclean_recovery = \"require\"\n",
    );

    check!(
        cfg.freeze
            == crate::config::FreezeConfig {
                max_entries: 25,
                require_signature: true,
                signature_max_skew: secs(90),
            }
    );
    check!(
        cfg.break_glass
            == crate::config::BreakGlassConfig {
                approvers: vec![
                    "User:alice".to_owned(),
                    "User:bob".to_owned(),
                    "User:carol".to_owned(),
                ],
                required_approvals: 3,
                proposal_ttl: hours(2),
                signed_actions: vec!["delete_topic".to_owned()],
                background_unclean_recovery: BackgroundUncleanRecovery::Require,
            }
    );
}

#[test]
fn break_glass_required_approvals_below_two_is_a_config_error() {
    let dir = TempDir::new().expect("tempdir");
    let keys = operator_key_fixture(&dir, "alice-yubi", "User:alice");
    for (name, required, accepted) in [
        ("no approvals at all", 0_usize, false),
        ("a one-person two-person rule", 1, false),
        ("the documented minimum", 2, true),
        ("three of five", 3, true),
    ] {
        let toml = format!(
            "{keys}\n[break_glass]\napprovers = [\"User:alice\"]\n\
             required_approvals = {required}\n"
        );
        let file: FileConfig = toml::from_str(&toml).expect("parse config");
        let mut cfg = BrokerConfig::default();

        let outcome = file.apply_to(&mut cfg);

        check!(outcome.is_ok() == accepted, "case {name}");
        if accepted {
            check!(
                cfg.break_glass.required_approvals == required,
                "case {name}"
            );
        } else {
            assert!(let Err(error) = outcome, "case {name}");
            check!(
                matches!(error, FileConfigError::InvalidConfig(_)),
                "case {name}"
            );
        }
    }
}

#[test]
fn freeze_max_entries_of_zero_is_a_config_error() {
    let file: FileConfig =
        toml::from_str("[freeze]\nmax_entries = 0\n").expect("parse freeze section");
    let mut cfg = BrokerConfig::default();

    let error = file
        .apply_to(&mut cfg)
        .expect_err("a registry that holds nothing must be rejected");

    assert!(matches!(error, FileConfigError::InvalidConfig(_)));
}

#[test]
fn demanding_a_signature_with_no_operator_key_is_a_startup_error() {
    // Both rules exist so the refusal happens at boot with an explanation,
    // not at run time on every request with none.
    for (name, toml) in [
        (
            "signed_actions names an action",
            "[break_glass]\napprovers = [\"User:alice\"]\n\
             signed_actions = [\"delete_topic\"]\n",
        ),
        (
            "signed_actions defaults to the irreversible set",
            "[break_glass]\napprovers = [\"User:alice\"]\n",
        ),
        (
            "freeze.require_signature is on",
            "[freeze]\nrequire_signature = true\n",
        ),
    ] {
        let file: FileConfig = toml::from_str(toml).expect("parse config");
        let mut cfg = BrokerConfig::default();

        check_operator_key_refusal(file, &mut cfg, name);
    }
}

#[test]
fn an_empty_signed_actions_list_needs_no_operator_key() {
    // The explicit opt-out. It is distinct from omitting the key, which
    // selects the irreversible set.
    let cfg = configured(
        "[break_glass]\napprovers = [\"User:alice\"]\nsigned_actions = []\n",
        "parse break_glass section",
    )
    .expect("apply break_glass section");

    check!(cfg.break_glass.signed_actions.is_empty());
    check!(cfg.operator_keys.is_empty());
}

#[test]
fn a_signed_action_that_names_no_action_is_a_startup_error() {
    // A name that matches no action demands a signature for nothing. The
    // operator believes the action is protected and it is not, so the
    // broker refuses to boot rather than run the downgrade silently.
    for (label, spelling) in [
        ("a plural misspelling", "delete_topics"),
        ("a hyphenated spelling", "delete-topic"),
        ("a capitalised spelling", "Delete_Topic"),
        ("the wire spelling", "DeleteTopic"),
        ("an invented action", "reformat_cluster"),
    ] {
        // No `[[operator_keys]]`. The name check runs inside the
        // `[break_glass]` block, ahead of the rule that a demanded
        // signature needs a key to verify it, so the refusal here is the
        // name one and the assertions below can say so.
        let file: FileConfig =
            toml::from_str(&format!("[break_glass]\nsigned_actions = [{spelling:?}]\n"))
                .expect("parse break_glass section");
        let mut cfg = BrokerConfig::default();

        let result = file.apply_to(&mut cfg);

        assert!(let Err(_) = &result, "case {label}");
        let message = result.expect_err("refusal").to_string();
        check!(message.contains("signed_actions"), "case {label}");
        check!(message.contains(spelling), "case {label}");
        check!(message.contains("delete_topic"), "case {label}");
    }
}

#[test]
fn every_signed_action_spelling_the_broker_accepts_is_a_real_action() {
    // The default set and each name in turn. This is the positive half of
    // the check above: the validation must not refuse a correct spelling.
    let mut names = vec![crate::config::DEFAULT_BREAK_GLASS_SIGNED_ACTIONS.join(",")];
    names.extend(
        ALL_ACTIONS
            .into_iter()
            .map(|action| action_name(action).to_owned()),
    );
    for name in names {
        let list = name
            .split(',')
            .map(|one| format!("{one:?}"))
            .collect::<Vec<_>>()
            .join(", ");
        let file: FileConfig =
            toml::from_str(&format!("[break_glass]\nsigned_actions = [{list}]\n"))
                .expect("parse break_glass section");
        let mut cfg = BrokerConfig::default();

        let result = file.apply_to(&mut cfg);

        // With no operator key configured, a correct spelling still
        // refuses, because a demanded signature needs a key to verify it.
        // That refusal is fine here; the name refusal is not.
        if let Err(error) = result {
            check!(
                !error.to_string().contains("names no break-glass action"),
                "{name}"
            );
        }
    }
}

#[test]
fn an_unloadable_operator_key_is_a_startup_error() {
    let dir = TempDir::new().expect("tempdir");
    let good = dir.path().join("alice.pub");
    std::fs::write(&good, [7u8; 32]).expect("write good key");
    let short = dir.path().join("short.pub");
    std::fs::write(&short, [7u8; 31]).expect("write short key");
    let missing = dir.path().join("absent.pub");
    let entry = |key_id: &str, principal: &str, path: &std::path::Path| {
        format!(
            "[[operator_keys]]\nkey_id = {key_id:?}\nprincipal = {principal:?}\n\
             public_key_path = {:?}\n",
            path.display().to_string()
        )
    };

    for (name, toml) in [
        (
            "an unreadable public_key_path",
            entry("alice-yubi", "User:alice", &missing),
        ),
        (
            "an ill-formed Ed25519 public key",
            entry("alice-yubi", "User:alice", &short),
        ),
        (
            "a duplicate key_id",
            format!(
                "{}{}",
                entry("alice-yubi", "User:alice", &good),
                entry("alice-yubi", "User:bob", &good)
            ),
        ),
        (
            "a duplicate principal",
            format!(
                "{}{}",
                entry("alice-yubi", "User:alice", &good),
                entry("alice-backup", "User:alice", &good)
            ),
        ),
    ] {
        let file: FileConfig = toml::from_str(&toml).expect("parse operator_keys section");
        let mut cfg = BrokerConfig::default();

        check_operator_key_refusal(file, &mut cfg, name);
        check!(cfg.operator_keys.is_empty(), "case {name}");
    }
}
