//! The generated timing methods preserve borrowing, bounds and unrelated fields.

use std::{borrow::Cow, collections::BTreeMap, time::Duration};

mod coordinator {
    pub(crate) mod unified {
        pub(crate) mod config {
            use std::{collections::BTreeMap, time::Duration};

            pub(crate) const MIN_ASSIGNMENT_INTERVAL: Duration = Duration::ZERO;
            pub(crate) const MAX_ASSIGNMENT_INTERVAL: Duration = Duration::from_secs(15);

            pub(crate) fn group_millis(
                overrides: Option<&BTreeMap<String, String>>,
                key: &str,
                min: Duration,
                max: Duration,
            ) -> Option<Duration> {
                overrides
                    .and_then(|values| values.get(key))
                    .and_then(|text| text.trim().parse::<i32>().ok())
                    .map(|millis| Duration::from_millis(u64::try_from(millis).unwrap_or(0)))
                    .map(|duration| duration.clamp(min, max))
            }
        }
    }
}

const SESSION: &str = "consumer.session.timeout.ms";
const HEARTBEAT: &str = "consumer.heartbeat.interval.ms";
const ASSIGNMENT: &str = "consumer.assignment.interval.ms";

#[krabka_macros::membership_defaults]
#[derive(
    Debug, Clone, PartialEq, krabka_macros::FieldDefaults, krabka_macros::GroupTimingConfig,
)]
#[group_timing(session = SESSION, heartbeat = HEARTBEAT, assignment = ASSIGNMENT)]
struct Config {
    session_timeout: Duration,
    heartbeat_interval: Duration,
    #[default(Duration::from_secs(1))]
    assignment_interval: Duration,
    min_session_timeout: Duration,
    max_session_timeout: Duration,
    min_heartbeat_interval: Duration,
    max_heartbeat_interval: Duration,
    #[default(77)]
    unrelated: u32,
}

struct Override<'a> {
    key: &'a str,
    value: &'a str,
}

fn overrides(entries: &[Override<'_>]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|entry| (entry.key.into(), entry.value.into()))
        .collect()
}

#[yare::parameterized(absent = { "absent" }, empty = { "empty" }, ignored = { "ignored" })]
fn absent_invalid_and_other_protocol_timings_borrow_the_original(kind: &str) {
    let config = Config::default();
    let ignored = overrides(&[
        Override {
            key: "share.session.timeout.ms",
            value: "50000",
        },
        Override {
            key: HEARTBEAT,
            value: "invalid",
        },
    ]);
    let empty = BTreeMap::new();
    let values = match kind {
        "absent" => None,
        "empty" => Some(&empty),
        "ignored" => Some(&ignored),
        _ => unreachable!(),
    };
    let Cow::Borrowed(actual) = config.for_group(values) else {
        panic!("no valid timing overrides must borrow the broker defaults")
    };
    assert2::assert!(std::ptr::eq(actual, &raw const config));
}

#[yare::parameterized(
    session_45 = { vec![Override { key: SESSION, value: "45000" }], 45, 5, 1 },
    session_50 = { vec![Override { key: SESSION, value: "50000" }], 50, 5, 1 },
    heartbeat = { vec![Override { key: HEARTBEAT, value: "7000" }], 45, 7, 1 },
    assignment = { vec![Override { key: ASSIGNMENT, value: "0" }], 45, 5, 0 },
    multiple_clamped = {
        vec![
            Override {
                key: SESSION,
                value: "0",
            },
            Override {
                key: HEARTBEAT,
                value: "20000",
            },
            Override {
                key: ASSIGNMENT,
                value: "20000",
            },
        ],
        45,
        15,
        15,
    },
)]
fn applies_each_timing_and_keeps_unrelated_configuration(
    entries: Vec<Override<'static>>,
    session: u64,
    heartbeat: u64,
    assignment: u64,
) {
    let config = Config::default();
    let values = overrides(&entries);
    let Cow::Owned(actual) = config.for_group(Some(&values)) else {
        panic!("a valid override must create a configuration")
    };
    assert2::assert!(
        actual
            == Config {
                session_timeout: Duration::from_secs(session),
                heartbeat_interval: Duration::from_secs(heartbeat),
                assignment_interval: Duration::from_secs(assignment),
                ..config.clone()
            }
    );
    assert2::assert!(config == Config::default());
    assert2::assert!(
        Config::assigning_at_once()
            == Config {
                assignment_interval: Duration::ZERO,
                ..config
            }
    );
}
