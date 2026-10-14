//! The dedup topics of a run and the churn that reconfigures them.
//!
//! [`SLOTS`] topics exist at any time. After each cycle's idle drain the churn
//! changes one of them, in a fixed rotation, so that over a run every kind of
//! change lands on every slot:
//!
//! | `cycle % 4` | change |
//! | --- | --- |
//! | 0 | delete the slot's topic and create a new one, the identity rule flipped |
//! | 1 | turn the slot's dedup off, or back on |
//! | 2 | move the slot's window to the next of [`WINDOWS_MS`] |
//! | 3 | flip the slot's identity rule between the key and the header |
//!
//! and, when [`Settings::restart_every`] is set, every so many cycles the
//! churn also restarts [`RESTARTED_NODE`](crate::settings::RESTARTED_NODE).
//!
//! [`Settings::restart_every`]: crate::settings::Settings::restart_every

use {
    crate::{
        settings::{RETENTION_MS, SEGMENT_MS, SLOTS, WINDOWS_MS},
        support::dedup::DEDUP_HEADER,
        workload::{Identity, Rule},
    },
    krabka_protocol::primitives::uuid::Uuid,
    std::fmt,
};

/// The topic config key that turns dedup on and sets its window.
pub const WINDOW_KEY: &str = "dedup.window.ms";

/// The topic config key that makes a header the identity.
pub const HEADER_KEY: &str = "dedup.key.header";

/// The bytes after which a soak topic's segment rolls whatever its age.
const SEGMENT_BYTES: u64 = 1024 * 1024;

/// How long a deleted segment's files wait before they are unlinked.
const FILE_DELETE_DELAY_MS: i64 = 1_000;

/// The configs every soak topic carries besides its dedup settings: segments
/// that roll every few seconds and retention that deletes them soon after, so
/// a run of hours keeps a few hundred MiB on disk.
fn storage_configs() -> [(&'static str, String); 4] {
    [
        ("retention.ms", RETENTION_MS.to_string()),
        ("segment.ms", SEGMENT_MS.to_string()),
        ("internal.segment.bytes", SEGMENT_BYTES.to_string()),
        ("file.delete.delay.ms", FILE_DELETE_DELAY_MS.to_string()),
    ]
}

/// One topic of the run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoakTopic {
    pub slot: usize,
    pub generation: usize,
    pub id: Uuid,
    pub identity: Identity,
    pub window_ms: i64,
    pub dedup_on: bool,
}

impl SoakTopic {
    /// The first topic of `slot`: the even slots deduplicate by key, the odd
    /// ones by header, and each starts on a different window.
    pub fn first(slot: usize) -> Self {
        Self {
            slot,
            generation: 0,
            id: Uuid::default(),
            identity: if slot.is_multiple_of(2) {
                Identity::Key
            } else {
                Identity::Header
            },
            window_ms: WINDOWS_MS[slot % WINDOWS_MS.len()],
            dedup_on: true,
        }
    }

    /// The topic that replaces this one: the next generation of the slot,
    /// the other identity rule, dedup on.
    pub fn successor(&self) -> Self {
        let generation = self.generation + 1;
        Self {
            slot: self.slot,
            generation,
            id: Uuid::default(),
            identity: flipped(self.identity),
            window_ms: WINDOWS_MS[(self.slot + generation) % WINDOWS_MS.len()],
            dedup_on: true,
        }
    }

    pub fn name(&self) -> String { format!("dedup-soak-{}-{}", self.slot, self.generation) }

    pub fn rule(&self) -> Rule {
        Rule {
            identity: self.identity,
            window_ms: self.window_ms,
        }
    }

    /// Every config the topic is created with.
    pub fn create_configs(&self) -> Vec<(&'static str, String)> {
        let mut configs = storage_configs().to_vec();
        if self.dedup_on {
            configs.push((WINDOW_KEY, self.window_ms.to_string()));
        }
        if self.identity == Identity::Header {
            configs.push((HEADER_KEY, DEDUP_HEADER.to_owned()));
        }
        configs
    }
}

fn flipped(identity: Identity) -> Identity {
    match identity {
        Identity::Key => Identity::Header,
        Identity::Header => Identity::Key,
    }
}

/// One change to one topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Change {
    Replace,
    ToggleDedup,
    NextWindow,
    FlipIdentity,
}

/// One topic config change: set `key` to `value`, or delete it when `value`
/// is `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEdit {
    pub key: &'static str,
    pub value: Option<String>,
}

impl Change {
    /// The change the churn makes after `cycle`, and the slot it makes it to.
    pub fn after(cycle: usize) -> (Self, usize) {
        let change = match cycle % 4 {
            0 => Self::Replace,
            1 => Self::ToggleDedup,
            2 => Self::NextWindow,
            _ => Self::FlipIdentity,
        };
        (change, (cycle / 4 + cycle % 4) % SLOTS)
    }

    /// Apply a config change to `topic`, returning the edit that makes it on
    /// the broker. `None` for [`Change::Replace`], which is no config edit.
    pub fn edit(self, topic: &mut SoakTopic) -> Option<ConfigEdit> {
        let (key, value) = match self {
            Self::Replace => return None,
            Self::ToggleDedup => {
                topic.dedup_on = !topic.dedup_on;
                (
                    WINDOW_KEY,
                    topic.dedup_on.then(|| topic.window_ms.to_string()),
                )
            }
            Self::NextWindow => {
                let at = WINDOWS_MS.iter().position(|w| *w == topic.window_ms);
                topic.window_ms = WINDOWS_MS[at.map_or(0, |at| (at + 1) % WINDOWS_MS.len())];
                topic.dedup_on = true;
                (WINDOW_KEY, Some(topic.window_ms.to_string()))
            }
            Self::FlipIdentity => {
                topic.identity = flipped(topic.identity);
                let header = topic.identity == Identity::Header;
                (HEADER_KEY, header.then(|| DEDUP_HEADER.to_owned()))
            }
        };
        Some(ConfigEdit { key, value })
    }
}

impl fmt::Display for Change {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Replace => "replace",
            Self::ToggleDedup => "toggle",
            Self::NextWindow => "window",
            Self::FlipIdentity => "rule",
        })
    }
}

mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[yare::parameterized(
        replace = { 0, Change::Replace, 0 },
        toggle = { 1, Change::ToggleDedup, 1 },
        window = { 2, Change::NextWindow, 2 },
        rule = { 3, Change::FlipIdentity, 3 },
        second_replace = { 4, Change::Replace, 1 },
        second_rule = { 7, Change::FlipIdentity, 0 },
        fourth_replace = { 12, Change::Replace, 3 },
    )]
    fn the_churn_rotates_changes_across_slots(cycle: usize, change: Change, slot: usize) {
        check!(Change::after(cycle) == (change, slot));
    }

    #[test]
    fn every_slot_sees_every_change_within_sixteen_cycles() {
        let mut seen = std::collections::BTreeSet::new();
        for cycle in 0..16 {
            seen.insert(Change::after(cycle));
        }
        assert!(seen.len() == 16);
    }

    #[test]
    fn a_replacement_flips_the_rule_and_turns_dedup_on() {
        let mut first = SoakTopic::first(1);
        first.dedup_on = false;
        let next = first.successor();
        assert!(
            next == SoakTopic {
                slot: 1,
                generation: 1,
                id: Uuid::default(),
                identity: Identity::Key,
                window_ms: 3_000,
                dedup_on: true,
            }
        );
        assert!(next.name() == "dedup-soak-1-1");
    }

    #[test]
    fn a_header_topic_is_created_with_both_dedup_keys() {
        let configs = SoakTopic::first(1).create_configs();
        assert!(
            configs[4..]
                == [
                    (WINDOW_KEY, "2000".to_owned()),
                    (HEADER_KEY, "dedup-id".to_owned())
                ]
        );
        let mut off = SoakTopic::first(0);
        off.dedup_on = false;
        assert!(off.create_configs()[..] == storage_configs()[..]);
    }

    #[yare::parameterized(
        toggle_off = { Change::ToggleDedup, Some(ConfigEdit { key: WINDOW_KEY, value: None }) },
        next_window = { Change::NextWindow, Some(ConfigEdit { key: WINDOW_KEY, value: Some("2000".into()) }) },
        flip_to_header = { Change::FlipIdentity, Some(ConfigEdit { key: HEADER_KEY, value: Some("dedup-id".into()) }) },
        replace = { Change::Replace, None },
    )]
    fn each_change_is_one_config_edit(change: Change, edit: Option<ConfigEdit>) {
        let mut topic = SoakTopic::first(0);
        check!(change.edit(&mut topic) == edit);
    }

    #[test]
    fn toggling_twice_turns_dedup_back_on_with_its_window() {
        let mut topic = SoakTopic::first(2);
        Change::ToggleDedup.edit(&mut topic);
        let edit = Change::ToggleDedup.edit(&mut topic);
        assert!(topic.dedup_on);
        assert!(
            edit == Some(ConfigEdit {
                key: WINDOW_KEY,
                value: Some("3000".into()),
            })
        );
    }
}
