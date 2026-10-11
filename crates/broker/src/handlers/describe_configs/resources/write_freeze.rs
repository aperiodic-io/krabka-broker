//! The synthesised `write.freeze` value a topic resource reports (KFC-9).
//!
//! A freeze lives in the freeze registry and never in a topic's override map,
//! so the topic branch of `describe_one` cannot read this key the way it reads
//! a stored override. This module answers it from the registry instead, and
//! holds the vocabulary the value is spelled in.

use crate::freeze::{freeze_target, resolve::resolve_freeze_verdict};

/// The `write.freeze` value on a frozen topic, before the scope that matched.
///
/// The rest of the value is [`crate::freeze::freeze_target`], so a frozen
/// topic reads `frozen:prefixed:tenant-a.` or `frozen:literal:orders`.
const WRITE_FREEZE_FROZEN_PREFIX: &str = "frozen:";

/// The `write.freeze` override for one topic, or `None` when no freeze covers
/// it.
///
/// The freeze registry answers the question, because a freeze is never stored
/// as a topic config. A frozen topic reports the scope that matched, at
/// `DYNAMIC_TOPIC_CONFIG`; every other topic falls through to the key's
/// default, `false` at `DEFAULT_CONFIG`. The key is read-only either way,
/// which its registry row states.
pub(super) fn write_freeze_override(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
) -> Option<String> {
    let verdict = resolve_freeze_verdict(image, topic)?;
    let target = freeze_target(verdict.pattern_type, &verdict.scope);
    Some(format!("{WRITE_FREEZE_FROZEN_PREFIX}{target}"))
}

#[cfg(test)]
mod tests;
