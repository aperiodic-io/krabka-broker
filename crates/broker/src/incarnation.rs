//! Broker incarnation ID persistence.
//!
//! A UUID, the `incarnation_id`, identifies a broker in its
//! `BrokerRegistration`. The broker generates the UUID once on first boot,
//! writes it to `{log_dir}/incarnation_id` as a lowercase hyphenated UUID, and
//! reloads it on every subsequent start.
//!
//! ## Not part of the strict on-disk contract
//!
//! The file has no version marker, and an unreadable value is replaced with a
//! fresh UUID rather than refused. That is deliberate. The id is a
//! regenerable identity, not durable state:
//!
//! - Kafka never persists it. `BrokerLifecycleManager` draws a new
//!   `Uuid.randomUuid()` for every process, so every Kafka restart presents a
//!   new incarnation, and the controller is built to accept that.
//! - Nothing fences on it across processes. Self-registration always submits
//!   a new registration at epoch `-1`, which the controller stamps with a
//!   fresh broker epoch whatever the incarnation; the incarnation check in
//!   the controller's amend path guards only an amend of a registration this
//!   same process made. The broker epoch, not the incarnation, fences the
//!   previous process, and the clean-shutdown proof (see
//!   `crate::clean_shutdown`), not the incarnation, decides whether a restart
//!   was clean.
//! - The worst a new id costs is Kafka's ordinary restart behavior: a
//!   `BrokerRegistration` under a new incarnation is refused with
//!   `DUPLICATE_BROKER_REGISTRATION` until the previous incarnation's
//!   heartbeat session expires.
//!
//! So a later 1.x build that changes this file's shape needs no reader for the
//! old one: an older build that cannot parse it regenerates the id, which is
//! exactly what a Kafka broker does on every start.

use {
    std::{
        io::{Read, Write},
        path::Path,
    },
    uuid::Uuid,
};

const FILE_NAME: &str = "incarnation_id";

/// Load the persisted incarnation ID from `{log_dir}/incarnation_id`, or
/// generate a fresh [`Uuid::new_v4`], persist it, and return it.
///
/// This function treats any I/O error on the read path as a missing file and
/// generates a new UUID. If it cannot persist the UUID, it logs a warning and
/// returns the ephemeral UUID anyway. The next restart then gets a different
/// UUID.
pub fn load_or_generate(log_dir: &Path) -> Uuid {
    let path = log_dir.join(FILE_NAME);
    if let Ok(mut f) = std::fs::File::open(&path) {
        let mut s = String::new();
        if f.read_to_string(&mut s).is_ok()
            && let Ok(id) = s.trim().parse::<Uuid>()
        {
            return id;
        }
    }
    let id = Uuid::new_v4();
    match std::fs::File::create(&path) {
        Ok(mut f) => {
            if let Err(e) = f.write_all(id.to_string().as_bytes()) {
                tracing::warn!(path = %path.display(), error = %e, "failed to persist incarnation_id");
            }
        }
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "failed to create incarnation_id file");
        }
    }
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_uuid_when_file_absent() {
        let dir = tempfile::tempdir().unwrap();
        let id = load_or_generate(dir.path());
        assert2::assert!(!id.is_nil(), "generated UUID must not be nil");
    }

    /// The bytes a broker leaves behind: the hyphenated lowercase UUID and
    /// nothing else.
    #[test]
    fn writes_the_lowercase_hyphenated_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let id = load_or_generate(dir.path());
        let on_disk = std::fs::read_to_string(dir.path().join(FILE_NAME)).unwrap();
        assert2::assert!(on_disk == id.hyphenated().to_string().to_lowercase());
    }

    /// A value that reads back is kept; one that does not is replaced with a
    /// fresh id, and the fresh id is what the file holds afterwards.
    #[test]
    fn reads_a_valid_id_and_regenerates_an_unreadable_one() {
        let kept = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        // (file contents, the id `load_or_generate` must return, or `None`
        // when it must be a fresh one)
        let cases: &[(&str, Option<Uuid>)] = &[
            ("01020304-0506-0708-090a-0b0c0d0e0f10", Some(kept)),
            ("01020304-0506-0708-090a-0b0c0d0e0f10\n", Some(kept)),
            ("", None),
            ("not-a-uuid", None),
            ("0\n01020304-0506-0708-090a-0b0c0d0e0f10", None),
        ];
        for (contents, want) in cases {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join(FILE_NAME), contents).unwrap();
            let id = load_or_generate(dir.path());
            let reread = load_or_generate(dir.path());
            match want {
                Some(want) => assert2::assert!((id, reread) == (*want, *want), "{contents:?}"),
                None => assert2::assert!(
                    (id.is_nil(), id == kept, reread == id) == (false, false, true),
                    "{contents:?}"
                ),
            }
        }
    }

    #[test]
    fn persists_and_reloads_uuid() {
        let dir = tempfile::tempdir().unwrap();
        let id1 = load_or_generate(dir.path());
        let id2 = load_or_generate(dir.path());
        assert2::assert!((id1) == (id2), "UUID must be stable across reloads");
    }
}
