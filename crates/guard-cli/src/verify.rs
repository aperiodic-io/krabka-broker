//! Proves the freeze registry against operator public keys on this machine.
//!
//! `freeze list --verify-signatures` reads the registry from a broker and then
//! checks every entry here, against a trust set on the operator's own disk.
//! That is what makes the operator's laptop, and not the broker that served the
//! rows, the thing that says the registry is authentic. A broker that minted a
//! record naming somebody else cannot make a signature to go with it, because
//! the signing key never reached it.
//!
//! # What a proof is worth
//!
//! A verified entry proves authorship. It proves that the person the entry
//! names authored that entry, for that scope, in that cluster, at that time. It
//! does not prove that any broker then refused a write, which is a different
//! kind of evidence that the produce-path tests supply.
//!
//! # The mixture
//!
//! `freeze.require_signature` is off by default, because a freeze is the safe
//! direction and an operator has to reach it in one command on a cluster where
//! nobody installed key material yet. So a registry can hold proved entries and
//! attested ones side by side. An attested entry carries no signature, and the
//! broker's word is all that stands behind it.
//!
//! This is what `--verify-signatures` separates, so an unsigned entry is
//! reported as unsigned rather than counted as a failure. An operator who wants
//! the mixture gone sets `freeze.require_signature = true` on the brokers.
//!
//! # Three outcomes, not two
//!
//! An entry that names a `key_id` the local trust set does not hold is not the
//! same as an entry whose signature is wrong. The first says the tool could not
//! check. The second says the tool checked and the answer is wrong. KFC-5's
//! verifier draws the same line, and the caller turns the two into different
//! exit codes.

use std::path::{Path, PathBuf};

use krabka_broker::operator_keys::{OperatorKeyEntry, OperatorKeys, freeze_signing_input};
use krabka_protocol::krabka::freeze::DescribedTopicFreeze;

use crate::signing::freeze_signing_bytes;

/// Why one registry entry is not proved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unproved {
    /// The entry carries no signature. The broker's word is all that stands
    /// behind it.
    Unsigned,
    /// The local trust set holds no key under the entry's `key_id`, so the
    /// tool could not check this entry at all.
    UnknownKeyId,
    /// The tool checked and the answer is wrong: the signature does not verify
    /// over the entry's canonical bytes, or the key is not bound to the
    /// principal the entry names as its author.
    SignatureDidNotVerify,
}

impl Unproved {
    /// The word a report prints for this outcome.
    #[must_use]
    pub fn reason(self) -> &'static str {
        match self {
            Unproved::Unsigned => "unsigned, so the broker's word is all that stands behind it",
            Unproved::UnknownKeyId => {
                "no local operator key carries that key_id, so this entry cannot be checked here"
            }
            Unproved::SignatureDidNotVerify => {
                "the signature does not verify against the local operator key"
            }
        }
    }
}

/// One registry entry, and what the local trust set says about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckedEntry {
    /// The scope the entry freezes.
    pub scope: String,
    /// `3` literal, `4` prefixed.
    pub pattern_type: i8,
    /// The principal the entry names as its author.
    pub set_by: String,
    /// The operator key the entry names, empty when the entry is unsigned.
    pub key_id: String,
    /// `None` when the signature verified.
    pub unproved: Option<Unproved>,
}

/// What one `--verify-signatures` pass found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VerifyOutcome {
    /// Every entry, in the order the broker returned them.
    pub entries: Vec<CheckedEntry>,
}

impl VerifyOutcome {
    /// Whether any entry's signature was checked and found wrong.
    #[must_use]
    pub fn any_signature_failed(&self) -> bool {
        self.has(Unproved::SignatureDidNotVerify)
    }

    /// Whether any entry names a key the local trust set does not hold.
    #[must_use]
    pub fn any_key_is_unknown(&self) -> bool {
        self.has(Unproved::UnknownKeyId)
    }

    /// How many entries verified.
    #[must_use]
    pub fn proved(&self) -> usize {
        self.entries
            .iter()
            .filter(|entry| entry.unproved.is_none())
            .count()
    }

    fn has(&self, unproved: Unproved) -> bool {
        self.entries
            .iter()
            .any(|entry| entry.unproved == Some(unproved))
    }
}

/// Check every returned registry entry against the local trust set.
///
/// A live registry entry is a freeze, so the rebuilt bytes carry `frozen` set
/// and no proposal. A thaw removes the entry, so no thaw is ever in the list
/// this checks.
#[must_use]
pub fn verify_registry(
    cluster_id: &str,
    keys: &OperatorKeys,
    freezes: &[DescribedTopicFreeze],
) -> VerifyOutcome {
    VerifyOutcome {
        entries: freezes
            .iter()
            .map(|freeze| CheckedEntry {
                scope: freeze.scope.clone(),
                pattern_type: freeze.pattern_type,
                set_by: freeze.set_by.clone(),
                key_id: freeze.key_id.clone(),
                unproved: check_entry(cluster_id, keys, freeze),
            })
            .collect(),
    }
}

/// Decide what the local trust set says about one entry.
fn check_entry(
    cluster_id: &str,
    keys: &OperatorKeys,
    freeze: &DescribedTopicFreeze,
) -> Option<Unproved> {
    if freeze.key_id.is_empty() && freeze.signature.is_empty() {
        return Some(Unproved::Unsigned);
    }
    if keys.get(&freeze.key_id).is_none() {
        return Some(Unproved::UnknownKeyId);
    }
    let message = freeze_signing_bytes(
        &freeze_signing_input()
            .cluster_id(cluster_id)
            .entry(freeze)
            .frozen(true)
            .call(),
    );
    // `verify` also checks that the key is bound to the principal the entry
    // names, so a signature from Alice's key over a record that claims Bob
    // wrote it does not pass.
    if keys.verify(&freeze.key_id, &freeze.set_by, &message, &freeze.signature) {
        None
    } else {
        Some(Unproved::SignatureDidNotVerify)
    }
}

/// Read a trust set from a TOML file that carries `[[operator_keys]]` entries.
///
/// The block is the one the broker reads out of its own `broker.toml`, and
/// every other key in the file is ignored. So an operator points this at the
/// same file the brokers run on, and the tool and the cluster cannot disagree
/// about which key belongs to whom.
///
/// # Errors
///
/// Returns a message when the file cannot be read, when it is not TOML, when it
/// declares no `[[operator_keys]]` array, and when an entry names a public key
/// file that is missing or does not hold a raw 32-byte Ed25519 public key.
pub fn load_trust_set(path: &Path) -> Result<OperatorKeys, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    let file: TrustFile = toml::from_str(&text)
        .map_err(|error| format!("cannot read {} as operator keys: {error}", path.display()))?;
    let entries: Vec<OperatorKeyEntry> = file
        .operator_keys
        .into_iter()
        .map(|key| OperatorKeyEntry {
            key_id: key.key_id,
            principal: key.principal,
            public_key_path: key.public_key_path,
        })
        .collect();
    if entries.is_empty() {
        return Err(format!(
            "{} declares an empty [[operator_keys]] array, so nothing can be verified",
            path.display()
        ));
    }
    OperatorKeys::load(&entries).map_err(|error| format!("{}: {error}", path.display()))
}

/// The part of an operator's TOML file that this tool reads.
#[derive(serde::Deserialize)]
struct TrustFile {
    /// The `[[operator_keys]]` array. A file without it is refused, because a
    /// verify against an empty trust set is a check that silently does
    /// nothing.
    operator_keys: Vec<TrustFileKey>,
}

/// One `[[operator_keys]]` entry, in the shape `broker.toml` writes it.
#[derive(serde::Deserialize)]
struct TrustFileKey {
    /// Stable identifier that a signed record names.
    key_id: String,
    /// The principal this key speaks for.
    principal: String,
    /// Path to the raw 32-byte Ed25519 public key.
    public_key_path: PathBuf,
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_audit::FileEd25519Signer;
    use krabka_protocol::primitives::uuid::Uuid;
    use ring::signature::Ed25519KeyPair;
    use tempfile::TempDir;

    use super::*;

    const CLUSTER: &str = "krabka-test";
    const ALICE: &str = "User:alice";

    /// A signer and the raw public key that goes with it.
    fn fresh_signer(key_id: &str) -> (FileEd25519Signer, Vec<u8>) {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let signer = FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), key_id.to_owned())
            .expect("parse pkcs8");
        let public = signer.public_key();
        (signer, public)
    }

    /// A trust set holding one key bound to one principal.
    fn trust_set(dir: &TempDir, key_id: &str, principal: &str, public: &[u8]) -> OperatorKeys {
        let path = dir.path().join(format!("{key_id}.pub"));
        std::fs::write(&path, public).expect("write public key");
        OperatorKeys::load(&[OperatorKeyEntry {
            key_id: key_id.to_owned(),
            principal: principal.to_owned(),
            public_key_path: path,
        }])
        .expect("load trust set")
    }

    /// A registry entry signed by `signer` under `key_id`.
    fn signed_entry(signer: &FileEd25519Signer, key_id: &str, scope: &str) -> DescribedTopicFreeze {
        let mut entry = DescribedTopicFreeze {
            scope: scope.to_owned(),
            pattern_type: 3,
            reason: "DR cutover".to_owned(),
            set_by: ALICE.to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: Uuid::ZERO,
            key_id: key_id.to_owned(),
            ..DescribedTopicFreeze::default()
        };
        entry.signature = signer.sign(&freeze_signing_bytes(
            &freeze_signing_input()
                .cluster_id(CLUSTER)
                .entry(&entry)
                .frozen(true)
                .call(),
        ));
        entry
    }

    fn signed_fixture() -> (TempDir, OperatorKeys, DescribedTopicFreeze) {
        let dir = TempDir::new().expect("tempdir");
        let (signer, public) = fresh_signer("alice-yubi");
        let keys = trust_set(&dir, "alice-yubi", ALICE, &public);
        let entry = signed_entry(&signer, "alice-yubi", "orders");
        (dir, keys, entry)
    }

    #[test]
    fn a_signature_that_the_local_key_makes_verifies_locally() {
        let (_dir, keys, entry) = signed_fixture();

        let outcome = verify_registry(CLUSTER, &keys, std::slice::from_ref(&entry));

        check!(
            outcome
                == VerifyOutcome {
                    entries: vec![CheckedEntry {
                        scope: "orders".to_owned(),
                        pattern_type: 3,
                        set_by: ALICE.to_owned(),
                        key_id: "alice-yubi".to_owned(),
                        unproved: None,
                    }],
                }
        );
        check!(outcome.proved() == 1);
        check!(!outcome.any_signature_failed());
        check!(!outcome.any_key_is_unknown());
    }

    /// Each of these would let a registry read as authentic when it is not, so
    /// each has to be reported. The three outcomes are kept apart because a
    /// runbook branches on them differently: an unsigned entry is expected on a
    /// cluster that does not demand signatures, an unknown key means the tool
    /// could not check, and a failed signature means the tool checked and the
    /// answer is wrong.
    #[test]
    fn an_entry_the_local_trust_set_cannot_prove_is_reported() {
        let dir = TempDir::new().expect("tempdir");
        let (signer, public) = fresh_signer("alice-yubi");
        let (other, _) = fresh_signer("mallory-yubi");
        let keys = trust_set(&dir, "alice-yubi", ALICE, &public);

        let unsigned = DescribedTopicFreeze {
            scope: "orders".to_owned(),
            set_by: ALICE.to_owned(),
            ..DescribedTopicFreeze::default()
        };
        let unknown_key = signed_entry(&signer, "bob-yubi", "orders");
        let another_key = signed_entry(&other, "alice-yubi", "orders");
        let mut tampered = signed_entry(&signer, "alice-yubi", "orders");
        tampered.scope = "payments".to_owned();
        let mut another_author = signed_entry(&signer, "alice-yubi", "orders");
        another_author.set_by = "User:bob".to_owned();

        let cases: [(&'static str, DescribedTopicFreeze, Unproved); 5] = [
            ("no signature at all", unsigned, Unproved::Unsigned),
            (
                "a key_id the trust set does not hold",
                unknown_key,
                Unproved::UnknownKeyId,
            ),
            (
                "a signature from another key",
                another_key,
                Unproved::SignatureDidNotVerify,
            ),
            (
                "a scope changed after signing",
                tampered,
                Unproved::SignatureDidNotVerify,
            ),
            (
                "an author the key does not speak for",
                another_author,
                Unproved::SignatureDidNotVerify,
            ),
        ];
        for (case, entry, expected) in cases {
            let outcome = verify_registry(CLUSTER, &keys, &[entry]);
            check!(outcome.entries[0].unproved == Some(expected), "{case}");
        }

        // The control: the table above rejects for real reasons, and not
        // because every entry is rejected.
        let good = signed_entry(&signer, "alice-yubi", "orders");
        check!(verify_registry(CLUSTER, &keys, &[good]).proved() == 1);
    }

    /// The cluster id is inside the signed bytes, so a signature captured from
    /// one cluster does not verify in another.
    #[test]
    fn a_signature_made_for_another_cluster_does_not_verify() {
        let (_dir, keys, entry) = signed_fixture();

        let outcome = verify_registry("another-cluster", &keys, &[entry]);

        check!(outcome.any_signature_failed());
        check!(outcome.proved() == 0);
    }

    #[test]
    fn a_trust_file_reads_the_operator_keys_block_of_a_broker_file() {
        let dir = TempDir::new().expect("tempdir");
        let (_, public) = fresh_signer("alice-yubi");
        let public_path = dir.path().join("alice.pub");
        std::fs::write(&public_path, &public).expect("write public key");
        let file = dir.path().join("broker.toml");
        std::fs::write(
            &file,
            format!(
                "node_id = 1\n\n[[operator_keys]]\nkey_id = \"alice-yubi\"\nprincipal = \
                 \"{ALICE}\"\npublic_key_path = \"{}\"\n",
                public_path.display()
            ),
        )
        .expect("write trust file");

        let keys = load_trust_set(&file).expect("load trust set");

        check!(keys.len() == 1);
        check!(
            keys.get("alice-yubi")
                .map(krabka_broker::operator_keys::OperatorKey::principal)
                == Some(ALICE)
        );
    }

    /// Each of these would otherwise leave the tool checking against an empty
    /// or partial trust set, which is a check that silently does nothing.
    #[test]
    fn a_trust_file_that_proves_nothing_is_refused() {
        let dir = TempDir::new().expect("tempdir");
        let missing = dir.path().join("absent.toml");
        let not_toml = dir.path().join("not-toml.toml");
        std::fs::write(&not_toml, "this is not toml").expect("write file");
        let no_block = dir.path().join("no-block.toml");
        std::fs::write(&no_block, "node_id = 1\n").expect("write file");
        let empty_block = dir.path().join("empty-block.toml");
        std::fs::write(&empty_block, "operator_keys = []\n").expect("write file");
        let bad_key = dir.path().join("bad-key.toml");
        std::fs::write(
            &bad_key,
            "[[operator_keys]]\nkey_id = \"k\"\nprincipal = \"User:k\"\npublic_key_path = \
             \"/nonexistent/k.pub\"\n",
        )
        .expect("write file");

        let cases: [(&'static str, PathBuf); 5] = [
            ("a file that is not there", missing),
            ("a file that is not TOML", not_toml),
            ("a file with no [[operator_keys]]", no_block),
            ("an empty [[operator_keys]] array", empty_block),
            ("a public key file that is missing", bad_key),
        ];
        for (case, path) in cases {
            assert!(load_trust_set(&path).is_err(), "{case}");
        }
    }
}
