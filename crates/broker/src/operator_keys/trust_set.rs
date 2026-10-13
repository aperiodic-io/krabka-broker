//! The loaded trust set: the keys it holds, and the check a signature passes.
//!
//! [`OperatorKeys::load`] validates every entry before it keeps one, so a set
//! that loads holds one key per `key_id` and one key per principal. A lookup
//! is by `key_id` alone. [`OperatorKeys::verify`] adds the principal binding
//! to the signature check, so one operator's key never signs in another's
//! name.

use {
    super::{OperatorKeyEntry, OperatorKeyError, key_file::read_public_key},
    std::collections::BTreeMap,
};

/// One loaded operator key and the principal it is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorKey {
    key_id: String,
    principal: String,
    public_key: Vec<u8>,
}

impl OperatorKey {
    /// Stable identifier that a signed record names.
    #[must_use]
    pub fn key_id(&self) -> &str { &self.key_id }

    /// The principal this key speaks for.
    ///
    /// A record that claims another author is refused even when its signature
    /// verifies, so one operator's key cannot sign in another's name.
    #[must_use]
    pub fn principal(&self) -> &str { &self.principal }

    /// The raw 32-byte Ed25519 public key.
    #[must_use]
    pub fn public_key(&self) -> &[u8] { &self.public_key }
}

/// The loaded, validated operator key trust set.
///
/// An empty set is the default and means no operator key is provisioned. The
/// file-config layer refuses a configuration that demands a signature against
/// an empty set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorKeys {
    by_id: BTreeMap<String, OperatorKey>,
}

impl OperatorKeys {
    /// Read every configured public key file and build the trust set.
    ///
    /// # Errors
    ///
    /// Returns [`OperatorKeyError`] when an entry has a blank `key_id` or
    /// `principal`, when its `public_key_path` cannot be read, when the file
    /// does not hold a 32-byte Ed25519 public key, or when two entries share a
    /// `key_id` or a `principal`.
    pub fn load(entries: &[OperatorKeyEntry]) -> Result<Self, OperatorKeyError> {
        let mut by_id: BTreeMap<String, OperatorKey> = BTreeMap::new();
        let mut by_principal: BTreeMap<String, String> = BTreeMap::new();
        for (index, entry) in entries.iter().enumerate() {
            if entry.key_id.trim().is_empty() {
                return Err(OperatorKeyError::BlankField {
                    index,
                    field: "key_id",
                });
            }
            if entry.principal.trim().is_empty() {
                return Err(OperatorKeyError::BlankField {
                    index,
                    field: "principal",
                });
            }
            if by_id.contains_key(&entry.key_id) {
                return Err(OperatorKeyError::DuplicateKeyId {
                    key_id: entry.key_id.clone(),
                });
            }
            if let Some(first) = by_principal.get(&entry.principal) {
                return Err(OperatorKeyError::DuplicatePrincipal {
                    principal: entry.principal.clone(),
                    first: first.clone(),
                    second: entry.key_id.clone(),
                });
            }
            let public_key = read_public_key(&entry.key_id, &entry.public_key_path)?;
            by_principal.insert(entry.principal.clone(), entry.key_id.clone());
            by_id.insert(entry.key_id.clone(), OperatorKey {
                key_id: entry.key_id.clone(),
                principal: entry.principal.clone(),
                public_key,
            });
        }
        Ok(Self { by_id })
    }

    /// The key registered under `key_id`, with the principal it is bound to.
    #[must_use]
    pub fn get(&self, key_id: &str) -> Option<&OperatorKey> { self.by_id.get(key_id) }

    /// How many keys the trust set holds.
    #[must_use]
    pub fn len(&self) -> usize { self.by_id.len() }

    /// Whether no operator key is provisioned.
    #[must_use]
    pub fn is_empty(&self) -> bool { self.by_id.is_empty() }

    /// Verify `signature` over `message` as made by `key_id` on behalf of
    /// `principal`.
    ///
    /// The principal binding is part of the check: a signature made by Alice's
    /// key over a record that claims Bob wrote it does not verify. The caller
    /// supplies the canonical bytes of its own signed payload, under its own
    /// domain separator.
    #[must_use]
    pub fn verify(&self, key_id: &str, principal: &str, message: &[u8], signature: &[u8]) -> bool {
        self.get(key_id).is_some_and(|key| {
            key.principal == principal
                && krabka_audit::signing::verify_signature(&key.public_key, message, signature)
        })
    }
}
