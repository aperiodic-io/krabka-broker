//! One configured `[[operator_keys]]` entry and the key file it names.
//!
//! An entry carries the `key_id` that a signed record names, the principal the
//! key speaks for, and the path to the public key. The file holds a raw
//! 32-byte Ed25519 public key and nothing else. A file of any other length is
//! a malformed key, so an encoded key or a trailing newline stops the load.

use {
    super::OperatorKeyError,
    std::path::{Path, PathBuf},
};

/// Byte length of a raw Ed25519 public key.
const ED25519_PUBLIC_KEY_LEN: usize = 32;

/// One `[[operator_keys]]` entry, before its public key file is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorKeyEntry {
    /// Stable identifier that a signed record names.
    pub key_id: String,
    /// The principal this key speaks for.
    pub principal: String,
    /// Path to the raw 32-byte Ed25519 public key.
    pub public_key_path: PathBuf,
}

pub fn read_public_key(key_id: &str, path: &Path) -> Result<Vec<u8>, OperatorKeyError> {
    let bytes = std::fs::read(path).map_err(|source| OperatorKeyError::Unreadable {
        key_id: key_id.to_owned(),
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.len() != ED25519_PUBLIC_KEY_LEN {
        return Err(OperatorKeyError::Malformed {
            key_id: key_id.to_owned(),
            path: path.to_path_buf(),
            found: bytes.len(),
        });
    }
    Ok(bytes)
}
