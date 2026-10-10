//! The key pairs and the key files that the operator key unit tests share.
//!
//! Every fixture writes a real Ed25519 public key into a temporary directory,
//! so a test loads a trust set the way a broker does: from files on disk.

use std::path::PathBuf;

use ring::signature::{Ed25519KeyPair, KeyPair as _};
use tempfile::TempDir;

use super::OperatorKeyEntry;

mod fingerprint;
mod load;
mod verify;

// A fresh Ed25519 key pair plus its raw public key bytes.
fn fresh_key() -> (Ed25519KeyPair, Vec<u8>) {
    let pair = crate::test_support::fresh_ed25519_key();
    let public = pair.public_key().as_ref().to_vec();
    (pair, public)
}

// Write `bytes` to `<dir>/<name>` and return the path.
fn write_key_file(dir: &TempDir, name: &str, bytes: &[u8]) -> PathBuf {
    let path = dir.path().join(name);
    std::fs::write(&path, bytes).expect("write key file");
    path
}

fn entry(key_id: &str, principal: &str, path: PathBuf) -> OperatorKeyEntry {
    OperatorKeyEntry {
        key_id: key_id.to_owned(),
        principal: principal.to_owned(),
        public_key_path: path,
    }
}
