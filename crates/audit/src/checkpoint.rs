//! Signed chain-checkpoint records.
//!
//! A checkpoint is a special audit record with `event_class = "checkpoint"`. It
//! is NOT part of the hash-chain. It asserts and signs the chain head at its
//! position, so an attacker without the signing key cannot forge a consistent
//! rewrite of the records before it.

use {
    crate::{
        chain::{from_hex32, to_hex},
        event::AuditEventClass,
        ids::{EpochMs, Seq},
        signing::{FileEd25519Signer, checkpoint_signing_bytes, verify_signature},
        sink::AuditRecord,
    },
    serde_json::json,
};

/// `event_class` header value that identifies a checkpoint record.
pub const EVENT_CLASS_CHECKPOINT: &str = "checkpoint";

/// A signed checkpoint over the chain head at sequence `seq_high`.
#[derive(Debug, Clone)]
pub struct Checkpoint {
    pub key_id: String,
    pub seq_high: Seq,
    pub chain_head: [u8; 32],
    pub time_ms: EpochMs,
    pub signature: Vec<u8>,
    pub public_key: Vec<u8>,
}

impl Checkpoint {
    /// Build and sign a checkpoint with `signer`.
    #[must_use]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(key_id = %signer.key_id(), seq_high, time_ms)
    )]
    pub fn signed(
        signer: &FileEd25519Signer,
        seq_high: Seq,
        chain_head: &[u8; 32],
        time_ms: EpochMs,
    ) -> Self {
        let msg = checkpoint_signing_bytes(signer.key_id(), seq_high, chain_head, time_ms);
        Self {
            key_id: signer.key_id().to_string(),
            seq_high,
            chain_head: *chain_head,
            time_ms,
            signature: signer.sign(&msg),
            public_key: signer.public_key(),
        }
    }

    /// Verify this checkpoint's signature against a trusted public key.
    #[must_use]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(key_id = %self.key_id, seq_high = self.seq_high.0)
    )]
    pub fn verify(&self, public_key: &[u8]) -> bool {
        let msg =
            checkpoint_signing_bytes(&self.key_id, self.seq_high, &self.chain_head, self.time_ms);
        verify_signature(public_key, &msg, &self.signature)
    }

    /// Encode this checkpoint as an audit record for the audit topic.
    #[must_use]
    pub fn to_record(&self) -> AuditRecord {
        let value = serde_json::to_vec(&json!({
            "type": "checkpoint",
            "key_id": self.key_id,
            "seq_high": self.seq_high.0,
            "chain_head": to_hex(&self.chain_head),
            "time": self.time_ms.0,
            "signature": to_hex(&self.signature),
            "public_key": to_hex(&self.public_key),
        }))
        .unwrap_or_else(|_| b"{}".to_vec());
        AuditRecord {
            class: AuditEventClass::Checkpoint,
            value,
            headers: vec![(
                "event_class".to_string(),
                EVENT_CLASS_CHECKPOINT.as_bytes().to_vec(),
            )],
        }
    }

    /// Parse a checkpoint from its record value JSON.
    ///
    /// Returns `None` if any field is missing or malformed.
    #[must_use]
    pub fn from_value(v: &serde_json::Value) -> Option<Self> {
        let key_id = v.get("key_id")?.as_str()?.to_string();
        let seq_high = Seq(v.get("seq_high")?.as_u64()?);
        let chain_head = from_hex32(v.get("chain_head")?.as_str()?)?;
        let time_ms = EpochMs(v.get("time")?.as_i64()?);
        let signature = hex::decode(v.get("signature")?.as_str()?).ok()?;
        let public_key = hex::decode(v.get("public_key")?.as_str()?).ok()?;
        Some(Self {
            key_id,
            seq_high,
            chain_head,
            time_ms,
            signature,
            public_key,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::test_support::{TestSigner, signer},
        assert2::check,
    };

    #[test]
    fn signed_checkpoint_round_trips_through_record_and_verifies() {
        let TestSigner {
            signer: s,
            public_key: pubkey,
        } = signer();
        let head = [3u8; 32];
        let cp = Checkpoint::signed(&s, Seq(41), &head, EpochMs(1_700_000_000_000));
        check!(cp.verify(&pubkey));

        // serialize to a record, parse back from its value, still verifies
        let rec = cp.to_record();
        check!(rec.class == crate::event::AuditEventClass::Checkpoint);
        check!(
            rec.headers
                .iter()
                .any(|(k, v)| k == "event_class" && v == b"checkpoint")
        );
        let value: serde_json::Value = serde_json::from_slice(&rec.value).unwrap();
        let parsed = Checkpoint::from_value(&value).expect("parse");
        check!(
            (parsed.seq_high, parsed.chain_head, parsed.verify(&pubkey)) == (Seq(41), head, true)
        );
    }

    #[test]
    fn tampered_checkpoint_fails_verification() {
        let TestSigner {
            signer: s,
            public_key: pubkey,
        } = signer();
        let mut cp = Checkpoint::signed(&s, Seq(41), &[3u8; 32], EpochMs(10));
        // flip the head the signature was computed over
        cp.chain_head = [4u8; 32];
        check!(!cp.verify(&pubkey));
    }

    #[test]
    fn from_value_rejects_malformed_hex_without_panicking() {
        let TestSigner { signer: s, .. } = signer();
        let cp = Checkpoint::signed(&s, Seq(1), &[5u8; 32], EpochMs(10));
        for (name, bad) in [
            ("non-ASCII straddling a pair", "a\u{e9}a"),
            ("sign prefix", "+f"),
            ("odd length", "abc"),
        ] {
            let mut v: serde_json::Value = serde_json::from_slice(&cp.to_record().value).unwrap();
            v["signature"] = serde_json::Value::from(bad);
            check!(Checkpoint::from_value(&v).is_none(), "case {name}");
        }
    }
}
