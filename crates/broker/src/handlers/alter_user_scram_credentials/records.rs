//! The metadata records that an accepted SCRAM alteration produces.
//!
//! An accepted upsertion carries the client-side PBKDF2 output that KIP-554
//! puts on the wire, so the key derivation that turns it into `stored_key`
//! and `server_key` lives beside the record it fills in.

use {
    krabka_metadata::{DeleteScramCredentialRecord, MetadataRecord, ScramCredentialRecord},
    krabka_protocol::owned::alter_user_scram_credentials_request::{
        ScramCredentialDeletion, ScramCredentialUpsertion,
    },
    krabka_security::SaslMechanism,
};

pub(super) fn delete_record(
    deletion: &ScramCredentialDeletion,
    mechanism: SaslMechanism,
) -> MetadataRecord {
    MetadataRecord::V1DeleteScramCredential(DeleteScramCredentialRecord {
        user: deletion.name.clone(),
        mechanism,
    })
}

pub(super) fn upsertion_record(
    upsertion: &ScramCredentialUpsertion,
    mechanism: SaslMechanism,
) -> MetadataRecord {
    // Per KIP-554 the wire `salted_password` is the PBKDF2 output; recompute
    // `stored_key` and `server_key` from the supplied bytes for storage in the
    // metadata image.
    let (stored_key, server_key) =
        krabka_security::derive_keys_from_salted(mechanism, &upsertion.salted_password);
    MetadataRecord::V1ScramCredential(ScramCredentialRecord {
        user: upsertion.name.clone(),
        mechanism,
        salt: upsertion.salt.to_vec(),
        stored_key,
        server_key,
        iterations: u32::try_from(upsertion.iterations)
            .expect("validated SCRAM iterations fit u32"),
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::alter_user_scram_credentials::test_support::{
            expected_result, process_upsertion, valid_upsertion,
        },
        assert2::assert,
        bytes::Bytes,
    };

    #[test]
    fn process_upsertion_accepts_empty_salt() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("empty-salt");
        upsertion.salt = Bytes::new();

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("empty-salt", 0, None));
        assert!(records.len() == 1);
        let MetadataRecord::V1ScramCredential(record) = &records[0] else {
            panic!("accepted upsertion must persist a SCRAM credential record");
        };
        assert!(record.salt.is_empty());
    }

    #[test]
    fn process_upsertion_accepts_non_hash_length_salted_password() {
        let mut records = Vec::new();
        let mut upsertion = valid_upsertion("odd-bytes");
        let salted_password = Bytes::from_static(b"not-a-sha-sized-secret");
        upsertion.salted_password = salted_password.clone();

        let r = process_upsertion(upsertion, true, &mut records);

        assert!(r == expected_result("odd-bytes", 0, None));
        assert!(records.len() == 1);
        let MetadataRecord::V1ScramCredential(record) = &records[0] else {
            panic!("accepted upsertion must persist a SCRAM credential record");
        };
        let (stored_key, server_key) =
            krabka_security::derive_keys_from_salted(SaslMechanism::ScramSha256, &salted_password);
        assert!(record.stored_key == stored_key);
        assert!(record.server_key == server_key);
    }
}
