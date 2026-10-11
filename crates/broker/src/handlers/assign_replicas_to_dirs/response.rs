//! The fixed responses this handler can send, and the encoder that every
//! response passes through. `changes` builds the per-partition response of
//! the success path.

use krabka_protocol::owned::assign_replicas_to_dirs_response::AssignReplicasToDirsResponse;

use crate::codes;

pub(super) fn not_controller_response() -> AssignReplicasToDirsResponse {
    AssignReplicasToDirsResponse {
        error_code: codes::NOT_CONTROLLER,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        owned::assign_replicas_to_dirs_response::{
            DirectoryData as RespDirData, PartitionData as RespPartData, TopicData as RespTopicData,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    };

    use super::*;
    use crate::handlers::{
        assign_replicas_to_dirs::test_support::{VERSION, decode_response},
        encode_response,
    };

    #[test]
    fn not_controller_response_preserves_error_code() {
        let resp = not_controller_response();
        assert!(resp.error_code == codes::NOT_CONTROLLER, "{resp:?}");
        assert!(resp.directories.is_empty(), "{resp:?}");
    }

    #[test]
    fn encode_resp_preserves_encoded_body() {
        let resp = unthrottled_wire!(AssignReplicasToDirsResponse {
            error_code: codes::NONE,
            directories: vec![tagged_wire!(RespDirData {
                id: ProtocolUuid(uuid::Uuid::from_u128(0xAA).into_bytes()),
                topics: vec![tagged_wire!(RespTopicData {
                    topic_id: ProtocolUuid(uuid::Uuid::from_u128(0xBB).into_bytes()),
                    partitions: vec![tagged_wire!(RespPartData {
                        partition_index: 3,
                        error_code: codes::UNKNOWN_TOPIC_ID,
                    })],
                })],
            })],
        });

        let bytes = encode_response(&resp, VERSION).expect("encode response");

        assert!(decode_response(&bytes) == resp);
    }
}
