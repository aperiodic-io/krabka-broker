//! Shared wire-body decoding and response encoding.
//!
//! It sizes the buffer from `encoded_len` before it encodes, so the encode
//! writes into a buffer that already holds the whole response.

use bytes::{Bytes, BytesMut};
use krabka_protocol::{Decode, Encode};

use super::wire_types::ApiVersion;
use crate::{codes::NONE, error::BrokerError};

/// Decode one request body at the supplied version. As at the wire entry
/// points, bytes beyond that body are left unread.
pub(crate) fn decode_request<'a, R: Decode<'a>>(
    bytes: &'a [u8],
    version: ApiVersion,
) -> Result<R, BrokerError> {
    let mut cursor = bytes;
    Ok(R::decode(&mut cursor, version)?)
}

pub(crate) fn encode_response<R: Encode>(
    resp: &R,
    version: ApiVersion,
) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
    resp.encode(&mut buf, version)?;
    Ok(buf.freeze())
}

/// A response whose whole-request refusal is its top-level `error_code` and
/// `error_message`, with every other field at its generated default.
pub(crate) trait ErrorResponse: Default {
    fn error(error_code: i16, error_message: Option<String>) -> Self;
}

/// An [`ErrorResponse`] for a response that has no top-level `error_message`.
pub(crate) trait ErrorCodeResponse: Default {
    fn error(error_code: i16) -> Self;
}

macro_rules! impl_error_response {
    (code_only: $($ty:path),* $(,)?) => {
        $(impl ErrorCodeResponse for $ty {
            fn error(error_code: i16) -> Self {
                Self {
                    error_code,
                    ..Self::default()
                }
            }
        })*
    };
    ($($ty:path),* $(,)?) => {
        $(impl ErrorResponse for $ty {
            fn error(error_code: i16, error_message: Option<String>) -> Self {
                Self {
                    error_code,
                    error_message,
                    ..Self::default()
                }
            }
        })*
    };
}

impl_error_response!(
    krabka_protocol::krabka::barrier::ListBarrierCutsResponse,
    krabka_protocol::owned::add_raft_voter_response::AddRaftVoterResponse,
    krabka_protocol::owned::alter_share_group_offsets_response::AlterShareGroupOffsetsResponse,
    krabka_protocol::owned::consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    krabka_protocol::owned::delete_share_group_offsets_response::DeleteShareGroupOffsetsResponse,
    krabka_protocol::owned::describe_acls_response::DescribeAclsResponse,
    krabka_protocol::owned::describe_quorum_response::DescribeQuorumResponse,
    krabka_protocol::owned::remove_raft_voter_response::RemoveRaftVoterResponse,
    // Kafka's `getErrorResponse` for both share RPCs leaves the acquisition
    // lock timeout at 0, not the configured one.
    krabka_protocol::owned::share_acknowledge_response::ShareAcknowledgeResponse,
    krabka_protocol::owned::share_fetch_response::ShareFetchResponse,
    krabka_protocol::owned::share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    krabka_protocol::owned::streams_group_topology_description_update_response::StreamsGroupTopologyDescriptionUpdateResponse,
    krabka_protocol::owned::unregister_broker_response::UnregisterBrokerResponse,
);

impl_error_response!(
    code_only:
    krabka_protocol::owned::add_offsets_to_txn_response::AddOffsetsToTxnResponse,
    krabka_protocol::owned::broker_heartbeat_response::BrokerHeartbeatResponse,
    krabka_protocol::owned::describe_delegation_token_response::DescribeDelegationTokenResponse,
    krabka_protocol::owned::expire_delegation_token_response::ExpireDelegationTokenResponse,
    krabka_protocol::owned::heartbeat_response::HeartbeatResponse,
    krabka_protocol::owned::leave_group_response::LeaveGroupResponse,
    krabka_protocol::owned::offset_delete_response::OffsetDeleteResponse,
    krabka_protocol::owned::renew_delegation_token_response::RenewDelegationTokenResponse,
    krabka_protocol::owned::sync_group_response::SyncGroupResponse,
    krabka_protocol::owned::update_raft_voter_response::UpdateRaftVoterResponse,
);

/// A per-item response row that carries its own `error_code` and
/// `error_message`.
pub(crate) trait ErrorRow {
    fn error_code(&self) -> i16;
    fn set_error(&mut self, error_code: i16, error_message: Option<String>);

    fn with_error(mut self, error_code: i16, error_message: String) -> Self
    where
        Self: Sized,
    {
        self.set_error(error_code, Some(error_message));
        self
    }
}

/// Stamps `error_code` and `error_message` on every row still at `NONE`, and
/// leaves a row an earlier check already refused as it is: the rewrite a
/// handler applies when the metadata submit behind its accepted rows fails.
pub(crate) fn stamp_unset<'a, R: ErrorRow + 'a>(
    rows: impl IntoIterator<Item = &'a mut R>,
    error_code: i16,
    error_message: &str,
) {
    for row in rows {
        if row.error_code() == NONE {
            row.set_error(error_code, Some(error_message.to_owned()));
        }
    }
}

macro_rules! impl_error_row {
    ($($ty:path),* $(,)?) => {
        $(impl ErrorRow for $ty {
            fn error_code(&self) -> i16 {
                self.error_code
            }

            fn set_error(&mut self, error_code: i16, error_message: Option<String>) {
                self.error_code = error_code;
                self.error_message = error_message;
            }
        })*
    };
}

impl_error_row!(
    krabka_protocol::owned::alter_configs_response::AlterConfigsResourceResponse,
    krabka_protocol::owned::incremental_alter_configs_response::AlterConfigsResourceResponse,
    krabka_protocol::owned::alter_client_quotas_response::EntryData,
    krabka_protocol::owned::alter_partition_reassignments_response::ReassignablePartitionResponse,
    krabka_protocol::owned::alter_user_scram_credentials_response::AlterUserScramCredentialsResult,
    krabka_protocol::owned::delete_acls_response::DeleteAclsFilterResult,
    krabka_protocol::owned::elect_leaders_response::PartitionResult,
);

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        Decode,
        owned::api_versions_response::{ApiVersion, ApiVersionsResponse},
    };

    use super::*;
    use crate::codes::{NONE, NOT_CONTROLLER};

    #[test]
    fn encode_response_round_trips_protocol_body() {
        let resp = ApiVersionsResponse {
            error_code: NONE,
            api_keys: vec![ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            }],
            throttle_time_ms: 0,
            ..Default::default()
        };

        let bytes = encode_response(&resp, 3).expect("encode response");
        let mut cur: &[u8] = &bytes;
        let decoded = ApiVersionsResponse::decode(&mut cur, 3).expect("decode response");

        assert!(decoded.error_code == crate::codes::NONE);
        assert!(decoded.api_keys.len() == 1);
        assert!(decoded.api_keys[0].api_key == 18);
        assert!(decoded.api_keys[0].min_version == 0);
        assert!(decoded.api_keys[0].max_version == 4);
    }

    #[test]
    fn stamp_unset_rewrites_only_rows_still_at_none() {
        use krabka_protocol::owned::alter_partition_reassignments_response::ReassignablePartitionResponse;

        let row = |partition_index, error_code, error_message: Option<&str>| {
            ReassignablePartitionResponse {
                partition_index,
                error_code,
                error_message: error_message.map(str::to_owned),
                ..Default::default()
            }
        };
        let mut rows = vec![
            row(7, crate::codes::NONE, None),
            row(
                8,
                crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("unknown partition"),
            ),
        ];

        stamp_unset(&mut rows, NOT_CONTROLLER, "submit failed: not controller");

        let expected = vec![
            row(
                7,
                crate::codes::NOT_CONTROLLER,
                Some("submit failed: not controller"),
            ),
            row(
                8,
                crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                Some("unknown partition"),
            ),
        ];
        assert!(rows == expected);
    }
}
