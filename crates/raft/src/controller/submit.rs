//! Record submission: the leader-local `submit_change`, the resolution of a
//! leader's controller-listener address, and the encode → dial → translate path
//! a follower forwards a rejected batch over. The transport seam that dials the
//! leader is isolated here so the encode and translate halves stay testable
//! without a live quorum.

use krabka_metadata::MetadataRecord;

use super::{ControllerHandle, connection_options};
use crate::{
    error::RaftError,
    network::OutboundDialer,
    types::{NodeId, controller_endpoint_addr as endpoint_addr_from_endpoints},
};

impl ControllerHandle {
    /// Submit a batch of metadata records. Returns `Ok(())` once committed AND
    /// applied on the leader. Pre-validation lives in the engine. On a follower
    /// (`NotLeader` with a known leader), forwards directly to the leader's
    /// controller listener via `API_KEY_SUBMIT_CHANGE`.
    ///
    /// # Errors
    /// Returns an error if validation, replication, or forwarding fails.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.self_node_id.0, records = records.len())
    )]
    pub async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        match self.engine.submit_change(records.clone()).await {
            Ok(result) => Ok(result),
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                if let Some(addr) = self.voter_addr(leader) {
                    self.forward_submit_to(leader, &addr, &records).await
                } else {
                    Err(RaftError::NotLeader {
                        current_leader: Some(leader),
                    })
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Submit token mutations whose expected records are checked by the leader
    /// in the same engine turn as the append.
    ///
    /// # Errors
    ///
    /// Returns a leadership, transport, protocol, or mutation-rejection error.
    pub async fn submit_delegation_token_mutations(
        &self,
        mutations: Vec<crate::DelegationTokenMutation>,
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        match self
            .engine
            .submit_delegation_token_mutations(mutations.clone())
            .await
        {
            Ok(result) => Ok(result),
            Err(RaftError::NotLeader {
                current_leader: Some(leader),
            }) => {
                if let Some(addr) = self.voter_addr(leader) {
                    self.forward_delegation_token_mutations_to(leader, &addr, &mutations)
                        .await
                } else {
                    Err(RaftError::NotLeader {
                        current_leader: Some(leader),
                    })
                }
            }
            Err(error) => Err(error),
        }
    }

    // cargo-mutants: an I/O-only wrapper with no in-process signal. It encodes the
    // already-validated mutations, dials the leader through
    // `krabka_client_core::Connection` -- which no test in this process can build --
    // and returns the leader's reply. The body it sends is produced by
    // `encode_delegation_token_mutation_body`, which is mutation-tested.
    #[cfg_attr(test, mutants::skip)]
    async fn forward_delegation_token_mutations_to(
        &self,
        leader: NodeId,
        addr: &str,
        mutations: &[crate::DelegationTokenMutation],
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        let body = encode_delegation_token_mutation_body(mutations)?;
        let version = crate::wire::private_request_version(
            krabka_metadata::PrivateRpc::DelegationTokenMutation,
            Some(&self.current_image()),
        );
        let options = connection_options(
            &self.client_id,
            self.client_dispatch_queue_capacity,
            self.client_frame_max,
        );
        let connection = self
            .dialer
            .dial(leader, addr, options)
            .await
            .map_err(RaftError::Network)?;
        let response = connection
            .raw_request(
                crate::wire::API_KEY_DELEGATION_TOKEN_MUTATION,
                version,
                bytes::Bytes::from(body),
            )
            .await
            .map_err(RaftError::Network)?;
        connection.close();
        translate_submit_change_response(
            &response,
            leader,
            crate::wire::API_KEY_DELEGATION_TOKEN_MUTATION,
            version,
        )
    }

    /// Forward a raw Kafka-wire request for `api_key`/`version` to the
    /// current quorum leader's controller listener and return its raw
    /// response bytes, unmodified.
    ///
    /// Used by the broker's `DescribeQuorum` handler: Kafka's `KafkaApis`
    /// forwards `DescribeQuorum` from the broker listener to the active
    /// controller unconditionally (`forwardToController`), so a combined or
    /// controller node that is not itself the leader must reach the leader
    /// the same way a broker-only observer already does.
    ///
    /// # Errors
    /// `RaftError::NotLeader` when no leader is known or the leader's
    /// address cannot be resolved, `RaftError::Network` when the dial or
    /// round trip fails.
    // cargo-mutants: an I/O-only wrapper with no in-process signal (dial + one
    // raw request + close, over a `krabka_client_core::Connection` that no
    // test in this process can build). Covered by a live two-node cluster
    // test below.
    #[cfg_attr(test, mutants::skip)]
    pub async fn forward_raw(
        &self,
        api_key: i16,
        version: i16,
        body: bytes::Bytes,
    ) -> Result<bytes::Bytes, RaftError> {
        let Some(leader) = self.engine.quorum_snapshot().leader_id else {
            return Err(RaftError::NotLeader {
                current_leader: None,
            });
        };
        let Some(addr) = self.voter_addr(leader) else {
            return Err(RaftError::NotLeader {
                current_leader: Some(leader),
            });
        };
        let opts = connection_options(
            &self.client_id,
            self.client_dispatch_queue_capacity,
            self.client_frame_max,
        );
        let conn = self
            .dialer
            .dial(leader, &addr, opts)
            .await
            .map_err(RaftError::Network)?;
        let resp = conn
            .raw_request(api_key, version, body)
            .await
            .map_err(RaftError::Network)?;
        conn.close();
        Ok(resp)
    }

    /// Resolve a voter's controller listener `<host>:<port>` from the static
    /// voter set's CONTROLLER endpoint. See [`controller_endpoint_addr`].
    fn voter_addr(&self, node_id: NodeId) -> Option<String> {
        let voters = self.engine.quorum_snapshot().voters;
        controller_endpoint_addr(&voters, node_id)
            .or_else(|| controller_endpoint_addr(&self.voters, node_id))
    }

    /// Open a one-shot authenticated connection to the leader's controller
    /// listener, send a wincode-encoded `Vec<MetadataRecord>` as
    /// `API_KEY_SUBMIT_CHANGE`, and translate the response into a `RaftError`.
    ///
    /// Decomposed into three killable steps: [`encode_submit_change_body`] builds
    /// the exact wire bytes, the [`SubmitChangeTransport`] seam performs the
    /// (un-mockable) dial→`raw_request`→close round trip and hands back the raw
    /// response body, and [`translate_submit_change_response`] decodes that body
    /// and maps the transport `error_code` into a `RaftError`.
    // cargo-mutants: thin wrapper; builds the live DialerSubmitTransport, needs a real dialer
    #[cfg_attr(test, mutants::skip)]
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.self_node_id.0, leader = leader.0, addr, records = records.len()),
        err
    )]
    async fn forward_submit_to(
        &self,
        leader: NodeId,
        addr: &str,
        records: &[krabka_metadata::MetadataRecord],
    ) -> Result<crate::SubmitChangeResult, RaftError> {
        let transport = DialerSubmitTransport {
            dialer: self.dialer.as_ref(),
            client_id: &self.client_id,
            client_dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            client_frame_max: self.client_frame_max,
        };
        let version = crate::wire::private_request_version(
            krabka_metadata::PrivateRpc::SubmitChange,
            Some(&self.current_image()),
        );
        forward_submit_via(&transport, leader, addr, version, records).await
    }
}

/// Resolve a voter's CONTROLLER-listener `<host>:<port>` from the voter set,
/// preferring the endpoint named `CONTROLLER` and falling back to the first.
///
/// The host is returned VERBATIM (a DNS name), never pre-resolved to a
/// `SocketAddr`. The dialer re-resolves it per connect (`TcpStream::connect`),
/// so a peer that restarts on a new pod IP stays reachable. Parsing to a
/// `SocketAddr` here would (a) freeze a restarted peer's boot-time IP and
/// (b) fail outright on a non-literal hostname — which silently disabled
/// leader-forwarding of `submit_change` (e.g. broker self-registration), since
/// `parse()` returned `None` and the forward was skipped.
fn controller_endpoint_addr(voters: &krabka_metadata::VoterSet, node_id: NodeId) -> Option<String> {
    let voter = voters.get(node_id)?;
    endpoint_addr_from_endpoints(&voter.endpoints)
}

/// The single un-mockable step of leader-forwarding a `submit_change`: dial the
/// leader's controller listener, issue one `API_KEY_SUBMIT_CHANGE` request with
/// the already-encoded `body`, and return the raw response body bytes. The
/// concrete [`krabka_client_core::Connection`] is opaque (it cannot be built in
/// a test), so this seam returns plain `Bytes` — every serialize/translate
/// decision around it stays unit-testable against a mock.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
trait SubmitChangeTransport: Send + Sync {
    /// Round-trip the encoded `API_KEY_SUBMIT_CHANGE` `body` to the leader at
    /// request `version` and return the raw response body.
    async fn send_submit_change(
        &self,
        leader: NodeId,
        addr: &str,
        version: i16,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, krabka_client_core::ClientError>;
}

/// Live [`SubmitChangeTransport`] over the injected [`OutboundDialer`]: dials a
/// one-shot authenticated connection, sends the request at the negotiated
/// `API_KEY_SUBMIT_CHANGE` version, closes the connection, and returns the response body. This is the
/// only part of the forward path that touches a real socket.
struct DialerSubmitTransport<'a> {
    dialer: &'a dyn OutboundDialer,
    client_id: &'a str,
    client_dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    client_frame_max: krabka_client_core::ClientFrameMax,
}

#[async_trait::async_trait]
impl SubmitChangeTransport for DialerSubmitTransport<'_> {
    // cargo-mutants: the only un-mockable step: dial + one `API_KEY_SUBMIT_CHANGE` + close, with
    // no offline signal (a `krabka_client_core::Connection` cannot be built in a
    // test). `#[mutants::skip]` rather than an `exclude_re` because cargo-mutants'
    // name-regex exclusions do not reliably match the struct-field-deletion mutant
    // its outbound connection options would otherwise generate.
    #[cfg_attr(test, mutants::skip)]
    async fn send_submit_change(
        &self,
        leader: NodeId,
        addr: &str,
        version: i16,
        body: Vec<u8>,
    ) -> Result<bytes::Bytes, krabka_client_core::ClientError> {
        let opts = connection_options(
            self.client_id,
            self.client_dispatch_queue_capacity,
            self.client_frame_max,
        );
        let conn = self.dialer.dial(leader, addr, opts).await?;
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_SUBMIT_CHANGE,
                version,
                bytes::Bytes::from(body),
            )
            .await?;
        conn.close();
        Ok(resp_body)
    }
}

/// `forward_submit_to`'s testable core: serialize → send (via the injected
/// [`SubmitChangeTransport`]) → translate. The real path supplies a
/// [`DialerSubmitTransport`]; tests supply a mock so the serialize/translate
/// decisions carry mutation signal without a live quorum.
async fn forward_submit_via(
    transport: &dyn SubmitChangeTransport,
    leader: NodeId,
    addr: &str,
    version: i16,
    records: &[krabka_metadata::MetadataRecord],
) -> Result<crate::SubmitChangeResult, RaftError> {
    let body = encode_submit_change_body(records)?;
    let resp_body = transport
        .send_submit_change(leader, addr, version, body)
        .await
        .map_err(RaftError::Network)?;
    translate_submit_change_response(
        &resp_body,
        leader,
        crate::wire::API_KEY_SUBMIT_CHANGE,
        version,
    )
}

/// Build the exact `API_KEY_SUBMIT_CHANGE` v0 request body for `records`:
/// wincode-encode the `Vec<MetadataRecord>`, then frame it with the
/// length-prefixed [`crate::wire::KrabkaSubmitChangeRequest`] codec. Kept
/// byte-for-byte identical to the inlined path so the wire stays exact.
fn encode_submit_change_body(
    records: &[krabka_metadata::MetadataRecord],
) -> Result<Vec<u8>, RaftError> {
    let body_bytes = <serde_wincode::SerdeCompat<Vec<krabka_metadata::MetadataRecord>> as wincode::Serialize>::serialize(
        &records.to_vec(),
    )
    .map_err(RaftError::from)?;
    submit_change_frame(body_bytes)
}

fn encode_delegation_token_mutation_body(
    mutations: &[crate::DelegationTokenMutation],
) -> Result<Vec<u8>, RaftError> {
    let payload = <serde_wincode::SerdeCompat<Vec<crate::DelegationTokenMutation>> as wincode::Serialize>::serialize(
        &mutations.to_vec(),
    )
    .map_err(RaftError::from)?;
    submit_change_frame(payload)
}

fn submit_change_frame(payload: Vec<u8>) -> Result<Vec<u8>, RaftError> {
    let request = crate::wire::KrabkaSubmitChangeRequest {
        records: bytes::Bytes::from(payload),
    };
    let mut body = Vec::with_capacity(request.records.len() + 4);
    request.encode_v0(&mut body)?;
    Ok(body)
}

/// Decode a `KrabkaSubmitChangeResponse` from the leader's `resp_body` and map
/// its transport `error_code` into the caller's `Result`:
/// - `0` → applied (`Ok`).
/// - `2` → the leader rejected at apply-time (topic already exists). The wire
///   carries only a code; the topic name is what the caller had in hand.
/// - [`crate::wire::SUBMIT_CHANGE_UNCOMMITTED_TAIL`] → the leader refused a
///   compare-and-set until its tail commits ([`RaftError::UncommittedTail`]).
/// - [`crate::wire::PRIVATE_CLUSTER_AUTHORIZATION_FAILED`] → the leader denied
///   `ClusterAction` to this node's principal
///   ([`RaftError::ClusterAuthorizationFailed`]).
/// - [`crate::wire::PRIVATE_UNSUPPORTED_VERSION`] → the leader does not
///   implement `version`, the version of `api_key` this node sent
///   ([`RaftError::UnsupportedPrivateVersion`]). `krabka.version` negotiation
///   keeps a sender at a version every registered node serves, so only a peer
///   outside that agreement answers it.
/// - anything else → collapse to `NotLeader` (`CreateTopics` maps that to the
///   retryable `NOT_CONTROLLER`), preferring the response's `leader_hint` when
///   non-negative and falling back to the dialed `leader`.
fn translate_submit_change_response(
    resp_body: &[u8],
    leader: NodeId,
    api_key: i16,
    version: i16,
) -> Result<crate::SubmitChangeResult, RaftError> {
    let mut cur: &[u8] = resp_body;
    let resp = crate::wire::KrabkaSubmitChangeResponse::decode_v0(&mut cur)?;
    match resp.error_code {
        0 => <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Deserialize>::deserialize(
            &resp.result,
        )
        .map_err(RaftError::from),
        2 => Err(RaftError::Metadata(
            krabka_metadata::MetadataError::TopicExists(String::new()),
        )),
        crate::wire::SUBMIT_CHANGE_UNCOMMITTED_TAIL => Err(RaftError::UncommittedTail),
        crate::wire::PRIVATE_CLUSTER_AUTHORIZATION_FAILED => Err(RaftError::ClusterAuthorizationFailed),
        crate::wire::PRIVATE_UNSUPPORTED_VERSION => {
            Err(RaftError::UnsupportedPrivateVersion { api_key, version })
        }
        _ => Err(RaftError::NotLeader {
            current_leader: (resp.leader_hint >= 0)
                .then(|| NodeId(u64::try_from(resp.leader_hint).unwrap_or(leader.0))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use uuid::Uuid;

    use super::*;
    use crate::controller::test_support::{submit_change_response_bytes, topic_record};

    #[test]
    fn controller_endpoint_addr_keeps_dns_hostname_not_parsed_socketaddr() {
        // Regression: a voter endpoint host is a per-pod DNS FQDN, NOT a
        // pre-resolved IP. The resolver must return "<host>:<port>" verbatim so
        // the dialer re-resolves it per connect. Parsing to a `SocketAddr`
        // returns None for a hostname, which silently disabled leader-forwarding
        // of `submit_change` — broker self-registration then failed with "not
        // leader" and RF=3 topics could not be placed.
        let host = "demo-broker-2-0.demo-broker-headless.default.svc.cluster.local";
        let voters = krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
            id: NodeId(2),
            directory_id: Uuid::nil(),
            endpoints: vec![krabka_metadata::VoterEndpoint {
                name: "CONTROLLER".to_string(),
                host: host.to_string(),
                port: 9093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        for (_name, node_id, expected) in [
            ("registered voter", NodeId(2), Some(format!("{host}:9093"))),
            ("unknown voter", NodeId(99), None),
        ] {
            assert2::assert!(controller_endpoint_addr(&voters, node_id) == expected);
        }
    }

    #[test]
    fn controller_endpoint_addr_prefers_controller_endpoint_over_others() {
        // A voter advertises several listeners. The resolver must pick the one
        // named CONTROLLER even when it is not first in the list — submit_change
        // must be forwarded to the controller listener, not (e.g.) the
        // inter-broker REPLICATION listener on a different port. The non-
        // CONTROLLER endpoint is placed FIRST so a flipped `name == "CONTROLLER"`
        // predicate (matching the first NON-controller endpoint instead) returns
        // the wrong address.
        let voters = krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
            id: NodeId(7),
            directory_id: Uuid::nil(),
            endpoints: vec![
                krabka_metadata::VoterEndpoint {
                    name: "REPLICATION".to_string(),
                    host: "replication-host".to_string(),
                    port: 9092,
                },
                krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "controller-host".to_string(),
                    port: 9093,
                },
            ],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        assert2::assert!(
            controller_endpoint_addr(&voters, NodeId(7))
                == Some("controller-host:9093".to_string())
        );
    }

    #[test]
    fn encode_submit_change_body_frames_wincode_records_with_i32_length_prefix() {
        // The forward path must produce the exact `KrabkaSubmitChangeRequest` v0
        // wire bytes: a 4-byte big-endian length prefix followed by the
        // wincode-encoded `Vec<MetadataRecord>`. Decoding the framed body back
        // and re-deserializing must round-trip to the original records, proving
        // the prefix length matches the payload length (a mutated length or a
        // dropped wincode step fails to decode or yields different records).
        let records = vec![topic_record("alpha"), topic_record("beta")];
        let body = encode_submit_change_body(&records).expect("encode");

        let expected_wincode = <serde_wincode::SerdeCompat<
            Vec<krabka_metadata::MetadataRecord>,
        > as wincode::Serialize>::serialize(&records)
        .expect("wincode");
        assert2::assert!(body.len() == expected_wincode.len() + 4);

        let mut cur: &[u8] = &body;
        let req =
            crate::wire::KrabkaSubmitChangeRequest::decode_v0(&mut cur).expect("decode frame");
        assert2::assert!(req.records.as_ref() == expected_wincode.as_slice());
        // The framed payload IS the wincode encoding of the original records, so
        // it deserializes back to them — proving no double-framing / corruption.
        let decoded = <serde_wincode::SerdeCompat<
            Vec<krabka_metadata::MetadataRecord>,
        > as wincode::Deserialize>::deserialize(&req.records)
        .expect("wincode decode");
        assert2::assert!(decoded == records);
    }

    /// A `SubmitChange` v0 request body that carries one `TopicRecord`.
    const SUBMIT_CHANGE_V0_GOLDEN: &[u8] = &[
        0, 0, 0, 51, // frame length
        1, 0, 0, 0, 0, 0, 0, 0, // one record
        0, 0, 0, 0, // V1Topic
        1, 0, 0, 0, 0, 0, 0, 0, b't', // name
        16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, // topic id
        1, 0, 0, 0, // partitions
        1, 0, // replication factor
    ];

    /// A `DelegationTokenMutation` v0 request body that carries one `Delete`.
    fn delegation_token_mutation_v0_golden() -> Vec<u8> {
        // Captured wincode principal, shared by owner, requester and renewer.
        // These chunks are literal oracle bytes, independent of serialization.
        const USER_ALICE: &[u8] = &[
            4, 0, 0, 0, 0, 0, 0, 0, b'U', b's', b'e', b'r', 5, 0, 0, 0, 0, 0, 0, 0, b'a', b'l',
            b'i', b'c', b'e',
        ];
        let prefix: &[u8] = &[
            0, 0, 0, 130, // frame length
            1, 0, 0, 0, 0, 0, 0, 0, // one mutation
            2, 0, 0, 0, // Delete
            3, 0, 0, 0, 0, 0, 0, 0, b't', b'o', b'k', // token id
        ];
        let timestamps_and_renewer_count: &[u8] = &[
            1, 0, 0, 0, 0, 0, 0, 0, // issue timestamp
            2, 0, 0, 0, 0, 0, 0, 0, // expiry timestamp
            3, 0, 0, 0, 0, 0, 0, 0, // max timestamp
            1, 0, 0, 0, 0, 0, 0, 0, // one renewer
        ];
        [
            prefix,
            USER_ALICE,
            USER_ALICE,
            timestamps_and_renewer_count,
            USER_ALICE,
        ]
        .concat()
    }

    /// The wincode `SubmitChangeResult` of a `SubmitChange` v0 response with
    /// one offset reservation.
    const SUBMIT_CHANGE_RESULT_V0_GOLDEN: &[u8] = &[
        1, 0, 0, 0, 0, 0, 0, 0, // one reservation
        1, 0, 0, 0, 0, 0, 0, 0, b't', // topic
        0, 0, 0, 0, // partition
        5, 0, 0, 0, 0, 0, 0, 0, // base offset
        2, 0, 0, 0, 0, 0, 0, 0, // count
        3, 0, 0, 0, 0, 0, 0, 0, // leader epoch
    ];

    /// One delegation token, the representative value of the
    /// `DelegationTokenMutation` golden body.
    fn golden_token() -> krabka_metadata::DelegationTokenRecord {
        let alice = krabka_security::KafkaPrincipal {
            principal_type: "User".into(),
            name: "alice".into(),
        };
        krabka_metadata::DelegationTokenRecord {
            token_id: "tok".into(),
            owner: alice.clone(),
            requester: alice.clone(),
            issue_timestamp_ms: 1,
            expiry_timestamp_ms: 2,
            max_timestamp_ms: 3,
            renewers: vec![alice],
        }
    }

    /// The bodies of `SubmitChange` and `DelegationTokenMutation` v0 requests and
    /// of a `SubmitChange` v0 result, byte for byte: the length-prefixed frame
    /// around the wincode payload. Nodes of two 1.x builds exchange these
    /// during a rolling upgrade, so a change to one of them is a change to the
    /// 1.x contract and takes a new api version.
    #[test]
    fn v0_payloads_match_their_golden_bytes() {
        let delegation_golden = delegation_token_mutation_v0_golden();
        let records = vec![krabka_metadata::MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: "t".into(),
                topic_id: Uuid::from_u128(1),
                partitions: 1,
                replication_factor: 1,
            },
        )];
        let mutations = vec![crate::DelegationTokenMutation::Delete {
            expected: golden_token(),
        }];
        let result = crate::SubmitChangeResult {
            offset_reservations: vec![crate::OffsetReservation {
                topic: "t".into(),
                partition: 0,
                base_offset: 5,
                count: 2,
                leader_epoch: 3,
            }],
        };
        let result_bytes =
            <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Serialize>::serialize(
                &result,
            )
            .expect("wincode");

        check!(encode_submit_change_body(&records).unwrap() == SUBMIT_CHANGE_V0_GOLDEN);
        check!(encode_delegation_token_mutation_body(&mutations).unwrap() == delegation_golden);
        check!(result_bytes == SUBMIT_CHANGE_RESULT_V0_GOLDEN);

        // Each golden body decodes back to the value it was made from.
        let mut cur: &[u8] = SUBMIT_CHANGE_V0_GOLDEN;
        let frame = crate::wire::KrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap();
        check!(
            <serde_wincode::SerdeCompat<Vec<krabka_metadata::MetadataRecord>> as wincode::Deserialize>::deserialize(&frame.records).unwrap()
                == records
        );
        let mut cur: &[u8] = &delegation_golden;
        let frame = crate::wire::KrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap();
        check!(
            <serde_wincode::SerdeCompat<Vec<crate::DelegationTokenMutation>> as wincode::Deserialize>::deserialize(&frame.records).unwrap()
                == mutations
        );
        check!(
            <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Deserialize>::deserialize(SUBMIT_CHANGE_RESULT_V0_GOLDEN).unwrap()
                == result
        );
    }

    #[test]
    fn translate_submit_change_response_maps_each_error_code() {
        // 0 => applied.
        assert2::assert!(
            translate_submit_change_response(
                &submit_change_response_bytes(0, -1),
                NodeId(5),
                crate::wire::API_KEY_SUBMIT_CHANGE,
                0
            )
            .is_ok()
        );

        // 2 => leader rejected at apply-time: a TopicExists metadata error.
        let err = translate_submit_change_response(
            &submit_change_response_bytes(2, -1),
            NodeId(5),
            crate::wire::API_KEY_SUBMIT_CHANGE,
            0,
        )
        .expect_err("code 2 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::Metadata(krabka_metadata::MetadataError::TopicExists(_))
        ));

        // The uncommitted-tail code keeps its meaning across the forward.
        let err = translate_submit_change_response(
            &submit_change_response_bytes(crate::wire::SUBMIT_CHANGE_UNCOMMITTED_TAIL, -1),
            NodeId(5),
            crate::wire::API_KEY_SUBMIT_CHANGE,
            0,
        )
        .expect_err("an uncommitted tail is an error");
        assert2::assert!(matches!(err, RaftError::UncommittedTail));

        // Cluster auth failure maps directly to ClusterAuthorizationFailed
        let err = translate_submit_change_response(
            &submit_change_response_bytes(crate::wire::PRIVATE_CLUSTER_AUTHORIZATION_FAILED, -1),
            NodeId(5),
            crate::wire::API_KEY_SUBMIT_CHANGE,
            0,
        )
        .expect_err("cluster auth failed");
        assert2::assert!(matches!(err, RaftError::ClusterAuthorizationFailed));

        // UNSUPPORTED_VERSION names the api and the version this build sent,
        // for each of the two APIs that share the response.
        for api_key in [
            crate::wire::API_KEY_SUBMIT_CHANGE,
            crate::wire::API_KEY_DELEGATION_TOKEN_MUTATION,
        ] {
            let err = translate_submit_change_response(
                &submit_change_response_bytes(crate::wire::PRIVATE_UNSUPPORTED_VERSION, -1),
                NodeId(5),
                api_key,
                2,
            )
            .expect_err("an unsupported version is an error");
            assert2::assert!(matches!(
                err,
                RaftError::UnsupportedPrivateVersion { api_key: got, version: 2 } if got == api_key
            ));
        }

        // Any other code collapses to NotLeader, taking the response's
        // leader_hint when non-negative.
        let err = translate_submit_change_response(
            &submit_change_response_bytes(1, 9),
            NodeId(5),
            crate::wire::API_KEY_SUBMIT_CHANGE,
            0,
        )
        .expect_err("code 1 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(9))
            }
        ));

        // A negative leader_hint falls back to None (unknown), NOT to the dialed
        // leader id — distinguishing the `>= 0` guard.
        let err = translate_submit_change_response(
            &submit_change_response_bytes(3, -1),
            NodeId(5),
            crate::wire::API_KEY_SUBMIT_CHANGE,
            0,
        )
        .expect_err("code 3 is an error");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: None
            }
        ));
    }

    #[test]
    fn translate_submit_change_response_propagates_decode_error() {
        // A truncated body (fewer than the fixed 10 response bytes) must surface
        // as a protocol error rather than being silently treated as success.
        let err = translate_submit_change_response(
            &[0u8; 3],
            NodeId(5),
            crate::wire::API_KEY_SUBMIT_CHANGE,
            0,
        )
        .expect_err("truncated decodes err");
        assert2::assert!(matches!(err, RaftError::Protocol(_)));
    }

    #[tokio::test]
    async fn forward_submit_via_sends_encoded_body_and_returns_ok_on_applied() {
        // End-to-end of the testable core: the transport must receive the exact
        // framed body for `records` (the wincode + length-prefix encoding) at the
        // dialed leader/addr, and an `error_code = 0` response yields Ok.
        let records = vec![topic_record("gamma")];
        let expected_body = encode_submit_change_body(&records).expect("encode");

        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .withf(move |leader, addr, version, body| {
                *leader == 7
                    && addr == "leader-host:9093"
                    && *version == 0
                    && body == &expected_body
            })
            .times(1)
            .returning(|_, _, _, _| Ok(submit_change_response_bytes(0, -1)));

        forward_submit_via(&transport, NodeId(7), "leader-host:9093", 0, &records)
            .await
            .expect("applied");
    }

    #[tokio::test]
    async fn forward_submit_via_translates_not_leader_hint() {
        // The transport's response leader_hint must flow through translation to
        // the caller's NotLeader error.
        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .returning(|_, _, _, _| Ok(submit_change_response_bytes(1, 4)));

        let err = forward_submit_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            0,
            &[topic_record("z")],
        )
        .await
        .expect_err("not leader");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: Some(NodeId(4))
            }
        ));
    }

    #[tokio::test]
    async fn forward_submit_via_maps_transport_error_to_network() {
        // A dial/send failure surfaces as RaftError::Network (so CreateTopics
        // retries), not a panic or a swallowed success.
        let mut transport = MockSubmitChangeTransport::new();
        transport
            .expect_send_submit_change()
            .returning(|_, _, _, _| {
                Err(krabka_client_core::ClientError::Io(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "refused",
                )))
            });

        let err = forward_submit_via(
            &transport,
            NodeId(7),
            "leader-host:9093",
            0,
            &[topic_record("z")],
        )
        .await
        .expect_err("network error");
        assert2::assert!(matches!(err, RaftError::Network(_)));
    }

    #[test]
    fn encode_delegation_token_mutation_body_frames_mutations() {
        use krabka_metadata::DelegationTokenRecord;
        use krabka_security::KafkaPrincipal;

        let rec = DelegationTokenRecord {
            token_id: "tok".into(),
            owner: KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            requester: KafkaPrincipal {
                principal_type: "User".into(),
                name: "alice".into(),
            },
            issue_timestamp_ms: 10,
            max_timestamp_ms: 20,
            expiry_timestamp_ms: 15,
            renewers: vec![],
        };
        let mutations = vec![crate::DelegationTokenMutation::Delete { expected: rec }];
        let body = encode_delegation_token_mutation_body(&mutations).expect("encode");
        assert2::assert!(body.len() > 10);
        let mut cur: &[u8] = &body;
        let req =
            crate::wire::KrabkaSubmitChangeRequest::decode_v0(&mut cur).expect("decode frame");
        let decoded = <serde_wincode::SerdeCompat<
            Vec<crate::DelegationTokenMutation>,
        > as wincode::Deserialize>::deserialize(&req.records)
        .expect("wincode decode");
        assert2::assert!(decoded == mutations);
    }

    #[tokio::test]
    async fn controller_handle_voter_addr_resolves_known_voter() {
        let (_dir, ctrl) = crate::controller::test_support::bootstrap_controller("start").await;
        let addr = ctrl.voter_addr(NodeId(1));
        assert2::assert!(addr == Some("127.0.0.1:0".to_string()));
        assert2::assert!(ctrl.voter_addr(NodeId(999)).is_none());
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn forward_raw_with_no_known_leader_rejects_not_leader() {
        let (_dir, ctrl) = crate::controller::test_support::joining_controller("join start").await;

        let err = ctrl
            .forward_raw(crate::wire::API_KEY_METADATA_FETCH, 0, bytes::Bytes::new())
            .await
            .expect_err("no leader is known yet");
        assert2::assert!(matches!(
            err,
            RaftError::NotLeader {
                current_leader: None
            }
        ));
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn submit_delegation_token_mutations_on_join_node_rejects_not_leader() {
        let (_dir, ctrl) = crate::controller::test_support::joining_controller("join start").await;
        let res = ctrl.submit_delegation_token_mutations(vec![]).await;
        assert2::assert!(matches!(
            res,
            Err(RaftError::NotLeader {
                current_leader: None
            })
        ));
        ctrl.shutdown().await;
    }
}
