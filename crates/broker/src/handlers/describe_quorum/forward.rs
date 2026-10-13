//! Unwraps the leader's `EnvelopeResponse` to a forwarded `DescribeQuorum`
//! back into the plain `DescribeQuorumResponse` body a caller expects. The
//! request side is wrapped in a KIP-590 `Envelope` by the shared
//! [`crate::handlers::forward_to_controller::build`].
//!
//! [`krabka_raft::ControllerHandle::forward_raw`] dials the leader's
//! controller listener as THIS node's own inter-broker service identity, not
//! the caller's. Forwarding the bare `DescribeQuorumRequest` bytes at
//! `api_key=55` would have the leader's controller listener re-authorize
//! `Describe` against that service identity instead of the caller who
//! already passed [`crate::handlers::cluster_describe_denied`] on this node --
//! fencing out a legitimately-authorized caller for the sole reason that it
//! happened to land on a follower (review of #1034). Sending an `Envelope`
//! (`api_key=58`, KIP-590) instead carries the caller's own principal and
//! address across the hop, so the leader's `controller_admin::serve_envelope`
//! reconstructs a `RequestContext` under the identity that actually asked,
//! and [`super::handle`] re-runs the same `Describe` gate correctly on the
//! leader.

use {
    crate::{broker::Broker, codes, envelope, error::BrokerError, handlers::encode_response},
    bytes::Bytes,
    krabka_protocol::{
        Decode,
        owned::{
            describe_quorum_request::API_KEY as DESCRIBE_QUORUM_API_KEY,
            envelope_request,
            envelope_response::{self, EnvelopeResponse},
        },
    },
};

/// `Envelope`'s api key (58, KIP-590): what `DescribeQuorum` forwarding sends
/// on the wire, in place of the bare `DescribeQuorum` api key (55).
pub(super) const ENVELOPE_API_KEY: i16 = envelope_request::API_KEY;

/// `Envelope`'s one wire version.
pub(super) const ENVELOPE_VERSION: i16 = envelope_request::MIN_VERSION;

/// Kafka's `EnvelopeUtils`'s `CLUSTER_AUTHORIZATION_FAILED`, which the
/// receiving side's `unwrap_envelope` answers with when the OUTER (forwarding
/// hop's own) principal lacks `ClusterAction`. Every other `EnvelopeResponse`
/// error code is a protocol-shape failure this module does not expect to
/// produce, so it is treated the same as a forwarding failure below.
const ENVELOPE_CLUSTER_AUTHORIZATION_FAILED: i16 = codes::CLUSTER_AUTHORIZATION_FAILED;

/// Decode the leader's `EnvelopeResponse` and return the plain
/// `DescribeQuorumResponse` body it wrapped, exactly as if this node had
/// answered locally.
///
/// # Errors
/// Returns an error if the bytes do not decode as an `EnvelopeResponse`, or
/// as an encoded `DescribeQuorumResponse` error frame if the envelope itself
/// was refused or malformed.
pub(super) fn unwrap_response(
    broker: &Broker,
    envelope_bytes: &[u8],
    version: i16,
) -> Result<Bytes, BrokerError> {
    let mut cur = envelope_bytes;
    let resp = EnvelopeResponse::decode(&mut cur, envelope_response::MIN_VERSION)?;

    if resp.error_code != codes::NONE {
        // The leader refused to open the envelope, or something about its
        // shape did not parse there. `ClusterAuthorizationFailed` is a real,
        // reportable answer (the forwarding hop's own service identity lacks
        // `ClusterAction`, a cluster misconfiguration this caller cannot fix
        // by retrying); every other code here is a protocol-shape failure
        // this module does not expect to produce, so it is folded into the
        // same transient answer as an unresolved leader.
        let error_code = if resp.error_code == ENVELOPE_CLUSTER_AUTHORIZATION_FAILED {
            codes::CLUSTER_AUTHORIZATION_FAILED
        } else {
            codes::NOT_LEADER_OR_FOLLOWER
        };
        return encode_response(&super::top_level_error_response(error_code), version);
    }

    // A served envelope's `response_data` is the embedded `ResponseHeader`
    // in front of the handler's own body (`envelope::wrap_response`). Strip
    // that header off to get the same bytes a local answer would have
    // produced. The correlation id is local to this hop, so it is not checked.
    let flexible = broker
        .handlers()
        .body_flexible(DESCRIBE_QUORUM_API_KEY, version);
    let data = resp.response_data.unwrap_or_default();
    let (_, body) = envelope::unwrap_response(DESCRIBE_QUORUM_API_KEY, flexible, &data)?;
    Ok(body)
}
