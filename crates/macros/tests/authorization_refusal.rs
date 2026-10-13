//! Refusals compile against real Kafka responses and preserve native message evaluation.

use {
    assert2::assert,
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Decode, Encode, ProtocolError,
        owned::{add_raft_voter_response::AddRaftVoterResponse, vote_response::VoteResponse},
    },
    std::cell::Cell,
};

const DENIED: i16 = 31;

fn bare(version: i16) -> Result<Bytes, ProtocolError> {
    let mut output = BytesMut::new();
    krabka_macros::authorization_refusal!(VoteResponse, output, version, DENIED);
    Ok(output.freeze())
}

fn with_message(version: i16, evaluated: &Cell<usize>) -> Result<Bytes, ProtocolError> {
    let mut output = BytesMut::new();
    krabka_macros::authorization_refusal!(AddRaftVoterResponse, output, version, DENIED, {
        evaluated.set(evaluated.get() + 1);
        Some("native refusal".to_string())
    });
    Ok(output.freeze())
}

#[test]
fn response_shapes_and_native_messages_survive_encoding_and_failures() {
    let bytes = bare(0).unwrap();
    let decoded = VoteResponse::decode(&mut &bytes[..], 0).unwrap();
    assert!(decoded.error_code == DENIED && decoded.topics.is_empty());
    let evaluated = Cell::new(0);
    let bytes = with_message(0, &evaluated).unwrap();
    let decoded = AddRaftVoterResponse::decode(&mut &bytes[..], 0).unwrap();
    assert!(
        decoded.error_code == DENIED && decoded.error_message.as_deref() == Some("native refusal")
    );
    assert!(evaluated.get() == 1);
    assert!(with_message(-1, &evaluated).is_err());
    assert!(evaluated.get() == 2);
    assert!(bare(-1).is_err());
}
