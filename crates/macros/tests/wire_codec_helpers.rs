//! Flat codec declarations preserve the caller's version and context contracts.

extern crate self as krabka_security;

use {
    assert2::assert,
    bytes::{Bytes, BytesMut},
    std::{
        cell::{Cell, RefCell},
        net::SocketAddr,
        panic::catch_unwind,
        ptr,
    },
};

pub struct Principal(&'static str);

trait Encode {
    fn encoded_len(&self, version: i16) -> usize;
    /// # Errors
    /// Returns an error for the fixture's reserved invalid payload.
    fn encode(&self, bytes: &mut BytesMut, version: i16) -> Result<(), &'static str>;
}

trait Decode<'a>: Sized {
    /// # Errors
    /// Returns an error for malformed or truncated fixture bytes.
    fn decode(bytes: &mut &[u8], version: i16) -> Result<Self, &'static str>;
}

#[derive(Debug, PartialEq, Eq)]
struct Message(u8);

impl Encode for Message {
    fn encoded_len(&self, version: i16) -> usize { if version >= 3 { 2 } else { 1 } }

    fn encode(&self, bytes: &mut BytesMut, version: i16) -> Result<(), &'static str> {
        if self.0 == 0xff {
            return Err("invalid payload");
        }
        bytes.extend_from_slice(&[self.0]);
        if version >= 3 {
            bytes.extend_from_slice(&[0]);
        }
        Ok(())
    }
}

impl Decode<'static> for Message {
    fn decode(bytes: &mut &[u8], version: i16) -> Result<Self, &'static str> {
        let (&value, rest) = bytes.split_first().ok_or("truncated payload")?;
        if value == 0xff {
            return Err("invalid payload");
        }
        *bytes = rest;
        if version >= 3 {
            let (&tag_count, rest) = bytes.split_first().ok_or("missing tags")?;
            if tag_count != 0 {
                return Err("invalid tags");
            }
            *bytes = rest;
        }
        Ok(Self(value))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Delegation {
    Encode(i16),
    Decode(i16),
    Context,
}

thread_local! { static DELEGATIONS: RefCell<Vec<Delegation>> = const { RefCell::new(Vec::new()) }; }

mod test_support {
    use super::{
        Bytes, BytesMut, DELEGATIONS, Decode, Delegation, Encode, Message, Principal, SocketAddr,
        assert, handlers::RequestContext,
    };

    pub(crate) fn encode_request(request: &Message, version: i16) -> Bytes {
        DELEGATIONS.with_borrow_mut(|calls| calls.push(Delegation::Encode(version)));
        let mut output = BytesMut::with_capacity(request.encoded_len(version));
        request
            .encode(&mut output, version)
            .expect("encode request");
        output.freeze()
    }

    pub(crate) fn decode_response(input: &Bytes, version: i16) -> Message {
        DELEGATIONS.with_borrow_mut(|calls| calls.push(Delegation::Decode(version)));
        let mut remaining = input.as_ref();
        let decoded = Message::decode(&mut remaining, version).expect("decode response");
        assert!(remaining.is_empty(), "response decoder consumed all bytes");
        decoded
    }

    pub(crate) fn request_context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
        client: &'a str,
    ) -> RequestContext<'a> {
        DELEGATIONS.with_borrow_mut(|calls| calls.push(Delegation::Context));
        RequestContext::new(
            principal,
            peer,
            client,
            "test-connection",
            false,
            "PLAINTEXT",
        )
    }
}

mod handlers {
    use super::{Principal, SocketAddr};

    pub(crate) struct RequestContext<'a> {
        pub principal: &'a Principal,
        pub peer: &'a SocketAddr,
        pub client_id: Option<&'a str>,
        pub connection_id: &'a str,
        pub sendfile_capable: bool,
        pub connection_listener_name: &'a str,
        pub correlation_id: i32,
    }

    impl<'a> RequestContext<'a> {
        pub(crate) fn new(
            principal: &'a Principal,
            peer: &'a SocketAddr,
            client_id: &'a str,
            connection_id: &'a str,
            sendfile_capable: bool,
            connection_listener_name: &'a str,
        ) -> Self {
            Self {
                principal,
                peer,
                client_id: Some(client_id),
                connection_id,
                sendfile_capable,
                connection_listener_name,
                correlation_id: 0,
            }
        }
    }
}

mod variable {
    use super::Message;

    krabka_macros::wire_codec_helpers! {
        pub(super) encode_request(Message);
        pub(super) decode_response(Message);
        pub(super) test_context [{
            if principal.0.is_empty() {
                "anonymous-client"
            } else {
                principal.0
            }
        }];
    }
}

thread_local! { static VERSION_READS: Cell<usize> = const { Cell::new(0) }; }

fn selected_version<const LEGACY: i16, const FLEXIBLE: i16>() -> i16 {
    VERSION_READS.with(|reads| reads.set(reads.get() + 1));
    assert!(LEGACY < FLEXIBLE);
    FLEXIBLE
}

mod pinned {
    use super::{Message, selected_version};

    krabka_macros::wire_codec_helpers! {
        pub(super) encode_request(Message);
        pub(super) decode_response(Message);
        version [{ selected_version::<1, 3>() }];
    }
}

mod encoder_only {
    use super::Message;
    krabka_macros::wire_codec_helpers! { pub(super) encode_request(Message); }
}

mod decoder_only {
    use super::Message;
    krabka_macros::wire_codec_helpers! {
        pub(super) decode_response(Message);
        version [1];
    }
}

mod context_only {
    krabka_macros::wire_codec_helpers! { pub(super) test_context ["fixed-client"]; }
}

#[test]
fn codecs_keep_version_selection_flexible_tags_and_complete_consumption() {
    DELEGATIONS.with_borrow_mut(Vec::clear);
    let request = Message(42);
    for (version, expected) in [(1, &[42][..]), (3, &[42, 0][..])] {
        let encoded = variable::encode_request(&request, version);
        assert!(encoded.as_ref() == expected);
        assert!(variable::decode_response(&encoded, version) == request);
    }
    assert!(encoder_only::encode_request(&request, 1).as_ref() == [42]);
    assert!(decoder_only::decode_response(&Bytes::from_static(&[42])) == request);

    VERSION_READS.with(|reads| reads.set(0));
    let encoded = pinned::encode_request(&request);
    VERSION_READS.with(|reads| assert!(reads.get() == 1));
    assert!(encoded.as_ref() == [42, 0]);
    assert!(pinned::decode_response(&encoded) == request);
    VERSION_READS.with(|reads| assert!(reads.get() == 2));
    DELEGATIONS.with_borrow(|calls| {
        assert!(
            *calls
                == [
                    Delegation::Encode(1),
                    Delegation::Decode(1),
                    Delegation::Encode(3),
                    Delegation::Decode(3),
                    Delegation::Encode(1),
                    Delegation::Decode(1),
                    Delegation::Encode(3),
                    Delegation::Decode(3)
                ]
        );
    });
}

#[test]
fn malformed_or_trailing_response_bytes_and_encoding_errors_still_panic() {
    for bytes in [&[][..], &[0xff], &[42], &[42, 1], &[42, 0, 7]] {
        let bytes = Bytes::copy_from_slice(bytes);
        assert!(catch_unwind(|| variable::decode_response(&bytes, 3)).is_err());
    }
    assert!(catch_unwind(|| encoder_only::encode_request(&Message(0xff), 1)).is_err());
}

#[test]
fn context_keeps_borrowed_identity_client_scope_and_constructor_defaults() {
    DELEGATIONS.with_borrow_mut(Vec::clear);
    let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
    for (principal, expected_client) in [
        (Principal("admin-client"), "admin-client"),
        (Principal(""), "anonymous-client"),
    ] {
        let context = variable::test_context(&principal, &peer);
        assert!(ptr::eq(
            ptr::from_ref(context.principal),
            ptr::from_ref(&principal)
        ));
        assert!(ptr::eq(ptr::from_ref(context.peer), ptr::from_ref(&peer)));
        assert!(context.client_id == Some(expected_client));
        assert!(context.connection_id == "test-connection");
        assert!(!context.sendfile_capable);
        assert!(context.connection_listener_name == "PLAINTEXT");
        assert!(context.correlation_id == 0);
        assert!(context_only::test_context(&principal, &peer).client_id == Some("fixed-client"));
    }
    DELEGATIONS.with_borrow(|calls| {
        assert!(
            *calls
                == [
                    Delegation::Context,
                    Delegation::Context,
                    Delegation::Context,
                    Delegation::Context
                ]
        );
    });
}
