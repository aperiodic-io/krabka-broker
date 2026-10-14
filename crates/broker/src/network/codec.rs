//! Kafka uses a 4-byte big-endian length prefix followed by the frame body.
//! Both directions of every connection share this framing, but the two
//! directions do not share a size limit: see [`KafkaCodec`].

#[cfg(test)]
use tokio::net::TcpStream;
#[cfg(test)]
use tokio_util::codec::Framed;
use {
    bytes::{BufMut, Bytes, BytesMut},
    tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec},
};

pub(crate) fn validate_frame_length(
    frame_body_len: usize,
    max_frame_bytes: usize,
) -> std::io::Result<()> {
    if frame_body_len > max_frame_bytes {
        return Err(std::io::Error::other(
            "frame exceeds configured maximum size",
        ));
    }
    Ok(())
}

/// The size prefix of a response frame of `frame_body_len` bytes.
///
/// Kafka bounds no response by `socket.request.max.bytes`: that limit is the
/// `maxSize` of the `NetworkReceive` that reads a request, and
/// `Processor.sendResponse` writes a `NetworkSend` of any size. The one bound
/// a response has is the signed int32 its size prefix is written as.
pub(crate) fn response_frame_length(frame_body_len: usize) -> std::io::Result<u32> {
    i32::try_from(frame_body_len)
        .map(i32::cast_unsigned)
        .map_err(|_| std::io::Error::other("response frame exceeds the int32 size prefix"))
}

/// Whether `error` is the codec's refusal of a size prefix over the request
/// limit: Kafka's `InvalidReceiveException`. Every other decode error is a
/// broken transport.
pub(crate) fn is_invalid_receive(error: &std::io::Error) -> bool {
    error.kind() == std::io::ErrorKind::InvalidData
}

/// Kafka's length-prefixed framing.
///
/// Decoding refuses a request whose size prefix exceeds the current request
/// limit, before its body is read, as Kafka's `NetworkReceive.readFrom` does
/// with `socket.request.max.bytes`. The connection loop reads requests directly
/// and applies its authentication-dependent receive limit before reserving and
/// reading the body; it uses this codec only to encode responses.
///
/// Encoding has no configurable limit. A response is bounded only by its int32
/// size prefix, see [`response_frame_length`].
#[derive(Debug)]
pub struct KafkaCodec {
    requests: LengthDelimitedCodec,
}

#[cfg(test)]
impl KafkaCodec {
    /// Sets the largest request frame the next [`Decoder::decode`] accepts.
    pub fn set_max_request_bytes(&mut self, max_request_bytes: usize) {
        self.requests.set_max_frame_length(max_request_bytes);
    }
}

impl Decoder for KafkaCodec {
    type Error = std::io::Error;
    type Item = BytesMut;

    fn decode(&mut self, src: &mut BytesMut) -> std::io::Result<Option<BytesMut>> {
        self.requests.decode(src)
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<BytesMut>> {
        self.requests.decode_eof(buf)
    }
}

impl Encoder<Bytes> for KafkaCodec {
    type Error = std::io::Error;

    fn encode(&mut self, frame: Bytes, dst: &mut BytesMut) -> std::io::Result<()> {
        let size_prefix = response_frame_length(frame.len())?;
        dst.reserve(4 + frame.len());
        dst.put_u32(size_prefix);
        dst.extend_from_slice(&frame);
        Ok(())
    }
}

/// Builds the [`KafkaCodec`] for a connection whose requests may be up to
/// `max_request_bytes`.
#[must_use]
pub fn codec(max_request_bytes: usize) -> KafkaCodec {
    let requests = LengthDelimitedCodec::builder()
        .length_field_offset(0)
        .length_field_length(4)
        .length_field_type::<u32>()
        .max_frame_length(max_request_bytes)
        .big_endian()
        .new_codec();
    KafkaCodec { requests }
}

/// Wraps a [`TcpStream`] with the Kafka length-delimited codec.
#[must_use]
#[cfg(test)]
pub fn frame(stream: TcpStream, max_request_bytes: usize) -> Framed<TcpStream, KafkaCodec> {
    Framed::new(stream, codec(max_request_bytes))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        bytes::{BufMut, Bytes, BytesMut},
        futures_util::{SinkExt, StreamExt},
        tokio::{
            io::AsyncWriteExt,
            net::{TcpListener, TcpStream},
        },
    };

    const DEFAULT_MAX_FRAME_BYTES: usize = 100 * 1024 * 1024;

    #[tokio::test]
    async fn roundtrips_a_frame() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut framed = frame(stream, DEFAULT_MAX_FRAME_BYTES);
            framed.next().await.unwrap().unwrap().freeze()
        });

        let client = TcpStream::connect(addr).await.unwrap();
        let mut framed = frame(client, DEFAULT_MAX_FRAME_BYTES);
        framed
            .send(Bytes::from_static(b"hello broker"))
            .await
            .unwrap();
        framed.into_inner().shutdown().await.unwrap();

        let received = server.await.unwrap();
        assert!(received.as_ref() == b"hello broker");
    }

    #[test]
    fn kafka_max_frame_size_is_one_hundred_mib() {
        assert!(DEFAULT_MAX_FRAME_BYTES == 100 * 1024 * 1024);
        assert!(DEFAULT_MAX_FRAME_BYTES == 104_857_600);
    }

    #[test]
    fn codec_decodes_frames_larger_than_tokio_default_but_within_kafka_max() {
        let payload_len = 9 * 1024 * 1024;
        assert!(payload_len < DEFAULT_MAX_FRAME_BYTES);

        let mut bytes = BytesMut::with_capacity(4 + payload_len);
        bytes.put_u32(u32::try_from(payload_len).expect("payload length fits u32"));
        bytes.resize(4 + payload_len, 0xA5);

        let decoded = codec(DEFAULT_MAX_FRAME_BYTES)
            .decode(&mut bytes)
            .expect("decode")
            .expect("frame");
        check!(decoded.len() == payload_len);
        check!(decoded[0] == 0xA5);
        check!(decoded[payload_len - 1] == 0xA5);
    }

    #[test]
    fn codec_limits_requests_only_and_does_not_limit_responses() {
        let mut exact = BytesMut::with_capacity(12);
        exact.put_u32(8);
        exact.resize(12, 0xA5);
        assert!(
            codec(8)
                .decode(&mut exact)
                .expect("decode exact maximum")
                .is_some()
        );

        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32(9);

        let err = codec(8).decode(&mut bytes).expect_err("oversized frame");
        assert!(err.to_string().contains("frame size too big"));

        // `socket.request.max.bytes` bounds a request. A response over it is
        // framed whole, with the size prefix of the frame that follows.
        let response = Bytes::from_static(b"123456789");
        let mut encoded = BytesMut::new();
        codec(8)
            .encode(response.clone(), &mut encoded)
            .expect("a response over the request limit is written");
        let mut expected = BytesMut::new();
        expected.put_u32(9);
        expected.put_slice(&response);
        assert!(encoded == expected);
    }

    #[test]
    fn set_max_request_bytes_changes_the_limit_of_the_next_frame() {
        let mut codec = codec(8);
        let mut frame_of_nine = BytesMut::new();
        frame_of_nine.put_u32(9);
        frame_of_nine.resize(13, 0xA5);

        let mut refused = frame_of_nine.clone();
        assert!(codec.decode(&mut refused).is_err());

        codec.set_max_request_bytes(9);
        assert!(
            codec
                .decode(&mut frame_of_nine)
                .expect("decode under the raised limit")
                .is_some()
        );

        codec.set_max_request_bytes(4);
        let mut frame_of_five = BytesMut::new();
        frame_of_five.put_u32(5);
        frame_of_five.resize(9, 0xA5);
        assert!(codec.decode(&mut frame_of_five).is_err());
    }

    #[yare::parameterized(
        empty = { 0, Some(0_u32) },
        hundred_mebibytes_plus_one = { 100 * 1024 * 1024 + 1, Some(104_857_601) },
        i32_max = {
            usize::try_from(i32::MAX).expect("i32::MAX fits usize"),
            Some(i32::MAX.cast_unsigned()),
        },
        above_i32_max = {
            usize::try_from(i32::MAX).expect("i32::MAX fits usize") + 1,
            None,
        },
    )]
    fn response_frame_length_is_bounded_by_the_int32_size_prefix(
        len: usize,
        accepted: Option<u32>,
    ) {
        check!(
            response_frame_length(len).ok() == accepted,
            "a {len}-byte response frame"
        );
    }

    #[test]
    fn codec_rejects_frames_over_kafka_max() {
        let mut bytes = BytesMut::with_capacity(4);
        bytes.put_u32(
            u32::try_from(DEFAULT_MAX_FRAME_BYTES + 1).expect("max frame length fits u32"),
        );

        let err = codec(DEFAULT_MAX_FRAME_BYTES)
            .decode(&mut bytes)
            .expect_err("oversized frame");
        assert!(
            err.to_string().contains("frame size too big"),
            "unexpected error: {err}"
        );
    }
}
