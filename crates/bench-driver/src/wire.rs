//! A lean produce connection for the raw load generator.
//!
//! The client library's connection hands every request to a writer task and
//! every response back through a channel, so each request costs the sender
//! two cross-task wakeups on top of the network round trip. On a machine the
//! brokers share, those wakeups are a large part of a sub-millisecond
//! produce latency, and they are the generator's, not the cluster's.
//!
//! Here the sending task writes its frame to the socket itself, and one reader
//! task per connection takes the responses in order -- a broker answers a
//! connection's requests in the order it read them -- and settles each one
//! where it lands: it tallies the ack and wakes a waiting sender, if any.

use {
    anyhow::{Context, Result},
    bytes::{BufMut, Bytes, BytesMut},
    krabka_protocol::{
        Decode, ProtocolRequest,
        owned::{produce_request::ProduceRequest, produce_response::ProduceResponse},
        primitives::fixed::get_i32,
        tagged_fields::read_tagged_fields,
    },
    std::{
        collections::VecDeque,
        io::IoSlice,
        net::SocketAddr,
        sync::{Arc, Mutex},
    },
    tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::{
            TcpStream,
            tcp::{OwnedReadHalf, OwnedWriteHalf},
        },
        sync::oneshot,
        task::JoinHandle,
        time::Instant,
    },
};

/// The `Produce` version the wire speaks: flexible, and still naming topics.
pub const PRODUCE_VERSION: i16 = 9;

/// The client id every wire request carries.
const CLIENT_ID: &[u8] = b"bench-raw-producer";

/// How one settled request went.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled {
    /// Every partition answered without an error.
    Acked,
    /// A partition answered this error code.
    Failed { partition: i32, error_code: i16 },
    /// The response did not decode.
    Undecodable(String),
}

/// What the reader does with one settled request.
pub trait Settle: Send + Sync + 'static {
    /// Called on the reader task with the request's tag, its round trip, and
    /// how it went.
    fn settle(&self, tag: Tag, round_trip: std::time::Duration, settled: Settled);
}

/// What the sender attached to one request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tag {
    /// The epoch nanosecond the request's records were stamped with.
    pub sent_ns: u64,
    /// How many records the request carried.
    pub records: u64,
}

struct Pending {
    sent: Instant,
    tag: Tag,
    done: Option<oneshot::Sender<()>>,
}

struct Writer {
    half: OwnedWriteHalf,
    next_correlation_id: i32,
    frame: BytesMut,
}

/// One connection to a broker that produces with pipelined requests.
pub struct ProduceWire {
    writer: tokio::sync::Mutex<Writer>,
    pending: Mutex<VecDeque<Pending>>,
}

impl ProduceWire {
    /// Connects to `addr` and starts the reader task, which settles every
    /// response through `settle`.
    ///
    /// # Errors
    ///
    /// When the connection cannot be made.
    pub async fn connect(
        addr: SocketAddr,
        settle: Arc<dyn Settle>,
    ) -> Result<(Arc<Self>, JoinHandle<()>)> {
        let stream = TcpStream::connect(addr)
            .await
            .with_context(|| format!("connect to {addr}"))?;
        stream.set_nodelay(true)?;
        let (read, write) = stream.into_split();
        let wire = Arc::new(Self {
            writer: tokio::sync::Mutex::new(Writer {
                half: write,
                next_correlation_id: 0,
                frame: BytesMut::new(),
            }),
            pending: Mutex::new(VecDeque::new()),
        });
        let reader = tokio::spawn(read_responses(read, Arc::clone(&wire), settle));
        Ok((wire, reader))
    }

    /// Writes `request` and returns once it is on the socket. With `done`,
    /// the reader signals it when the response is settled.
    ///
    /// # Errors
    ///
    /// When the request does not encode or the write fails.
    ///
    /// # Panics
    ///
    /// If the pending queue's lock is poisoned, which only a panicking reader
    /// can do.
    pub async fn send(
        &self,
        request: &ProduceFrame<'_>,
        tag: Tag,
        done: Option<oneshot::Sender<()>>,
    ) -> Result<()> {
        let mut writer = self.writer.lock().await;
        let Writer {
            half,
            next_correlation_id,
            frame,
        } = &mut *writer;
        let correlation_id = *next_correlation_id;
        *next_correlation_id = next_correlation_id.wrapping_add(1);
        let segments = request.segments(frame, correlation_id)?;
        // Queue before writing, under the writer lock: the queue order is
        // the wire order, which is the order the responses come back in.
        self.pending
            .lock()
            .expect("pending queue")
            .push_back(Pending {
                sent: Instant::now(),
                tag,
                done,
            });
        write_all_vectored(half, &segments)
            .await
            .context("write a produce request")
    }
}

/// A `Produce` request of record batches already encoded, one per partition
/// of one topic.
pub struct ProduceFrame<'a> {
    pub topic: &'a str,
    pub acks: i16,
    pub timeout_ms: i32,
    pub batches: &'a [(i32, Bytes)],
}

impl ProduceFrame<'_> {
    /// The request frame as the bytes around each batch and the batches
    /// themselves, so the batches go out without being copied into a frame
    /// buffer. `scratch` holds the bytes between them.
    fn segments(&self, scratch: &mut BytesMut, correlation_id: i32) -> Result<Vec<Bytes>> {
        let mut segments = Vec::with_capacity(self.batches.len() * 2 + 1);
        scratch.clear();
        scratch.put_i32(0); // the length, filled in below
        scratch.put_i16(<ProduceRequest as ProtocolRequest>::API_KEY);
        scratch.put_i16(PRODUCE_VERSION);
        scratch.put_i32(correlation_id);
        scratch.put_i16(i16::try_from(CLIENT_ID.len())?);
        scratch.put_slice(CLIENT_ID);
        scratch.put_u8(0); // no header tagged fields
        scratch.put_u8(0); // a null transactional id
        scratch.put_i16(self.acks);
        scratch.put_i32(self.timeout_ms);
        put_uvarint(scratch, 2); // one topic
        put_uvarint(scratch, u64::try_from(self.topic.len())? + 1);
        scratch.put_slice(self.topic.as_bytes());
        put_uvarint(scratch, u64::try_from(self.batches.len())? + 1);
        let mut length = 0usize;
        for (index, batch) in self.batches {
            scratch.put_i32(*index);
            put_uvarint(scratch, u64::try_from(batch.len())? + 1);
            length += scratch.len();
            segments.push(scratch.split().freeze());
            length += batch.len();
            segments.push(batch.clone());
            scratch.put_u8(0); // no partition tagged fields
        }
        scratch.put_u8(0); // no topic tagged fields
        scratch.put_u8(0); // no request tagged fields
        length += scratch.len();
        segments.push(scratch.split().freeze());
        let mut first = BytesMut::from(&segments[0][..]);
        first[..4].copy_from_slice(&i32::try_from(length - 4)?.to_be_bytes());
        segments[0] = first.freeze();
        Ok(segments)
    }
}

/// Appends `value` as an unsigned varint.
fn put_uvarint(buf: &mut BytesMut, mut value: u64) {
    while value >= 0x80 {
        buf.put_u8(u8::try_from(value & 0x7f).unwrap_or(0) | 0x80);
        value >>= 7;
    }
    buf.put_u8(u8::try_from(value).unwrap_or(0));
}

/// Writes every segment, in order, with as few vectored writes as the socket
/// takes them in.
async fn write_all_vectored(half: &mut OwnedWriteHalf, segments: &[Bytes]) -> std::io::Result<()> {
    let mut slices: Vec<IoSlice<'_>> = segments.iter().map(|s| IoSlice::new(s)).collect();
    let mut slices = slices.as_mut_slice();
    while !slices.is_empty() {
        let written = half.write_vectored(slices).await?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut slices, written);
    }
    Ok(())
}

/// Reads responses in order and settles the oldest pending request with
/// each, until the connection closes.
async fn read_responses(mut read: OwnedReadHalf, wire: Arc<ProduceWire>, settle: Arc<dyn Settle>) {
    let mut frame = BytesMut::new();
    loop {
        let Ok(length) = read.read_i32().await else {
            return;
        };
        let Ok(length) = usize::try_from(length) else {
            return;
        };
        frame.clear();
        frame.resize(length, 0);
        if read.read_exact(&mut frame).await.is_err() {
            return;
        }
        let Some(Pending { sent, tag, done }) =
            wire.pending.lock().expect("pending queue").pop_front()
        else {
            return;
        };
        let round_trip = sent.elapsed();
        settle.settle(tag, round_trip, settled(frame.split().freeze()));
        if let Some(done) = done {
            let _ = done.send(());
        }
    }
}

/// How the request a response frame answers went. The frame starts at the
/// correlation id.
fn settled(mut frame: Bytes) -> Settled {
    let decoded = (|| {
        let _correlation_id = get_i32(&mut frame)?;
        read_tagged_fields(&mut frame, |_tag, _payload| Ok(false))?;
        ProduceResponse::decode(&mut frame, PRODUCE_VERSION)
    })();
    match decoded {
        Ok(response) => response
            .responses
            .iter()
            .flat_map(|topic| &topic.partition_responses)
            .find(|partition| partition.error_code != 0)
            .map_or(Settled::Acked, |partition| Settled::Failed {
                partition: partition.index,
                error_code: partition.error_code,
            }),
        Err(error) => Settled::Undecodable(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_protocol::{
            Encode as _,
            owned::produce_response::{PartitionProduceResponse, TopicProduceResponse},
        },
    };

    fn response_frame(error_code: i16) -> Bytes {
        let mut frame = BytesMut::new();
        frame.put_i32(7);
        frame.put_u8(0);
        ProduceResponse {
            responses: vec![TopicProduceResponse {
                name: "t".to_owned(),
                partition_responses: vec![PartitionProduceResponse {
                    index: 3,
                    error_code,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut frame, PRODUCE_VERSION)
        .expect("encode");
        frame.freeze()
    }

    #[test]
    fn a_response_settles_as_its_partitions_answered() {
        check!(settled(response_frame(0)) == Settled::Acked);
        check!(
            settled(response_frame(6))
                == Settled::Failed {
                    partition: 3,
                    error_code: 6
                }
        );
        check!(matches!(
            settled(Bytes::from_static(&[0, 0])),
            Settled::Undecodable(_)
        ));
    }

    #[test]
    fn a_produce_frame_matches_the_typed_encoding() {
        use krabka_protocol::{
            owned::produce_request::{PartitionProduceData, TopicProduceData},
            records::RecordsPayload,
        };
        let batches = [
            (0, Bytes::from_static(b"first batch bytes")),
            (5, Bytes::from(vec![7_u8; 300])),
        ];
        let frame = ProduceFrame {
            topic: "bench-topic",
            acks: 1,
            timeout_ms: 30_000,
            batches: &batches,
        };
        let mut scratch = BytesMut::new();
        let wire = frame.segments(&mut scratch, 42).expect("segments").concat();

        let mut expected = BytesMut::new();
        expected.put_i32(0);
        expected.put_i16(0);
        expected.put_i16(PRODUCE_VERSION);
        expected.put_i32(42);
        expected.put_i16(i16::try_from(CLIENT_ID.len()).expect("short"));
        expected.put_slice(CLIENT_ID);
        expected.put_u8(0);
        ProduceRequest {
            acks: 1,
            timeout_ms: 30_000,
            topic_data: vec![TopicProduceData {
                name: "bench-topic".to_owned(),
                partition_data: batches
                    .iter()
                    .map(|(index, batch)| PartitionProduceData {
                        index: *index,
                        records: Some(RecordsPayload::Raw(batch.clone())),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode(&mut expected, PRODUCE_VERSION)
        .expect("encode");
        let length = i32::try_from(expected.len() - 4).expect("short");
        expected[..4].copy_from_slice(&length.to_be_bytes());

        check!(wire == expected.to_vec());
    }
}
