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
        Decode, Encode, ProtocolRequest,
        owned::{produce_request::ProduceRequest, produce_response::ProduceResponse},
        primitives::fixed::get_i32,
        tagged_fields::read_tagged_fields,
    },
    std::{
        collections::VecDeque,
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
        request: &ProduceRequest,
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
        frame.clear();
        frame.put_i32(0); // the length, filled in below
        frame.put_i16(<ProduceRequest as ProtocolRequest>::API_KEY);
        frame.put_i16(PRODUCE_VERSION);
        frame.put_i32(correlation_id);
        frame.put_i16(i16::try_from(CLIENT_ID.len())?);
        frame.put_slice(CLIENT_ID);
        frame.put_u8(0); // no tagged fields
        request.encode(frame, PRODUCE_VERSION)?;
        let length = i32::try_from(frame.len() - 4)?;
        frame[..4].copy_from_slice(&length.to_be_bytes());
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
        half.write_all(frame)
            .await
            .context("write a produce request")
    }
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
        krabka_protocol::owned::produce_response::{
            PartitionProduceResponse, TopicProduceResponse,
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
}
