//! The observer's KIP-630 snapshot transfer.
//!
//! When a metadata fetch (1004) comes back carrying a `snapshot_id`, the
//! records the observer asked for have been pruned off the controller's log and
//! there is nothing to replicate from. It must install that snapshot first,
//! which it does over the genuine KIP-595 `FetchSnapshot` (api key 59) the
//! controller listener already serves for its own followers: request one byte
//! range at a time until the artifact is reassembled, decode it, publish the
//! image it holds, and resume fetching at the snapshot's end offset.
//!
//! Reassembly is the shared [`SnapshotFetchState`] the controller's follower
//! path uses, so an out-of-order chunk, a changed snapshot id, or a peer that
//! streams more bytes than the configured maximum aborts here exactly as it
//! does there.
//!
//! The request asks for as much of the artifact as one response can hold:
//! whichever is smaller of `snapshot_fetch_max` and what fits inside this
//! connection's frame budget. Both bounds matter. `FetchSnapshot` carries the
//! bytes in the KIP-595 `unalignedRecords` field, and that field decodes
//! leniently: a range that stops part-way through a record batch has the
//! partial batch dropped, so a range cut at an arbitrary byte arrives empty,
//! while a whole artifact — a complete run of batches — survives. And a range
//! larger than the frame budget cannot be delivered at all: the responder
//! closes the connection rather than sending an oversized frame. Asking for
//! more than either bound turns a snapshot that is merely too big into a
//! connection that keeps dying.
//!
//! The loop below still requests successive ranges — the responder may serve
//! fewer bytes than asked for — and gives up on a range that carries nothing
//! rather than re-asking for the same position forever. A snapshot larger than
//! the frame budget therefore fails cleanly, with a warning, instead of
//! stalling.

use std::sync::Arc;

use krabka_client_core::Connection;
use krabka_metadata::MetadataImage;
use krabka_raft::{
    NodeId,
    kraft::{
        snapshot_fetch::{SnapshotFetchState, SnapshotFetchStep},
        transport::{
            api_key,
            wire::{FETCH_SNAPSHOT_VERSION, PeerRequest, PeerResponse},
        },
    },
};
use krabka_units::{ByteSize, convert::ByteSizeExt as _, kibibytes};
use tokio::sync::watch;
use tracing::{debug, info, warn};

use super::{ObserverConfig, store::ObserverStore};

/// Room left inside the connection's frame budget for everything wrapping the
/// snapshot bytes: the response header and the `FetchSnapshot` envelope around
/// the `unalignedRecords` field. Generous — the envelope is a topic name, a few
/// ints and the snapshot id — because overshooting the frame maximum costs the
/// whole connection while undershooting costs one extra range.
const SNAPSHOT_RESPONSE_OVERHEAD: ByteSize = kibibytes(64);

/// The largest byte range this observer may ask for: as much as it will
/// reassemble, capped by what one response frame on this connection can carry.
fn range_max_bytes(config: &ObserverConfig) -> i32 {
    let frame_budget = config
        .client_frame_max
        .size()
        .bytes_u64()
        .saturating_sub(SNAPSHOT_RESPONSE_OVERHEAD.bytes_u64());
    let capped = config
        .snapshot_fetch_max
        .byte_size()
        .bytes_u64()
        .min(frame_budget)
        // A frame budget smaller than the overhead would ask for nothing at
        // all, and a zero-byte range never makes progress.
        .max(1);
    i32::try_from(capped).unwrap_or(i32::MAX)
}

/// Fetch, install, and persist `snapshot_id` from the controller on `conn`.
///
/// Returns the offset to fetch from next — the snapshot's end offset — or
/// `None` when the transfer failed, which leaves the caller's fetch offset
/// unchanged so the next poll asks again (and rotates to another voter).
///
/// `target` carries the controller and the quorum epoch its metadata fetch
/// reported. `FetchSnapshot` is a leader-only KIP-595 request, so a controller
/// that is not the leader of that epoch refuses the transfer, and the observer
/// rotates until it reaches the leader.
pub(super) async fn install_snapshot(
    config: &ObserverConfig,
    conn: &Connection,
    target: (NodeId, i32),
    snapshot_id: (i64, i32),
    image_tx: &watch::Sender<Arc<MetadataImage>>,
    store: &mut ObserverStore,
) -> Option<u64> {
    let (end_offset, epoch) = snapshot_id;
    let next_fetch_offset = u64::try_from(end_offset).ok()?;
    let bytes = transfer(config, conn, target, snapshot_id).await?;
    let records = match krabka_raft::deserialize_metadata_snapshot_image(&bytes) {
        Ok(records) => records,
        Err(error) => {
            warn!(%error, end_offset, epoch, "observer could not decode the fetched snapshot");
            return None;
        }
    };
    info!(
        end_offset,
        epoch,
        records = records.len(),
        bytes = bytes.len(),
        "observer installed a metadata snapshot"
    );
    let _ = image_tx.send_replace(Arc::new(MetadataImage::from_records(
        config.cluster_id,
        &records,
    )));
    store.save_fetched_snapshot(snapshot_id, &bytes);
    Some(next_fetch_offset)
}

/// Request `snapshot_id` until it is whole.
async fn transfer(
    config: &ObserverConfig,
    conn: &Connection,
    (target, leader_epoch): (NodeId, i32),
    snapshot_id: (i64, i32),
) -> Option<bytes::Bytes> {
    let mut state = SnapshotFetchState::with_max(snapshot_id, target, config.snapshot_fetch_max);
    loop {
        let position = state.next_position();
        let request = PeerRequest::FetchSnapshot {
            cluster_id: Some(config.cluster_id),
            from: config.node_id,
            current_leader_epoch: leader_epoch,
            snapshot_id,
            position,
            // KIP-595 `FetchSnapshot.MaxBytes` is an `int32`; the range bound
            // converts here, at the wire boundary.
            max_bytes: range_max_bytes(config),
        }
        .encode();
        let body = match conn
            .raw_request(api_key::FETCH_SNAPSHOT, FETCH_SNAPSHOT_VERSION, request)
            .await
        {
            Ok(body) => body,
            Err(error) => {
                debug!(%error, "observer snapshot fetch request failed");
                return None;
            }
        };
        let Some(PeerResponse::FetchSnapshot {
            snapshot_id: served,
            size,
            position: served_position,
            bytes,
            error_code,
        }) = PeerResponse::decode_fetch_snapshot(&body)
        else {
            warn!("observer snapshot fetch response decode failed");
            return None;
        };
        if error_code != 0 {
            // The controller has pruned past this snapshot, or never held it.
            // The next metadata fetch names whichever snapshot it does hold.
            debug!(error_code, "observer snapshot fetch was refused");
            return None;
        }
        match state.on_chunk(served, size, served_position, &bytes) {
            SnapshotFetchStep::Complete(assembled) => return Some(assembled),
            SnapshotFetchStep::Restart => {
                warn!("observer snapshot transfer aborted mid-stream");
                return None;
            }
            // A range that carried nothing leaves the transfer exactly where it
            // was. Asking again would repeat the same request against the same
            // responder forever, so give up and let the caller back off and
            // rotate to another voter.
            SnapshotFetchStep::Continue { next_position } if next_position == position => {
                warn!(
                    position,
                    size, "observer snapshot range carried no bytes; abandoning the transfer"
                );
                return None;
            }
            SnapshotFetchStep::Continue { .. } => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataRecord;
    use krabka_protocol::owned::api_versions_request;
    use krabka_raft::kraft::transport::wire::decode_fetch_snapshot;
    use uuid::Uuid;

    use super::*;
    use crate::{
        metadata_observer::{
            fetch::{FetchOutcome, fetch_once},
            test_support::{api_versions_response_v0, observer_config},
        },
        test_support::single_partition_topic,
    };

    struct SnapshotFixture {
        directory: tempfile::TempDir,
        config: ObserverConfig,
        image: watch::Sender<Arc<MetadataImage>>,
        _receiver: watch::Receiver<Arc<MetadataImage>>,
        store: ObserverStore,
    }

    impl SnapshotFixture {
        fn new(cluster_id: Uuid, address: std::net::SocketAddr) -> Self {
            let directory = tempfile::tempdir().unwrap();
            let config = ObserverConfig {
                voters: vec![(NodeId(1), address.to_string())],
                ..observer_config(cluster_id, directory.path().to_path_buf())
            };
            let (image, receiver) = watch::channel(Arc::new(MetadataImage::new(cluster_id)));
            let store = ObserverStore::open(directory.path(), 0);
            Self {
                directory,
                config,
                image,
                _receiver: receiver,
                store,
            }
        }

        async fn fetch(&mut self) -> Option<FetchOutcome> {
            fetch_once(
                &self.config,
                &self.config.voters[0].1,
                NodeId(1),
                0,
                &self.image,
                &mut self.store,
            )
            .await
        }
    }

    #[bon::builder]
    async fn assert_snapshot_fetch_was_not_applied(
        cluster_id: Uuid,
        address: std::net::SocketAddr,
    ) {
        let mut fixture = SnapshotFixture::new(cluster_id, address);
        assert!(fixture.fetch().await.is_none());
        assert!(fixture.image.borrow().topics().next().is_none());
        assert!(
            ObserverStore::open(fixture.directory.path(), 0)
                .resume(cluster_id)
                .is_none()
        );
    }

    async fn snapshot_broker(
        snapshot_id: (i64, i32),
        mut response: impl FnMut(&[u8]) -> Option<PeerResponse> + Send + 'static,
    ) -> krabka_client_core::MockBroker {
        krabka_client_core::MockBroker::start(move |api_key, _version, _corr_id, body| {
            if api_key == api_versions_request::API_KEY {
                Some(api_versions_response_v0())
            } else if api_key == krabka_raft::API_KEY_METADATA_FETCH {
                Some(metadata_fetch_redirect(snapshot_id))
            } else if api_key == api_key::FETCH_SNAPSHOT {
                response(body).map(|response| framed(&response.encode()))
            } else {
                None
            }
        })
        .await
    }

    /// Prefix every mock response body with the flexible `ResponseHeader` v1
    /// tagged-fields byte the client strips before it sees the body.
    fn framed(body: &bytes::Bytes) -> Vec<u8> {
        let mut out = vec![0u8];
        out.extend_from_slice(body);
        out
    }

    /// The request body itself.
    ///
    /// `MockBroker` splits each frame after the fixed
    /// `api_key`/`api_version`/`correlation_id` prefix, so what it calls the
    /// body still carries the rest of the `RequestHeader`: the nullable
    /// `client_id` string and, on the flexible header a KIP-595 RPC uses, its
    /// tagged-fields byte.
    fn request_body(after_correlation_id: &[u8]) -> &[u8] {
        let (length, rest) = after_correlation_id.split_at(2);
        let length = i16::from_be_bytes([length[0], length[1]]);
        let rest = match usize::try_from(length) {
            Ok(length) => &rest[length..],
            Err(_) => rest, // a null client_id has no bytes to skip
        };
        &rest[1..]
    }

    fn metadata_fetch_redirect(snapshot_id: (i64, i32)) -> Vec<u8> {
        let mut out = vec![0u8];
        krabka_raft::KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 1,
            leader_epoch: 3,
            log_start_offset: snapshot_id.0,
            high_watermark: snapshot_id.0,
            quorum_high_watermark: snapshot_id.0,
            snapshot_id: Some(snapshot_id),
            records: bytes::Bytes::new(),
        }
        .encode_v0(&mut out)
        .expect("encode redirect");
        out
    }

    fn snapshot_of(cluster_id: Uuid, topic: &str) -> bytes::Bytes {
        let mut image = MetadataImage::new(cluster_id);
        image.apply(&MetadataRecord::V1Topic(single_partition_topic(
            topic,
            Uuid::new_v4(),
        )));
        krabka_raft::serialize_metadata_snapshot(&image, 0).expect("serialize snapshot")
    }

    /// The whole restart path in one round trip: the controller answers a fetch
    /// below its pruned log start with a snapshot id, the observer transfers
    /// that snapshot over `FetchSnapshot`, publishes the image it holds,
    /// resumes at its end offset, and leaves it on disk so the *next* restart
    /// resumes from there without transferring anything.
    #[tokio::test]
    async fn a_pruned_fetch_installs_the_snapshot_and_resumes_at_its_end_offset() {
        let cluster_id = Uuid::new_v4();
        let snapshot_id = (4_096_i64, 7_i32);
        let artifact = snapshot_of(cluster_id, "pruned-away");
        let served = artifact.clone();
        let chunks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::clone(&chunks);
        let mock = snapshot_broker(snapshot_id, move |body| {
            let Some(PeerRequest::FetchSnapshot {
                snapshot_id: wanted,
                position,
                max_bytes,
                ..
            }) = decode_fetch_snapshot(request_body(body))
            else {
                return None;
            };
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let start = usize::try_from(position).unwrap().min(served.len());
            let end = start
                .saturating_add(usize::try_from(max_bytes).unwrap())
                .min(served.len());
            Some(PeerResponse::FetchSnapshot {
                snapshot_id: wanted,
                size: i64::try_from(served.len()).unwrap(),
                position,
                bytes: served.slice(start..end),
                error_code: 0,
            })
        })
        .await;

        let mut fixture = SnapshotFixture::new(cluster_id, mock.addr);

        let outcome = fixture
            .fetch()
            .await
            .expect("the redirected fetch installs the snapshot");

        assert!(outcome.next_fetch_offset == 4_096);
        assert!(outcome.log_start_offset == 4_096);
        assert!(fixture.image.borrow().topic("pruned-away").is_some());
        // One range, because the request asks for the whole artifact: a
        // `FetchSnapshot` range cut part-way through a record batch loses the
        // partial batch in the records codec and arrives empty.
        assert!(chunks.load(std::sync::atomic::Ordering::SeqCst) == 1);

        let (restored, fetch_offset) = ObserverStore::open(fixture.directory.path(), 0)
            .resume(cluster_id)
            .expect("the installed snapshot was persisted");
        assert!(fetch_offset == 4_096);
        assert!(restored.topic("pruned-away").is_some());

        mock.stop();
    }

    /// The range a request asks for is bounded by both limits that can stop a
    /// response reaching this node: what it will reassemble, and what one frame
    /// on this connection can carry. Asking past the frame maximum does not
    /// yield a big response — it makes the responder close the connection, so
    /// every retry fails and the node never becomes ready.
    #[test]
    fn a_snapshot_range_never_exceeds_the_connections_frame_budget() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = observer_config(Uuid::nil(), dir.path().to_path_buf());

        // The default reassembly ceiling is 1 GiB and the frame maximum is
        // 100 MiB, so the frame is what bounds the request.
        let frame_max = i32::try_from(config.client_frame_max.bytes()).expect("frame max fits");
        let asked = range_max_bytes(&config);
        assert!(
            asked < frame_max,
            "asked {asked} against frame max {frame_max}"
        );
        assert!(
            asked > frame_max / 2,
            "the budget should be nearly the whole frame"
        );

        // A lower reassembly ceiling wins instead, and it is asked for whole.
        let smaller = ObserverConfig {
            snapshot_fetch_max: krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax::new(
                krabka_units::kibibytes(512),
            )
            .expect("valid maximum"),
            ..observer_config(Uuid::nil(), dir.path().to_path_buf())
        };
        assert!(range_max_bytes(&smaller) == 512 * 1024);
    }

    /// A responder that answers with a range carrying no bytes leaves the
    /// transfer exactly where it started. Re-asking would repeat the same
    /// request against the same voter forever, so the transfer gives up and the
    /// caller backs off instead — the loop must not spin on it.
    #[tokio::test]
    async fn a_range_that_carries_no_bytes_abandons_the_transfer() {
        let cluster_id = Uuid::new_v4();
        let snapshot_id = (4_096_i64, 7_i32);
        let mock = snapshot_broker(snapshot_id, move |_body| {
            // A non-zero total with an empty range: the shape a records
            // field takes when it drops a batch it could not complete.
            Some(PeerResponse::FetchSnapshot {
                snapshot_id,
                size: 512,
                position: 0,
                bytes: bytes::Bytes::new(),
                error_code: 0,
            })
        })
        .await;

        let mut fixture = SnapshotFixture::new(cluster_id, mock.addr);

        let outcome = tokio::time::timeout(std::time::Duration::from_secs(10), fixture.fetch())
            .await
            .expect("the transfer gives up rather than re-asking forever");

        assert!(outcome.is_none());
        mock.stop();
    }

    /// A transfer can complete and still be worthless: the bytes arrive whole
    /// but are not a checkpoint. The observer must not publish an image built
    /// from them, must not persist them, and must report the round trip as
    /// failed so its fetch offset stays where it was.
    #[tokio::test]
    async fn a_snapshot_that_will_not_decode_is_neither_published_nor_persisted() {
        let cluster_id = Uuid::new_v4();
        let snapshot_id = (4_096_i64, 7_i32);
        let garbage = bytes::Bytes::from_static(b"this is not a KIP-630 checkpoint");
        let served = garbage.clone();
        let mock = snapshot_broker(snapshot_id, move |_body| {
            Some(PeerResponse::FetchSnapshot {
                snapshot_id,
                size: i64::try_from(served.len()).unwrap(),
                position: 0,
                bytes: served.clone(),
                error_code: 0,
            })
        })
        .await;

        assert_snapshot_fetch_was_not_applied()
            .cluster_id(cluster_id)
            .address(mock.addr)
            .call()
            .await;

        mock.stop();
    }

    /// A controller that has pruned past the snapshot it named answers the
    /// transfer with an error. The observer must not treat that as progress:
    /// it reports the round trip as failed, so the loop backs off, rotates, and
    /// asks again rather than jumping its fetch offset over records it never
    /// applied.
    #[tokio::test]
    async fn a_refused_snapshot_transfer_leaves_the_fetch_offset_alone() {
        let cluster_id = Uuid::new_v4();
        let snapshot_id = (4_096_i64, 7_i32);
        let mock = snapshot_broker(snapshot_id, move |_body| {
            Some(PeerResponse::FetchSnapshot {
                snapshot_id,
                size: 0,
                position: 0,
                bytes: bytes::Bytes::new(),
                // Krabka-internal "snapshot not available".
                error_code: 98,
            })
        })
        .await;

        assert_snapshot_fetch_was_not_applied()
            .cluster_id(cluster_id)
            .address(mock.addr)
            .call()
            .await;

        mock.stop();
    }
}
