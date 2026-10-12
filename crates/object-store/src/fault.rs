//! A fault-injecting [`ObjectStore`] wrapper, for drills against a store that
//! misbehaves.
//!
//! Every production bound in this crate -- the retry budget, the retry
//! deadline, the request and connect timeouts -- exists because a real object
//! store throttles, stalls, and fails. None of that is reachable from a test
//! that only ever meets a store which answers. [`FaultInjectingStore`] wraps
//! any other store and, per operation, delays the call and then fails it with
//! the error the backend would have produced.
//!
//! The failure schedule is deterministic, not random: a rate of 500 per
//! thousand fails every second call to that operation, always the same ones,
//! so a suite that asserts on which call failed asserts on a fact rather than
//! on a seed.
//!
//! This is a testing seam. Nothing in the broker's serving path constructs it.
//! Tests can wrap an existing store directly or use [`build_faulty_object_store`]
//! to hand a fault policy to code that takes an `Arc<dyn ObjectStore>`.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use futures_util::stream::{BoxStream, StreamExt as _};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

use crate::{build::build_object_store, config::ObjectStoreConfig, error::ObjectStoreError};

/// The operation classes a [`FaultPolicy`] can single out.
///
/// The granularity is the one an operator's incident is described in -- "puts
/// are being throttled", "listing is slow" -- rather than one variant per
/// trait method, so a policy written for `Put` covers the multipart path too.
///
/// `ALL` lists every variant, so a policy can be applied across the board and
/// a table test can walk them. `label` is the `store` field of the errors an
/// op's faults produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, krabka_macros::EnumStr)]
#[enum_str(as_str = label, all)]
pub enum StoreOp {
    /// `put_opts` and `put_multipart_opts`.
    #[enum_str(name = "FaultInjectingStore::put")]
    Put,
    /// `get_opts` and everything layered on it, `head` included.
    #[enum_str(name = "FaultInjectingStore::get")]
    Get,
    /// `delete_stream`.
    #[enum_str(name = "FaultInjectingStore::delete")]
    Delete,
    /// `list` and `list_with_delimiter`.
    #[enum_str(name = "FaultInjectingStore::list")]
    List,
    /// `copy_opts`.
    #[enum_str(name = "FaultInjectingStore::copy")]
    Copy,
}

/// What a failing call fails with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// `503 SlowDown`, the answer S3 gives a client it is rate-limiting. This
    /// is the one that matters: it is retryable, so it is the shape that turns
    /// into a long hang rather than a fast failure.
    Throttled,
    /// `500 Internal Error`.
    ServerError,
    /// The object is not there. Unlike the other two this is not a transport
    /// fault, and a caller is entitled to treat it as an answer.
    NotFound,
}

impl FaultKind {
    /// Render this fault as the `object_store` error a backend would return.
    fn error(self, store: &'static str, location: &Path) -> object_store::Error {
        match self {
            Self::Throttled => object_store::Error::Generic {
                store,
                source: "503 SlowDown: Please reduce your request rate.".into(),
            },
            Self::ServerError => object_store::Error::Generic {
                store,
                source: "500 InternalError: We encountered an internal error.".into(),
            },
            Self::NotFound => object_store::Error::NotFound {
                path: location.to_string(),
                source: "injected".into(),
            },
        }
    }
}

/// What one operation class does under fault injection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpFault {
    /// The error a failing call produces.
    pub kind: FaultKind,
    /// How many calls out of every thousand fail. `0` never fails, `1000`
    /// always does, and the calls that fail are the same ones on every run:
    /// call `n` fails when `(n + 1) * rate / 1000` exceeds `n * rate / 1000`.
    pub fail_per_thousand: u32,
    /// How long every call to this operation sleeps before it is answered or
    /// failed. This is the stall: the call still completes, eventually, which
    /// is exactly what makes an unbounded caller hang on it.
    pub latency: Duration,
}

impl OpFault {
    /// Fail every call with `kind`, immediately.
    #[must_use]
    pub const fn always(kind: FaultKind) -> Self {
        Self {
            kind,
            fail_per_thousand: 1_000,
            latency: Duration::ZERO,
        }
    }

    /// Fail `per_thousand` calls out of every thousand with `kind`.
    #[must_use]
    pub const fn rate(kind: FaultKind, per_thousand: u32) -> Self {
        Self {
            kind,
            fail_per_thousand: per_thousand,
            latency: Duration::ZERO,
        }
    }

    /// Answer every call correctly, `latency` late. A store that is merely
    /// slow, which is the fault no error counter ever sees.
    #[must_use]
    pub const fn stall(latency: Duration) -> Self {
        Self {
            kind: FaultKind::ServerError,
            fail_per_thousand: 0,
            latency,
        }
    }

    /// The same fault, `latency` late.
    #[must_use]
    pub const fn with_latency(mut self, latency: Duration) -> Self {
        self.latency = latency;
        self
    }

    /// Whether the zero-based call number `n` to this operation fails.
    const fn fails_call(self, n: u64) -> bool {
        let rate = self.fail_per_thousand as u64;
        ((n + 1) * rate) / 1_000 > (n * rate) / 1_000
    }
}

/// Which operations misbehave, and how.
///
/// An operation with no entry is passed straight through to the inner store.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FaultPolicy {
    faults: BTreeMap<StoreOp, OpFault>,
}

impl FaultPolicy {
    /// A policy that injects nothing.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    /// Apply `fault` to `op`, replacing any previous entry for it.
    #[must_use]
    pub fn with(mut self, op: StoreOp, fault: OpFault) -> Self {
        self.faults.insert(op, fault);
        self
    }

    /// Apply `fault` to every operation class.
    #[must_use]
    pub fn with_all(mut self, fault: OpFault) -> Self {
        for op in StoreOp::ALL {
            self.faults.insert(op, fault);
        }
        self
    }

    /// The fault configured for `op`, if any.
    #[must_use]
    pub fn fault(&self, op: StoreOp) -> Option<OpFault> {
        self.faults.get(&op).copied()
    }
}

/// Per-operation call counters, so a test can assert how many times the store
/// was reached rather than inferring it.
#[derive(Debug, Default)]
struct Counters {
    put: AtomicU64,
    get: AtomicU64,
    delete: AtomicU64,
    list: AtomicU64,
    copy: AtomicU64,
}

impl Counters {
    fn slot(&self, op: StoreOp) -> &AtomicU64 {
        match op {
            StoreOp::Put => &self.put,
            StoreOp::Get => &self.get,
            StoreOp::Delete => &self.delete,
            StoreOp::List => &self.list,
            StoreOp::Copy => &self.copy,
        }
    }
}

/// An [`ObjectStore`] that delays and fails calls according to a
/// [`FaultPolicy`], delegating everything it does not fail to an inner store.
#[derive(Debug)]
pub struct FaultInjectingStore {
    inner: Arc<dyn ObjectStore>,
    policy: FaultPolicy,
    counters: Counters,
    get_replacement: Option<Bytes>,
}

fn failed_stream<T: Send + 'static>(
    kind: FaultKind,
    operation: StoreOp,
) -> BoxStream<'static, object_store::Result<T>> {
    let error = kind.error(operation.label(), &listing_path());
    futures_util::stream::once(async move { Err(error) }).boxed()
}

impl FaultInjectingStore {
    /// A fault-free in-memory store for consistency tests that inject one
    /// targeted GET replacement.
    #[must_use]
    pub fn memory() -> Self {
        Self::new(
            Arc::new(object_store::memory::InMemory::new()),
            FaultPolicy::none(),
        )
    }

    /// Wrap `inner` with `policy`.
    #[must_use]
    pub fn new(inner: Arc<dyn ObjectStore>, policy: FaultPolicy) -> Self {
        Self {
            inner,
            policy,
            counters: Counters::default(),
            get_replacement: None,
        }
    }

    /// Overwrite the requested object immediately before each GET.
    ///
    /// HEAD calls still report the existing object. This injects a deterministic
    /// overwrite between a metadata check and its read, for consistency drills.
    /// The write bypasses put faults and uses the inner store's normal overwrite.
    #[must_use]
    pub fn with_get_overwrite(mut self, replacement: Bytes) -> Self {
        self.get_replacement = Some(replacement);
        self
    }

    /// How many calls of class `op` have reached this store, failed ones
    /// included.
    #[must_use]
    pub fn attempts(&self, op: StoreOp) -> u64 {
        self.counters.slot(op).load(Ordering::SeqCst)
    }

    /// Count the call, sleep out any injected latency, and report whether the
    /// caller should be failed.
    async fn enter(&self, op: StoreOp, location: &Path) -> Result<(), object_store::Error> {
        let n = self.counters.slot(op).fetch_add(1, Ordering::SeqCst);
        let Some(fault) = self.policy.fault(op) else {
            return Ok(());
        };
        if !fault.latency.is_zero() {
            tokio::time::sleep(fault.latency).await;
        }
        if fault.fails_call(n) {
            return Err(fault.kind.error(op.label(), location));
        }
        Ok(())
    }
}

impl std::fmt::Display for FaultInjectingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultInjectingStore({})", self.inner)
    }
}

/// The path a listing fault reports. Listing has no single location, and the
/// error kinds that matter carry the store name rather than the path.
fn listing_path() -> Path {
    Path::from("")
}

#[async_trait::async_trait]
impl ObjectStore for FaultInjectingStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.enter(StoreOp::Put, location).await?;
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.enter(StoreOp::Put, location).await?;
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.enter(StoreOp::Get, location).await?;
        if !options.head
            && let Some(replacement) = &self.get_replacement
        {
            self.inner
                .put_opts(
                    location,
                    PutPayload::from_bytes(replacement.clone()),
                    PutOptions::default(),
                )
                .await?;
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let n = self
            .counters
            .slot(StoreOp::Delete)
            .fetch_add(1, Ordering::SeqCst);
        match self.policy.fault(StoreOp::Delete) {
            // The delete stream is built synchronously, so an injected
            // latency here is applied per yielded path rather than once.
            Some(fault) if fault.fails_call(n) => failed_stream(fault.kind, StoreOp::Delete),
            Some(fault) if !fault.latency.is_zero() => {
                let stream = self.inner.delete_stream(locations);
                stream
                    .then(move |item| async move {
                        tokio::time::sleep(fault.latency).await;
                        item
                    })
                    .boxed()
            }
            _ => self.inner.delete_stream(locations),
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let n = self
            .counters
            .slot(StoreOp::List)
            .fetch_add(1, Ordering::SeqCst);
        match self.policy.fault(StoreOp::List) {
            Some(fault) if fault.fails_call(n) => failed_stream(fault.kind, StoreOp::List),
            Some(fault) if !fault.latency.is_zero() => {
                let stream = self.inner.list(prefix);
                let latency = fault.latency;
                futures_util::stream::once(async move { tokio::time::sleep(latency).await })
                    .flat_map(move |()| futures_util::stream::empty())
                    .chain(stream)
                    .boxed()
            }
            _ => self.inner.list(prefix),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.enter(StoreOp::List, &listing_path()).await?;
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.enter(StoreOp::Copy, from).await?;
        self.inner.copy_opts(from, to, options).await
    }
}

/// Build the store `cfg` selects and wrap it in `policy`.
///
/// **Testing seam.** This exists so a fault drill can hand a misbehaving store
/// to code whose only entry point takes an `Arc<dyn ObjectStore>`. The broker's
/// serving path never calls it; [`crate::build_object_store`] is the builder
/// that does.
///
/// # Errors
///
/// Whatever [`crate::build_object_store`] returns for `cfg`.
pub fn build_faulty_object_store(
    cfg: &ObjectStoreConfig,
    policy: FaultPolicy,
) -> Result<Arc<FaultInjectingStore>, ObjectStoreError> {
    Ok(Arc::new(FaultInjectingStore::new(
        build_object_store(cfg)?,
        policy,
    )))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use object_store::ObjectStoreExt as _;

    use super::*;
    use crate::{config::ObjectStoreConfig, test_support::round_trip};

    fn memory(policy: FaultPolicy) -> Arc<FaultInjectingStore> {
        build_faulty_object_store(&ObjectStoreConfig::InMemory, policy).unwrap()
    }

    #[tokio::test]
    async fn get_overwrite_changes_the_body_after_head_without_changing_head_itself() {
        let inner: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let key = Path::from("mutable");
        inner
            .put(&key, PutPayload::from_static(b"old"))
            .await
            .unwrap();
        let store = FaultInjectingStore::new(inner, FaultPolicy::none())
            .with_get_overwrite(Bytes::from_static(b"replacement"));
        check!(store.head(&key).await.unwrap().size == 3);
        check!(
            store
                .get(&key)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .as_ref()
                == b"replacement"
        );
        check!(store.head(&key).await.unwrap().size == 11);
        check!(store.attempts(StoreOp::Get) == 3);
    }

    /// A rate is a schedule, not a coin flip: the same rate must fail the same
    /// call numbers every run, or a suite that asserts on which call failed is
    /// asserting on luck.
    #[test]
    fn fail_rates_pick_the_same_calls_every_run() {
        let cases: [(u32, [bool; 4]); 4] = [
            (0, [false, false, false, false]),
            (250, [false, false, false, true]),
            (500, [false, true, false, true]),
            (1_000, [true, true, true, true]),
        ];
        for (rate, want) in cases {
            let fault = OpFault::rate(FaultKind::Throttled, rate);
            let got: [bool; 4] = std::array::from_fn(|n| fault.fails_call(n as u64));
            check!(got == want, "rate {rate}");
        }
    }

    /// A `503 SlowDown` on `Put` must reach the caller as an error, and the
    /// object must not land: a fault that quietly succeeded would let a copy
    /// path pass a drill it never survived.
    #[tokio::test]
    async fn a_throttled_put_fails_and_writes_nothing() {
        let store =
            memory(FaultPolicy::none().with(StoreOp::Put, OpFault::always(FaultKind::Throttled)));
        let path = Path::from("seg.log");

        let err = store
            .put(&path, PutPayload::from(b"bytes".to_vec()))
            .await
            .unwrap_err();

        check!(err.to_string().contains("503 SlowDown"));
        assert!(store.get(&path).await.is_err());
        check!(store.attempts(StoreOp::Put) == 1);
    }

    /// Only the named operation misbehaves. A policy aimed at `Put` must leave
    /// `Get` alone, or every drill would be indistinguishable from a store
    /// that is entirely down.
    #[tokio::test]
    async fn faults_are_scoped_to_the_named_operation() {
        let store =
            memory(FaultPolicy::none().with(StoreOp::Get, OpFault::always(FaultKind::NotFound)));
        let path = Path::from("seg.log");

        store
            .put(&path, PutPayload::from(b"bytes".to_vec()))
            .await
            .unwrap();
        let err = store.get(&path).await.unwrap_err();

        check!(matches!(err, object_store::Error::NotFound { .. }));
        check!(store.attempts(StoreOp::Put) == 1);
        check!(store.attempts(StoreOp::Get) == 1);
    }

    /// A stall answers correctly, late. That is the fault an error counter
    /// never sees, and the reason a caller needs a deadline of its own.
    #[tokio::test(start_paused = true)]
    async fn a_stall_succeeds_but_only_after_the_injected_latency() {
        let store =
            memory(FaultPolicy::none().with(StoreOp::Put, OpFault::stall(Duration::from_secs(30))));
        let path = Path::from("seg.log");

        let started = tokio::time::Instant::now();
        store
            .put(&path, PutPayload::from(b"bytes".to_vec()))
            .await
            .unwrap();

        check!(started.elapsed() >= Duration::from_secs(30));
        assert!(store.get(&path).await.is_ok());
    }

    /// The policy is a wrapper, so a store with no faults must behave exactly
    /// like the one it wraps.
    #[tokio::test]
    async fn an_empty_policy_passes_everything_through() {
        let store = memory(FaultPolicy::none());
        let path = Path::from("seg.log");

        let got = round_trip(&store, &path, b"bytes").await;

        check!(&got[..] == b"bytes");
        check!(store.attempts(StoreOp::Put) == 1);
    }

    /// `with_all` reaches every class, including the two whose faults are
    /// injected into a stream rather than an `async fn`.
    #[tokio::test]
    async fn with_all_fails_listing_and_deleting_too() {
        let store = memory(FaultPolicy::none().with_all(OpFault::always(FaultKind::ServerError)));

        let listed = store.list(None).collect::<Vec<_>>().await;
        let deleted = store.delete(&Path::from("seg.log")).await;

        check!(listed.len() == 1);
        check!(listed[0].is_err());
        check!(deleted.is_err());
        check!(store.attempts(StoreOp::List) == 1);
        check!(store.attempts(StoreOp::Delete) == 1);
    }
}
