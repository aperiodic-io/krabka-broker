//! Per-broker KIP-714 client-metrics state: the instance registry,
//! subscription matching, stable subscription-id computation, and the request
//! checks of Kafka's `ClientMetricsManager`.
//!
//! All state is in memory. KIP-714 is per-broker, because a client pins its
//! telemetry to one broker, so this state needs no raft replication. A client
//! may still reach a broker that holds no instance for it, after a restart or
//! an eviction, or when it moves to another broker. Every telemetry request
//! therefore builds a missing instance from the current subscriptions, as
//! Kafka does, and the subscription id is a function of the subscription and
//! the instance id alone, so every broker computes the same id.

use std::{
    collections::{BTreeMap, HashMap, hash_map::Entry},
    sync::Mutex,
    time::{Duration, Instant},
};

use krabka_metadata::MetadataImage;
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use uuid::Uuid;

use crate::{api_catalog::UnstableApiVersions, codes};

mod subscription;
#[cfg(test)]
mod test_support;

pub(crate) use self::subscription::{compute_subscription, subscription_id};

/// Connection-derived attributes: the ones subscription matching reads, and
/// the id of the connection that carried the request, which
/// [`ClientMetricsManager::connection_closed`] keys on.
#[derive(Debug, Clone)]
pub(crate) struct ClientAttributes {
    pub connection_id: String,
    pub client_instance_id: Uuid,
    pub client_id: String,
    pub software_name: String,
    pub software_version: String,
    pub source_address: String,
    pub source_port: u16,
}

/// The metric prefixes a client should send, and the push interval it must
/// use, after the union of every matched subscription.
///
/// The broker enforces the interval on each request. It does not inspect the
/// payload, so the prefixes are advisory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ComputedSubscription {
    pub metrics: Vec<String>,
    pub push_interval_ms: i32,
}

/// Kafka's `ClientMetricsInstance`.
///
/// The request timestamps start unset, which is Kafka's timestamp 0: the first
/// request of a new or rebuilt instance always passes the throttle check.
/// `Option`'s ordering puts `None` below every `Some`, the same as 0 below
/// every real time.
#[derive(Debug)]
struct ClientInstance {
    attrs: ClientAttributes,
    subscription_version: u64,
    subscription: ComputedSubscription,
    subscription_id: i32,
    push_interval: Duration,
    last_get: Option<Instant>,
    last_push: Option<Instant>,
    /// The last request of any kind, which drives eviction the way Kafka's
    /// expiration timer, re-armed on every request, does.
    last_seen: Instant,
    terminating: bool,
    last_error: i16,
}

impl ClientInstance {
    fn new(
        image: &MetadataImage,
        attrs: ClientAttributes,
        default_interval_ms: i32,
        subscription_version: u64,
        now: Instant,
    ) -> Self {
        let subscription = compute_subscription(image, &attrs, default_interval_ms);
        let subscription_id = subscription_id(&subscription, attrs.client_instance_id);
        // `interval.ms` is validated in [100, 3_600_000], and the default is
        // positive, so the interval is always positive.
        let push_interval = Duration::from_millis(
            u64::try_from(subscription.push_interval_ms).expect("validated positive push interval"),
        );
        Self {
            attrs,
            subscription_version,
            subscription,
            subscription_id,
            push_interval,
            last_get: None,
            last_push: None,
            last_seen: now,
            terminating: false,
            last_error: codes::NONE,
        }
    }

    /// Kafka's `maybeUpdateGetRequestTimestamp`.
    fn maybe_update_get_timestamp(&mut self, now: Instant) -> bool {
        let accept = self
            .last_get
            .max(self.last_push)
            .is_none_or(|last| now.duration_since(last) >= self.push_interval);
        if accept {
            self.last_get = Some(now);
        }
        accept
    }

    /// Kafka's `maybeUpdatePushRequestTimestamp`. The first push after a
    /// `GetTelemetrySubscriptions` is accepted early, because the client
    /// jitters its push interval.
    fn maybe_update_push_timestamp(&mut self, now: Instant) -> bool {
        let accept = self.last_get > self.last_push
            || self
                .last_push
                .is_none_or(|last| now.duration_since(last) >= self.push_interval);
        if accept {
            self.last_push = Some(now);
        }
        accept
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriptionAssignment {
    pub client_instance_id: Uuid,
    pub subscription_id: i32,
    pub push_interval_ms: i32,
    pub metrics: Vec<String>,
}

/// The outcome of a `GetTelemetrySubscriptions`. A rejection carries no
/// throttle time: Kafka answers with `throttle_time_ms` 0 and only the request
/// quota can raise it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubscriptionDecision {
    Assign(SubscriptionAssignment),
    Reject { error_code: i16 },
}

/// The fields of a `PushTelemetry` request that the checks read.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PushCheck {
    pub subscription_id: i32,
    pub terminating: bool,
    pub compression_supported: bool,
    pub payload_len: usize,
    /// Kafka's `ClientMetricsManager` changed in trunk after 4.3.1 (KAFKA-20898
    /// and KAFKA-21076), and neither change has a flag of its own, so
    /// [`UnstableApiVersions::Enabled`] selects trunk's behavior. See
    /// [`ClientMetricsManager::authorize_push`].
    pub unstable: UnstableApiVersions,
}

/// The outcome of the `PushTelemetry` checks. A rejection carries no throttle
/// time, for the same reason as [`SubscriptionDecision::Reject`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushDecision {
    Accept,
    Reject { error_code: i16 },
}

struct State {
    instances: HashMap<Uuid, ClientInstance>,
    /// Kafka's `clientConnectionIdMap`: the instance that a connection created
    /// or rebuilt. Closing the connection drops that instance.
    connections: HashMap<String, Uuid>,
    /// The subscriptions last seen in the metadata image, and a version that
    /// moves on every change to them: Kafka's `subscriptionUpdateVersion`.
    subscriptions: HashMap<String, BTreeMap<String, String>>,
    subscription_version: u64,
    /// The push interval of an instance that no subscription lowers, which is
    /// what `interval.ms` reads as when no subscription sets it.
    default_interval_ms: i32,
}

impl State {
    fn observe_subscriptions(&mut self, image: &MetadataImage) {
        let unchanged = image.client_metrics_subscriptions().count() == self.subscriptions.len()
            && image
                .client_metrics_subscriptions()
                .all(|(name, configs)| self.subscriptions.get(name) == Some(configs));
        if !unchanged {
            self.subscriptions = image
                .client_metrics_subscriptions()
                .map(|(name, configs)| (name.clone(), configs.clone()))
                .collect();
            self.subscription_version += 1;
        }
    }

    /// Kafka's `ClientMetricsManager.clientInstance`: the instance for `attrs`,
    /// created when this broker holds none, and created again with its first
    /// attributes when the subscriptions changed since it was built.
    fn client_instance(
        &mut self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
        now: Instant,
    ) -> &mut ClientInstance {
        self.observe_subscriptions(image);
        let version = self.subscription_version;
        let default_interval_ms = self.default_interval_ms;
        let id = attrs.client_instance_id;
        let inst = match self.instances.entry(id) {
            Entry::Vacant(slot) => {
                self.connections.insert(attrs.connection_id.clone(), id);
                slot.insert(ClientInstance::new(
                    image,
                    attrs.clone(),
                    default_interval_ms,
                    version,
                    now,
                ))
            }
            Entry::Occupied(slot) => {
                let inst = slot.into_mut();
                if inst.subscription_version < version {
                    // The rebuilt instance keeps the attributes of the first
                    // request and belongs to the connection of this one.
                    let first_attrs = ClientAttributes {
                        connection_id: attrs.connection_id.clone(),
                        ..inst.attrs.clone()
                    };
                    self.connections.insert(attrs.connection_id.clone(), id);
                    *inst =
                        ClientInstance::new(image, first_attrs, default_interval_ms, version, now);
                }
                inst
            }
        };
        inst.last_seen = now;
        inst
    }
}

pub(crate) struct ClientMetricsManager {
    state: Mutex<State>,
    telemetry_max: ByteSize,
}

/// Compression codecs that the broker advertises, in Kafka's fixed order:
/// ZSTD(4), LZ4(3), GZIP(1), and SNAPPY(2). The broker deliberately does not
/// advertise NONE.
pub(crate) const ACCEPTED_COMPRESSION_TYPES: [i8; 4] = [4, 3, 1, 2];

/// Kafka's `Uuid.RESERVED`: the zero UUID and `Uuid.ONE_UUID`, which no
/// client instance may use.
fn is_reserved_instance_id(id: Uuid) -> bool {
    id.is_nil() || id == Uuid::from_u128(1)
}

impl ClientMetricsManager {
    pub(crate) fn new(telemetry_max: ByteSize, default_interval_ms: i32) -> Self {
        Self {
            state: Mutex::new(State {
                instances: HashMap::new(),
                connections: HashMap::new(),
                subscriptions: HashMap::new(),
                subscription_version: 0,
                default_interval_ms,
            }),
            telemetry_max,
        }
    }

    /// The KIP-714 `telemetry.max.bytes` ceiling: the largest compressed push,
    /// and the largest payload a push may decompress to.
    pub(crate) fn telemetry_max(&self) -> ByteSize {
        self.telemetry_max
    }

    /// The KIP-714 `PushTelemetry` size ceiling in the `int32` byte form the
    /// wire response carries.
    pub(crate) fn telemetry_max_bytes(&self) -> i32 {
        self.telemetry_max.bytes_i32()
    }

    /// Kafka's `processGetTelemetrySubscriptionRequest`. A zero
    /// `attrs.client_instance_id` asks for a new id, and the assignment
    /// carries the id the broker used either way.
    pub(crate) fn get_subscription(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
    ) -> SubscriptionDecision {
        self.get_subscription_at(image, attrs, Instant::now())
    }

    fn get_subscription_at(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
        now: Instant,
    ) -> SubscriptionDecision {
        let mut state = self.state.lock().expect("client-metrics mutex poisoned");
        let mut attrs = attrs.clone();
        if attrs.client_instance_id.is_nil() {
            attrs.client_instance_id = loop {
                let fresh = Uuid::new_v4();
                if !state.instances.contains_key(&fresh) {
                    break fresh;
                }
            };
        }
        let inst = state.client_instance(image, &attrs, now);

        // Kafka's `validateGetRequest`: the timestamp check runs first and
        // moves the timestamp only when it passes. A client that the last push
        // told to fetch again may do so early.
        if !inst.maybe_update_get_timestamp(now)
            && !matches!(
                inst.last_error,
                codes::UNKNOWN_SUBSCRIPTION_ID | codes::UNSUPPORTED_COMPRESSION_TYPE
            )
        {
            return SubscriptionDecision::Reject {
                error_code: codes::THROTTLING_QUOTA_EXCEEDED,
            };
        }

        inst.last_error = codes::NONE;
        SubscriptionDecision::Assign(SubscriptionAssignment {
            client_instance_id: attrs.client_instance_id,
            subscription_id: inst.subscription_id,
            push_interval_ms: inst.subscription.push_interval_ms,
            metrics: inst.subscription.metrics.clone(),
        })
    }

    /// Kafka's `processPushTelemetryRequest` up to the export: the reserved
    /// id check, then `validatePushRequest`, then the terminating flag. The
    /// flag follows 4.3.1 (every push sets it) unless `push.unstable` is
    /// enabled, which follows trunk (only an accepted push sets it).
    pub(crate) fn authorize_push(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
        push: PushCheck,
    ) -> PushDecision {
        self.authorize_push_at(image, attrs, push, Instant::now())
    }

    fn authorize_push_at(
        &self,
        image: &MetadataImage,
        attrs: &ClientAttributes,
        push: PushCheck,
        now: Instant,
    ) -> PushDecision {
        if is_reserved_instance_id(attrs.client_instance_id) {
            return PushDecision::Reject {
                error_code: codes::INVALID_REQUEST,
            };
        }
        let max_payload_len = self.telemetry_max.bytes_usize();
        let mut state = self.state.lock().expect("client-metrics mutex poisoned");
        let inst = state.client_instance(image, attrs, now);

        let error_code = if inst.terminating {
            codes::INVALID_REQUEST
        } else if !inst.maybe_update_push_timestamp(now) && !push.terminating {
            codes::THROTTLING_QUOTA_EXCEEDED
        } else if push.subscription_id != inst.subscription_id {
            codes::UNKNOWN_SUBSCRIPTION_ID
        } else if !push.compression_supported {
            codes::UNSUPPORTED_COMPRESSION_TYPE
        } else if push.payload_len > max_payload_len {
            codes::TELEMETRY_TOO_LARGE
        } else {
            codes::NONE
        };

        // Kafka 4.3.1 records the flag in a `finally` after the checks, so
        // every push sets it, a rejected one included: a rejected terminating
        // push locks the instance out until it expires, and a later push
        // resets the flag to its own. Trunk (KAFKA-20898) records it only for
        // a push that passes the checks, so that a rejected push cannot lock
        // the client out.
        if push.unstable == UnstableApiVersions::Disabled || error_code == codes::NONE {
            inst.terminating = push.terminating;
        }
        if error_code != codes::NONE {
            inst.last_error = error_code;
            return PushDecision::Reject { error_code };
        }
        // Kafka records the export's outcome as the last error. The only
        // errors the next `GetTelemetrySubscriptions` reads are the two the
        // checks above give, so an accepted push leaves none behind.
        inst.last_error = codes::NONE;
        PushDecision::Accept
    }

    /// Drops an instance that has been idle for longer than
    /// `max(interval * factor, floor)`: Kafka's expiration task, which is
    /// re-armed by every request with `max(60 s, 3 * interval)`. A terminating
    /// instance expires like any other, so it keeps refusing pushes until then.
    ///
    /// The caller sweeps on a tick, so an instance can outlive its expiry by
    /// up to one tick where Kafka's timer wheel fires at the deadline.
    pub(crate) fn evict_stale(&self, factor: u32, floor: Duration) {
        self.evict_stale_at(factor, floor, Instant::now());
    }

    fn evict_stale_at(&self, factor: u32, floor: Duration, now: Instant) {
        let mut state = self.state.lock().expect("client-metrics mutex poisoned");
        let State {
            instances,
            connections,
            ..
        } = &mut *state;
        instances.retain(|_, inst| {
            let ttl = (inst.push_interval * factor).max(floor);
            now.duration_since(inst.last_seen) < ttl
        });
        connections.retain(|_, id| instances.contains_key(id));
    }

    /// Kafka's `ClientConnectionDisconnectListener.onDisconnect`: closing the
    /// connection that created an instance drops the instance, so a client
    /// that reconnects starts from a new one and its first
    /// `GetTelemetrySubscriptions` passes the throttle check.
    pub(crate) fn connection_closed(&self, connection_id: &str) {
        let mut state = self.state.lock().expect("client-metrics mutex poisoned");
        if let Some(id) = state.connections.remove(connection_id) {
            state.instances.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::{
        test_support::{attrs, expect_assignment, img_with},
        *,
    };
    use crate::{client_metrics::config::INTERVAL_MS_DEFAULT, test_support::KafkaErrorCode};

    fn manager() -> ClientMetricsManager {
        ClientMetricsManager::new(krabka_units::kibibytes(1), INTERVAL_MS_DEFAULT)
    }

    #[derive(Clone, Copy, Default)]
    struct TelemetrySubscriptionId(i32);

    fn push(subscription_id: TelemetrySubscriptionId) -> PushCheck {
        PushCheck {
            subscription_id: subscription_id.0,
            terminating: false,
            compression_supported: true,
            payload_len: 10,
            unstable: UnstableApiVersions::Disabled,
        }
    }

    #[derive(Clone, Copy)]
    enum Step {
        Get,
        Push(PushCheck),
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Outcome {
        Get(SubscriptionDecision),
        Push(PushDecision),
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct TelemetryStepSetup {
        #[default(Instant::now())]
        now: Instant,
        #[default(Step::Get)]
        step: Step,
    }

    fn run(
        m: &ClientMetricsManager,
        img: &MetadataImage,
        attrs: &ClientAttributes,
        setup: TelemetryStepSetup,
    ) -> Outcome {
        match setup.step {
            Step::Get => Outcome::Get(m.get_subscription_at(img, attrs, setup.now)),
            Step::Push(check) => Outcome::Push(m.authorize_push_at(img, attrs, check, setup.now)),
        }
    }

    fn reject_get(error_code: KafkaErrorCode) -> Outcome {
        Outcome::Get(SubscriptionDecision::Reject {
            error_code: error_code.0,
        })
    }

    fn reject_push(error_code: KafkaErrorCode) -> Outcome {
        Outcome::Push(PushDecision::Reject {
            error_code: error_code.0,
        })
    }

    const ACCEPT: Outcome = Outcome::Push(PushDecision::Accept);

    /// Kafka's `validatePushRequest` order after a `GetTelemetrySubscriptions`
    /// at T: terminating, throttle, subscription id, compression, size. The
    /// push timestamp moves once the throttle check passes (#678).
    #[test]
    fn push_checks_follow_kafkas_order_and_timestamps() {
        let m = manager();
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = attrs();
        let t = Instant::now();
        let right = expect_assignment(m.get_subscription_at(&img, &attrs, t)).subscription_id;
        let wrong = right ^ 0x5555;
        let secs = |s| t + Duration::from_secs(s);
        let steps = [
            (
                "first push after the get",
                secs(1),
                push(TelemetrySubscriptionId(right)),
                ACCEPT,
            ),
            (
                "early push",
                secs(2),
                push(TelemetrySubscriptionId(right)),
                reject_push(KafkaErrorCode(codes::THROTTLING_QUOTA_EXCEEDED)),
            ),
            (
                "early push with a wrong id is throttled first",
                secs(3),
                push(TelemetrySubscriptionId(wrong)),
                reject_push(KafkaErrorCode(codes::THROTTLING_QUOTA_EXCEEDED)),
            ),
            (
                "timely push with a wrong id",
                secs(61),
                push(TelemetrySubscriptionId(wrong)),
                reject_push(KafkaErrorCode(codes::UNKNOWN_SUBSCRIPTION_ID)),
            ),
            (
                "the rejected push moved the push timestamp",
                secs(62),
                push(TelemetrySubscriptionId(right)),
                reject_push(KafkaErrorCode(codes::THROTTLING_QUOTA_EXCEEDED)),
            ),
            (
                "unsupported compression",
                secs(122),
                PushCheck {
                    compression_supported: false,
                    payload_len: 4096,
                    ..push(TelemetrySubscriptionId(right))
                },
                reject_push(KafkaErrorCode(codes::UNSUPPORTED_COMPRESSION_TYPE)),
            ),
            (
                "oversized payload",
                secs(183),
                PushCheck {
                    payload_len: 1025,
                    ..push(TelemetrySubscriptionId(right))
                },
                reject_push(KafkaErrorCode(codes::TELEMETRY_TOO_LARGE)),
            ),
            (
                "a payload of exactly telemetry.max.bytes",
                secs(244),
                PushCheck {
                    payload_len: 1024,
                    ..push(TelemetrySubscriptionId(right))
                },
                ACCEPT,
            ),
            (
                "an early terminating push skips the throttle",
                secs(245),
                PushCheck {
                    terminating: true,
                    ..push(TelemetrySubscriptionId(right))
                },
                ACCEPT,
            ),
            (
                "nothing is accepted after a terminating push",
                secs(400),
                push(TelemetrySubscriptionId(right)),
                reject_push(KafkaErrorCode(codes::INVALID_REQUEST)),
            ),
        ];
        for (name, at, check, expected) in steps {
            assert!(
                run(
                    &m,
                    &img,
                    &attrs,
                    TelemetryStepSetup {
                        now: at,
                        step: Step::Push(check)
                    }
                ) == expected,
                "step {name}"
            );
        }
    }

    /// Where Kafka records the terminating flag (#1246). 4.3.1 records it in
    /// a `finally`, after the checks read the old flag, on every push: a
    /// rejected terminating push locks the instance out, and the push that the
    /// lock refuses resets the flag to its own. Trunk (KAFKA-20898) records it
    /// only for a push that passes the checks. A refused push does not move the
    /// push timestamp, so the push after the lock waits from the last one that
    /// passed the throttle.
    #[test]
    fn the_terminating_flag_follows_the_target() {
        use UnstableApiVersions::{Disabled, Enabled};

        // (seconds after the get, terminating, compression supported, outcome)
        type Move = (u64, bool, bool, Outcome);
        let unsupported = || reject_push(KafkaErrorCode(codes::UNSUPPORTED_COMPRESSION_TYPE));
        let locked = || reject_push(KafkaErrorCode(codes::INVALID_REQUEST));
        let rows: [(&str, UnstableApiVersions, [Move; 3]); 4] = [
            (
                "4.3.1: a rejected terminating push locks the instance",
                Disabled,
                [
                    (0, true, false, unsupported()),
                    (60, false, true, locked()),
                    (120, false, true, ACCEPT),
                ],
            ),
            (
                "trunk: a rejected terminating push leaves the instance open",
                Enabled,
                [
                    (0, true, false, unsupported()),
                    (60, false, true, ACCEPT),
                    (120, false, true, ACCEPT),
                ],
            ),
            (
                "4.3.1: the push that the lock refuses resets the flag",
                Disabled,
                [
                    (0, true, true, ACCEPT),
                    (60, false, true, locked()),
                    (120, false, true, ACCEPT),
                ],
            ),
            (
                "trunk: an accepted terminating push locks until expiry",
                Enabled,
                [
                    (0, true, true, ACCEPT),
                    (60, false, true, locked()),
                    (120, false, true, locked()),
                ],
            ),
        ];
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = attrs();
        for (name, unstable, steps) in rows {
            let m = manager();
            let t = Instant::now();
            let id = expect_assignment(m.get_subscription_at(&img, &attrs, t)).subscription_id;
            for (secs, terminating, compression_supported, expected) in steps {
                let check = PushCheck {
                    terminating,
                    compression_supported,
                    unstable,
                    ..push(TelemetrySubscriptionId(id))
                };
                let at = t + Duration::from_secs(secs);
                assert!(
                    run(
                        &m,
                        &img,
                        &attrs,
                        TelemetryStepSetup {
                            now: at,
                            step: Step::Push(check)
                        }
                    ) == expected,
                    "row {name} at {secs} s"
                );
            }
        }
    }

    /// A push reaches a broker that holds no instance for it: Kafka builds the
    /// instance from the current subscriptions and checks the push. Only the
    /// reserved ids get `INVALID_REQUEST` (#673).
    #[test]
    fn push_without_an_instance_builds_one() {
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = attrs();
        let current = subscription_id(
            &compute_subscription(&img, &attrs, INTERVAL_MS_DEFAULT),
            attrs.client_instance_id,
        );
        let rows = [
            ("current subscription id", attrs.clone(), current, ACCEPT),
            (
                "other subscription id",
                attrs.clone(),
                current ^ 1,
                reject_push(KafkaErrorCode(codes::UNKNOWN_SUBSCRIPTION_ID)),
            ),
            (
                "zero instance id",
                ClientAttributes {
                    client_instance_id: Uuid::nil(),
                    ..attrs.clone()
                },
                current,
                reject_push(KafkaErrorCode(codes::INVALID_REQUEST)),
            ),
            (
                "Uuid.ONE_UUID",
                ClientAttributes {
                    client_instance_id: Uuid::from_u128(1),
                    ..attrs.clone()
                },
                current,
                reject_push(KafkaErrorCode(codes::INVALID_REQUEST)),
            ),
        ];
        for (name, attrs, id, expected) in rows {
            let m = manager();
            assert!(
                run(
                    &m,
                    &img,
                    &attrs,
                    TelemetryStepSetup {
                        now: Instant::now(),
                        step: Step::Push(push(TelemetrySubscriptionId(id)))
                    }
                ) == expected,
                "row {name}"
            );
        }
    }

    /// A subscription change rebuilds the instance on its next request: the
    /// old subscription id gets `UNKNOWN_SUBSCRIPTION_ID`, and a get inside
    /// the old interval gets the new subscription at once (#672, #673).
    #[test]
    fn a_subscription_change_rebuilds_the_instance() {
        let m = manager();
        let before = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let after = img_with("all", &[("metrics", "*"), ("interval.ms", "30000")]);
        let attrs = attrs();
        let t = Instant::now();
        let secs = |s| t + Duration::from_secs(s);
        let old = expect_assignment(m.get_subscription_at(&before, &attrs, t));
        let new_id = subscription_id(
            &compute_subscription(&after, &attrs, INTERVAL_MS_DEFAULT),
            attrs.client_instance_id,
        );
        let assignment = |push_interval_ms, subscription_id| {
            Outcome::Get(SubscriptionDecision::Assign(SubscriptionAssignment {
                client_instance_id: attrs.client_instance_id,
                subscription_id,
                push_interval_ms,
                metrics: vec!["*".into()],
            }))
        };
        let steps = [
            (
                "early get",
                &before,
                secs(1),
                Step::Get,
                reject_get(KafkaErrorCode(codes::THROTTLING_QUOTA_EXCEEDED)),
            ),
            (
                "push of the old id after the change",
                &after,
                secs(2),
                Step::Push(push(TelemetrySubscriptionId(old.subscription_id))),
                reject_push(KafkaErrorCode(codes::UNKNOWN_SUBSCRIPTION_ID)),
            ),
            (
                "get after the change",
                &after,
                secs(3),
                Step::Get,
                assignment(30_000, new_id),
            ),
            (
                "early get on the rebuilt instance",
                &after,
                secs(4),
                Step::Get,
                reject_get(KafkaErrorCode(codes::THROTTLING_QUOTA_EXCEEDED)),
            ),
            (
                "get after a second change",
                &before,
                secs(5),
                Step::Get,
                assignment(60_000, old.subscription_id),
            ),
        ];
        for (name, img, at, step, expected) in steps {
            assert!(
                run(&m, img, &attrs, TelemetryStepSetup { now: at, step }) == expected,
                "step {name}"
            );
        }
    }

    /// Kafka's `validateGetRequest`: a get inside the interval since the last
    /// get or push is throttled, unless the last push asked the client to
    /// fetch again. A throttled get leaves the timestamps alone.
    #[test]
    fn get_throttle_follows_kafka() {
        let m = manager();
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "100")]);
        let attrs = attrs();
        let t = Instant::now();
        let ms = |n| t + Duration::from_millis(n);
        let id = expect_assignment(m.get_subscription_at(&img, &attrs, t)).subscription_id;
        let assigned = Outcome::Get(SubscriptionDecision::Assign(SubscriptionAssignment {
            client_instance_id: attrs.client_instance_id,
            subscription_id: id,
            push_interval_ms: 100,
            metrics: vec!["*".into()],
        }));
        let throttled = || reject_get(KafkaErrorCode(codes::THROTTLING_QUOTA_EXCEEDED));
        let steps = [
            ("get at 99 ms", ms(99), Step::Get, throttled()),
            ("get at the interval", ms(100), Step::Get, assigned.clone()),
            (
                "push at 150 ms",
                ms(150),
                Step::Push(push(TelemetrySubscriptionId(id))),
                ACCEPT,
            ),
            ("get after a recent push", ms(200), Step::Get, throttled()),
            (
                "push with a wrong id",
                ms(260),
                Step::Push(push(TelemetrySubscriptionId(id ^ 1))),
                reject_push(KafkaErrorCode(codes::UNKNOWN_SUBSCRIPTION_ID)),
            ),
            ("early get after 117", ms(261), Step::Get, assigned.clone()),
            (
                "the recovered get did not move the get timestamp",
                ms(262),
                Step::Get,
                throttled(),
            ),
            (
                "push with an unsupported codec",
                ms(400),
                Step::Push(PushCheck {
                    compression_supported: false,
                    ..push(TelemetrySubscriptionId(id))
                }),
                reject_push(KafkaErrorCode(codes::UNSUPPORTED_COMPRESSION_TYPE)),
            ),
            ("early get after 76", ms(401), Step::Get, assigned.clone()),
        ];
        for (name, at, step, expected) in steps {
            assert!(
                run(&m, &img, &attrs, TelemetryStepSetup { now: at, step }) == expected,
                "step {name}"
            );
        }
    }

    /// A zero id asks for a new one, and any other id is kept (#665).
    #[test]
    fn get_answers_with_the_instance_id_it_used() {
        let m = manager();
        let img = MetadataImage::new(Uuid::nil());
        let fresh = expect_assignment(m.get_subscription(
            &img,
            &ClientAttributes {
                client_instance_id: Uuid::nil(),
                ..attrs()
            },
        ));
        assert!(!fresh.client_instance_id.is_nil());
        for id in [Uuid::from_u128(1), Uuid::from_u128(42)] {
            let assigned = expect_assignment(m.get_subscription(
                &img,
                &ClientAttributes {
                    client_instance_id: id,
                    ..attrs()
                },
            ));
            assert!(assigned.client_instance_id == id);
        }
    }

    fn attrs_on(connection_id: &str) -> ClientAttributes {
        ClientAttributes {
            connection_id: connection_id.into(),
            ..attrs()
        }
    }

    /// Kafka's `ClientConnectionDisconnectListener`: closing the connection
    /// that created an instance drops it, so the client that reconnects gets
    /// an assignment where the old instance would have throttled it. The
    /// connection map is by creating connection, so a request on another
    /// connection does not take the instance over (#1246).
    #[test]
    fn closing_the_connection_drops_the_instance() {
        let m = manager();
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let t = Instant::now();
        let get = |connection: &str, secs| {
            m.get_subscription_at(&img, &attrs_on(connection), t + Duration::from_secs(secs))
        };
        let assigned = get("a", 0);
        let SubscriptionDecision::Assign(_) = &assigned else {
            panic!("the first get must be assigned");
        };
        let throttled = SubscriptionDecision::Reject {
            error_code: codes::THROTTLING_QUOTA_EXCEEDED,
        };

        assert!(get("a", 1) == throttled, "early get");
        m.connection_closed("b");
        assert!(
            get("b", 2) == throttled,
            "a connection that created nothing owns nothing"
        );
        m.connection_closed("b");
        assert!(get("a", 3) == throttled, "closing b did not drop it");
        m.connection_closed("a");
        assert!(get("b", 4) == assigned, "the reconnect is a new instance");
        assert!(get("b", 5) == throttled, "b owns the new instance");
        m.connection_closed("b");
        assert!(get("c", 6) == assigned, "closing b dropped it again");
    }

    /// Kafka's expiration task is armed with `max(60 s, 3 * interval)` and the
    /// sweep drops an instance idle for that long, whether or not it is
    /// terminating. A terminating instance stays locked until then (#1246).
    #[test]
    fn an_idle_instance_expires_after_the_larger_of_a_minute_and_three_intervals() {
        let floor = Duration::from_secs(60);
        // (name, interval.ms, idle seconds, expired)
        let rows = [
            ("terminating, before the floor", "10000", 30, false),
            ("just before the floor", "10000", 59, false),
            ("at the floor", "10000", 60, true),
            ("just before three intervals", "60000", 179, false),
            ("at three intervals", "60000", 180, true),
        ];
        for (name, interval, idle, expired) in rows {
            let m = manager();
            let img = img_with("all", &[("metrics", "*"), ("interval.ms", interval)]);
            let attrs = attrs();
            let id = subscription_id(
                &compute_subscription(&img, &attrs, INTERVAL_MS_DEFAULT),
                attrs.client_instance_id,
            );
            let t = Instant::now();
            let terminate = PushCheck {
                terminating: true,
                ..push(TelemetrySubscriptionId(id))
            };
            assert!(
                run(
                    &m,
                    &img,
                    &attrs,
                    TelemetryStepSetup {
                        now: t,
                        step: Step::Push(terminate)
                    }
                ) == ACCEPT
            );

            m.evict_stale_at(3, floor, t + Duration::from_secs(idle));

            let next = run(
                &m,
                &img,
                &attrs,
                TelemetryStepSetup {
                    now: t + Duration::from_secs(idle),
                    step: Step::Push(push(TelemetrySubscriptionId(id))),
                },
            );
            let expected = if expired {
                ACCEPT
            } else {
                reject_push(KafkaErrorCode(codes::INVALID_REQUEST))
            };
            assert!(next == expected, "row {name}");
        }
    }

    /// An expired instance takes its connection entry with it, so closing the
    /// old connection later does not drop the instance built since.
    #[test]
    fn an_expired_instances_connection_does_not_own_its_successor() {
        let m = manager();
        let img = img_with("all", &[("metrics", "*"), ("interval.ms", "60000")]);
        let attrs = attrs();
        let id = subscription_id(
            &compute_subscription(&img, &attrs, INTERVAL_MS_DEFAULT),
            attrs.client_instance_id,
        );
        let t = Instant::now();
        let secs = |s| t + Duration::from_secs(s);
        let terminate = PushCheck {
            terminating: true,
            ..push(TelemetrySubscriptionId(id))
        };
        let push_on = |connection: &str, at, check| {
            run(
                &m,
                &img,
                &attrs_on(connection),
                TelemetryStepSetup {
                    now: secs(at),
                    step: Step::Push(check),
                },
            )
        };

        assert!(push_on("a", 0, terminate) == ACCEPT, "built on a");
        m.evict_stale_at(3, Duration::from_secs(60), secs(200));
        assert!(push_on("b", 200, terminate) == ACCEPT, "rebuilt on b");
        m.connection_closed("a");
        assert!(
            push_on("b", 201, push(TelemetrySubscriptionId(id)))
                == reject_push(crate::test_support::KafkaErrorCode(codes::INVALID_REQUEST)),
            "the rebuilt instance is still locked after a closed"
        );
    }
}
