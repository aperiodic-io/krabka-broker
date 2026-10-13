//! `OffsetForLeaderEpoch` (`api_key=23`). For each requested (topic,
//! partition, `leader_epoch`), this handler returns the `end_offset` of that
//! epoch. That offset is the first offset of the *next* epoch, and it is the
//! truncation point a follower should use when it recovers from
//! `FENCED_LEADER_EPOCH`.
//!
//! Protocol:
//! - `requested_epoch == current_leader_epoch` → `end_offset = log_end_offset`
//! - `requested_epoch != current_leader_epoch` (above or below) →
//!   `end_offset` from the checkpoint, or `-1` (`UNDEFINED_OFFSET`) with
//!   `leader_epoch = -1` (`UNDEFINED_EPOCH`) when the checkpoint has no
//!   entry for it. No error either way: `LeaderEpochFileCache.endOffsetFor`
//!   (`storage/.../LeaderEpochFileCache.java:299-303`) answers a requested
//!   epoch above everything the same way it answers an untracked one below
//!   it, and only the hosting checks above ever set an error code here.
//!
//! Reference: KIP-101 (Alter Replication Protocol to use Leader Epoch
//! rather than High Watermark for Truncation).
//!
//! Kafka's `ReplicaManager.lastOffsetForLeaderEpoch` first decides whether
//! this broker hosts the partition at all, ahead of everything below: an
//! offline log dir is `KAFKA_STORAGE_ERROR`, a partition the metadata holds
//! but this broker does not is `NOT_LEADER_OR_FOLLOWER`, and one the metadata
//! does not hold is `UNKNOWN_TOPIC_OR_PARTITION`.
//!
//! KIP-320 layers two more checks on top, in the order Kafka's
//! `ReplicaManager.lastOffsetForLeaderEpoch` -> `Partition.getLocalLog`
//! applies them, both ahead of the KIP-101 domain logic above:
//!
//! 1. The `current_leader_epoch` field (decodes from v2 up, this API's
//!    `MIN_VERSION`) is fenced against the partition's live epoch via
//!    `Partition::list_offsets_leader_epoch_fence`. `ReplicaManager` builds
//!    this field's `Optional` the same way `RequestUtils.getLeaderEpoch`
//!    does for `ListOffsets` -- only the exact `NO_PARTITION_LEADER_EPOCH`
//!    sentinel (`-1`) counts as "no epoch asserted", so every other
//!    negative value is fenced too. That is *not* `Fetch`'s rule, which
//!    treats every negative epoch as unasserted; the two request schemas
//!    just happen to share a field name.
//! 2. Only this node's locally installed leader may answer, matching
//!    `fetchOnlyFromLeader = true` in `getLocalLog`. Checked against both
//!    the metadata image's `record.leader` and the partition's installed
//!    `current_leader` atomic, mirroring
//!    `crate::handlers::fetch::plan::leader_refusal`: the image can name a
//!    new leader before the supervisor installs the new role, and the
//!    installed role can lag the other way while a promotion prepares the
//!    log.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        broker::Broker,
        codes,
        handlers::{RequestContext, acl_denied, cluster_action_denied},
        partition::Partition,
    },
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        offset_for_leader_epoch_request::OffsetForLeaderEpochRequest,
        offset_for_leader_epoch_response::{
            EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
        },
    },
    std::sync::atomic::Ordering,
};

/// The two-state leader check Kafka's `ReplicaManager.lastOffsetForLeaderEpoch`
/// applies via `Partition.getLocalLog(currentLeaderEpoch, fetchOnlyFromLeader
/// = true)`. Mirrors `crate::handlers::fetch::plan::leader_refusal`: checked
/// against both the metadata image's `record.leader` and the partition's
/// installed `current_leader` atomic, because the image can name a new leader
/// before the supervisor installs the new role, and the installed role can
/// lag the other way while a promotion prepares the log.
fn not_leader(
    image: &krabka_metadata::MetadataImage,
    topic: &str,
    partition_index: i32,
    partition: &Partition,
    node_id: krabka_metadata::NodeId,
) -> bool {
    let installed = partition.current_leader.load(Ordering::Acquire) == node_id.0;
    let committed_elsewhere = image
        .partition(topic, partition_index)
        .is_some_and(|record| record.leader != node_id);
    !installed || committed_elsewhere
}

pub(crate) fn handle(
    broker: &Broker,
    req: &OffsetForLeaderEpochRequest,
    _version: i16,
    ctx: &RequestContext<'_>,
) -> OffsetForLeaderEpochResponse {
    let partitions = broker.partitions.clone();
    // Test-only: count served OFLE requests so the KIP-320 proactive-validation
    // integration test can prove the consumer's validate pass issued an OFLE
    // RPC (vs. the reactive in-band fetch paths, which issue none).
    #[cfg(any(test, feature = "test-helpers"))]
    let ofle_counter = broker.offset_for_leader_epoch_requests.clone();
    {
        #[cfg(any(test, feature = "test-helpers"))]
        ofle_counter.fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        // ── ACL preamble ────────────────────────────────────────────
        // Kafka checks `ClusterAction` on `Cluster("kafka-cluster")` once as
        // a shortcut: an Allow there authorizes every topic in the request,
        // with no further lookup. This is what lets a follower broker (whose
        // principal typically holds `ClusterAction` but no topic ACLs) fetch
        // leader-epoch info from other brokers. A Deny falls back to
        // per-topic `Describe` on `Topic(name)`; a denied topic gets
        // `TOPIC_AUTHORIZATION_FAILED (29)` on every partition row it
        // requested, with `leader_epoch` and `end_offset` both `-1` (Kafka's
        // schema default for an unauthorized row), and authorized topics
        // proceed unchanged.
        let acl_image = broker.controller.current_image();
        let cluster_action_allowed =
            !cluster_action_denied(broker.config.authorizer.as_ref(), &acl_image, ctx);

        let mut authorized_out: Vec<OffsetForLeaderTopicResult> =
            Vec::with_capacity(req.topics.len());
        let mut unauthorized_out: Vec<OffsetForLeaderTopicResult> = Vec::new();

        for topic in &req.topics {
            if !cluster_action_allowed
                && acl_denied(
                    broker.config.authorizer.as_ref(),
                    &acl_image,
                    ctx,
                    ResourceType::Topic,
                    &topic.topic,
                    AclOperation::Describe,
                )
            {
                let parts_out = topic
                    .partitions
                    .iter()
                    .map(|part| EpochEndOffset {
                        partition: part.partition,
                        leader_epoch: -1,
                        end_offset: -1,
                        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                        ..Default::default()
                    })
                    .collect();
                unauthorized_out.push(OffsetForLeaderTopicResult {
                    topic: topic.topic.clone(),
                    partitions: parts_out,
                    ..Default::default()
                });
                continue;
            }
            let mut parts_out: Vec<EpochEndOffset> = Vec::with_capacity(topic.partitions.len());

            for part in &topic.partitions {
                // `ReplicaManager.lastOffsetForLeaderEpoch` builds every error
                // row from `new EpochEndOffset()`, whose schema defaults put
                // -1 in both `leader_epoch` and `end_offset`.
                let mut out = EpochEndOffset {
                    partition: part.partition,
                    leader_epoch: -1,
                    end_offset: -1,
                    ..Default::default()
                };

                // The three hosting outcomes of `ReplicaManager
                // .lastOffsetForLeaderEpoch`, decided before the fence: a
                // replica this broker does not host is `NOT_LEADER_OR_FOLLOWER`
                // when the metadata still holds the partition (a reassignment
                // moved it away, so the client refreshes its metadata) and
                // `UNKNOWN_TOPIC_OR_PARTITION` when it does not, and a
                // partition in an offline log dir is `KAFKA_STORAGE_ERROR`.
                let Some(p) =
                    partitions.get(&topic.topic, krabka_ids::PartitionIndex(part.partition))
                else {
                    out.error_code = if acl_image.partition(&topic.topic, part.partition).is_some()
                    {
                        codes::NOT_LEADER_OR_FOLLOWER
                    } else {
                        codes::UNKNOWN_TOPIC_OR_PARTITION
                    };
                    parts_out.push(out);
                    continue;
                };
                if broker.log_dir_status.is_offline(&p.log_dir.load()) {
                    out.error_code = codes::KAFKA_STORAGE_ERROR;
                    parts_out.push(out);
                    continue;
                }

                // KIP-320 leader-epoch fence, ahead of the leader-only gate and
                // the KIP-101 domain logic below, exactly as
                // `Partition.getLocalLog` runs `checkCurrentLeaderEpoch` before
                // its leader check. `list_offsets_leader_epoch_fence` reads
                // only `-1` as "no epoch asserted", the rule
                // `ReplicaManager.lastOffsetForLeaderEpoch` applies here too.
                if let Some((error_code, _)) =
                    p.list_offsets_leader_epoch_fence(part.current_leader_epoch)
                {
                    out.error_code = error_code;
                    out.leader_epoch = -1;
                    parts_out.push(out);
                    continue;
                }

                if not_leader(
                    &acl_image,
                    &topic.topic,
                    part.partition,
                    &p,
                    broker.config.node_id,
                ) {
                    out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
                    out.leader_epoch = -1;
                    parts_out.push(out);
                    continue;
                }

                // `Partition.lastOffsetForLeaderEpoch`: Kafka's
                // `LeaderEpochFileCache.endOffsetFor` pair, verbatim. An epoch
                // the checkpoint cannot place answers `(-1, -1)`, which is
                // also what `UnifiedLog.endOffsetForEpoch`'s `None` leaves in
                // the row. No error either way: the hosting checks above are
                // what set an error code, not this KIP-101 lookup.
                let log = p.log.lock().expect("log mutex poisoned");
                let leo = log.log_end_offset();
                let (found_epoch, end_offset) = log
                    .epoch_checkpoint()
                    .epoch_and_offset_for(krabka_log::LeaderEpoch(part.leader_epoch), leo);
                drop(log);
                out.error_code = codes::NONE;
                out.leader_epoch = found_epoch.0;
                out.end_offset = end_offset.0;

                parts_out.push(out);
            }

            authorized_out.push(OffsetForLeaderTopicResult {
                topic: topic.topic.clone(),
                partitions: parts_out,
                ..Default::default()
            });
        }

        // Authorized rows first, then unauthorized rows -- matches Kafka's
        // `endOffsetsForAuthorizedPartitions ++ endOffsetsForUnauthorizedPartitions`.
        authorized_out.extend(unauthorized_out);

        OffsetForLeaderEpochResponse {
            throttle_time_ms: 0,
            topics: authorized_out,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::AuthorizationResult,
            broker::BrokerHandle,
            handlers::{
                produce::test_support::image_with_topic, test_support::seed_partition_replicas,
            },
            test_support::{
                self as broker_fixture_support, TemporaryPartition, append_partition_batch,
                record_values, start_broker_no_audit, start_broker_with_authorizer_no_audit,
                temporary_partition,
            },
        },
        assert2::assert,
        krabka_protocol::owned::{
            offset_for_leader_epoch_request::{
                OffsetForLeaderEpochRequest, OffsetForLeaderPartition, OffsetForLeaderTopic,
            },
            offset_for_leader_epoch_response::{
                self, EpochEndOffset, OffsetForLeaderEpochResponse, OffsetForLeaderTopicResult,
            },
        },
        std::sync::Arc,
    };

    const VERSION: i16 = offset_for_leader_epoch_response::MAX_VERSION;

    /// A topic named `"orders"` is always `Describe`-authorized (it stands
    /// in for a topic a follower's principal has an ACL on); `"payments"`'s
    /// `Describe` and the cluster's `ClusterAction` both vary per test case.
    /// Every other request is denied.
    #[derive(Debug)]
    struct TestAuthorizer {
        cluster_action: bool,
        payments_describe: bool,
    }

    krabka_macros::test_authorizer!(TestAuthorizer, req; {
        let allow = match (req.resource_type, req.operation) {
            (ResourceType::Cluster, AclOperation::ClusterAction) => self.cluster_action,
            (ResourceType::Topic, AclOperation::Describe) if req.resource_name == "orders" => true,
            (ResourceType::Topic, AclOperation::Describe) if req.resource_name == "payments" => {
                self.payments_describe
            }
            _ => false,
        };
        if allow {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    });

    fn request() -> OffsetForLeaderEpochRequest {
        OffsetForLeaderEpochRequest {
            topics: vec![
                OffsetForLeaderTopic {
                    topic: "orders".into(),
                    partitions: vec![OffsetForLeaderPartition {
                        partition: 0,
                        leader_epoch: 7,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                OffsetForLeaderTopic {
                    topic: "payments".into(),
                    partitions: vec![OffsetForLeaderPartition {
                        partition: 0,
                        leader_epoch: 7,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }
    }

    /// The row a topic that isn't hosted anywhere on this broker gets, once
    /// it clears authorization: `UNKNOWN_TOPIC_OR_PARTITION`, built by
    /// `ReplicaManager.lastOffsetForLeaderEpoch` from `new EpochEndOffset()`,
    /// so the schema defaults put `-1` in both `leader_epoch` and
    /// `end_offset`.
    fn unknown_topic_row(topic: &str) -> OffsetForLeaderTopicResult {
        error_topic_row(topic, codes::UNKNOWN_TOPIC_OR_PARTITION)
    }

    /// The row a `Describe`-denied topic gets: only the partition index and
    /// `TOPIC_AUTHORIZATION_FAILED (29)` are meaningful, and Kafka's schema
    /// default puts `-1` in both `leader_epoch` and `end_offset` rather than
    /// echoing the request.
    fn denied_row(topic: &str) -> OffsetForLeaderTopicResult {
        error_topic_row(topic, codes::TOPIC_AUTHORIZATION_FAILED)
    }

    fn error_topic_row(topic: &str, error_code: i16) -> OffsetForLeaderTopicResult {
        OffsetForLeaderTopicResult {
            topic: topic.into(),
            partitions: vec![EpochEndOffset {
                partition: 0,
                leader_epoch: -1,
                end_offset: -1,
                error_code,
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    /// One row of the fast-path table: whether the principal holds cluster
    /// `ClusterAction`, whether it may describe `"payments"`, and the topic
    /// rows expected.
    struct FastPathCase {
        cluster_action: bool,
        payments_describe: bool,
        expected_topics: Vec<OffsetForLeaderTopicResult>,
    }

    /// `(cluster ClusterAction, topic Describe on "payments")` ->
    /// `expected topics`, in the order the response must carry them.
    ///
    /// When `ClusterAction` is allowed, Kafka's `ClusterAction` fast path
    /// authorizes every topic without a per-topic `Describe` lookup, so
    /// `"payments"` is never denied even when its own `Describe` ACL would
    /// deny it. Only the `(deny, deny)` case denies `"payments"`, and its
    /// row is appended after the authorized `"orders"` row rather than kept
    /// in request order.
    #[tokio::test]
    async fn cluster_action_fast_path_table() {
        let cases: [FastPathCase; 4] = [
            FastPathCase {
                cluster_action: true,
                payments_describe: false,
                expected_topics: vec![unknown_topic_row("orders"), unknown_topic_row("payments")],
            },
            FastPathCase {
                cluster_action: false,
                payments_describe: true,
                expected_topics: vec![unknown_topic_row("orders"), unknown_topic_row("payments")],
            },
            FastPathCase {
                cluster_action: false,
                payments_describe: false,
                expected_topics: vec![unknown_topic_row("orders"), denied_row("payments")],
            },
            FastPathCase {
                cluster_action: true,
                payments_describe: true,
                expected_topics: vec![unknown_topic_row("orders"), unknown_topic_row("payments")],
            },
        ];

        for FastPathCase {
            cluster_action,
            payments_describe,
            expected_topics,
        } in cases
        {
            let authorizer = Arc::new(TestAuthorizer {
                cluster_action,
                payments_describe,
            });
            krabka_macros::broker_test_setup! {
                bind(broker_handle, _dir, broker, p, peer, ctx);
                boot [start_broker_with_authorizer_no_audit(authorizer)];
                user ["follower"];
                client ["follower-client"];
            };
            let resp = handle(&broker, &request(), VERSION, &ctx);

            let expected = OffsetForLeaderEpochResponse {
                throttle_time_ms: 0,
                topics: expected_topics,
                ..Default::default()
            };
            assert!(
                resp == expected,
                "cluster_action={cluster_action} payments_describe={payments_describe}: \
                 got {resp:?}, want {expected:?}"
            );

            broker_handle.shutdown().await;
        }
    }

    /// The leader check needs this node in the installed local role, and no
    /// other leader in the committed image. Mirrors
    /// `fetch::plan::a_read_needs_the_installed_role_and_the_image_to_name_this_node`.
    /// The third case is the installed-role-vs-image race: the image already
    /// commits this node as leader, but the supervisor has not installed the
    /// role locally yet, so the request must still be refused.
    #[tokio::test]
    async fn leader_check_needs_the_installed_role_and_the_image_to_name_this_node() {
        let image = image_with_topic("orders", &[1, 2]);

        let TemporaryPartition {
            dir: _dir,
            partition,
        } = temporary_partition().call();

        // (this node, installed leader, refused)
        let cases = [
            ("installed and committed", 1, 1, false),
            ("installed, committed to another node", 2, 2, true),
            (
                "committed, not installed yet (installed/image race)",
                1,
                2,
                true,
            ),
        ];
        for (name, node, installed, want_refused) in cases {
            partition
                .install_replication_target(None, installed, 0)
                .await;
            let got = not_leader(
                &image,
                "orders",
                0,
                &partition,
                krabka_metadata::NodeId(node),
            );
            assert!(got == want_refused, "{name}");
        }
    }

    /// Create `topic` with replicas 1 and 2, led by `leader`, wait until this
    /// broker (node 1) holds the partition in that role, and append two
    /// records under `leader_epoch` so `epoch_and_offset_for` has an entry
    /// for that epoch to answer -- the leader-epoch checkpoint only learns
    /// an epoch from a batch actually carrying it (`Log::append`), so the
    /// appended `partition_leader_epoch` has to agree with the partition's
    /// `current_leader_epoch`, which this also sets via
    /// `test_set_leader_epoch`.
    use krabka_ids::LeaderEpoch;
    use krabka_metadata::NodeId;

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct SeededEpochTopicSetup<'a> {
        #[default("orders")]
        topic: &'a str,
        #[default(uuid::Uuid::from_u128(1))]
        topic_id: uuid::Uuid,
        #[default(NodeId(1))]
        leader: NodeId,
        #[default(LeaderEpoch(0))]
        leader_epoch: LeaderEpoch,
    }

    #[bon::builder]
    async fn seeded_topic(
        broker_handle: &BrokerHandle,
        #[builder(default = "orders")] topic: &'_ str,
        #[builder(default = uuid :: Uuid :: from_u128 (1))] topic_id: uuid::Uuid,
        #[builder(into, default = NodeId (1))] leader: NodeId,
        #[builder(into, default = LeaderEpoch (0))] leader_epoch: LeaderEpoch,
    ) -> std::sync::Arc<Partition> {
        let setup = SeededEpochTopicSetup {
            topic,
            topic_id,
            leader,
            leader_epoch,
        };
        let SeededEpochTopicSetup {
            topic,
            topic_id,
            leader,
            leader_epoch,
        } = setup;
        seed_partition_replicas()
            .broker(broker_handle)
            .topic(topic)
            .topic_id(topic_id)
            .leader(leader)
            .call()
            .await;

        let shared = broker_handle.broker_arc_for_test();
        let partition = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(partition) = shared.partitions.get(topic, krabka_ids::PartitionIndex(0))
                    && partition.current_leader.load(Ordering::Acquire) == leader.0
                {
                    return partition;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the broker holds the partition in its role");

        partition.test_set_leader_epoch(leader_epoch.0);

        let records = record_values(&[&b"first"[..], &b"second"[..]]);
        let mut batch = krabka_protocol::records::RecordBatch {
            last_offset_delta: 1,
            partition_leader_epoch: leader_epoch.0,
            records,
            ..Default::default()
        };
        append_partition_batch(&partition, &mut batch);
        partition
    }

    fn ofle_request(
        topic: &str,
        leader_epoch: LeaderEpoch,
        current_leader_epoch: LeaderEpoch,
    ) -> OffsetForLeaderEpochRequest {
        use krabka_protocol::owned::offset_for_leader_epoch_request::{
            OffsetForLeaderPartition, OffsetForLeaderTopic,
        };

        OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderTopic {
                topic: topic.to_owned(),
                partitions: vec![OffsetForLeaderPartition {
                    partition: 0,
                    current_leader_epoch: current_leader_epoch.0,
                    leader_epoch: leader_epoch.0,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn ofle(
        broker_handle: &BrokerHandle,
        version: i16,
        request: &OffsetForLeaderEpochRequest,
    ) -> EpochEndOffset {
        let shared = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(user, address, ctx);
            user ["client"];
            client ["ofle-client"];
        };
        let decoded = handle(&shared, request, version, &ctx);
        decoded
            .topics
            .into_iter()
            .next()
            .expect("one topic in response")
            .partitions
            .into_iter()
            .next()
            .expect("one partition in response")
    }

    /// KIP-320's leader-epoch fence and leader-only gate, table-driven across
    /// the scenarios the two checks must cover together: a leader answers
    /// normally, a follower is refused, an unknown partition short-circuits
    /// before either check runs, a stale or future `current_leader_epoch` is
    /// fenced ahead of the leadership check (on both a leader and a follower
    /// partition, to prove the fence really does run first), and a negative
    /// `current_leader_epoch` other than the `-1` sentinel is fenced too --
    /// `ReplicaManager.lastOffsetForLeaderEpoch` shares `ListOffsets`'
    /// stricter sentinel rule, not `Fetch`'s "every negative epoch is
    /// unasserted" one.
    #[tokio::test]
    async fn kip_320_fence_and_leader_gate_answer_offset_for_leader_epoch_rows() {
        use {
            krabka_protocol::owned::offset_for_leader_epoch_request,
            offset_for_leader_epoch_request::MAX_VERSION as VERSION,
        };

        let HandlerStartedBroker { broker, dir: _dir } = start_broker_no_audit().await;

        let leader_topic = seeded_topic()
            .broker_handle(&broker)
            .topic("ofle-leader")
            .call()
            .await;
        seeded_topic()
            .broker_handle(&broker)
            .topic("ofle-follower")
            .topic_id(uuid::Uuid::from_u128(2))
            .leader(2)
            .call()
            .await;
        seeded_topic()
            .broker_handle(&broker)
            .topic("ofle-epoch")
            .topic_id(uuid::Uuid::from_u128(3))
            .leader_epoch(3)
            .call()
            .await;

        let refused = |error_code| EpochEndOffset {
            partition: 0,
            error_code,
            leader_epoch: -1,
            end_offset: -1,
            ..Default::default()
        };
        let resolved = |leader_epoch, end_offset| EpochEndOffset {
            partition: 0,
            error_code: codes::NONE,
            leader_epoch,
            end_offset,
            ..Default::default()
        };

        let cases = [
            (
                "leader answers correctly",
                "ofle-leader",
                0,
                -1,
                resolved(0, 2),
            ),
            (
                "a requested leader_epoch above the checkpoint's latest is UNDEFINED_EPOCH/-1, not an error -- \
                 LeaderEpochFileCache.endOffsetFor finds no higherEntry",
                "ofle-leader",
                5,
                -1,
                resolved(-1, -1),
            ),
            (
                "a requested leader_epoch below every recorded epoch answers that epoch and the first \
                 recorded start, as endOffsetFor does when there is no floorEntry",
                "ofle-epoch",
                1,
                -1,
                resolved(1, 0),
            ),
            (
                "follower is refused",
                "ofle-follower",
                0,
                -1,
                refused(codes::NOT_LEADER_OR_FOLLOWER),
            ),
            (
                "stale current_leader_epoch is fenced ahead of the leader check",
                "ofle-epoch",
                3,
                2,
                refused(codes::FENCED_LEADER_EPOCH),
            ),
            (
                "future current_leader_epoch is fenced ahead of the leader check",
                "ofle-epoch",
                3,
                4,
                refused(codes::UNKNOWN_LEADER_EPOCH),
            ),
            (
                "a negative current_leader_epoch that is not the -1 sentinel is fenced",
                "ofle-epoch",
                3,
                -2,
                refused(codes::FENCED_LEADER_EPOCH),
            ),
            (
                "the -1 sentinel passes the fence and reaches the KIP-101 domain logic",
                "ofle-epoch",
                3,
                -1,
                resolved(3, 2),
            ),
            (
                "the fence outranks the leader-only gate on a follower partition too",
                "ofle-follower",
                0,
                -2,
                refused(codes::FENCED_LEADER_EPOCH),
            ),
        ];
        for (name, topic, leader_epoch, current_leader_epoch, want) in cases {
            let got = ofle(
                &broker,
                VERSION,
                &ofle_request(
                    topic,
                    LeaderEpoch(leader_epoch),
                    LeaderEpoch(current_leader_epoch),
                ),
            );
            assert!(got == want, "{name}");
        }

        // Unknown partition: the fence and leader-only gate never run, since
        // the partition lookup answers first. Like every refused row above it
        // carries the schema defaults, -1 for both offsets.
        let unknown = ofle(
            &broker,
            VERSION,
            &ofle_request("ofle-missing", LeaderEpoch(5), LeaderEpoch(-1)),
        );
        assert!(
            unknown
                == EpochEndOffset {
                    partition: 0,
                    error_code: codes::UNKNOWN_TOPIC_OR_PARTITION,
                    leader_epoch: -1,
                    end_offset: -1,
                    ..Default::default()
                }
        );

        drop(leader_topic);
        broker.shutdown().await;
    }

    /// The three hosting outcomes of `ReplicaManager.lastOffsetForLeaderEpoch`
    /// come before the epoch fence. A partition in an offline log dir is
    /// `KAFKA_STORAGE_ERROR`, where the epoch checkpoint in memory would have
    /// answered it with no error. A partition the metadata holds and this
    /// broker does not host (a reassignment moved it away) is
    /// `NOT_LEADER_OR_FOLLOWER`, the code that makes a client refresh its
    /// metadata, and only a partition the metadata does not hold is
    /// `UNKNOWN_TOPIC_OR_PARTITION`. Each request asserts a stale epoch, which
    /// the fence would have refused with `FENCED_LEADER_EPOCH` had it run
    /// first.
    #[tokio::test]
    async fn hosting_outcomes_are_decided_before_the_epoch_fence() {
        krabka_macros::broker_test_setup! {
            bind(broker, _dir, shared);
            boot [broker_fixture_support::start_broker_no_audit()];
        };

        let offline = seeded_topic()
            .broker_handle(&broker)
            .topic("ofle-offline")
            .leader_epoch(3)
            .call()
            .await;
        shared
            .log_dir_status
            .mark_offline(&offline.log_dir.load(), "test: EIO");

        // Node 1 is not a replica of this partition, so it never hosts it.
        seed_partition_replicas()
            .broker(&broker)
            .topic("ofle-moved")
            .topic_id(uuid::Uuid::from_u128(9))
            .leader(2)
            .replicas(&[krabka_audit::NodeId(2), krabka_audit::NodeId(3)])
            .leader_epoch(3)
            .call()
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while shared
                .controller
                .current_image()
                .partition("ofle-moved", 0)
                .is_none()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the image holds the partition");

        let refused = |error_code| EpochEndOffset {
            partition: 0,
            error_code,
            leader_epoch: -1,
            end_offset: -1,
            ..Default::default()
        };
        let cases = [
            (
                "a partition in an offline log dir",
                "ofle-offline",
                codes::KAFKA_STORAGE_ERROR,
            ),
            (
                "a partition the metadata holds and this broker does not host",
                "ofle-moved",
                codes::NOT_LEADER_OR_FOLLOWER,
            ),
            (
                "a partition the metadata does not hold",
                "ofle-missing",
                codes::UNKNOWN_TOPIC_OR_PARTITION,
            ),
        ];
        for (name, topic, error_code) in cases {
            let got = ofle(
                &broker,
                VERSION,
                &ofle_request(topic, LeaderEpoch(3), LeaderEpoch(2)),
            );
            assert!(got == refused(error_code), "{name}");
        }

        broker.shutdown().await;
    }
}
