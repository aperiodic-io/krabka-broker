use assert2::assert;
use tempfile::tempdir;

use super::*;

/// The gap `wait_for_self_registration_published` closes: a `submit_change`
/// reply that has already committed and applied the registration, while the
/// `Arc<MetadataImage>` `current_image()`/`watch_image()` hand back has not
/// yet been republished to carry it. This is exactly the trailing-publish gap
/// `spawn_deferred_controller_registration`'s own comment names for the
/// controller-registration record; this fixture reproduces the same gap for
/// the broker-registration record `register_broker` submits.
mod publish_race {
    use std::{sync::Arc, time::Duration};

    use assert2::assert;
    use krabka_metadata::{MetadataImage, MetadataRecord, NodeId};
    use tempfile::tempdir;
    use tokio::sync::watch;

    use super::{super::register_broker, self_registration_record};
    use crate::{config::BrokerConfig, error::BrokerError, test_support::FakeMetadataSource};

    fn registration_config(log_dir: &std::path::Path) -> BrokerConfig {
        BrokerConfig {
            node_id: NodeId(9),
            ..BrokerConfig::for_tests(log_dir.to_path_buf())
        }
    }

    fn spawn_registration(
        config: BrokerConfig,
        image_tx: &watch::Sender<Arc<MetadataImage>>,
    ) -> tokio::task::JoinHandle<Result<Option<i64>, BrokerError>> {
        let source = FakeMetadataSource::published_image_channel(image_tx.clone());
        tokio::spawn(async move { register_broker(&config, &source).await })
    }

    /// Without the wait this regression test's fixture would time out on the
    /// side that matters: `register_broker` must not return while
    /// `current_image()` still holds no registration for this node at all --
    /// the exact image `site_broker_views` reads for its own "self-
    /// registration record has not reached the local image yet" fallback,
    /// and the one a `CreateTopics` run in that same window sees.
    #[tokio::test(start_paused = true)]
    async fn register_broker_waits_for_the_image_to_publish_the_registration() {
        // `register_broker` mints and persists a directory id in each log
        // dir, so the config needs a real one, not the working directory.
        let log_dir = tempdir().expect("tempdir");
        let config = registration_config(log_dir.path());
        // The controller stamps the offset the registration commits at.
        let mut registration = self_registration_record(&config);
        registration.broker_epoch = 0;
        let (image_tx, _keep_alive) =
            watch::channel(Arc::new(MetadataImage::new(uuid::Uuid::nil())));
        let mut call = spawn_registration(config, &image_tx);

        // `submit_change` inside `register_broker` already returned `Ok`, but
        // the image the test controls has not published the registration
        // yet. A `register_broker` that trusted the reply alone would have
        // returned here already.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !call.is_finished(),
            "register_broker must not return before its own registration is visible \
             in current_image()"
        );

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(registration));
        image_tx
            .send(Arc::new(image))
            .expect("test receiver kept alive");

        let result = tokio::time::timeout(Duration::from_secs(1), &mut call)
            .await
            .expect("register_broker did not observe the published image")
            .expect("register_broker task");
        assert!(result.ok() == Some(Some(0)));
    }

    /// The restart case: `image` already carries a registration for this node
    /// -- from before this call's own submit -- so a wait that only checks
    /// "is a registration present" would return immediately on that stale
    /// entry, before the controller republished anything at all. Every
    /// commit assigns a fresh `broker_epoch`, so the fixture tells the two
    /// apart by epoch, the same way the real controller would. The controller
    /// fencing the stale registration at its own epoch is a change to the
    /// image, but not the new registration, and `register_broker` returns the
    /// new epoch, as Kafka's `BrokerLifecycleManager` keeps the epoch of its
    /// `BrokerRegistration` response (krabka-io/krabka-broker#1014).
    #[tokio::test(start_paused = true)]
    async fn register_broker_on_restart_waits_for_the_new_epoch_not_the_stale_one() {
        // `register_broker` mints and persists a directory id in each log
        // dir, so the config needs a real one, not the working directory.
        let log_dir = tempdir().expect("tempdir");
        let config = registration_config(log_dir.path());
        let mut stale = self_registration_record(&config);
        stale.broker_epoch = 3;
        stale.fenced = false;
        let mut fresh = self_registration_record(&config);
        fresh.broker_epoch = 7;

        let mut initial = MetadataImage::new(uuid::Uuid::nil());
        initial.apply(&MetadataRecord::V1BrokerRegistration(stale.clone()));
        let (image_tx, _keep_alive) = watch::channel(Arc::new(initial));
        let mut call = spawn_registration(config, &image_tx);

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !call.is_finished(),
            "register_broker must not return on the strength of the pre-existing (stale) \
             registration already in the image at boot"
        );

        let mut fenced_stale = MetadataImage::new(uuid::Uuid::nil());
        fenced_stale.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                fenced: true,
                ..stale
            },
        ));
        image_tx
            .send(Arc::new(fenced_stale))
            .expect("test receiver kept alive");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !call.is_finished(),
            "register_broker must not return on the controller fencing the stale registration"
        );

        let mut published = MetadataImage::new(uuid::Uuid::nil());
        published.apply(&MetadataRecord::V1BrokerRegistration(fresh));
        image_tx
            .send(Arc::new(published))
            .expect("test receiver kept alive");

        let result = tokio::time::timeout(Duration::from_secs(1), &mut call)
            .await
            .expect("register_broker did not observe the freshly published registration")
            .expect("register_broker task");
        assert!(result.ok() == Some(Some(7)));
    }

    /// A publish that never arrives -- the observer's connection to the
    /// controller drops right after a forwarded submit succeeded, say -- must
    /// not hang `Broker::start` forever. The wait gives up once
    /// `startup_leader_wait_timeout` elapses and reports a startup error.
    #[tokio::test(start_paused = true)]
    async fn register_broker_gives_up_once_the_startup_deadline_elapses() {
        // `register_broker` mints and persists a directory id in each log
        // dir, so the config needs a real one, not the working directory.
        let log_dir = tempdir().expect("tempdir");
        let config = registration_config(log_dir.path());
        let (image_tx, _keep_alive) =
            watch::channel(Arc::new(MetadataImage::new(uuid::Uuid::nil())));
        let source = FakeMetadataSource::published_image_channel(image_tx);

        let err = register_broker(&config, &source)
            .await
            .expect_err("the image never publishes the registration");
        assert!(matches!(err, BrokerError::Startup(_)));
    }
}

#[test]
fn file_config_self_registration_uses_advertised_listener_for_legacy_endpoint() {
    let file: crate::file_config::FileConfig = toml::from_str(
        r#"
inter_broker_listener_name = "INTERNAL"

[[listeners]]
name = "EXTERNAL"
bind_addr = "127.0.0.1:19094"
advertised = "external.example:29094"
protocol = "Plaintext"

[[listeners]]
name = "INTERNAL"
bind_addr = "127.0.0.1:19093"
advertised = "internal.example:29093"
protocol = "Plaintext"
"#,
    )
    .expect("parse file config");
    // `self_registration_record` mints and persists a directory id in the
    // log dir, so point the default config's `./krabka-data` somewhere real.
    let log_dir = tempdir().expect("tempdir");
    let mut config = BrokerConfig {
        log_dir: log_dir.path().to_path_buf(),
        ..BrokerConfig::default()
    };
    assert!(
        config.listen_addr.port() == 9092,
        "preserve CLI default precondition"
    );
    file.apply_to(&mut config).expect("apply file config");

    let registration = self_registration_record(&config);

    assert!(registration.host == "internal.example");
    assert!(registration.port == 29093);
    assert!(
        registration
            .endpoints
            .iter()
            .map(|endpoint| (
                endpoint.name.as_str(),
                endpoint.host.as_str(),
                endpoint.port
            ))
            .collect::<Vec<_>>()
            == vec![
                ("EXTERNAL", "external.example", 29094),
                ("INTERNAL", "internal.example", 29093),
            ]
    );
}

/// A stretch profile with two data sites and one witness site, with
/// leadership pinned to `dc-a`.
fn three_site_profile() -> crate::config::StretchProfile {
    crate::config::StretchProfile {
        sites: vec!["dc-a".to_string(), "dc-b".to_string(), "dc-w".to_string()],
        witness_site: "dc-w".to_string(),
        preferred_leader_site: "dc-a".to_string(),
    }
}

/// A node of node id 4 that carries `roles` and logs into `log_dir`.
///
/// The registration record mints a directory id under `log_dir`, so the
/// caller passes a temporary directory rather than the source tree.
fn node_with_roles(log_dir: &std::path::Path, roles: Vec<crate::config::NodeRole>) -> BrokerConfig {
    BrokerConfig {
        node_id: krabka_metadata::NodeId(4),
        roles,
        ..BrokerConfig::for_tests(log_dir.to_path_buf())
    }
}

fn broker_witness_record(node_id: u64, value: Option<&str>) -> krabka_metadata::MetadataRecord {
    krabka_metadata::MetadataRecord::V1BrokerConfig(krabka_metadata::BrokerConfigRecord {
        node_id: krabka_metadata::NodeId(node_id),
        config_name: "broker.witness".into(),
        config_value: value.map(str::to_string),
    })
}

#[test]
fn registration_batch_publishes_or_clears_the_witness_role() {
    use crate::config::NodeRole::{Broker, Controller, Witness};
    for (roles, expected_witness) in [
        (vec![Controller, Broker, Witness], Some("true")),
        (vec![Controller, Broker], None),
    ] {
        let log_dir = tempdir().expect("temp log dir");
        let config = node_with_roles(log_dir.path(), roles);
        assert!(
            broker_registration_batch(&config)
                == vec![
                    krabka_metadata::MetadataRecord::V1BrokerRegistration(
                        self_registration_record(&config)
                    ),
                    broker_witness_record(4, expected_witness),
                ]
        );
    }
}

#[test]
fn a_stretch_profile_publishes_the_preferred_leader_site_as_a_cluster_default() {
    let config = BrokerConfig {
        stretch: Some(three_site_profile()),
        ..BrokerConfig::for_tests(std::path::PathBuf::new())
    };

    assert!(
        stretch_default_records(&config)
            == vec![krabka_metadata::MetadataRecord::V1BrokerConfig(
                krabka_metadata::BrokerConfigRecord {
                    node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    config_name: "stretch.preferred.leader.site".into(),
                    config_value: Some("dc-a".into()),
                }
            )]
    );
}

#[test]
fn a_node_without_a_stretch_profile_publishes_no_cluster_default() {
    let config = BrokerConfig::for_tests(std::path::PathBuf::new());

    assert!(stretch_default_records(&config) == vec![]);
}

/// Only a controller that starts a new cluster publishes the stretch cluster
/// default. A broker-only node, and a controller that restarts on its log,
/// publish nothing, and no node publishes a bootstrap record.
#[test]
fn only_a_bootstrapping_controller_publishes_the_stretch_cluster_default() {
    use crate::{BootstrapMode, config::NodeRole};

    let combined = vec![NodeRole::Broker, NodeRole::Controller];
    let stretch = BrokerConfig {
        stretch: Some(three_site_profile()),
        ..BrokerConfig::for_tests(std::path::PathBuf::new())
    };
    let default = stretch_default_records(&stretch);
    // (roles, bootstrap mode, stretch profile, the records submitted)
    let cases = [
        (
            combined.clone(),
            BootstrapMode::Bootstrap,
            true,
            default.clone(),
        ),
        (
            vec![NodeRole::Controller],
            BootstrapMode::Bootstrap,
            true,
            default,
        ),
        (
            vec![NodeRole::Broker],
            BootstrapMode::Bootstrap,
            true,
            vec![],
        ),
        (combined.clone(), BootstrapMode::Rejoin, true, vec![]),
        (combined, BootstrapMode::Bootstrap, false, vec![]),
    ];
    let mut submitted = Vec::new();
    let mut expected = Vec::new();
    for (roles, bootstrap_mode, has_stretch, records) in cases {
        let config = BrokerConfig {
            roles: roles.clone(),
            bootstrap_mode,
            stretch: has_stretch.then(three_site_profile),
            ..BrokerConfig::for_tests(std::path::PathBuf::new())
        };
        submitted.push((
            roles.clone(),
            bootstrap_mode,
            has_stretch,
            stretch_defaults_to_submit(&config),
        ));
        expected.push((roles, bootstrap_mode, has_stretch, records));
    }
    assert!(submitted == expected);
}

#[test]
fn self_controller_registration_uses_quorum_endpoint_and_feature_ranges() {
    let config = BrokerConfig {
        node_id: krabka_metadata::NodeId(7),
        incarnation_id: uuid::Uuid::from_u128(0xCAFE),
        controller_quorum_voters: vec![(
            krabka_metadata::NodeId(7),
            "controller.example:19093".into(),
        )],
        controller_listener_protocol: krabka_security::ListenerProtocol::Ssl,
        ..Default::default()
    };

    let registration = self_controller_registration_record(&config);

    assert!(registration.node_id == krabka_metadata::NodeId(7));
    assert!(registration.incarnation_id == uuid::Uuid::from_u128(0xCAFE));
    // #784: `metadata.version` and `share.version` capped at 4.3.1's latest
    // production levels, the node's default
    // `unstable.feature.versions.enable=false`.
    let mut expected = krabka_metadata::supported_feature_ranges();
    expected.insert(
        "metadata.version".into(),
        (krabka_metadata::metadata_version::METADATA_VERSION_MIN, 30),
    );
    expected.insert("share.version".into(), (0, 1));
    assert!(registration.features == expected);
    assert!(
        registration.endpoints
            == vec![krabka_metadata::BrokerEndpoint {
                name: "CONTROLLER".into(),
                host: "controller.example".into(),
                port: 19093,
                protocol: krabka_security::ListenerProtocol::Ssl,
            }]
    );
}

/// A controller of a dynamic quorum has no `controller_quorum_voters` entry,
/// so it registers its listener. A wildcard bind registers this machine's host
/// name, as Kafka's does, and never the wildcard address, which no peer can
/// dial.
#[test]
fn a_dynamic_controller_registers_its_listener_with_the_host_name_for_a_wildcard() {
    let host_name = crate::host_port::local_host_name().unwrap_or_else(|| "127.0.0.1".into());
    let cases = [
        ("192.0.2.10:9592", "192.0.2.10".to_owned()),
        ("0.0.0.0:9592", host_name.clone()),
        ("[::]:9592", host_name),
    ];
    let registered: Vec<(String, u16)> = cases
        .iter()
        .map(|(bound, _)| {
            let config = BrokerConfig {
                controller_listen_addr: bound.parse().expect("listen address"),
                controller_quorum_voters: vec![],
                ..Default::default()
            };
            let endpoint = self_controller_registration_record(&config)
                .endpoints
                .into_iter()
                .next()
                .expect("one controller endpoint");
            (endpoint.host, endpoint.port)
        })
        .collect();
    let expected: Vec<(String, u16)> = cases.into_iter().map(|(_, host)| (host, 9592)).collect();
    assert!(registered == expected);
}

#[test]
fn controller_registration_starts_at_kip_919_floor_and_is_idempotent() {
    let config = BrokerConfig::default();
    let registration = self_controller_registration_record(&config);
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL - 1,
        },
    ));

    assert!(controller_registration_update(&image, &registration).is_none());

    image.apply(&krabka_metadata::MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
            level: krabka_metadata::metadata_version::ONLINE_DOWNGRADE_MIN_LEVEL,
        },
    ));
    let update = controller_registration_update(&image, &registration)
        .expect("crossing the KIP-919 floor registers the controller");
    image.apply(&update);

    assert!(image.controller(config.node_id) == Some(&registration));
    assert!(controller_registration_update(&image, &registration).is_none());
}

/// KIP-966, the crash half: a broker that restarts without proving it stopped
/// gracefully gives up its eligible-leader-replica membership.
///
/// The pair below drives the real self-registration path against a live
/// controller and reads the published ELR back out of the committed metadata.
/// Both restarts reuse the node's incarnation id -- krabka keeps that id in the
/// log dir, so a broker that crashes and comes back on the same disk really
/// does present the same one -- which is the whole point: identity says
/// nothing about whether the log this node brings back is the log its ELR
/// membership claims, and only the clean-shutdown proof does.
mod unclean_restart {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_metadata::{LeaderEpoch, MetadataRecord, NodeId, PartitionRecord};

    use crate::{
        broker::{Broker, registration::register_broker},
        config::BrokerConfig,
        elr::{TopicElr, state::PartitionElr},
        test_support::start_broker_with_authorizer,
    };

    const TOPIC: &str = "orders";
    const TOPIC_ID_BYTES: [u8; 16] = [9; 16];
    const LEADER_EPOCH: i32 = 7;
    /// `AlterPartition` v2, whose `new_isr` is a plain broker-id list, for the
    /// reason [`crate::elr::tests`] gives: v3 drags the KIP-903 broker-epoch
    /// eligibility check into a fixture that is not about it.
    /// The restarting broker. Node 1 is the controller and stays up.
    const RESTARTING: NodeId = NodeId(2);

    fn nodes(ids: &[u64]) -> Vec<NodeId> {
        ids.iter().copied().map(NodeId).collect()
    }

    /// One RF=3 partition with a full ISR, and a `min.insync.replicas` of 2,
    /// which is what gives the partition an ELR to fall below.
    fn seed_records() -> Vec<MetadataRecord> {
        crate::test_support::elr_topic_records(
            PartitionRecord {
                topic: TOPIC.into(),
                partition: 0,
                leader: NodeId(1),
                replicas: nodes(&[1, 2, 3]),
                isr: nodes(&[1, 2, 3]),
                leader_epoch: LeaderEpoch(LEADER_EPOCH),
                adding_replicas: vec![],
                removing_replicas: vec![],
                directories: vec![uuid::Uuid::nil(); 3],
                partition_epoch: 4,
            },
            uuid::Uuid::from_bytes(TOPIC_ID_BYTES),
            "2",
        )
    }

    /// The published ELR of partition 0.
    fn published_elr(broker: &Arc<Broker>) -> PartitionElr {
        TopicElr::of_topic(&broker.controller.current_image(), TOPIC).partition(0)
    }

    /// Bring the restarting broker up: read whatever clean-shutdown proof its
    /// log dir holds, then register through the real self-registration path.
    ///
    /// This is what `start_metadata_phase` does, in the order it does it.
    async fn boot(broker: &Arc<Broker>, config: &mut BrokerConfig) {
        config.previous_broker_epoch = crate::clean_shutdown::take(&config.log_dir);
        register_broker(config, &*broker.controller)
            .await
            .expect("self-registration");
    }

    /// Stand the fixture up: node 1 leading an RF=3 partition, node 2 booted
    /// once and then dropped out of the ISR, so it holds ELR membership.
    ///
    /// Returns node 2's config, its log dir, and the broker epoch the cluster
    /// now holds for it -- the epoch a graceful stop would leave behind.
    async fn cluster_with_node_two_eligible(
        broker: &Arc<Broker>,
    ) -> (BrokerConfig, tempfile::TempDir, i64) {
        crate::test_support::wait_for_controller_leader(broker).await;
        broker
            .controller
            .submit_change(seed_records())
            .await
            .expect("seed orders");

        let dir = tempfile::TempDir::new().expect("tempdir");
        let mut config = BrokerConfig::for_tests(dir.path().to_path_buf());
        config.node_id = RESTARTING;
        boot(broker, &mut config).await;

        crate::test_support::accepted_isr_proposal(
            broker,
            crate::test_support::LiveIsrSetup::default(),
            crate::test_support::IsrResponseCheck::PartitionOnly,
        )
        .await;
        assert!(
            published_elr(broker)
                == PartitionElr {
                    eligible_leader_replicas: vec![2, 3],
                    last_known_elr: vec![],
                },
            "precondition: the shrink leaves nodes 2 and 3 eligible"
        );

        let epoch = broker
            .controller
            .current_image()
            .broker_epoch(RESTARTING)
            .expect("node 2 is registered");
        (config, dir, epoch)
    }

    /// The defect this closes: a broker that crashed -- no clean-shutdown
    /// proof left behind -- comes back on the same log dir with the same
    /// incarnation id, and must no longer be eligible to be elected. Its log
    /// may be shorter than the records its ELR membership asserts it holds.
    #[tokio::test]
    async fn a_crashed_broker_loses_its_elr_membership_on_restart() {
        let (handle, _dir) =
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = handle.broker_arc_for_test();
        let (mut config, _victim_dir, _epoch) = cluster_with_node_two_eligible(&broker).await;

        // A crash writes nothing, so the restart can prove nothing.
        boot(&broker, &mut config).await;

        assert!(
            published_elr(&broker)
                == PartitionElr {
                    eligible_leader_replicas: vec![3],
                    last_known_elr: vec![],
                }
        );

        handle.shutdown().await;
    }

    /// The other end of the round trip: a graceful stop leaves behind exactly
    /// the epoch the cluster holds, which is what its own next start spends.
    /// The two tests above hand that value over by writing it themselves; this
    /// one says the broker really does write it on the way down.
    #[tokio::test]
    async fn a_graceful_stop_leaves_the_proof_its_own_restart_spends() {
        let (handle, dir, broker) = crate::test_support::started_controller_broker().await;
        let node_id = broker.config.node_id;
        let log_dir = broker.config.log_dir.clone();
        let epoch = broker
            .controller
            .current_image()
            .broker_epoch(node_id)
            .expect("the broker registered itself at startup");
        drop(broker);

        handle.shutdown().await;

        assert!(crate::clean_shutdown::take(&log_dir) == epoch);
        drop(dir);
    }

    /// The companion, without which the fix could pass by withdrawing
    /// everyone: a broker that stopped gracefully offers back the epoch the
    /// cluster still holds for it, and keeps its membership.
    #[tokio::test]
    async fn a_gracefully_stopped_broker_keeps_its_elr_membership_on_restart() {
        let (handle, _dir) =
            start_broker_with_authorizer(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = handle.broker_arc_for_test();
        let (mut config, victim_dir, epoch) = cluster_with_node_two_eligible(&broker).await;

        // The graceful stop leaves its proof; the restart spends it.
        crate::clean_shutdown::write(victim_dir.path(), epoch);
        boot(&broker, &mut config).await;

        assert!(
            published_elr(&broker)
                == PartitionElr {
                    eligible_leader_replicas: vec![2, 3],
                    last_known_elr: vec![],
                }
        );

        handle.shutdown().await;
    }
}
