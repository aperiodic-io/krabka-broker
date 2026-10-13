//! The election of one partition, from the break-glass gate through to the
//! response row the request carries back for it.
//!
//! This is where the two unclean paths part: a topic that opted into an
//! offset-aware recovery strategy goes to the Unclean Recovery Manager, and
//! every other target elects straight out of the metadata image.

use {
    super::{
        batch::ElectionBatch,
        env::ElectionEnv,
        recovery::run_offset_aware_recovery,
        response::elect_error_to_wire,
        unclean_gate::{authorize_unclean, refuse_unclean, unclean_target},
    },
    crate::{
        config_keys::{RecoveryStrategy, resolve_recovery_strategy},
        leader_election::{ElectionType, select_new_leader_for_partition},
    },
    krabka_metadata::{LeaderRecoveryState, MetadataRecord, PartitionUpdateRecord},
    krabka_protocol::owned::elect_leaders_response::PartitionResult,
};

/// Elect one partition, and answer the row the response carries for it.
pub(super) async fn elect_one(
    env: &ElectionEnv<'_>,
    batch: &mut ElectionBatch,
    topic: &str,
    partition: i32,
) -> PartitionResult {
    // KFC-9: the break-glass gate runs before any election work, because it is
    // an authority gate and not a content gate. It never sees a preferred
    // election.
    let consumed = if matches!(env.election, ElectionType::Unclean) {
        match authorize_unclean(env.image, &env.broker.config.break_glass, topic, partition) {
            Ok(consumed) => consumed,
            Err(denial) => return refuse_unclean(env, topic, partition, &denial),
        }
    } else {
        None
    };

    // KIP-966: an UNCLEAN election on a topic that opted into an offset-aware
    // recovery strategy is routed through the Unclean Recovery Manager, which
    // polls surviving replicas for their log state before electing. The URM
    // owns the guarded election append for these, so we must NOT push a record into the
    // batch here — we just await the outcome and translate it to a
    // per-partition row.
    let strategy = resolve_recovery_strategy(env.image, topic);
    let use_offset_aware = matches!(env.election, ElectionType::Unclean)
        && !matches!(strategy, RecoveryStrategy::None);
    if use_offset_aware {
        return run_offset_aware_recovery(env, batch, topic, partition, strategy, consumed).await;
    }

    let result = select_new_leader_for_partition(
        env.image,
        env.alive,
        env.witnesses,
        topic,
        partition,
        env.election,
    );
    match result {
        Ok(new_pr) => {
            let proposal_id = batch.spend(consumed);
            let recovering = matches!(env.election, ElectionType::Unclean)
                && !env
                    .image
                    .partition_elr(topic, partition)
                    .0
                    .contains(&new_pr.leader);
            if recovering {
                batch
                    .records
                    .push(MetadataRecord::V1PartitionUpdate(PartitionUpdateRecord {
                        partition: new_pr,
                        eligible_leader_replicas: Some(Vec::new()),
                        last_known_elr: Some(Vec::new()),
                        recovery_state: Some(LeaderRecoveryState::Recovering),
                    }));
            } else {
                batch.records.push(MetadataRecord::V1Partition(new_pr));
            }
            if matches!(env.election, ElectionType::Unclean) {
                batch
                    .applied
                    .push((unclean_target(topic, partition), proposal_id));
            }
            PartitionResult {
                partition_id: partition,
                error_code: 0,
                error_message: None,
                ..Default::default()
            }
        }
        Err(err) => {
            let (code, msg) = elect_error_to_wire(err, env.image, topic, partition);
            PartitionResult {
                partition_id: partition,
                error_code: code,
                error_message: Some(msg),
                ..Default::default()
            }
        }
    }
}
