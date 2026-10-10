//! Background task that subscribes to `MetadataImage` changes and
//! updates the throttle bucket rates. The task runs unconditionally on
//! every broker. The bucket itself handles the unthrottled fast path.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, NodeId, ThrottleKind};
use krabka_units::{ByteRate, convert::ByteRateExt};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::ThrottleState;
use crate::metadata_source::watch_image_loop;

pub async fn run(
    images: watch::Receiver<Arc<MetadataImage>>,
    node_id: NodeId,
    throttle: Arc<ThrottleState>,
    shutdown: CancellationToken,
) {
    watch_image_loop(images, "throttle refresh", shutdown, |image| {
        apply_image(image, node_id, &throttle);
    })
    .await;
}

/// The KIP-73 throttle rate the image holds for `kind`.
///
/// An unset or disabled config is the bucket's "unthrottled" sentinel,
/// [`ByteRateExt::ZERO`].
fn image_rate(image: &MetadataImage, node_id: NodeId, kind: ThrottleKind) -> ByteRate {
    image
        .broker_throttle_rate(node_id, kind)
        .unwrap_or(<ByteRate as ByteRateExt>::ZERO)
}

pub(crate) fn apply_image(image: &MetadataImage, node_id: NodeId, throttle: &ThrottleState) {
    let leader_rate = image_rate(image, node_id, ThrottleKind::Leader);
    let follower_rate = image_rate(image, node_id, ThrottleKind::Follower);
    let alter_log_dirs_rate = image_rate(image, node_id, ThrottleKind::AlterLogDirs);
    if throttle.leader_out.byte_rate() != leader_rate {
        debug!(
            node_id = node_id.0,
            leader_rate = leader_rate.bytes_per_sec_i64(),
            "throttle: leader-out rate update"
        );
        throttle.leader_out.set_byte_rate(leader_rate);
    }
    if throttle.follower_in.byte_rate() != follower_rate {
        debug!(
            node_id = node_id.0,
            follower_rate = follower_rate.bytes_per_sec_i64(),
            "throttle: follower-in rate update"
        );
        throttle.follower_in.set_byte_rate(follower_rate);
    }
    if throttle.alter_log_dirs.byte_rate() != alter_log_dirs_rate {
        debug!(
            node_id = node_id.0,
            alter_log_dirs_rate = alter_log_dirs_rate.bytes_per_sec_i64(),
            "throttle: alter-log-dirs rate update"
        );
        throttle.alter_log_dirs.set_byte_rate(alter_log_dirs_rate);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerConfigRecord, MetadataRecord};
    use krabka_units::bytes_per_sec;
    use uuid::Uuid;

    use super::*;

    fn set_broker_rate(image: &mut MetadataImage, name: &str, value: Option<&str>) {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: NodeId(1),
            config_name: name.into(),
            config_value: value.map(str::to_owned),
        }));
    }

    fn image_with_leader_and_directory_rates() -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        set_broker_rate(&mut img, "leader.replication.throttled.rate", Some("2048"));
        set_broker_rate(
            &mut img,
            "replica.alter.log.dirs.io.max.bytes.per.second",
            Some("512"),
        );
        img
    }

    #[test]
    fn apply_image_sets_rates() {
        let mut img = image_with_leader_and_directory_rates();
        set_broker_rate(
            &mut img,
            "follower.replication.throttled.rate",
            Some("1024"),
        );
        let throttle = ThrottleState::new();
        apply_image(&img, NodeId(1), &throttle);
        assert!(throttle.leader_out.byte_rate() == bytes_per_sec(2048));
        assert!(throttle.follower_in.byte_rate() == bytes_per_sec(1024));
        assert!(throttle.alter_log_dirs.byte_rate() == bytes_per_sec(512));
    }

    #[test]
    fn apply_image_resets_to_zero_when_config_deleted() {
        let mut img = image_with_leader_and_directory_rates();
        let throttle = ThrottleState::new();
        apply_image(&img, NodeId(1), &throttle);
        assert!(throttle.leader_out.byte_rate() == bytes_per_sec(2048));
        // Delete the config.
        set_broker_rate(&mut img, "leader.replication.throttled.rate", None);
        set_broker_rate(
            &mut img,
            "replica.alter.log.dirs.io.max.bytes.per.second",
            None,
        );
        apply_image(&img, NodeId(1), &throttle);
        assert!(throttle.leader_out.byte_rate() == <ByteRate as ByteRateExt>::ZERO);
        assert!(throttle.alter_log_dirs.byte_rate() == <ByteRate as ByteRateExt>::ZERO);
    }
}
