//! KIP-392 replica selection. The partition leader runs `select` on every
//! consumer Fetch that carries a `client.rack` (`rack_id`) and reports the
//! chosen node id in `FetchResponse.preferred_read_replica`. Returning `-1`
//! means "no preference, read from the leader".

use std::time::Duration;

/// One replica's view as the leader sees it, for selection purposes.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaView {
    /// Wire replica id (broker node id as `i32`).
    pub node_id: i32,
    /// The broker's configured rack, if any.
    pub rack: Option<String>,
    /// Whether this replica is currently in the ISR.
    pub in_isr: bool,
    /// Whether this replica is a data-bearing witness. A witness replicates
    /// the partition and stays in the ISR, but it serves no client traffic.
    pub is_witness: bool,
    /// Kafka's `ReplicaView.logEndOffset`: the leader's own log end for the
    /// leader, and the last fetch offset the leader recorded for a follower.
    pub log_end_offset: i64,
    /// Kafka's `ReplicaView.timeSinceLastCaughtUpMs`: zero for the leader, and
    /// for a follower the time since its fetch offset last reached the
    /// leader's log end, [`Duration::MAX`] when it never has.
    pub time_since_caught_up: Duration,
}

impl ReplicaView {
    /// Kafka's `ReplicaView.comparator()`, the order of "most caught up":
    /// the higher log end offset, then the shorter time since last caught up,
    /// then the higher node id. The greatest key wins.
    fn caught_up_rank(&self) -> (i64, std::cmp::Reverse<Duration>, i32) {
        (
            self.log_end_offset,
            std::cmp::Reverse(self.time_since_caught_up),
            self.node_id,
        )
    }
}

/// Which built-in selector the broker uses. Maps to Kafka's
/// `replica.selector.class`, but as a native enum. Krabka does not load
/// JVM classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicaSelectorKind {
    /// Always read from the leader. Default.
    #[default]
    Leader,
    /// Prefer a same-rack in-sync replica when the client advertises a rack.
    RackAware,
}

impl ReplicaSelectorKind {
    /// Parse the `replica.selector` config value. Accepts `"leader"` and
    /// `"rack-aware"`. Returns `Err(value)` on anything else.
    /// # Errors
    /// Returns an error if the trimmed string is neither `leader` nor `rack-aware`.
    pub fn from_config_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "leader" => Ok(Self::Leader),
            "rack-aware" => Ok(Self::RackAware),
            other => Err(other.to_string()),
        }
    }

    /// Choose the preferred read replica. Returns a node id, or `-1` for
    /// "no preference, use the leader".
    ///
    /// The rack-aware rule is Kafka's `RackAwareReplicaSelector.select`,
    /// followed by `ReplicaManager.findPreferredReadReplica` dropping a leader
    /// it selected from the response: a leader in the client's rack is the
    /// answer, and no follower outranks it. Otherwise the most caught-up
    /// same-rack in-sync follower wins, by log end offset, then by the shorter
    /// time since it last caught up, then by the higher node id. No same-rack
    /// candidate is the leader as well.
    ///
    /// A witness is never a candidate. It replicates the partition and counts
    /// toward the ISR, but it serves no client traffic. The rule matters most
    /// where the witness looks most attractive: a consumer whose `client.rack`
    /// names the witness site sees an in-ISR same-rack replica there, and a
    /// redirect would send every read to a broker that answers none.
    pub(crate) fn select(
        self,
        client_rack: Option<&str>,
        leader_id: i32,
        replicas: &[ReplicaView],
    ) -> i32 {
        match self {
            Self::Leader => -1,
            Self::RackAware => {
                let Some(rack) = client_rack.filter(|r| !r.is_empty()) else {
                    return -1;
                };
                let same_rack = |replica: &&ReplicaView| replica.rack.as_deref() == Some(rack);
                if replicas
                    .iter()
                    .filter(same_rack)
                    .any(|replica| replica.node_id == leader_id)
                {
                    return -1;
                }
                replicas
                    .iter()
                    .filter(same_rack)
                    .filter(|replica| replica.in_isr && !replica.is_witness)
                    .max_by_key(|replica| replica.caught_up_rank())
                    .map_or(-1, |replica| replica.node_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    /// A follower in the ISR that has fetched to offset 100 and caught up a
    /// moment ago; the cases below change the fields they are about.
    fn view(node_id: i32, rack: &str, in_isr: bool) -> ReplicaView {
        ReplicaView {
            node_id,
            rack: Some(rack.to_string()),
            in_isr,
            is_witness: false,
            log_end_offset: 100,
            time_since_caught_up: Duration::ZERO,
        }
    }

    fn witness(node_id: i32, rack: &str, in_isr: bool) -> ReplicaView {
        ReplicaView {
            is_witness: true,
            ..view(node_id, rack, in_isr)
        }
    }

    fn behind(replica: ReplicaView, log_end_offset: i64, since_caught_up_ms: u64) -> ReplicaView {
        ReplicaView {
            log_end_offset,
            time_since_caught_up: Duration::from_millis(since_caught_up_ms),
            ..replica
        }
    }

    #[test]
    fn parse_known_values() {
        for (input, want) in [
            ("leader", ReplicaSelectorKind::Leader),
            ("rack-aware", ReplicaSelectorKind::RackAware),
        ] {
            assert!(
                ReplicaSelectorKind::from_config_str(input) == Ok(want),
                "{input}"
            );
        }
        assert!(ReplicaSelectorKind::from_config_str("bogus").is_err());
    }

    #[test]
    fn leader_kind_always_returns_minus_one() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert!(ReplicaSelectorKind::Leader.select(Some("b"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_picks_same_rack_isr_member() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        // leader is node 1 (rack a); client in rack b -> the same-rack ISR
        // member is node 2.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == 2);
    }

    #[test]
    fn rack_aware_none_when_client_rack_missing() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert!(ReplicaSelectorKind::RackAware.select(None, 1, &replicas) == -1);
        assert!(ReplicaSelectorKind::RackAware.select(Some(""), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_none_when_no_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "a", true)];
        assert!(ReplicaSelectorKind::RackAware.select(Some("z"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_ignores_non_isr_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "b", false)];
        // Node 2 is same-rack but out of ISR -> no redirect.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_never_redirects_a_consumer_to_a_witness() {
        // Node 1 leads in rack "a". The client rack is "b", the witness site,
        // so the witness there is an in-ISR same-rack replica and looks like
        // the best pick.
        for (name, replicas, want) in [
            (
                "witness is the only same-rack ISR member",
                vec![view(1, "a", true), witness(2, "b", true)],
                -1,
            ),
            (
                "a same-rack non-witness wins over a witness that is more caught up",
                vec![
                    view(1, "a", true),
                    witness(2, "b", true),
                    behind(view(3, "b", true), 50, 9_000),
                ],
                3,
            ),
        ] {
            let got = ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas);
            assert!(got == want, "{name}: got {got}, want {want}");
        }
    }

    /// Kafka's `RackAwareReplicaSelector` answers the leader when it is in the
    /// client's rack, and `findPreferredReadReplica` drops a selected leader:
    /// a same-rack follower never outranks it, whatever its id or progress.
    #[test]
    fn rack_aware_keeps_the_read_on_a_leader_in_the_clients_rack() {
        for (name, replicas, leader_id) in [
            (
                "a lower-id same-rack follower",
                vec![view(1, "b", true), view(2, "b", true)],
                2,
            ),
            (
                "a same-rack follower that is further ahead",
                vec![behind(view(1, "b", true), 500, 0), view(2, "b", true)],
                2,
            ),
            (
                "the only same-rack replica",
                vec![view(1, "b", true), view(2, "a", true)],
                1,
            ),
        ] {
            let got = ReplicaSelectorKind::RackAware.select(Some("b"), leader_id, &replicas);
            assert!(got == -1, "{name}: got {got}");
        }
    }

    /// Among several same-rack followers Kafka picks the most caught up: the
    /// highest log end offset, then the shortest time since it last caught up,
    /// then the highest node id.
    #[test]
    fn rack_aware_picks_the_most_caught_up_same_rack_follower() {
        // Node 1 leads in rack "a"; the client is in rack "b".
        for (name, followers, want) in [
            (
                "the higher log end offset wins over a lower id",
                vec![behind(view(2, "b", true), 90, 0), view(3, "b", true)],
                3,
            ),
            (
                "the higher log end offset wins over a lower lag time",
                vec![
                    behind(view(2, "b", true), 95, 5_000),
                    behind(view(3, "b", true), 90, 0),
                ],
                2,
            ),
            (
                "the shorter time since caught up breaks a tie in log end offset",
                vec![
                    behind(view(2, "b", true), 100, 5_000),
                    behind(view(3, "b", true), 100, 100),
                ],
                3,
            ),
            (
                "a follower that never caught up loses a tie in log end offset",
                vec![
                    ReplicaView {
                        time_since_caught_up: Duration::MAX,
                        ..view(2, "b", true)
                    },
                    behind(view(3, "b", true), 100, 60_000),
                ],
                3,
            ),
            (
                "the higher id breaks a full tie",
                vec![view(2, "b", true), view(4, "b", true), view(3, "b", true)],
                4,
            ),
            (
                "an out-of-sync follower is not a candidate however far ahead",
                vec![behind(view(2, "b", false), 999, 0), view(3, "b", true)],
                3,
            ),
        ] {
            let mut replicas = vec![view(1, "a", true)];
            replicas.extend(followers);
            let got = ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas);
            assert!(got == want, "{name}: got {got}, want {want}");
        }
    }
}
