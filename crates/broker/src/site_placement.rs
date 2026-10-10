//! Replica placement for a stretch cluster. A "site" is the `broker.rack`
//! value of a broker, so two brokers with the same rack are in the same site.
//!
//! The placement gives two properties:
//!
//! - **Site spread.** The replicas of one partition go in different sites. The
//!   loss of a full site then costs one replica, and a producer with
//!   `acks=all` and `min.insync.replicas=2` continues to commit.
//! - **Leadership pinning.** In Kafka, `replicas[0]` *is* the preferred leader
//!   of the partition. The order of the replica list is thus the full
//!   mechanism that pins leadership to a site. This module puts a broker of
//!   the preferred site first. The KIP-460 auto-rebalance in
//!   [`crate::leader_rebalance`] and `kafka-leader-election --election-type
//!   preferred` then keep the leader there. A leader in the site of the
//!   producers removes one inter-site trip from the write path.
//!
//! A witness is a data-bearing node in a third, cheap site. It replicates the
//! partition data and it votes in `KRaft`, but it does not serve clients. Thus
//! a witness is a replica, but it is never `replicas[0]`.
//!
//! The module is pure. It does no I/O and it reads no clock. It sorts the
//! brokers by node id and never iterates a hash container, so the only source
//! of variation is the [`PlacementRng`] that the caller hands in. Like Kafka's
//! `StripedReplicaPlacer`, the placement starts at a random site and at a
//! random broker inside each site, and it advances by one per partition. A
//! fixed seed makes the placement reproducible, and the tests rely on that.
//!
//! A fenced broker is a last resort, as in `StripedReplicaPlacer`: a site
//! offers its unfenced brokers first, no fenced broker leads a partition, and
//! the placement refuses only when the replication factor exceeds the number
//! of brokers it got, fenced ones included, or when none of them is unfenced.
//!
//! Like the WAL voter placement in `crate::wal::quorum::placement`, this code
//! fails closed. When it cannot satisfy the site guarantee, it returns an
//! empty outer vec, and the caller reports `INVALID_REPLICATION_FACTOR`.

use krabka_raft::NodeId;

/// One broker as the placement code sees it.
#[derive(Debug, Clone)]
pub(crate) struct SiteBrokerView {
    /// The node id of the broker.
    pub node_id: NodeId,
    /// The broker's configured rack, which is its site. `None` means the
    /// broker declared no site.
    pub site: Option<String>,
    /// A witness replicates data but never leads, so it is never `replicas[0]`.
    pub is_witness: bool,
    /// A fenced broker takes a replica only after the unfenced brokers of its
    /// site run out, and it never takes `replicas[0]`.
    pub fenced: bool,
}

krabka_macros::splitmix64_step!(placement_random_step);

/// The random source of the placement, a `SplitMix64` generator.
///
/// A handler passes [`PlacementRng::from_entropy`] to [`stretch_replicas`], as
/// Kafka's controller builds its placer with `new Random()`. A test passes
/// [`PlacementRng::seeded`] and gets the same placement on every run.
#[derive(Debug, Clone)]
pub(crate) struct PlacementRng(u64);

impl PlacementRng {
    /// A generator that repeats one sequence for one `seed`.
    pub(crate) fn seeded(seed: u64) -> Self {
        Self(seed)
    }

    /// A generator seeded from the hash randomness of the process.
    pub(crate) fn from_entropy() -> Self {
        use std::hash::BuildHasher;
        Self::seeded(std::collections::hash_map::RandomState::new().hash_one(0_u8))
    }

    fn next_u64(&mut self) -> u64 {
        placement_random_step(&mut self.0)
    }

    /// A value in `0..bound`, or 0 when `bound` is 0.
    fn below(&mut self, bound: usize) -> usize {
        let bound = u64::try_from(bound).unwrap_or(u64::MAX).max(1);
        usize::try_from(self.next_u64() % bound).unwrap_or(0)
    }

    /// Java's `Collections.shuffle`: a Fisher-Yates pass over `items`.
    fn shuffle<T>(&mut self, items: &mut [T]) {
        for upper in (1..items.len()).rev() {
            let pick = self.below(upper + 1);
            items.swap(upper, pick);
        }
    }
}

/// One placement decision: a broker and the site that holds it. Both values
/// are indexes, into the node-id-sorted broker slice and into the site table.
#[derive(Debug, Clone, Copy)]
struct Placed {
    site: usize,
    broker: usize,
}

/// The brokers of the cluster, grouped by site.
struct SiteTable {
    /// One entry per site, in the order of the first broker of that site.
    /// Each entry holds the indexes of the brokers of the site, in node-id
    /// order.
    sites: Vec<Vec<usize>>,
    /// The site of each broker, in node-id order. `None` marks a broker that
    /// declared no site.
    site_of: Vec<Option<usize>>,
    /// The random offset at which the site rotation starts.
    site_start: usize,
    /// The random offset at which the brokers of each site start, one entry
    /// per site.
    broker_start: Vec<usize>,
}

impl SiteTable {
    /// Groups the sorted brokers by site and draws the random starts.
    fn new(sorted: &[&SiteBrokerView], place_unracked: bool, rng: &mut PlacementRng) -> Self {
        let mut names: Vec<&str> = Vec::new();
        let mut sites: Vec<Vec<usize>> = Vec::new();
        let mut site_of: Vec<Option<usize>> = Vec::new();
        for (index, broker) in sorted.iter().enumerate() {
            let name = match broker.site.as_deref() {
                Some(name) => name,
                None if place_unracked => "",
                None => {
                    site_of.push(None);
                    continue;
                }
            };
            let known = names.iter().position(|candidate| *candidate == name);
            let site = if let Some(site) = known {
                site
            } else {
                names.push(name);
                sites.push(Vec::new());
                sites.len() - 1
            };
            sites[site].push(index);
            site_of.push(Some(site));
        }
        let broker_start = sites.iter().map(|site| rng.below(site.len())).collect();
        let site_start = rng.below(sites.len());
        Self {
            sites,
            site_of,
            site_start,
            broker_start,
        }
    }
}

/// Places the replicas of `num_partitions` partitions across the sites of
/// `brokers`.
///
/// The result holds one replica list per partition, and `replicas[0]` of each
/// list is the preferred leader of that partition. The rules, in priority
/// order:
///
/// 1. When no broker declares a site, the cluster is not a stretch cluster.
///    The result is then the plain Kafka placement, [`striped_replicas`] over
///    the same brokers. A cluster without a site has no witness site either,
///    so this rule also ignores `is_witness` and `preferred_site`.
/// 2. In every other cluster, the replicas of a partition go in different
///    sites. The rotation of the sites starts at a random site and advances
///    with the partition index, so the partitions spread over the sites.
///    Inside a site, the brokers also rotate from a random start.
/// 3. A `replication_factor` above the site count takes a second broker from
///    a site. The site that holds the fewest replicas of that partition comes
///    first.
/// 4. `replicas[0]` is an unfenced, non-witness broker of `preferred_site`.
///    When the preferred site has no such broker, or when `preferred_site` is
///    `None`, `replicas[0]` is any unfenced, non-witness broker.
/// 5. A witness is never `replicas[0]`, and neither is a fenced broker.
/// 6. A site gives up its unfenced brokers before its fenced ones.
///
/// The result is an empty outer vec, which makes the caller report
/// `INVALID_REPLICATION_FACTOR`, when the request is impossible:
///
/// - `replication_factor` is 0, or it is more than the broker count, fenced
///   brokers included.
/// - Every broker is fenced. [`placement_failure_reason`] words this and the
///   previous case as Kafka does.
/// - Every unfenced broker is a witness, thus no broker can lead. This is a
///   degenerate cluster.
/// - The sites do not hold enough brokers for one partition. A broker that
///   declared no site is not placeable, because the code cannot show that it
///   is in a different site from another such broker.
pub(crate) fn stretch_replicas(
    brokers: &[SiteBrokerView],
    num_partitions: i32,
    replication_factor: i16,
    preferred_site: Option<&str>,
    rng: &mut PlacementRng,
) -> Vec<Vec<NodeId>> {
    let replicas_per_partition = usize::try_from(replication_factor).unwrap_or(0);
    if replicas_per_partition == 0
        || replicas_per_partition > brokers.len()
        || brokers.iter().all(|broker| broker.fenced)
    {
        return Vec::new();
    }
    let partition_count = usize::try_from(num_partitions).unwrap_or(0);

    let mut sorted = brokers.iter().collect::<Vec<_>>();
    sorted.sort_by_key(|broker| broker.node_id);

    if sorted.iter().all(|broker| broker.site.is_none()) {
        return striped_replicas(&sorted, partition_count, replicas_per_partition, rng);
    }

    // Kafka treats every broker without `broker.rack` as a member of one
    // anonymous rack in an ordinary mixed-rack cluster. An explicit stretch
    // profile stays fail-closed: its preferred-site marker means an unracked
    // broker cannot satisfy the declared site topology.
    let table = SiteTable::new(&sorted, preferred_site.is_none(), rng);
    let mut leaders = leader_candidates(&sorted, &table, preferred_site);
    let leader_start = rng.below(leaders.len());
    leaders.rotate_left(leader_start);
    (0..partition_count)
        .map(|partition| {
            partition_replicas(&sorted, &table, &leaders, partition, replicas_per_partition)
        })
        .collect::<Option<Vec<_>>>()
        .unwrap_or_default()
}

/// One list of brokers that [`striped_replicas`] walks: Kafka's
/// `StripedReplicaPlacer.BrokerList`.
struct BrokerList {
    /// The brokers, in node-id order until a shuffle mixes them.
    brokers: Vec<NodeId>,
    /// How many brokers `next` has returned in the current epoch.
    index: usize,
    /// The offset added to `index` to find the broker to return.
    offset: usize,
    /// The epoch that `index` and `offset` belong to.
    epoch: usize,
}

impl BrokerList {
    /// A list over `brokers`, which arrive sorted, with a random start.
    fn new(brokers: Vec<NodeId>, rng: &mut PlacementRng) -> Self {
        let offset = rng.below(brokers.len());
        Self {
            brokers,
            index: 0,
            offset,
            epoch: 0,
        }
    }

    /// The next broker of `epoch`, or `None` once the list ran through in it.
    /// A new epoch starts the walk again, one broker further on.
    fn next_broker(&mut self, epoch: usize) -> Option<NodeId> {
        let len = self.brokers.len();
        if len == 0 {
            return None;
        }
        if self.epoch != epoch {
            self.epoch = epoch;
            self.index = 0;
            self.offset = (self.offset + 1) % len;
        }
        if self.index >= len {
            return None;
        }
        let broker = self.brokers[(self.index + self.offset) % len];
        self.index += 1;
        Some(broker)
    }
}

/// Kafka's `StripedReplicaPlacer` for a cluster without racks: one rack that
/// holds every broker, in an unfenced list and a fenced list.
///
/// A partition takes its first replica from the unfenced list, and the others
/// from the unfenced list too until it runs out, then from the fenced list.
/// The first broker is random, and each partition starts one broker further
/// on than the last. Once the partitions have gone round every unfenced
/// broker, both lists shuffle, so the next round does not repeat the same
/// replica lists.
///
/// `sorted` is in node-id order and holds at least `replicas` brokers and one
/// unfenced broker, which [`stretch_replicas`] checks.
fn striped_replicas(
    sorted: &[&SiteBrokerView],
    partitions: usize,
    replicas: usize,
    rng: &mut PlacementRng,
) -> Vec<Vec<NodeId>> {
    let ids = |fenced: bool| {
        sorted
            .iter()
            .filter(|broker| broker.fenced == fenced)
            .map(|broker| broker.node_id)
            .collect::<Vec<_>>()
    };
    let mut fenced = BrokerList::new(ids(true), rng);
    let mut unfenced = BrokerList::new(ids(false), rng);
    let unfenced_count = unfenced.brokers.len();
    let mut epoch = 0;
    (0..partitions)
        .map(|_| {
            if epoch == unfenced_count && unfenced_count > 1 {
                rng.shuffle(&mut fenced.brokers);
                rng.shuffle(&mut unfenced.brokers);
                epoch = 0;
            }
            let current = epoch;
            epoch += 1;
            let leader = unfenced.next_broker(current);
            let others = std::iter::from_fn(|| {
                unfenced
                    .next_broker(current)
                    .or_else(|| fenced.next_broker(current))
            });
            leader.into_iter().chain(others).take(replicas).collect()
        })
        .collect()
}

/// The brokers that can take `replicas[0]`.
///
/// A witness never leads, a fenced broker never leads, and a broker without a
/// site is not placeable, so none of them is a candidate. The result holds only
/// the brokers of `preferred_site` when that site has at least one candidate.
/// Otherwise it holds every candidate of the cluster.
///
/// The list interleaves the sites: it takes the first candidate of every
/// site, then the second candidate of every site, and so on. Because the
/// leader of a partition comes from this list at the partition index, a topic
/// with fewer partitions than brokers still leads in every site.
fn leader_candidates(
    sorted: &[&SiteBrokerView],
    table: &SiteTable,
    preferred_site: Option<&str>,
) -> Vec<Placed> {
    let mut per_site = vec![Vec::new(); table.sites.len()];
    for (broker, _) in sorted
        .iter()
        .enumerate()
        .filter(|(_, broker)| !broker.is_witness && !broker.fenced)
    {
        if let Some(site) = table.site_of[broker] {
            per_site[site].push(Placed { site, broker });
        }
    }
    let deepest = per_site.iter().map(Vec::len).max().unwrap_or(0);
    let candidates = (0..deepest)
        .flat_map(|rank| per_site.iter().filter_map(move |site| site.get(rank)))
        .copied()
        .collect::<Vec<_>>();
    let Some(preferred) = preferred_site else {
        return candidates;
    };
    let in_preferred = candidates
        .iter()
        .copied()
        .filter(|placed| sorted[placed.broker].site.as_deref() == Some(preferred))
        .collect::<Vec<_>>();
    if in_preferred.is_empty() {
        candidates
    } else {
        in_preferred
    }
}

/// Selects the replicas of one partition, leader first.
///
/// Returns `None` when the cluster cannot hold the partition: no broker can
/// lead it, or the sites do not hold enough brokers.
fn partition_replicas(
    sorted: &[&SiteBrokerView],
    table: &SiteTable,
    leaders: &[Placed],
    partition: usize,
    replicas_per_partition: usize,
) -> Option<Vec<NodeId>> {
    if leaders.is_empty() {
        return None;
    }
    // The leader rotates with the partition index, so the partitions of a
    // topic do not all lead on one broker of the preferred site.
    let leader = leaders[partition % leaders.len()];
    let mut chosen = vec![leader];
    let mut load = vec![0_usize; table.sites.len()];
    load[leader.site] += 1;
    while chosen.len() < replicas_per_partition {
        let follower = next_follower(sorted, table, &load, &chosen, partition)?;
        load[follower.site] += 1;
        chosen.push(follower);
    }
    Some(
        chosen
            .into_iter()
            .map(|placed| sorted[placed.broker].node_id)
            .collect(),
    )
}

/// The next replica of a partition, from the site that holds the fewest
/// replicas of it. Returns `None` when every site is exhausted.
fn next_follower(
    sorted: &[&SiteBrokerView],
    table: &SiteTable,
    load: &[usize],
    chosen: &[Placed],
    partition: usize,
) -> Option<Placed> {
    let site_count = table.sites.len();
    // The rotation starts at a random site and advances with the partition
    // index, so consecutive partitions do not stack on one site.
    // `min_by_key` keeps the first minimum, thus the rotation is also the
    // tie-break between two sites that hold the same number of replicas.
    (0..site_count)
        .map(|step| (table.site_start + partition + step) % site_count)
        .filter_map(|site| {
            free_broker(sorted, table, site, partition, chosen)
                .map(|broker| Placed { site, broker })
        })
        .min_by_key(|placed| load[placed.site])
}

/// The first broker of `site` that this partition does not use yet, an
/// unfenced one before a fenced one. The scan starts at the random offset of
/// the site and advances with the partition index, so the partitions spread
/// over the brokers of the site.
fn free_broker(
    sorted: &[&SiteBrokerView],
    table: &SiteTable,
    site: usize,
    partition: usize,
    chosen: &[Placed],
) -> Option<usize> {
    let brokers = &table.sites[site];
    let start = table.broker_start[site] + partition;
    let scan = |fenced: bool| {
        (0..brokers.len())
            .map(|step| brokers[(start + step) % brokers.len()])
            .find(|broker| {
                sorted[*broker].fenced == fenced
                    && !chosen.iter().any(|placed| placed.broker == *broker)
            })
    };
    scan(false).or_else(|| scan(true))
}

/// Why a placement of `replication_factor` replicas on `brokers` cannot be
/// met, in the words of Kafka's `StripedReplicaPlacer`. `brokers` holds every
/// broker that the placement got, fenced ones included.
///
/// Kafka checks the unfenced count first, so an empty or all-fenced cluster
/// reads "All brokers are currently fenced" whatever the replication factor.
pub(crate) fn placement_failure_reason(
    replication_factor: i16,
    brokers: &[SiteBrokerView],
) -> String {
    if brokers.iter().all(|broker| broker.fenced) {
        "All brokers are currently fenced, or have all their log directories cordoned.".to_owned()
    } else {
        format!(
            "The target replication factor of {replication_factor} cannot be reached because \
             only {} broker(s) are registered or some brokers have all their log directories \
             cordoned.",
            brokers.len()
        )
    }
}

#[cfg(test)]
mod tests;
