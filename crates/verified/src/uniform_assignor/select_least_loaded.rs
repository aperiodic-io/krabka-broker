use {
    super::{HomogeneousMemberQuota, SubscriberLoad, UniformQuotaSplit},
    creusot_std::prelude::*,
};

/// Split `total_partitions` over `member_count` members, as
/// `UniformHomogeneousAssignmentBuilder.build` does.
///
/// The contract is the Euclidean division property, which names the floor
/// and the remainder uniquely: the quotas add up to the total, and fewer
/// members than there are members get the extra partition.
#[requires(member_count@ > 0)]
#[ensures(result.minimum_quota@ * member_count@ + result.extra_quotas@ == total_partitions@)]
#[ensures(result.extra_quotas@ < member_count@)]
#[must_use]
pub fn uniform_quota_split(total_partitions: usize, member_count: usize) -> UniformQuotaSplit {
    UniformQuotaSplit {
        minimum_quota: total_partitions / member_count,
        extra_quotas: total_partitions % member_count,
    }
}

/// Kafka's rule for the tentative extra partition in
/// `UniformHomogeneousAssignmentBuilder.maybeRevokePartitions`.
///
/// While extra partitions are left to hand out, a member keeps the extra slot
/// when it already owns more than the minimum. Otherwise it gives the slot up
/// for a later member, unless the members that are left, this one included,
/// are no more than the extra partitions that are left. Then it must take it.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn keeps_extra_quota(
    minimum_quota: Int,
    extras_left: Int,
    owned: Int,
    members_left: Int,
) -> bool {
    pearlite! { extras_left > 0 && (owned > minimum_quota || members_left <= extras_left) }
}

/// The extra partitions that are still unclaimed when the member at `index`
/// is visited. Members are visited in order, and each member that keeps the
/// extra slot claims one.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[variant(index)]
pub fn extras_left_before(
    minimum_quota: Int,
    extra_quotas: Int,
    owned: Seq<usize>,
    index: Int,
) -> Int {
    pearlite! {
        if index <= 0 {
            extra_quotas
        } else {
            extras_left_before(minimum_quota, extra_quotas, owned, index - 1)
                - (if keeps_extra_quota(
                    minimum_quota,
                    extras_left_before(minimum_quota, extra_quotas, owned, index - 1),
                    owned[index - 1]@,
                    owned.len() - (index - 1),
                ) { 1 } else { 0 })
        }
    }
}

/// The partition target for a member: the minimum, plus one when it keeps
/// the extra slot.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn member_target(minimum_quota: Int, quota: HomogeneousMemberQuota) -> Int {
    pearlite! { minimum_quota + (if quota.takes_extra { 1 } else { 0 }) }
}

/// Every member's quota in the homogeneous builder, in member order.
///
/// `owned[i]` is how many partitions of the subscribed topics member `i`
/// holds in its current target assignment. The contract states Kafka's
/// balance-then-stickiness rule:
///
/// - Balance: each member's target is `minimum_quota` or `minimum_quota + 1`,
///   and exactly `extra_quotas` members get the larger target, because no
///   extra partition is left unclaimed after the last member.
/// - Stickiness: each member keeps as much of its current ownership as its
///   target allows, and receives only the difference.
/// - The extra slots go by `keeps_extra_quota`, in member order.
#[requires(extra_quotas@ <= owned@.len())]
#[requires(minimum_quota@ < usize::MAX@)]
#[ensures(result@.len() == owned@.len())]
#[ensures(forall<i: Int> 0 <= i && i < owned@.len() ==> result@[i].takes_extra == keeps_extra_quota(
    minimum_quota@,
    extras_left_before(minimum_quota@, extra_quotas@, owned@, i),
    owned@[i]@,
    owned@.len() - i,
))]
#[ensures(forall<i: Int> 0 <= i && i < owned@.len()
    ==> result@[i].retain@ + result@[i].fill@ == member_target(minimum_quota@, result@[i]))]
#[ensures(forall<i: Int> 0 <= i && i < owned@.len()
    ==> result@[i].retain@ == (if owned@[i]@ < member_target(minimum_quota@, result@[i]) {
        owned@[i]@
    } else {
        member_target(minimum_quota@, result@[i])
    }))]
#[ensures(extras_left_before(minimum_quota@, extra_quotas@, owned@, owned@.len()) == 0)]
#[must_use]
pub fn homogeneous_member_quotas(
    minimum_quota: usize,
    extra_quotas: usize,
    owned: &[usize],
) -> Vec<HomogeneousMemberQuota> {
    let mut quotas: Vec<HomogeneousMemberQuota> = Vec::new();
    let mut extras_left = extra_quotas;
    let mut i = 0usize;
    #[invariant(i@ <= owned@.len())]
    #[invariant(quotas@.len() == i@)]
    #[invariant(extras_left@ == extras_left_before(minimum_quota@, extra_quotas@, owned@, i@))]
    #[invariant(extras_left@ <= owned@.len() - i@)]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==> quotas@[k].takes_extra == keeps_extra_quota(
        minimum_quota@,
        extras_left_before(minimum_quota@, extra_quotas@, owned@, k),
        owned@[k]@,
        owned@.len() - k,
    ))]
    #[invariant(forall<k: Int> 0 <= k && k < i@
        ==> quotas@[k].retain@ + quotas@[k].fill@ == member_target(minimum_quota@, quotas@[k]))]
    #[invariant(forall<k: Int> 0 <= k && k < i@
        ==> quotas@[k].retain@ == (if owned@[k]@ < member_target(minimum_quota@, quotas@[k]) {
            owned@[k]@
        } else {
            member_target(minimum_quota@, quotas@[k])
        }))]
    #[variant(owned@.len() - i@)]
    while i < owned.len() {
        let members_left = owned.len() - i;
        let takes_extra =
            extras_left > 0 && (owned[i] > minimum_quota || members_left <= extras_left);
        let target = if takes_extra {
            minimum_quota + 1
        } else {
            minimum_quota
        };
        let retain = if owned[i] < target { owned[i] } else { target };
        quotas.push(HomogeneousMemberQuota {
            takes_extra,
            retain,
            fill: target - retain,
        });
        if takes_extra {
            extras_left -= 1;
        }
        i += 1;
    }
    quotas
}

/// `a` goes before `b` in the heterogeneous builder's least-loaded order: the
/// smaller current load first, then the smaller load at the start of the
/// topic. The candidate position breaks the remaining ties.
///
/// `MemberAssignmentBalancer.nextLeastLoadedMember` sorts the subscribers once
/// per topic by `(load, member index)` and then fills them level by level in
/// that order. Among the members at the lowest current level, the next one it
/// picks is therefore the first by starting load, then by member index.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
pub fn load_precedes(a: SubscriberLoad, b: SubscriberLoad) -> bool {
    pearlite! {
        a.assigned@ < b.assigned@
            || (a.assigned@ == b.assigned@ && a.assigned_at_topic_start@ < b.assigned_at_topic_start@)
    }
}

/// Choose the subscriber that receives the next unassigned partition of a
/// topic in `UniformHeterogeneousAssignmentBuilder.assignRemainingPartitions`.
///
/// The host passes the topic's subscribers in ascending member order. The
/// result is `None` exactly for an empty slice. Otherwise no candidate
/// precedes the chosen one, and the chosen one strictly precedes every
/// earlier candidate, so the first of the least-loaded candidates wins.
#[ensures((result == None) == (candidates@.len() == 0))]
#[ensures(forall<chosen: usize> result == Some(chosen) ==> chosen@ < candidates@.len()
    && (forall<j: Int> 0 <= j && j < candidates@.len()
        ==> !load_precedes(candidates@[j], candidates@[chosen@]))
    && (forall<j: Int> 0 <= j && j < chosen@
        ==> load_precedes(candidates@[chosen@], candidates@[j])))]
#[must_use]
pub fn select_least_loaded(candidates: &[SubscriberLoad]) -> Option<usize> {
    let count = candidates.len();
    if count == 0 {
        return None;
    }
    let mut best = 0usize;
    let mut i = 1usize;
    #[invariant(1 <= i@ && i@ <= candidates@.len())]
    #[invariant(best@ < i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@
        ==> !load_precedes(candidates@[j], candidates@[best@]))]
    #[invariant(forall<j: Int> 0 <= j && j < best@
        ==> load_precedes(candidates@[best@], candidates@[j]))]
    #[variant(candidates@.len() - i@)]
    while i < count {
        let candidate = candidates[i];
        let current = candidates[best];
        if candidate.assigned < current.assigned
            || (candidate.assigned == current.assigned
                && candidate.assigned_at_topic_start < current.assigned_at_topic_start)
        {
            best = i;
        }
        i += 1;
    }
    Some(best)
}
