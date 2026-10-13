use {super::WalFetchAdmission, creusot_std::prelude::*};

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open(crate))]
pub(crate) fn wal_fetch_authorized(
    authenticated_node: Option<u64>,
    claimed_node: u64,
    local_node: u64,
    voters: Seq<u64>,
) -> bool {
    pearlite! {
        authenticated_node == Some(claimed_node)
            && voters.len() > 0
            && voters[0] == local_node
            && exists<i: Int> 0 <= i && i < voters.len() && voters[i] == claimed_node
    }
}

/// Whether `value` occurs in `values`; used for voter ids and rack ids alike.
#[ensures(result == (exists<i: Int>
    0 <= i && i < values@.len() && values@[i] == value))]
pub(super) fn contains(values: &[u64], value: u64) -> bool {
    let mut i = 0;
    #[cfg_attr(creusot, invariant(i@ <= values@.len()))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@ ==> values@[k] != value))]
    #[cfg_attr(creusot, variant(values@.len() - i@))]
    while i < values.len() {
        if values[i] == value {
            return true;
        }
        i += 1;
    }
    false
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A `(node, rack)` candidate may not become a WAL voter when its node or its
/// rack is already used, or when only the local broker is eligible and the
/// candidate is another broker.
fn voter_blocked(
    candidate: (u64, u64),
    used_nodes: Seq<u64>,
    used_racks: Seq<u64>,
    local_node: u64,
    require_local: bool,
) -> bool {
    pearlite! {
        (exists<j: Int> 0 <= j && j < used_nodes.len() && used_nodes[j] == candidate.0)
            || (exists<j: Int> 0 <= j && j < used_racks.len() && used_racks[j] == candidate.1)
            || (require_local && candidate.0 != local_node)
    }
}

/// Select the first candidate, in the caller's order, whose node and rack are
/// both unused. When `require_local` is set, only the local broker is
/// eligible.
#[ensures(match result {
    None => forall<i: Int> 0 <= i && i < candidates@.len() ==>
        voter_blocked(candidates@[i], used_nodes@, used_racks@, local_node, require_local),
    Some(index) => index@ < candidates@.len()
        && !voter_blocked(candidates@[index@], used_nodes@, used_racks@, local_node, require_local)
        && forall<i: Int> 0 <= i && i < index@ ==>
            voter_blocked(candidates@[i], used_nodes@, used_racks@, local_node, require_local),
})]
#[must_use]
pub fn select_wal_voter_index(
    candidates: &[(u64, u64)],
    used_nodes: &[u64],
    used_racks: &[u64],
    local_node: u64,
    require_local: bool,
) -> Option<usize> {
    let mut i = 0usize;
    #[invariant(i@ <= candidates@.len())]
    #[invariant(forall<k: Int> 0 <= k && k < i@ ==>
        voter_blocked(candidates@[k], used_nodes@, used_racks@, local_node, require_local))]
    #[variant(candidates@.len() - i@)]
    while i < candidates.len() {
        let candidate = candidates[i];
        if !contains(used_nodes, candidate.0)
            && !contains(used_racks, candidate.1)
            && (!require_local || candidate.0 == local_node)
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

/// Admit exactly a complete, nonempty, local-first set of distinct voter IDs.
/// A repeated ID cannot vote twice through the same durable-offset map entry.
#[ensures(result == (expected@ > 0 && voters@.len() == expected@
    && voters@[0] == local_node
    && crate::sequence::distinct(voters@)))]
#[must_use]
pub fn wal_voter_set_valid(voters: &[u64], local_node: u64, expected: usize) -> bool {
    if expected == 0 || voters.len() != expected || voters[0] != local_node {
        return false;
    }
    let mut i = 0usize;
    #[invariant(i@ <= voters@.len())]
    #[invariant(forall<j: Int, k: Int> 0 <= j && j < k && k < i@ ==> voters@[j] != voters@[k])]
    #[variant(voters@.len() - i@)]
    while i < voters.len() {
        if contains(&voters[..i], voters[i]) {
            return false;
        }
        i += 1;
    }
    true
}

/// Authorize a diskless WAL Fetch and classify its leader epoch.
///
/// Authorization deliberately precedes epoch classification, so an
/// unauthenticated caller learns no placement epoch.
#[ensures((result == WalFetchAdmission::Denied)
    == !wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@))]
#[ensures((result == WalFetchAdmission::FencedLeaderEpoch)
    == (wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@)
        && request_epoch@ >= 0 && request_epoch@ < leader_epoch@))]
#[ensures((result == WalFetchAdmission::UnknownLeaderEpoch)
    == (wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@)
        && request_epoch@ >= 0 && request_epoch@ > leader_epoch@))]
#[ensures((result == WalFetchAdmission::Serve)
    == (wal_fetch_authorized(authenticated_node, claimed_node, local_node, voters@)
        && (request_epoch@ < 0 || request_epoch@ == leader_epoch@)))]
#[must_use]
pub fn wal_fetch_admission(
    authenticated_node: Option<u64>,
    claimed_node: u64,
    local_node: u64,
    voters: &[u64],
    request_epoch: i32,
    leader_epoch: i32,
) -> WalFetchAdmission {
    if authenticated_node != Some(claimed_node)
        || voters.first() != Some(&local_node)
        || !contains(voters, claimed_node)
    {
        return WalFetchAdmission::Denied;
    }
    if request_epoch < 0 || request_epoch == leader_epoch {
        return WalFetchAdmission::Serve;
    }
    if request_epoch < leader_epoch {
        WalFetchAdmission::FencedLeaderEpoch
    } else {
        WalFetchAdmission::UnknownLeaderEpoch
    }
}

/// Assemble the existing greedy, local-first placement across distinct nodes
/// and racks. An incomplete result must be rejected by the quorum installer.
#[ensures(result@.len() <= requested@)]
#[ensures((result@.len() == 0) == (requested@ == 0
    || forall<i: Int> 0 <= i && i < candidates@.len() ==> candidates@[i].0 != local_node))]
#[ensures(result@.len() > 0 ==> result@[0].0 == local_node)]
#[ensures(forall<i: Int> 0 <= i && i < result@.len()
    ==> exists<j: Int> 0 <= j && j < candidates@.len() && result@[i] == candidates@[j])]
#[ensures(crate::wal::placement_identities_distinct(result@))]
#[ensures(0 < result@.len() && result@.len() < requested@ ==>
    forall<i: Int> 0 <= i && i < candidates@.len() ==>
        exists<j: Int> 0 <= j && j < result@.len()
            && (candidates@[i].0 == result@[j].0 || candidates@[i].1 == result@[j].1))]
#[must_use]
pub fn select_wal_voters(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
) -> Vec<(u64, u64)> {
    let mut selected: Vec<(u64, u64)> = Vec::new();
    let mut nodes: Vec<u64> = Vec::new();
    let mut racks: Vec<u64> = Vec::new();
    #[invariant(selected@.len() <= requested@)]
    #[invariant(nodes@.len() == selected@.len() && racks@.len() == selected@.len())]
    #[invariant(selected@.len() > 0 ==> selected@[0].0 == local_node)]
    #[invariant(forall<i: Int> 0 <= i && i < selected@.len()
        ==> selected@[i].0 == nodes@[i] && selected@[i].1 == racks@[i])]
    #[invariant(forall<i: Int> 0 <= i && i < selected@.len()
        ==> exists<j: Int> 0 <= j && j < candidates@.len() && selected@[i] == candidates@[j])]
    #[invariant(crate::wal::placement_identities_distinct(selected@))]
    #[variant(requested@ - selected@.len())]
    while selected.len() < requested {
        let Some(index) = select_wal_voter_index(
            candidates,
            &nodes,
            &racks,
            local_node,
            matches!(selected.len(), 0),
        ) else {
            return selected;
        };
        let candidate = candidates[index];
        selected.push(candidate);
        nodes.push(candidate.0);
        racks.push(candidate.1);
    }
    selected
}
