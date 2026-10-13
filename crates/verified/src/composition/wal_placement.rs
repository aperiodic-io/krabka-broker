use {
    super::{election_has_quorum, select_wal_voters, wal_voter_set_valid},
    creusot_std::prelude::*,
};

type InstalledPlacement = (Vec<(u64, u64)>, Vec<u64>, bool);
type RackLossPlacement = (Vec<(u64, u64)>, Vec<u64>, bool, bool);

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic(open)]
/// A maximal local-first placement uses distinct nodes and racks from its input.
fn placement_valid(
    candidates: Seq<(u64, u64)>,
    selected: Seq<(u64, u64)>,
    local: u64,
    requested: Int,
) -> bool {
    pearlite! {
        selected.len() <= requested
            && ((selected.len() == 0) == (requested == 0 || forall<i: Int> 0 <= i && i < candidates.len() ==> candidates[i].0 != local))
            && (selected.len() > 0 ==> selected[0].0 == local)
            && (forall<i: Int> 0 <= i && i < selected.len() ==> exists<j: Int> 0 <= j && j < candidates.len() && selected[i] == candidates[j])
            && (crate::wal::placement_identities_distinct(selected))
            && (0 < selected.len() && selected.len() < requested ==> forall<i: Int> 0 <= i && i < candidates.len() ==> exists<j: Int> 0 <= j && j < selected.len() && (candidates[i].0 == selected[j].0 || candidates[i].1 == selected[j].1))
    }
}

/// Return the actual placement, projected node IDs and exact installer admission.
/// Greedy placement is maximal, not globally maximum on conflicting metadata.
/// Faithful node/rack projection and durable membership transitions are external.
#[ensures(placement_valid(candidates@, result.0@, local_node, requested@))]
#[ensures(result.1@.len() == result.0@.len()
    && forall<i: Int> 0 <= i && i < result.1@.len() ==> result.1@[i] == result.0@[i].0)]
#[ensures(result.2 == (requested@ > 0 && result.0@.len() == requested@))]
#[ensures(result.2 == (requested@ > 0 && result.1@.len() == requested@
    && result.1@[0] == local_node
    && crate::sequence::distinct(result.1@)))]
pub(super) fn constructed_wal_placement_is_installable(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
) -> InstalledPlacement {
    let selected = select_wal_voters(candidates, local_node, requested);
    let mut nodes: Vec<u64> = Vec::new();
    let mut i = 0usize;
    #[invariant(i@ <= selected@.len() && nodes@.len() == i@)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ ==> nodes@[j] == selected@[j].0)]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        nodes.push(selected[i].0);
        i += 1;
    }
    let installed = wal_voter_set_valid(&nodes, local_node, requested);
    (selected, nodes, installed)
}

/// Consume installer admission and return every surviving voter in placement
/// order. An installed placement retains its original majority after any one
/// configured rack disappears. The last flag requires installation, so an
/// incomplete configuration never exports an available installed quorum.
/// Physical rack identity, communication and fsync remain host obligations.
#[requires(requested@ >= 3)]
#[ensures(placement_valid(candidates@, result.0@, local_node, requested@))]
#[ensures(result.2 == (result.0@.len() == requested@) && result.3 == result.2)]
#[ensures(result.0@.len() - 1 <= result.1@.len() && result.1@.len() <= result.0@.len())]
#[ensures(forall<i: Int> 0 <= i && i < result.1@.len()
    ==> exists<j: Int> 0 <= j && j < result.0@.len()
        && result.1@[i] == result.0@[j].0 && result.0@[j].1 != failed_rack)]
#[ensures(forall<i: Int> 0 <= i && i < result.0@.len() && result.0@[i].1 != failed_rack
    ==> exists<j: Int> 0 <= j && j < result.1@.len() && result.1@[j] == result.0@[i].0)]
#[ensures(crate::sequence::distinct(result.1@))]
#[ensures(forall<i: Int, j: Int, a: Int, b: Int>
    0 <= i && i < j && j < result.1@.len() && 0 <= a && a < result.0@.len()
        && 0 <= b && b < result.0@.len()
        && result.1@[i] == result.0@[a].0 && result.1@[j] == result.0@[b].0 ==> a < b)]
#[ensures(result.3 ==> result.1@.len() >= requested@ / 2 + 1)]
pub(super) fn wal_placement_survives_one_rack_loss(
    candidates: &[(u64, u64)],
    local_node: u64,
    requested: usize,
    failed_rack: u64,
) -> RackLossPlacement {
    let (selected, nodes, installed) =
        constructed_wal_placement_is_installable(candidates, local_node, requested);
    // Losing one rack can remove at most one selected node.
    proof_assert!(crate::wal::placement_identities_distinct(selected@));
    let mut survivors: Vec<u64> = Vec::new();
    #[cfg(creusot)]
    let mut removed: Option<usize> = None;
    let mut i = 0usize;
    #[invariant(i@ <= selected@.len())]
    #[invariant(survivors@.len() + (if removed == None { 0 } else { 1 }) == i@)]
    #[invariant(match removed {
        Some(index) => index@ < i@ && selected@[index@].1 == failed_rack,
        None => true,
    })]
    #[invariant(forall<j: Int> 0 <= j && j < survivors@.len()
        ==> exists<k: Int> 0 <= k && k < i@
            && survivors@[j] == selected@[k].0 && selected@[k].1 != failed_rack)]
    #[invariant(forall<j: Int> 0 <= j && j < i@ && selected@[j].1 != failed_rack
        ==> exists<k: Int> 0 <= k && k < survivors@.len() && survivors@[k] == selected@[j].0)]
    #[invariant(crate::sequence::distinct(survivors@))]
    #[invariant(forall<j: Int, k: Int, a: Int, b: Int>
        0 <= j && j < k && k < survivors@.len() && 0 <= a && a < selected@.len()
            && 0 <= b && b < selected@.len()
            && survivors@[j] == selected@[a].0 && survivors@[k] == selected@[b].0 ==> a < b)]
    #[variant(selected@.len() - i@)]
    while i < selected.len() {
        if selected[i].1 == failed_rack {
            proof_assert!(removed == None);
            #[cfg(creusot)]
            {
                removed = Some(i);
            }
        } else {
            survivors.push(nodes[i]);
        }
        i += 1;
    }
    let quorum = installed && election_has_quorum(selected.len(), survivors.len());
    (selected, survivors, installed, quorum)
}
