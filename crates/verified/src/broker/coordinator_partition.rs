use {super::FindCoordinatorAdmission, creusot_std::prelude::*};

/// Decide whether one `FindCoordinator` key may proceed to coordinator lookup.
///
/// Kafka key types are GROUP=0, TRANSACTION=1, and SHARE=2. SHARE was added in
/// API version 6, uses `ClusterAction` authorization, and carries a composite key
/// that the host validates before calling this kernel. `share_key_valid` is
/// ignored for the two non-SHARE key types. Unknown key types never inherit an
/// allow result.
#[ensures(key_type@ == 0 ==> result == if acl_allowed {
    FindCoordinatorAdmission::AllowGroup
} else {
    FindCoordinatorAdmission::DenyGroup
})]
#[ensures(key_type@ == 1 ==> result == if acl_allowed {
    FindCoordinatorAdmission::AllowTransaction
} else {
    FindCoordinatorAdmission::DenyTransaction
})]
#[ensures(key_type@ == 2 ==> result == if api_version@ < 6 || !share_key_valid {
    FindCoordinatorAdmission::InvalidRequest
} else if acl_allowed {
    FindCoordinatorAdmission::AllowShare
} else {
    FindCoordinatorAdmission::DenyCluster
})]
#[ensures(key_type@ < 0 || key_type@ > 2
    ==> result == FindCoordinatorAdmission::InvalidRequest)]
#[must_use]
pub fn find_coordinator_admission(
    api_version: i16,
    key_type: i8,
    acl_allowed: bool,
    share_key_valid: bool,
) -> FindCoordinatorAdmission {
    match key_type {
        0 if acl_allowed => FindCoordinatorAdmission::AllowGroup,
        0 => FindCoordinatorAdmission::DenyGroup,
        1 if acl_allowed => FindCoordinatorAdmission::AllowTransaction,
        1 => FindCoordinatorAdmission::DenyTransaction,
        2 if api_version < 6 || !share_key_valid => FindCoordinatorAdmission::InvalidRequest,
        2 if acl_allowed => FindCoordinatorAdmission::AllowShare,
        2 => FindCoordinatorAdmission::DenyCluster,
        _ => FindCoordinatorAdmission::InvalidRequest,
    }
}

/// Admit an unclean-election commit only against the exact partition snapshot
/// used to select its winner.
#[ensures(result == (selected_partition_epoch@ == current_partition_epoch@
    && !current_leader_alive
    && selected_replicas@ == current_replicas@
    && (exists<i: Int> 0 <= i && i < current_replicas@.len()
        && current_replicas@[i] == winner)))]
#[must_use]
pub fn unclean_recovery_commit_admission(
    selected_partition_epoch: i32,
    current_partition_epoch: i32,
    selected_replicas: &[u64],
    current_replicas: &[u64],
    winner: u64,
    current_leader_alive: bool,
) -> bool {
    if selected_partition_epoch != current_partition_epoch
        || current_leader_alive
        || selected_replicas.len() != current_replicas.len()
    {
        return false;
    }

    let mut winner_assigned = false;
    let mut i = 0usize;
    #[cfg_attr(creusot, invariant(i@ <= selected_replicas@.len()))]
    #[cfg_attr(creusot, invariant(selected_replicas@.len() == current_replicas@.len()))]
    #[cfg_attr(creusot, invariant(forall<k: Int> 0 <= k && k < i@
        ==> selected_replicas@[k] == current_replicas@[k]))]
    #[cfg_attr(creusot, invariant(winner_assigned == (exists<k: Int>
        0 <= k && k < i@ && current_replicas@[k] == winner)))]
    #[cfg_attr(creusot, variant(selected_replicas@.len() - i@))]
    while i < selected_replicas.len() {
        if selected_replicas[i] != current_replicas[i] {
            return false;
        }
        if current_replicas[i] == winner {
            winner_assigned = true;
        }
        i += 1;
    }
    winner_assigned
}

/// Java `String.hashCode` over the first `limit` UTF-16 code units.
// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(0 <= limit && limit <= units.len())]
#[variant(limit)]
pub fn java_string_hash_prefix_model(units: Seq<u16>, limit: Int) -> i32 {
    pearlite! {
        if limit <= 0 {
            0i32
        } else {
            java_string_hash_prefix_model(units, limit - 1) * 31i32
                + units[limit - 1] as i32
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
fn java_string_abs_model(hash: i32) -> i32 {
    pearlite! {
        if hash == i32::MIN {
            0i32
        } else if hash < 0i32 {
            -hash
        } else {
            hash
        }
    }
}

// cargo-mutants: #[cfg(creusot)] spec function; not compiled outside Creusot, so no test can tell.
#[cfg(creusot)]
#[cfg_attr(test, mutants::skip)]
#[logic]
#[requires(partition_count@ > 0)]
fn java_string_hash_partition_model(units: Seq<u16>, partition_count: i32) -> Int {
    pearlite! {
        java_string_abs_model(java_string_hash_prefix_model(units, units.len()))@
            % partition_count@
    }
}

/// Java `String.hashCode` over UTF-16 code units, followed by Kafka's
/// `Utils.abs(hash) % partition_count` coordinator selection.
///
/// The host supplies `str::encode_utf16()` output so non-ASCII group ids use
/// the same surrogate-pair semantics as the JVM. Java's `Integer.MIN_VALUE`
/// absolute-value corner maps to zero, matching `Utils.abs`.
#[cfg_attr(
    creusot,
    ensures(partition_count@ > 0 ==>
        exists<partition: i32> result == Some(partition)
            && partition@ == java_string_hash_partition_model(units@, partition_count))
)]
#[ensures(result == None ==> partition_count@ <= 0)]
#[ensures(partition_count@ <= 0 ==> result == None)]
#[ensures(forall<partition: i32> result == Some(partition) ==>
    0 <= partition@ && partition@ < partition_count@)]
#[must_use]
pub fn java_string_hash_partition(units: &[u16], partition_count: i32) -> Option<i32> {
    if partition_count <= 0 {
        return None;
    }

    let mut hash = 0_i32;
    let mut index = 0_usize;
    #[invariant(index@ <= units@.len())]
    #[cfg_attr(creusot, invariant(hash == java_string_hash_prefix_model(units@, index@)))]
    #[variant(units@.len() - index@)]
    while index < units.len() {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(units[index]));
        index += 1;
    }
    let positive = if hash == i32::MIN {
        0
    } else if hash < 0 {
        -hash
    } else {
        hash
    };
    Some(positive % partition_count)
}
