//! KIP-1071 server-side task assignor: a port of Kafka's `StickyTaskAssignor`,
//! the only built-in streams assignor.
//!
//! This is a *pure* module, with no async, no I/O, and no metadata access. It
//! takes already-resolved inputs, an [`AssignorInput`] and a slice of
//! [`AssignorMember`], and returns a target [`StreamsAssignment`]. The
//! coordinator actor builds the inputs from the member state and the topology,
//! and applies the output as the next target assignment.
//!
//! A *task* is `(subtopology_id, partition)`. The assignor places one active
//! copy of every task and `num.standby.replicas` standby copies of every
//! stateful task, balancing the load across processes and keeping tasks where
//! their state already is. It assigns no warmup tasks: in Kafka those come
//! from the assignment refiner, whose default hands out none, so a task that
//! has to move does so at once.
//!
//! Determinism is mandatory. Kafka breaks some load ties by hash-map order;
//! this port breaks them by process id and member id, so the same inputs
//! always give the same assignment.
//!
//! The steps live in their own modules: `process` holds the per-process load
//! and the previous owners, `active` places the active tasks, `standby` places
//! the standby copies, and `types` holds the input and output shapes.

use {
    self::{
        active::assign_active,
        process::LocalState,
        standby::assign_standby,
        types::{Task, to_role_maps},
    },
    std::collections::{BTreeMap, BTreeSet},
};

mod active;
mod process;
mod standby;
mod types;

#[cfg(test)]
mod tests;

pub use self::types::{AssignorInput, AssignorMember, StreamsAssignment};

/// Computes the target [`StreamsAssignment`] for a streams group.
///
/// See the module docs for the algorithm. With no members, it returns an empty
/// assignment.
#[must_use]
pub fn assign(members: &[AssignorMember], input: &AssignorInput) -> StreamsAssignment {
    if members.is_empty() {
        return StreamsAssignment::default();
    }
    let mut ordered: Vec<&AssignorMember> = members.iter().collect();
    ordered.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    let tasks = flatten_tasks(&input.tasks);
    let stateful_tasks: Vec<Task> = tasks
        .iter()
        .filter(|(sub, _)| input.stateful.contains(sub))
        .cloned()
        .collect();
    let num_standby_replicas = i64::from(input.num_standby_replicas.max(0));

    let mut state = LocalState::new(&ordered, &tasks, stateful_tasks.len(), num_standby_replicas);
    assign_active(&mut state, &tasks);
    if num_standby_replicas > 0 {
        assign_standby(&mut state, &stateful_tasks);
    }

    let mut active: BTreeMap<String, BTreeSet<Task>> = BTreeMap::new();
    let mut standby: BTreeMap<String, BTreeSet<Task>> = BTreeMap::new();
    for process in state.processes.values() {
        for (member, tasks) in &process.active {
            active
                .entry(member.clone())
                .or_default()
                .extend(tasks.iter().cloned());
        }
        for (member, tasks) in &process.standby {
            standby
                .entry(member.clone())
                .or_default()
                .extend(tasks.iter().cloned());
        }
    }
    StreamsAssignment {
        active: to_role_maps(&active),
        standby: to_role_maps(&standby),
    }
}

/// Flattens the `subtopology -> partitions` universe into an ordered, de-duped
/// list of `(subtopology, partition)` tasks.
fn flatten_tasks(tasks: &BTreeMap<String, Vec<i32>>) -> Vec<Task> {
    let mut out: Vec<Task> = tasks
        .iter()
        .flat_map(|(sub, parts)| parts.iter().map(move |&p| (sub.clone(), p)))
        .collect();
    out.sort();
    out.dedup();
    out
}
