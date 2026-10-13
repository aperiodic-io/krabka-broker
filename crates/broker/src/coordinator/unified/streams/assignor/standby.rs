//! Standby placement: Kafka's `StickyTaskAssignor.assignStandby`.
//!
//! Each stateful task gets `num.standby.replicas` standby copies, each on a
//! process that holds no other copy of the task. A copy goes first to the
//! previous active owner, then to the previous standby owner with the least
//! load, both within the total task quota, and otherwise to the least loaded
//! process.

use super::{process::LocalState, types::Task};

/// A task and the number of standby copies of it that are still to place.
struct UnplacedStandby {
    task: Task,
    remaining: i64,
}

/// Places the standby copies of every task in `stateful_tasks`.
pub(super) fn assign_standby(state: &mut LocalState, stateful_tasks: &[Task]) {
    // Kafka walks the tasks by partition, then subtopology, in reverse.
    let mut tasks: Vec<Task> = stateful_tasks.to_vec();
    tasks.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)));

    let mut to_least_loaded: Vec<UnplacedStandby> = Vec::new();
    for task in &tasks {
        for placed in 0..state.num_standby_replicas {
            if place_on_prev_owner(state, task) {
                continue;
            }
            to_least_loaded.push(UnplacedStandby {
                task: task.clone(),
                remaining: state.num_standby_replicas - placed,
            });
            break;
        }
    }

    // By subtopology, then partition, in reverse.
    to_least_loaded.sort_by(|a, b| b.task.cmp(&a.task));
    for UnplacedStandby { task, remaining } in &to_least_loaded {
        for placed in 0..*remaining {
            if !place_on_least_loaded_process(state, task) {
                tracing::warn!(
                    "Unable to assign {} of {} standby tasks for task [{}_{}]. There is not \
                     enough available capacity. You should increase the number of threads \
                     and/or application instances to maintain the requested number of standby \
                     replicas.",
                    state.num_standby_replicas - placed,
                    state.num_standby_replicas,
                    task.0,
                    task.1,
                );
                break;
            }
        }
    }
}

/// Places one standby copy of `task` on its previous active owner, or else on
/// its least loaded previous standby owner, each only under the total quota.
/// Returns whether it placed the copy.
fn place_on_prev_owner(state: &mut LocalState, task: &Task) -> bool {
    if let Some(prev) = state.active_task_to_prev_member.get(task).cloned() {
        let holds = state.processes[&prev.process_id].has_task(task);
        if !holds && state.member_count(&prev) < state.total_tasks_per_member {
            let count = state.add_task(&prev, task, false);
            state.maybe_update_total_tasks_per_member(count);
            return true;
        }
    }
    let prev =
        state.prev_member_with_least_load(state.standby_task_to_prev_members.get(task), Some(task));
    if let Some(prev) = prev
        && state.member_count(&prev) < state.total_tasks_per_member
    {
        let count = state.add_task(&prev, task, false);
        state.maybe_update_total_tasks_per_member(count);
        return true;
    }
    false
}

/// Places one standby copy of `task` on the least loaded member of the least
/// loaded process that does not hold the task yet. Returns whether one did.
fn place_on_least_loaded_process(state: &mut LocalState, task: &Task) -> bool {
    let Some(process_id) = state
        .processes_by_load()
        .into_iter()
        .find(|id| !state.processes[id].has_task(task))
    else {
        return false;
    };
    let count = state.processes.get_mut(&process_id).map_or(0, |process| {
        process.add_task_to_least_loaded_member(task, false)
    });
    state.maybe_update_total_tasks_per_member(count);
    true
}
