//! The working state of one assignment: Kafka's `ProcessState` and the
//! assignor's `LocalState`.
//!
//! Load is balanced across processes. A process's load is its task count
//! divided by its member count, and inside a process a task goes to the member
//! with the fewest tasks. Kafka keeps processes and members in hash maps and
//! priority queues, so it breaks a load tie in no fixed order. This port
//! breaks every tie on the process id or the member id, so that the same
//! inputs always give the same assignment.

use {
    super::types::{AssignorMember, Task},
    std::{
        cmp::Ordering,
        collections::{BTreeMap, BTreeSet, HashMap},
    },
};

/// A member of a process, as Kafka's `StickyTaskAssignor.Member`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Member {
    pub(super) process_id: String,
    pub(super) member_id: String,
}

/// The tasks placed on one process so far: Kafka's `ProcessState`.
#[derive(Debug, Default)]
pub(super) struct ProcessState {
    /// The number of members of the process.
    capacity: i64,
    task_count: i64,
    member_task_counts: BTreeMap<String, i64>,
    pub(super) active: BTreeMap<String, BTreeSet<Task>>,
    pub(super) standby: BTreeMap<String, BTreeSet<Task>>,
    assigned: BTreeSet<Task>,
}

impl ProcessState {
    fn add_member(&mut self, member_id: &str) {
        self.member_task_counts.insert(member_id.to_owned(), 0);
        self.capacity += 1;
    }

    /// Compares the loads `task_count / capacity` of two processes exactly.
    pub(super) fn cmp_load(&self, other: &Self) -> Ordering {
        (self.task_count * other.capacity).cmp(&(other.task_count * self.capacity))
    }

    /// The number of tasks placed on `member_id`.
    pub(super) fn member_count(&self, member_id: &str) -> i64 {
        self.member_task_counts.get(member_id).copied().unwrap_or(0)
    }

    pub(super) fn has_task(&self, task: &Task) -> bool { self.assigned.contains(task) }

    /// Places `task` on `member_id` and returns the member's new task count.
    pub(super) fn add_task(&mut self, member_id: &str, task: &Task, is_active: bool) -> i64 {
        self.task_count += 1;
        self.assigned.insert(task.clone());
        let role = if is_active {
            &mut self.active
        } else {
            &mut self.standby
        };
        role.entry(member_id.to_owned())
            .or_default()
            .insert(task.clone());
        let count = self
            .member_task_counts
            .entry(member_id.to_owned())
            .or_default();
        *count += 1;
        *count
    }

    /// Places `task` on the member with the fewest tasks, the smaller id on a
    /// tie, and returns that member's new task count.
    pub(super) fn add_task_to_least_loaded_member(&mut self, task: &Task, is_active: bool) -> i64 {
        let member_id = self
            .member_task_counts
            .iter()
            .min_by_key(|(member_id, count)| (**count, (*member_id).clone()))
            .map(|(member_id, _)| member_id.clone())
            .unwrap_or_default();
        self.add_task(&member_id, task, is_active)
    }
}

/// The assignor's state for one computation: Kafka's `LocalState`.
pub(super) struct LocalState {
    /// The member that owns each task as active.
    pub(super) active_task_to_prev_member: HashMap<Task, Member>,
    /// The members that hold each task's state, most preferred first.
    pub(super) standby_task_to_prev_members: HashMap<Task, Vec<Member>>,
    pub(super) processes: BTreeMap<String, ProcessState>,
    pub(super) num_standby_replicas: i64,
    total_active_tasks: i64,
    total_tasks: i64,
    members_with_active_task_capacity: i64,
    members_with_task_capacity: i64,
    pub(super) active_tasks_per_member: i64,
    pub(super) total_tasks_per_member: i64,
}

/// A member that holds a task's state: Kafka's `StandbyCandidate`.
struct StandbyCandidate {
    member: Member,
    is_prev_standby: bool,
    offset_sum: i64,
}

impl LocalState {
    /// Kafka's `initialize`: the per-member quotas, the processes, and the
    /// previous owners of every task.
    pub(super) fn new(
        members: &[&AssignorMember],
        tasks: &[Task],
        stateful_tasks: usize,
        num_standby_replicas: i64,
    ) -> Self {
        let total_active_tasks = i64::try_from(tasks.len()).unwrap_or(i64::MAX);
        let total_tasks = total_active_tasks
            + i64::try_from(stateful_tasks).unwrap_or(i64::MAX) * num_standby_replicas;
        let member_count = i64::try_from(members.len()).unwrap_or(i64::MAX);
        let mut state = Self {
            active_task_to_prev_member: HashMap::new(),
            standby_task_to_prev_members: HashMap::new(),
            processes: BTreeMap::new(),
            num_standby_replicas,
            total_active_tasks,
            total_tasks,
            members_with_active_task_capacity: member_count,
            members_with_task_capacity: member_count,
            active_tasks_per_member: tasks_per_member(total_active_tasks, member_count),
            total_tasks_per_member: tasks_per_member(total_tasks, member_count),
        };

        let mut candidates: HashMap<Task, Vec<StandbyCandidate>> = HashMap::new();
        for m in members {
            let member = Member {
                process_id: m.process_id.clone(),
                member_id: m.member_id.clone(),
            };
            state
                .processes
                .entry(m.process_id.clone())
                .or_default()
                .add_member(&m.member_id);
            for task in role_tasks(&m.current_active) {
                state
                    .active_task_to_prev_member
                    .insert(task, member.clone());
            }
            collect_standby_candidates(&mut candidates, m, &member);
        }
        for (task, mut ranked) in candidates {
            // A current standby or warmup owner ranks ahead of a member known
            // only through its reported offsets; the most caught-up state
            // comes first within each group. The sort is stable.
            ranked.sort_by_key(|c| (!c.is_prev_standby, std::cmp::Reverse(c.offset_sum)));
            state
                .standby_task_to_prev_members
                .insert(task, ranked.into_iter().map(|c| c.member).collect());
        }
        state
    }

    /// Kafka's `maybeUpdateActiveTasksPerMember`.
    pub(super) fn maybe_update_active_tasks_per_member(&mut self, active_tasks: i64) {
        if active_tasks == self.active_tasks_per_member {
            self.members_with_active_task_capacity -= 1;
            self.total_active_tasks -= active_tasks;
            self.active_tasks_per_member = tasks_per_member(
                self.total_active_tasks,
                self.members_with_active_task_capacity,
            );
        }
    }

    /// Kafka's `maybeUpdateTotalTasksPerMember`.
    pub(super) fn maybe_update_total_tasks_per_member(&mut self, tasks: i64) {
        if tasks == self.total_tasks_per_member {
            self.members_with_task_capacity -= 1;
            self.total_tasks -= tasks;
            self.total_tasks_per_member =
                tasks_per_member(self.total_tasks, self.members_with_task_capacity);
        }
    }

    /// Kafka's `findPrevMemberWithLeastLoad`: the first previous owner, replaced
    /// only by one whose process load and member load are both lower. With
    /// `standby_task`, a member whose process already holds the task is
    /// skipped.
    pub(super) fn prev_member_with_least_load(
        &self,
        members: Option<&Vec<Member>>,
        standby_task: Option<&Task>,
    ) -> Option<Member> {
        let mut candidate: Option<(&Member, &ProcessState, i64)> = None;
        for member in members? {
            let process = &self.processes[&member.process_id];
            if standby_task.is_some_and(|task| process.has_task(task)) {
                continue;
            }
            let member_load = process.member_count(&member.member_id);
            let better = candidate.is_none_or(|(_, best_process, best_member_load)| {
                process.cmp_load(best_process) == Ordering::Less && member_load < best_member_load
            });
            if better {
                candidate = Some((member, process, member_load));
            }
        }
        candidate.map(|(member, _, _)| member.clone())
    }

    /// The process ids in ascending load order, the smaller id on a tie.
    pub(super) fn processes_by_load(&self) -> Vec<String> {
        let mut ids: Vec<&String> = self.processes.keys().collect();
        ids.sort_by(|a, b| {
            self.processes[*a]
                .cmp_load(&self.processes[*b])
                .then(a.cmp(b))
        });
        ids.into_iter().cloned().collect()
    }

    /// Places `task` on `member` and returns the member's new task count.
    pub(super) fn add_task(&mut self, member: &Member, task: &Task, is_active: bool) -> i64 {
        self.processes
            .get_mut(&member.process_id)
            .map_or(0, |process| {
                process.add_task(&member.member_id, task, is_active)
            })
    }

    /// The task count of `member`.
    pub(super) fn member_count(&self, member: &Member) -> i64 {
        self.processes
            .get(&member.process_id)
            .map_or(0, |process| process.member_count(&member.member_id))
    }
}

/// Kafka's `collectStandbyCandidates`: the members that hold a task as a
/// standby or a warmup, and the members that reported an offset for a task
/// that they hold in neither role.
fn collect_standby_candidates(
    candidates: &mut HashMap<Task, Vec<StandbyCandidate>>,
    m: &AssignorMember,
    member: &Member,
) {
    let offset_sum = |task: &Task| m.task_offsets.get(task).copied().unwrap_or(0);
    let held: BTreeSet<Task> = role_tasks(&m.current_standby)
        .chain(role_tasks(&m.current_warmup))
        .collect();
    for task in role_tasks(&m.current_standby).chain(role_tasks(&m.current_warmup)) {
        let offset_sum = offset_sum(&task);
        candidates.entry(task).or_default().push(StandbyCandidate {
            member: member.clone(),
            is_prev_standby: true,
            offset_sum,
        });
    }
    for (task, &offset_sum) in &m.task_offsets {
        if held.contains(task) {
            continue;
        }
        candidates
            .entry(task.clone())
            .or_default()
            .push(StandbyCandidate {
                member: member.clone(),
                is_prev_standby: false,
                offset_sum,
            });
    }
}

/// The tasks of a `subtopology -> partitions` role map.
fn role_tasks(role: &BTreeMap<String, Vec<i32>>) -> impl Iterator<Item = Task> + '_ {
    role.iter()
        .flat_map(|(sub, parts)| parts.iter().map(move |part| (sub.clone(), *part)))
}

/// Kafka's `computeTasksPerMember`: the tasks divided by the members, rounded
/// up, and 0 with no members.
fn tasks_per_member(tasks: i64, members: i64) -> i64 {
    if members == 0 {
        return 0;
    }
    let mut per_member = tasks / members;
    if tasks % members > 0 {
        per_member += 1;
    }
    per_member
}
