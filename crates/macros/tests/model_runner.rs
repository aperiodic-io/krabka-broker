//! Real Stateright checkers distinguish the two declared runner diagnostic orders.

use {
    assert2::assert,
    stateright::{Checker, Model, Property},
    std::{cell::RefCell, panic::catch_unwind},
};

thread_local! {
    static STEPS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ProbeAction;

#[derive(Clone)]
struct ProbeModel {
    safe: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProbeState {
    safe: bool,
    advanced: bool,
}

impl Model for ProbeModel {
    type Action = ProbeAction;
    type State = ProbeState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![ProbeState {
            safe: self.safe,
            advanced: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if !state.advanced {
            actions.push(ProbeAction);
        }
    }

    fn next_state(&self, state: &Self::State, _: Self::Action) -> Option<Self::State> {
        (!state.advanced).then_some(ProbeState {
            safe: state.safe,
            advanced: true,
        })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("safe", |_, state| state.safe),
            Property::<Self>::sometimes("advanced", |_, state| state.advanced),
        ]
    }
}

fn bounded_bfs(
    model: ProbeModel,
    label: &str,
    depth: usize,
    states: usize,
) -> impl Checker<ProbeModel> {
    assert!(label == "probe");
    assert!(depth == 4 && states == 10);
    STEPS.with(|steps| steps.borrow_mut().push("bfs"));
    model
        .checker()
        .target_max_depth(depth)
        .target_state_count(states)
        .spawn_bfs()
        .join()
}

fn pin(actual: usize, expected: usize, label: &str) {
    assert!(actual > 0 && expected == 2 && label == "probe");
    STEPS.with(|steps| steps.borrow_mut().push("pin"));
}

krabka_macros::model_runner! {
    fn properties_first(model: ProbeModel, label: &str, pinned: usize)
    [bfs = bounded_bfs, pin = pin, depth = 4, states = 10, checker = checker, order = "properties_first"]
    {
        assert!(checker.unique_state_count() > 0);
        STEPS.with(|steps| steps.borrow_mut().push("native_checks"));
    }
}

krabka_macros::model_runner! {
    fn properties_last(model: ProbeModel, label: &str, pinned: usize)
    [bfs = bounded_bfs, pin = pin, depth = 4, states = 10, checker = checker, order = "properties_last"]
    {
        assert!(checker.unique_state_count() > 0);
        STEPS.with(|steps| steps.borrow_mut().push("native_checks"));
    }
}

#[test]
fn valid_models_reach_checks_pin_and_properties() {
    for runner in [properties_first, properties_last] {
        STEPS.with(|steps| steps.borrow_mut().clear());
        runner(ProbeModel { safe: true }, "probe", 2);
        STEPS.with(|steps| assert!(*steps.borrow() == ["bfs", "native_checks", "pin"]));
    }
}

#[test]
fn failing_properties_abort_at_the_declared_diagnostic_position() {
    struct RunnerCase {
        runner: fn(ProbeModel, &str, usize),
        expected: Vec<&'static str>,
    }
    for case in [
        RunnerCase {
            runner: properties_first,
            expected: vec!["bfs", "native_checks"],
        },
        RunnerCase {
            runner: properties_last,
            expected: vec!["bfs", "native_checks", "pin"],
        },
    ] {
        STEPS.with(|steps| steps.borrow_mut().clear());
        assert!(catch_unwind(|| (case.runner)(ProbeModel { safe: false }, "probe", 2)).is_err());
        STEPS.with(|steps| assert!(*steps.borrow() == case.expected));
    }
}
