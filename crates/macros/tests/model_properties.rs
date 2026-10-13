//! Generated properties retain their declared order and execute native predicates.

use {
    assert2::assert,
    stateright::{Expectation, Model, Property},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct ProbeAction;

struct ProbeModel;

impl Model for ProbeModel {
    type Action = ProbeAction;
    type State = usize;

    fn init_states(&self) -> Vec<Self::State> { vec![0] }

    fn actions(&self, _: &Self::State, _: &mut Vec<Self::Action>) {}

    fn next_state(&self, _: &Self::State, _: Self::Action) -> Option<Self::State> { None }

    fn properties(&self) -> Vec<Property<Self>> {
        krabka_macros::model_properties! {
            usize;
            always "bounded" => |state| { std::matches!(*state, 0 | 1) },
            sometimes "advanced" => |state| { *state == 1 },
            eventually "finished" => |state| {
                let expected = 1;
                *state == expected
            },
        }
    }
}

#[test]
fn rows_keep_expectations_labels_and_native_bindings_in_order() {
    let model = ProbeModel;
    let properties = model.properties();
    assert!(
        properties
            .iter()
            .map(|property| property.name)
            .collect::<Vec<_>>()
            == ["bounded", "advanced", "finished"]
    );
    assert!(
        properties
            .iter()
            .map(|property| &property.expectation)
            .collect::<Vec<_>>()
            == [
                &Expectation::Always,
                &Expectation::Sometimes,
                &Expectation::Eventually
            ]
    );
    assert!((properties[0].condition)(&model, &0));
    assert!(!(properties[1].condition)(&model, &0));
    assert!((properties[1].condition)(&model, &1));
    assert!((properties[2].condition)(&model, &1));
}
