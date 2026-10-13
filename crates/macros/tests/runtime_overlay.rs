//! What `#[derive(RuntimeOverlay)]` copies onto its target.

use {assert2::assert, krabka_macros::RuntimeOverlay};

/// A refined newtype, as `krabka_broker::config_value` writes them.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Positive(u32);

impl Positive {
    const fn into_value(self) -> u32 { self.0 }
}

#[derive(Debug, Default, PartialEq)]
struct Target {
    plain: Option<u64>,
    refined: Option<u32>,
    cloned: Option<String>,
    skipped: Option<u64>,
}

#[derive(RuntimeOverlay)]
#[overlay(target = Target)]
struct Source {
    plain: Option<u64>,
    #[overlay(refined)]
    refined: Option<Positive>,
    #[overlay(clone)]
    cloned: Option<String>,
    #[overlay(skip)]
    skipped: Option<u64>,
}

#[test]
fn copy_into_assigns_every_overlaid_field() {
    let source = Source {
        plain: Some(7),
        refined: Some(Positive(3)),
        cloned: Some("name".to_owned()),
        skipped: Some(9),
    };
    let mut target = Target {
        skipped: Some(1),
        ..Target::default()
    };
    source.copy_into(&mut target);
    assert!(
        target
            == Target {
                plain: Some(7),
                refined: Some(3),
                cloned: Some("name".to_owned()),
                skipped: Some(1),
            }
    );
    // `skipped` is read only here, so the field is not dead code.
    assert!(source.skipped == Some(9));
}

#[test]
fn copy_into_overwrites_a_set_target_with_an_unset_flag() {
    let source = Source {
        plain: None,
        refined: None,
        cloned: None,
        skipped: None,
    };
    let mut target = Target {
        plain: Some(1),
        refined: Some(2),
        cloned: Some("old".to_owned()),
        skipped: Some(4),
    };
    source.copy_into(&mut target);
    assert!(
        target
            == Target {
                plain: None,
                refined: None,
                cloned: None,
                skipped: Some(4),
            }
    );
}
