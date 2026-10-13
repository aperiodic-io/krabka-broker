//! Both native headers consume the same exhaustive mutation inventory.

use assert2::assert;

#[derive(Clone, Copy)]
enum Kind {
    Write,
    Delete,
    Read,
}

krabka_macros::freeze_refusal_rules! {
    Kind;
    refuse [Write, Delete];
    allow [Read];
    runtime {
        #[must_use]
        const fn refuses(kind: Kind) -> bool
    }
    model {
        #[cfg(all())]
        fn logical_refusal(other_binding: Kind) -> bool
    }
}

const WRITE_IS_REFUSED: bool = refuses(Kind::Write);

#[test]
fn both_headers_classify_every_variant_with_their_original_bindings() {
    struct Case {
        kind: Kind,
        expected: bool,
    }
    assert!(WRITE_IS_REFUSED);
    for Case { kind, expected } in [
        Case {
            kind: Kind::Write,
            expected: true,
        },
        Case {
            kind: Kind::Delete,
            expected: true,
        },
        Case {
            kind: Kind::Read,
            expected: false,
        },
    ] {
        assert!(refuses(kind) == expected);
        assert!(logical_refusal(kind) == expected);
    }
}
