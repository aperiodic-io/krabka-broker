//! Both native headers consume the same exhaustive mutation inventory.

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

#[yare::parameterized(
    write = { Kind::Write, true },
    delete = { Kind::Delete, true },
    read = { Kind::Read, false },
)]
fn both_headers_classify_every_variant_with_their_original_bindings(kind: Kind, expected: bool) {
    assert2::assert!(WRITE_IS_REFUSED);
    assert2::assert!(refuses(kind) == expected);
    assert2::assert!(logical_refusal(kind) == expected);
}
