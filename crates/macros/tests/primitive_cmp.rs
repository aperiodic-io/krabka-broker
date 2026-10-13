//! What `#[derive(PrimitiveCmp)]` compares, in both directions.

use {assert2::assert, krabka_macros::PrimitiveCmp};

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, PrimitiveCmp)]
struct Seq(u64);

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, PrimitiveCmp)]
struct EpochMs(pub i64);

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, PrimitiveCmp)]
struct DomainSeq<const DOMAIN: u8>(u64);

#[test]
fn const_generic_domains_compare_from_both_sides() {
    assert!(DomainSeq::<1>(3) == 3_u64);
    assert!(3_u64 == DomainSeq::<2>(3));
    assert!(DomainSeq::<1>(3) < 4_u64);
    assert!(2_u64 < DomainSeq::<2>(3));
}

#[test]
fn compares_with_the_inner_type_from_either_side() {
    assert!(Seq(3) == 3_u64);
    assert!(3_u64 == Seq(3));
    assert!(Seq(3) != 4_u64);
    assert!(4_u64 != Seq(3));
    assert!(Seq(3) < 4_u64);
    assert!(2_u64 < Seq(3));
    assert!(EpochMs(-1) < 0_i64);
    assert!(0_i64 >= EpochMs(-1));
}

#[test]
fn ordering_matches_the_inner_types() {
    for (left, right) in [(1_i64, 2_i64), (2, 2), (3, -3)] {
        assert!(EpochMs(left).partial_cmp(&right) == left.partial_cmp(&right));
        assert!(left.partial_cmp(&EpochMs(right)) == left.partial_cmp(&right));
    }
}
