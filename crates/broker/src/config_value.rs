//! Validated scalar values accepted at broker configuration boundaries.

use {
    krabka_macros::RefinedNewtype,
    refined_type::rule::{GreaterI16, GreaterI32, GreaterI64, GreaterUsize, MinMaxU32},
};

/// A 32-bit signed integer greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterI32<0>), parse_fn = parse_positive_i32)]
pub struct PositiveI32(i32);

/// A 16-bit signed integer greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterI16<0>), parse_fn = parse_positive_i16)]
pub struct PositiveI16(i16);

/// A 64-bit signed integer greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterI64<0>), parse_fn = parse_positive_i64)]
pub struct PositiveI64(i64);

/// A platform-sized count greater than zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterUsize<0>), parse_fn = parse_positive_count)]
pub struct PositiveCount(usize);

/// An inclusive percentage from zero through one hundred.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(MinMaxU32<0, 100>), parse_fn = parse_percentage)]
pub struct Percentage(u32);

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn refined_scalar_boundaries() {
        assert!(parse_positive_i32("1").is_ok());
        assert!(parse_positive_i32("0").is_err());
        assert!(parse_positive_i16("1").is_ok());
        assert!(parse_positive_i16("0").is_err());
        assert!(parse_positive_i64("1").is_ok());
        assert!(parse_positive_i64("-1").is_err());
        assert!(parse_positive_count("1").is_ok());
        assert!(parse_positive_count("0").is_err());
        assert!(parse_percentage("0").is_ok());
        assert!(parse_percentage("100").is_ok());
        assert!(parse_percentage("101").is_err());
    }
}
