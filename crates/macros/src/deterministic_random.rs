//! One `SplitMix64` step for deterministic placement and benchmark streams.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn splitmix64_step(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let name = crate::fixtures::name(tokens)?;
    Ok(moxy::template! {
        pub(crate) fn {{ name }}(state: &mut u64) -> u64 {
            *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
            let mut z = *state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
            z ^ (z >> 31)
        }
    })
}
