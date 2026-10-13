//! Forward flush and shutdown polls through the first field of an unpinned tuple wrapper.

use crate::{fixtures::name, prelude::*};

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let policy = name(input)?;
    if policy != "tuple" {
        return Err(ParseError::new(
            policy.span(),
            "expected tuple delegation policy",
        ));
    }
    let mut output = TokenStream::new();
    for method in [
        moxy::template! { poll_flush },
        moxy::template! { poll_shutdown },
    ] {
        output.extend(moxy::template! {
            fn {{ method }}(
                mut self: ::std::pin::Pin<&mut Self>,
                cx: &mut ::std::task::Context<'_>,
            ) -> ::std::task::Poll<::std::io::Result<()>> {
                ::std::pin::Pin::new(&mut self.0).{{ method }}(cx)
            }
        });
    }
    Ok(output)
}
