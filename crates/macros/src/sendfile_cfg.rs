//! Emit an item only on platforms with a usable file→socket `sendfile(2)`
//! for the zero-copy fetch path: Linux, the Apple targets, and FreeBSD/DragonFly
//! (the "SENDFILE alias"). Windows is excluded because there is no safe
//! `TransmitFile` wrapper under `unsafe_code = "forbid"`. The fetch path therefore
//! uses `pread` and `write_all` there, and `WriteOp` carries only `Inline`.
//!
//! The shared predicate keeps every sendfile-gated item consistent: the file
//! drain helpers, `tcp_for_sendfile` trait method, and sendfile resolver.
//! File-region eligibility still uses the configured `sendfile_min_bytes`
//! threshold. The single per-OS syscall inside `sendfile_region` is gated
//! separately (Linux `rustix` versus Apple/BSD `nix`).

use {
    crate::{meta::no_arguments, network_fixtures::sendfile_platform},
    moxy::ast::ParseError,
    proc_macro::TokenStream as NativeTokenStream,
};

pub(crate) fn expand(
    meta: NativeTokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    no_arguments(meta.into(), "sendfile_cfg")?;
    // The existing helper prepends the shared cfg without rebuilding any item tokens.
    Ok(sendfile_platform(item))
}
