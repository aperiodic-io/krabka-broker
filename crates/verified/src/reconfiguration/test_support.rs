//! Domain values used by direct and composed reconfiguration fixtures.

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub(crate) struct VoterCount(pub usize);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub(crate) struct KraftFeatureLevel(pub u16);

#[derive(Clone, Copy)]
pub(crate) enum ControlRecordWrite {
    Emit,
    Skip,
}
