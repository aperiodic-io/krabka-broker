//! Domain values used by direct and composed reconfiguration fixtures.

#[derive(Clone, Copy)]
pub(crate) struct VoterCount(pub usize);

#[derive(Clone, Copy)]
pub(crate) struct KraftFeatureLevel(pub u16);

#[derive(Clone, Copy)]
pub(crate) enum ControlRecordWrite {
    Emit,
    Skip,
}
