//! KFC-9 fixtures shared by the `DeleteRecords` tests: the gated broker
//! configuration.
//!
//! The gate's unit tests and the end-to-end refusal test both build a broker
//! that names an approver set, so the fixture lives beside neither and is
//! reachable from both. The approved proposal and the image that holds it are
//! the crate-wide break-glass fixtures in `break_glass::gate::tests`.

pub(super) use crate::test_support::gated_break_glass_config as gated_config;
