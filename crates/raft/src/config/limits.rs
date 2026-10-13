//! The validated scalar policies a controller's runtime is spelled in.
//!
//! `ControllerFetchMissLimit`, `MetadataRaftCommandQueueCapacity`, and
//! `MetadataRaftFetchMax` each wrap one number that has to stay in range, so a
//! value the broker's configuration names is refused where it is parsed rather
//! than where it is used. They live apart from `ControllerConfig` because the
//! validation, the `FromStr` the configuration file goes through, and the
//! `Display` that renders the value back are a concern of their own. The
//! defaults they fall back to stay with the rest of the configuration
//! defaults, in the parent module.

use {
    super::{
        DEFAULT_CONTROLLER_FETCH_MISS_LIMIT, DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY,
        DEFAULT_METADATA_RAFT_FETCH_MAX,
    },
    krabka_macros::RefinedNewtype,
    refined_type::rule::{GreaterI32, GreaterU32, GreaterUsize},
};

#[derive(derive_more::From, derive_more::Into)]
/// The number of consecutive fetch misses a controller tolerates.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterU32<0>),
    string_error,
    label = "controller fetch miss limit",
    getter = get,
    default = DEFAULT_CONTROLLER_FETCH_MISS_LIMIT,
    from_str,
    display
)]
pub struct ControllerFetchMissLimit(u32);

#[derive(derive_more::From, derive_more::Into)]
/// The capacity of the metadata Raft command queue.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterUsize<0>),
    string_error,
    label = "metadata raft command queue capacity",
    getter = get,
    default = DEFAULT_METADATA_RAFT_COMMAND_QUEUE_CAPACITY,
    from_str,
    display
)]
pub struct MetadataRaftCommandQueueCapacity(usize);

#[derive(derive_more::From, derive_more::Into)]
/// The most bytes one metadata Raft fetch carries, a whole byte count that
/// fits the protocol's `int32` field.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterI32<0>),
    quantity = ByteSize,
    label = "metadata raft fetch max",
    getter = bytes,
    quantity_getter = size,
    default = DEFAULT_METADATA_RAFT_FETCH_MAX,
    from_str,
    display
)]
pub struct MetadataRaftFetchMax(i32);

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::{ByteSize, ByteSizeExt as _, mebibytes},
    };

    #[test]
    fn raft_runtime_policy_defaults_and_validation() {
        check!(ControllerFetchMissLimit::default().get() == 3);
        check!(ControllerFetchMissLimit::new(0).is_err());
        check!(
            "7".parse::<ControllerFetchMissLimit>()
                .expect("positive miss limit")
                .get()
                == 7
        );

        check!(MetadataRaftCommandQueueCapacity::default().get() == 256);
        check!(MetadataRaftCommandQueueCapacity::new(0).is_err());
        check!(
            "512"
                .parse::<MetadataRaftCommandQueueCapacity>()
                .expect("positive command queue capacity")
                .get()
                == 512
        );

        check!(MetadataRaftFetchMax::default().size() == mebibytes(8));
        check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(0)).is_err());
        check!(
            "4MiB"
                .parse::<MetadataRaftFetchMax>()
                .expect("positive whole-byte fetch maximum")
                .bytes()
                == 4 * 1024 * 1024
        );
        check!(MetadataRaftFetchMax::try_from(ByteSize::from_bytes_f64(1.5)).is_err());
        check!(
            MetadataRaftFetchMax::try_from(ByteSize::from_bytes_i64(i64::from(i32::MAX) + 1))
                .is_err()
        );
        check!(format!("{}", ControllerFetchMissLimit::default()) == "3");
        check!(format!("{}", MetadataRaftCommandQueueCapacity::default()) == "256");
        check!(format!("{}", MetadataRaftFetchMax::default()) == "8MiB");
    }
}
