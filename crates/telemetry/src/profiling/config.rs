//! The profiling policy: the `clap`-flattened `ProfilingConfig`, the validated
//! sampling frequency it carries, and the `ProfilingError` a bad policy raises.
//!
//! The policy lives apart from the routes and the admin server so that a
//! service can parse and validate its profiling settings without building a
//! router or binding a port.

use {
    clap::Args,
    krabka_macros::RefinedNewtype,
    krabka_units::{Time, parse, per_sec, secs},
    refined_type::rule::GreaterI32,
    thiserror::Error,
};

/// Profiling configuration or admin-server failure.
#[derive(Debug, Error)]
pub enum ProfilingError {
    #[error("invalid profiling configuration: {0}")]
    Config(String),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// A positive, finite, whole-Hz sampling frequency accepted by `pprof`, whose
/// frequency input is a signed `int`.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterI32<0>),
    quantity = Frequency,
    label = "profiling sample frequency",
    getter = hertz,
    quantity_getter = frequency,
    default = per_sec(99),
    from_str
)]
pub struct ProfilingSampleFrequency(i32);

/// Process-local CPU and heap profiling policy.
#[derive(Args, Clone, Debug, PartialEq, krabka_macros::FieldDefaults)]
pub struct ProfilingConfig {
    #[arg(long, env = "KRABKA_PROFILING_CPU_DEFAULT_DURATION", default_value = "30s", value_parser = parse::positive_time)]
    #[default(secs(30))]
    pub profiling_cpu_default_duration: Time,
    #[arg(long, env = "KRABKA_PROFILING_CPU_MAX_DURATION", default_value = "60s", value_parser = parse::positive_time)]
    #[default(secs(60))]
    pub profiling_cpu_max_duration: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILING_CPU_SAMPLE_FREQUENCY",
        default_value = "99Hz"
    )]
    pub profiling_cpu_sample_frequency: ProfilingSampleFrequency,
    #[arg(long, env = "KRABKA_PROFILING_HEAP_DEFAULT_DURATION", default_value = "5s", value_parser = parse::positive_time)]
    #[default(secs(5))]
    pub profiling_heap_default_duration: Time,
    #[arg(long, env = "KRABKA_PROFILING_HEAP_MAX_DURATION", default_value = "30s", value_parser = parse::positive_time)]
    #[default(secs(30))]
    pub profiling_heap_max_duration: Time,
    #[arg(
        long,
        env = "KRABKA_PROFILING_NATIVE_FRAME_BLOCKLIST",
        default_value = "libc,libgcc,pthread,vdso",
        value_delimiter = ','
    )]
    #[default(vec![
        "libc".to_string(),
        "libgcc".to_string(),
        "pthread".to_string(),
        "vdso".to_string(),
    ])]
    pub profiling_native_frame_blocklist: Vec<String>,
}

impl ProfilingConfig {
    /// Validate related profiling bounds.
    ///
    /// # Errors
    /// Returns an error when a default exceeds its maximum. Returns an error
    /// when a maximum is below the compatible one-second request floor.
    pub fn validate(&self) -> Result<(), String> {
        if self.profiling_cpu_default_duration > self.profiling_cpu_max_duration {
            return Err("profiling CPU default duration exceeds maximum".to_string());
        }
        if self.profiling_heap_default_duration > self.profiling_heap_max_duration {
            return Err("profiling heap default duration exceeds maximum".to_string());
        }
        if self.profiling_cpu_max_duration < secs(1) || self.profiling_heap_max_duration < secs(1) {
            return Err("profiling maximum duration must be at least 1s".to_string());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        clap::Parser,
        krabka_units::{Frequency, convert::FrequencyExt as _},
    };

    #[derive(Debug, Parser)]
    struct TestCli {
        #[command(flatten)]
        profiling: ProfilingConfig,
    }

    #[test]
    fn profiling_config_defaults_and_overrides() {
        let defaults = TestCli::parse_from(["test"]).profiling;
        assert!(defaults == ProfilingConfig::default());

        let configured = TestCli::try_parse_from([
            "test",
            "--profiling-cpu-default-duration=2s",
            "--profiling-cpu-max-duration=3s",
            "--profiling-cpu-sample-frequency=101Hz",
            "--profiling-heap-default-duration=4s",
            "--profiling-heap-max-duration=5s",
            "--profiling-native-frame-blocklist=libc,custom",
        ])
        .expect("valid profiling policy")
        .profiling;
        assert!(configured.profiling_cpu_default_duration == secs(2));
        assert!(configured.profiling_cpu_max_duration == secs(3));
        assert!(
            configured.profiling_cpu_sample_frequency.frequency() == Frequency::from_per_sec(101.0)
        );
        assert!(configured.profiling_heap_default_duration == secs(4));
        assert!(configured.profiling_heap_max_duration == secs(5));
        assert!(configured.profiling_native_frame_blocklist == ["libc", "custom"]);
        assert!(configured.validate().is_ok());
    }

    #[test]
    fn profiling_config_rejects_invalid_values_and_bounds() {
        for argument in [
            "--profiling-cpu-default-duration=0s",
            "--profiling-cpu-max-duration=-1s",
            "--profiling-cpu-sample-frequency=0Hz",
            "--profiling-cpu-sample-frequency=1.5Hz",
            "--profiling-heap-default-duration=0s",
            "--profiling-heap-max-duration=-1s",
        ] {
            assert!(TestCli::try_parse_from(["test", argument]).is_err());
        }

        let cpu_bounds = TestCli::parse_from([
            "test",
            "--profiling-cpu-default-duration=2s",
            "--profiling-cpu-max-duration=1s",
        ]);
        assert!(cpu_bounds.profiling.validate().is_err());

        let heap_bounds = TestCli::parse_from([
            "test",
            "--profiling-heap-default-duration=2s",
            "--profiling-heap-max-duration=1s",
        ]);
        assert!(heap_bounds.profiling.validate().is_err());
    }
}
