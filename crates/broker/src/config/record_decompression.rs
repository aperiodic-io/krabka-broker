//! The Kafka record decompression policy the broker derives from its
//! configured ratio and output bounds, which guard the produce path against
//! a decompression bomb.

use {
    crate::{BrokerError, config::BrokerConfig},
    krabka_compression::RecordDecompressionPolicy,
};

impl BrokerConfig {
    /// Builds the validated Kafka record decompression policy.
    ///
    /// # Errors
    ///
    /// Returns an invalid-runtime error when the configured values violate the
    /// fixed decompression security bounds.
    pub fn record_decompression_policy(&self) -> Result<RecordDecompressionPolicy, BrokerError> {
        RecordDecompressionPolicy::new(
            self.record_decompression_max_ratio,
            self.record_decompression_output_floor,
            self.record_decompression_output_ceiling,
        )
        .map_err(|error| {
            BrokerError::InvalidRuntimeConfig(format!(
                "record_decompression policy is invalid: {error}"
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{ByteSize, convert::ByteSizeExt, fraction, gibibytes, mebibytes},
    };

    #[test]
    fn record_decompression_defaults_match_shared_policy() {
        let cfg = BrokerConfig::default();
        assert!(
            cfg.record_decompression_policy().unwrap()
                == krabka_compression::RecordDecompressionPolicy::default()
        );
    }

    #[test]
    fn record_decompression_builds_custom_policy() {
        let cfg = BrokerConfig {
            record_decompression_max_ratio: fraction(50.0),
            record_decompression_output_floor: mebibytes(32),
            record_decompression_output_ceiling: mebibytes(512),
            ..BrokerConfig::default()
        };
        let policy = cfg.record_decompression_policy().unwrap();
        assert!(policy.max_ratio() == fraction(50.0));
        assert!(policy.output_floor() == mebibytes(32));
        assert!(policy.output_ceiling() == mebibytes(512));
    }

    #[yare::parameterized(
        ratio_over_limit = {
            BrokerConfig {
                record_decompression_max_ratio: fraction(101.0),
                ..BrokerConfig::default()
            },
        },
        floor_over_ceiling = {
            BrokerConfig {
                record_decompression_output_floor: gibibytes(1),
                record_decompression_output_ceiling: mebibytes(16),
                ..BrokerConfig::default()
            },
        },
        ceiling_over_limit = {
            BrokerConfig {
                record_decompression_output_ceiling: gibibytes(2),
                ..BrokerConfig::default()
            },
        },
        fractional_floor = {
            BrokerConfig {
                record_decompression_output_floor: ByteSize::from_bytes_f64(1.5),
                ..BrokerConfig::default()
            },
        },
    )]
    fn record_decompression_rejects_invalid_security_bounds(cfg: BrokerConfig) {
        let error = cfg.validate().expect_err("invalid policy must fail");
        assert2::assert!(error.to_string().contains("record_decompression"));
    }
}
