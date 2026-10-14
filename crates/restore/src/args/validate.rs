//! The flag combinations clap cannot express, checked after parsing.
//!
//! A backend sub-flag such as `--archive-s3-region` belongs to the backend its
//! `--archive-*` flag selected, and a bound belongs to a partition of a topic
//! `--topic` selects; neither is a constraint clap's `ArgGroup` can state, so
//! both are checked here and reported as `RestoreError::InvalidArgument`. The
//! backend rule is `krabka_object_store::ArchiveArgs::validate`, the one
//! `krabka backup` applies to the same flags.

use {
    super::{PartitionRef, RestoreArgs},
    crate::error::RestoreError,
};

impl RestoreArgs {
    /// Check the flag combinations clap cannot express.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError::InvalidArgument`] when a backend sub-flag names
    /// a backend that `--archive-*` did not select, when one partition carries
    /// two `--to-offset` bounds, or when a bound names a topic that `--topic`
    /// excludes. Each means the operator wrote a flag that can never apply.
    pub fn validate(&self) -> Result<(), RestoreError> {
        self.archive
            .location
            .validate()
            .map_err(|error| RestoreError::InvalidArgument(error.to_string()))?;

        if self.archive.worm_key_id.len() != self.archive.worm_public_key.len() {
            return Err(RestoreError::InvalidArgument(format!(
                "--worm-key-id was given {} time(s) and --worm-public-key {} time(s); they pair by position",
                self.archive.worm_key_id.len(),
                self.archive.worm_public_key.len()
            )));
        }
        if !self.worm_expect_head.is_empty() && self.archive.worm_key_id.is_empty() {
            return Err(RestoreError::InvalidArgument(
                "--worm-expect-head requires --worm-key-id and --worm-public-key".to_owned(),
            ));
        }
        let mut expected_partitions = std::collections::HashSet::new();
        for expected in &self.worm_expect_head {
            let Some((partition, head)) = expected.split_once('=') else {
                return Err(RestoreError::InvalidArgument(format!(
                    "--worm-expect-head must be PARTITION_DIR=64_HEX, got `{expected}`"
                )));
            };
            if partition.is_empty()
                || head.len() != 64
                || !head.bytes().all(|byte| byte.is_ascii_hexdigit())
            {
                return Err(RestoreError::InvalidArgument(format!(
                    "--worm-expect-head must be PARTITION_DIR=64_HEX, got `{expected}`"
                )));
            }
            if !expected_partitions.insert(partition) {
                return Err(RestoreError::InvalidArgument(format!(
                    "--worm-expect-head names `{partition}` more than once"
                )));
            }
        }

        let mut bounded: Vec<&PartitionRef> = Vec::with_capacity(self.to_offset.len());
        for bound in &self.to_offset {
            if bounded.contains(&&bound.partition) {
                return Err(RestoreError::InvalidArgument(format!(
                    "--to-offset names {} more than once",
                    bound.partition
                )));
            }
            bounded.push(&bound.partition);
        }

        for partition in super::offset_partitions(self) {
            if !self.selects_topic(&partition.topic) {
                return Err(RestoreError::InvalidArgument(format!(
                    "a bound names {partition}, which --topic does not select"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{Cli, args::test_support::args_from, error::RestoreError},
        assert2::check,
        clap::Parser as _,
    };

    #[yare::parameterized(
        s3_region = { &["--archive-s3-region", "eu-west-1"][..] },
        s3_endpoint = { &["--archive-s3-endpoint", "http://minio:9000"][..] },
        s3_access_key = { &["--archive-s3-access-key-id", "key"][..] },
        s3_secret_key = { &["--archive-s3-secret-access-key", "secret"][..] },
        s3_http = { &["--archive-s3-allow-http"][..] },
        gcs_service_account = { &["--archive-gcs-service-account-path", "/etc/sa.json"][..] },
        gcs_endpoint = { &["--archive-gcs-endpoint", "http://fake-gcs:4443"][..] },
        gcs_http = { &["--archive-gcs-allow-http"][..] },
    )]
    fn backend_sub_flags_require_their_bucket(stray: &[&str]) {
        // The archive source in `args_from` is `--archive-local`, so every one
        // of these names a backend that was not selected.
        let args = args_from(stray).expect("args");
        check!(
            matches!(args.validate(), Err(RestoreError::InvalidArgument(_))),
            "{stray:?}"
        );
    }

    #[test]
    fn backend_sub_flags_are_accepted_with_their_bucket() {
        let args = Cli::try_parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-s3-bucket",
            "backups",
            "--archive-s3-region",
            "eu-west-1",
            "--archive-s3-allow-http",
        ])
        .expect("args")
        .args;
        check!(args.validate().is_ok());
    }

    #[test]
    fn validate_rejects_two_bounds_on_one_partition() {
        let args =
            args_from(&["--to-offset", "orders:0=10", "--to-offset", "orders:0=20"]).expect("args");
        check!(args.validate().is_err());
    }

    #[test]
    fn validate_accepts_bounds_on_distinct_partitions() {
        let args =
            args_from(&["--to-offset", "orders:0=10", "--to-offset", "orders:1=20"]).expect("args");
        check!(args.validate().is_ok());
    }

    #[test]
    fn validate_rejects_a_bound_on_an_unselected_topic() {
        let args = args_from(&["--topic", "orders", "--to-offset", "payments:0=10"]).expect("args");
        check!(args.validate().is_err());

        let args =
            args_from(&["--topic", "orders", "--exclude-offset", "payments:0=1..2"]).expect("args");
        check!(args.validate().is_err());
    }

    #[test]
    fn validate_accepts_a_bound_on_a_selected_topic() {
        let args = args_from(&["--topic", "orders", "--to-offset", "orders:0=10"]).expect("args");
        check!(args.validate().is_ok());
    }
}
