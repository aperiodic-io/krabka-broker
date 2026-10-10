//! The `--archive-*` command-line flags, shared by the operator tools that
//! address a tiered-storage archive without a broker config.
//!
//! `krabka backup` writes captures into the archive `krabka restore` reads, so
//! both take the same flag names, the same backend rules and the same key
//! prefix handling from here. An operator does not learn two spellings for one
//! bucket, and the two tools cannot disagree about where a key lives.
//!
//! The struct sets no help heading. Each tool names the section at its
//! `#[command(flatten)]`, because the archive is a destination to one and a
//! source to the other.

use std::path::PathBuf;

use clap::{ArgGroup, Args};
use object_store::path::Path;

use crate::{GcsConfig, ObjectStoreConfig, S3Config};

/// Region used when `--archive-s3-region` is absent. AWS requires a region,
/// and `MinIO` and R2 accept this one as a placeholder.
pub const DEFAULT_S3_REGION: &str = "us-east-1";

/// A flag combination the archive flags cannot mean.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ArchiveArgsError {
    /// A backend sub-flag was given without the flag that selects its backend.
    #[error("{flag} needs {needs}")]
    OrphanFlag {
        /// The sub-flag that was given.
        flag: &'static str,
        /// The backend flag it belongs to.
        needs: &'static str,
    },
    /// No backend was selected. The parser makes one required, so this
    /// reports a caller that built [`ArchiveArgs`] by hand.
    #[error(
        "no archive backend selected: pass one of --archive-local, --archive-s3-bucket, or \
         --archive-gcs-bucket"
    )]
    NoBackend,
}

/// Where the archive is.
///
/// Exactly one backend is selected. The sub-flags of a backend are checked by
/// [`ArchiveArgs::validate`], not by clap: a mutually exclusive `ArgGroup`
/// makes clap's `requires` unenforceable, because clap treats a required
/// argument as acceptably absent when it conflicts with one that is present.
///
/// The fields are public so a test or an embedding tool can build the struct
/// directly rather than through an argv.
#[derive(Args, Debug, Default, Clone, PartialEq, Eq)]
// A flattened struct's implicit group is named after the struct, so a CLI that
// wraps this in its own `ArchiveArgs` would register the name twice.
#[group(id = "archive_location")]
#[command(group(
    ArgGroup::new("archive_backend")
        .required(true)
        .args(["local", "s3_bucket", "gcs_bucket"]),
))]
pub struct ArchiveArgs {
    /// Use a local directory tree as the archive.
    #[arg(long = "archive-local", value_name = "DIR")]
    pub local: Option<PathBuf>,

    /// Use this S3 or S3-compatible bucket as the archive.
    #[arg(long = "archive-s3-bucket", value_name = "BUCKET")]
    pub s3_bucket: Option<String>,

    /// S3 region. Defaults to `us-east-1`, which `MinIO` and R2 accept as a
    /// placeholder.
    #[arg(long = "archive-s3-region", value_name = "REGION")]
    pub s3_region: Option<String>,

    /// S3 endpoint URL, for a non-AWS S3-compatible store.
    #[arg(long = "archive-s3-endpoint", value_name = "URL")]
    pub s3_endpoint: Option<String>,

    /// S3 access key id. Without it the AWS credential chain applies.
    #[arg(long = "archive-s3-access-key-id", value_name = "ID")]
    pub s3_access_key_id: Option<String>,

    /// S3 secret access key. Without it the AWS credential chain applies.
    #[arg(long = "archive-s3-secret-access-key", value_name = "SECRET")]
    pub s3_secret_access_key: Option<String>,

    /// Allow plaintext HTTP to the S3 endpoint.
    #[arg(long = "archive-s3-allow-http")]
    pub s3_allow_http: bool,

    /// Use this Google Cloud Storage bucket as the archive.
    #[arg(long = "archive-gcs-bucket", value_name = "BUCKET")]
    pub gcs_bucket: Option<String>,

    /// Path to a GCS service-account JSON key. Without it Workload Identity or
    /// application default credentials apply.
    #[arg(long = "archive-gcs-service-account-path", value_name = "PATH")]
    pub gcs_service_account_path: Option<String>,

    /// GCS API base URL, for an emulator.
    #[arg(long = "archive-gcs-endpoint", value_name = "URL")]
    pub gcs_endpoint: Option<String>,

    /// Allow plaintext HTTP to the GCS endpoint.
    #[arg(long = "archive-gcs-allow-http")]
    pub gcs_allow_http: bool,

    /// Key prefix inside the archive, for a bucket that holds more than the
    /// tiered-storage tree. It applies to every backend.
    #[arg(long = "archive-prefix", value_name = "PREFIX")]
    pub prefix: Option<String>,
}

impl ArchiveArgs {
    /// Reject a sub-flag whose backend was not selected.
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveArgsError::OrphanFlag`] naming the first such flag and
    /// the backend flag it needs.
    pub fn validate(&self) -> Result<(), ArchiveArgsError> {
        let s3 = [
            ("--archive-s3-region", self.s3_region.is_some()),
            ("--archive-s3-endpoint", self.s3_endpoint.is_some()),
            (
                "--archive-s3-access-key-id",
                self.s3_access_key_id.is_some(),
            ),
            (
                "--archive-s3-secret-access-key",
                self.s3_secret_access_key.is_some(),
            ),
            ("--archive-s3-allow-http", self.s3_allow_http),
        ];
        let gcs = [
            (
                "--archive-gcs-service-account-path",
                self.gcs_service_account_path.is_some(),
            ),
            ("--archive-gcs-endpoint", self.gcs_endpoint.is_some()),
            ("--archive-gcs-allow-http", self.gcs_allow_http),
        ];
        for (flags, selected, needs) in [
            (&s3[..], self.s3_bucket.is_some(), "--archive-s3-bucket"),
            (&gcs[..], self.gcs_bucket.is_some(), "--archive-gcs-bucket"),
        ] {
            if selected {
                continue;
            }
            if let Some(&(flag, _)) = flags.iter().find(|(_, given)| *given) {
                return Err(ArchiveArgsError::OrphanFlag { flag, needs });
            }
        }
        Ok(())
    }

    /// Map the flags onto an object-store configuration.
    ///
    /// The prefix is not set here. It stays with the caller's archive handle,
    /// so the same handle can address a key inside the archive and list the
    /// archive root; see [`normalize_prefix`] and [`prefixed_key`].
    ///
    /// # Errors
    ///
    /// Returns [`ArchiveArgsError::NoBackend`] when no backend was selected.
    pub fn to_config(&self) -> Result<ObjectStoreConfig, ArchiveArgsError> {
        if let Some(root) = &self.local {
            return Ok(ObjectStoreConfig::Local { root: root.clone() });
        }
        if let Some(bucket) = &self.s3_bucket {
            return Ok(ObjectStoreConfig::S3(S3Config {
                bucket: bucket.clone(),
                prefix: None,
                region: self
                    .s3_region
                    .clone()
                    .unwrap_or_else(|| DEFAULT_S3_REGION.to_owned()),
                endpoint: self.s3_endpoint.clone(),
                access_key_id: self.s3_access_key_id.clone(),
                secret_access_key: self.s3_secret_access_key.clone(),
                allow_http: self.s3_allow_http,
                ..S3Config::default()
            }));
        }
        if let Some(bucket) = &self.gcs_bucket {
            return Ok(ObjectStoreConfig::Gcs(GcsConfig {
                bucket: bucket.clone(),
                prefix: None,
                service_account_path: self.gcs_service_account_path.clone(),
                endpoint: self.gcs_endpoint.clone(),
                allow_http: self.gcs_allow_http,
                ..GcsConfig::default()
            }));
        }
        Err(ArchiveArgsError::NoBackend)
    }

    /// `--archive-prefix`, through [`normalize_prefix`].
    #[must_use]
    pub fn normalized_prefix(&self) -> Option<String> {
        normalize_prefix(self.prefix.as_deref())
    }
}

/// Trim the whitespace and separators an operator copies out of a console URL,
/// so `/tier/`, `tier`, and ` tier/ ` all address the same archive root, and
/// map an empty prefix onto no prefix at all.
#[must_use]
pub fn normalize_prefix(prefix: Option<&str>) -> Option<String> {
    let trimmed = prefix?.trim().trim_matches('/');
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// The absolute object key for a path relative to the archive root, under a
/// prefix [`normalize_prefix`] already produced.
#[must_use]
pub fn prefixed_key(prefix: Option<&str>, relative: &str) -> Path {
    match prefix {
        Some(prefix) => Path::from(format!("{prefix}/{relative}")),
        None => Path::from(relative),
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::Parser;

    use super::*;

    #[derive(Parser, Debug)]
    struct Cli {
        #[command(flatten)]
        archive: ArchiveArgs,
    }

    fn parse(argv: &[&str]) -> Result<ArchiveArgs, clap::Error> {
        Cli::try_parse_from(std::iter::once("tool").chain(argv.iter().copied()))
            .map(|cli| cli.archive)
    }

    fn s3_archive_args(prefix: &str) -> ArchiveArgs {
        ArchiveArgs {
            s3_bucket: Some("backups".to_owned()),
            s3_region: Some("eu-west-1".to_owned()),
            s3_endpoint: Some("http://minio:9000".to_owned()),
            s3_access_key_id: Some("key".to_owned()),
            s3_secret_access_key: Some("secret".to_owned()),
            s3_allow_http: true,
            prefix: Some(prefix.to_owned()),
            ..ArchiveArgs::default()
        }
    }

    krabka_macros::s3_archive_config_fixture!(expected_s3_config, crate::S3Config);

    #[test]
    fn every_flag_parses_into_its_field() {
        check!(
            parse(&[
                "--archive-s3-bucket",
                "backups",
                "--archive-s3-region",
                "eu-west-1",
                "--archive-s3-endpoint",
                "http://minio:9000",
                "--archive-s3-access-key-id",
                "key",
                "--archive-s3-secret-access-key",
                "secret",
                "--archive-s3-allow-http",
                "--archive-prefix",
                "/tier/",
            ])
            .expect("args")
                == s3_archive_args("/tier/")
        );
        check!(
            parse(&[
                "--archive-gcs-bucket",
                "backups",
                "--archive-gcs-service-account-path",
                "/etc/sa.json",
                "--archive-gcs-endpoint",
                "http://fake-gcs:4443",
                "--archive-gcs-allow-http",
            ])
            .expect("args")
                == ArchiveArgs {
                    gcs_bucket: Some("backups".to_owned()),
                    gcs_service_account_path: Some("/etc/sa.json".to_owned()),
                    gcs_endpoint: Some("http://fake-gcs:4443".to_owned()),
                    gcs_allow_http: true,
                    ..ArchiveArgs::default()
                }
        );
    }

    #[test]
    fn exactly_one_backend_is_required() {
        for argv in [
            &[][..],
            &["--archive-prefix", "tier"][..],
            &["--archive-local", "/a", "--archive-s3-bucket", "b"][..],
            &["--archive-s3-bucket", "b", "--archive-gcs-bucket", "g"][..],
            &["--archive-local", "/a", "--archive-gcs-bucket", "g"][..],
        ] {
            check!(parse(argv).is_err(), "{argv:?}");
        }
        for argv in [
            &["--archive-local", "/a"][..],
            &["--archive-s3-bucket", "b"][..],
            &["--archive-gcs-bucket", "g"][..],
        ] {
            check!(parse(argv).is_ok(), "{argv:?}");
        }
    }

    #[test]
    fn every_sub_flag_needs_the_backend_it_belongs_to() {
        let cases: [(&[&str], &str, &str); 8] = [
            (
                &["--archive-s3-region", "eu-west-1"],
                "--archive-s3-region",
                "--archive-s3-bucket",
            ),
            (
                &["--archive-s3-endpoint", "http://minio:9000"],
                "--archive-s3-endpoint",
                "--archive-s3-bucket",
            ),
            (
                &["--archive-s3-access-key-id", "key"],
                "--archive-s3-access-key-id",
                "--archive-s3-bucket",
            ),
            (
                &["--archive-s3-secret-access-key", "secret"],
                "--archive-s3-secret-access-key",
                "--archive-s3-bucket",
            ),
            (
                &["--archive-s3-allow-http"],
                "--archive-s3-allow-http",
                "--archive-s3-bucket",
            ),
            (
                &["--archive-gcs-service-account-path", "/etc/sa.json"],
                "--archive-gcs-service-account-path",
                "--archive-gcs-bucket",
            ),
            (
                &["--archive-gcs-endpoint", "http://fake-gcs:4443"],
                "--archive-gcs-endpoint",
                "--archive-gcs-bucket",
            ),
            (
                &["--archive-gcs-allow-http"],
                "--archive-gcs-allow-http",
                "--archive-gcs-bucket",
            ),
        ];
        for (stray, flag, needs) in cases {
            // Each stray flag rides on the backend it does not belong to.
            let mut argv = vec!["--archive-local", "/archive"];
            argv.extend_from_slice(stray);
            let error = parse(&argv)
                .expect("clap accepts the stray flag")
                .validate()
                .expect_err("validate rejects it");
            check!(error == ArchiveArgsError::OrphanFlag { flag, needs });
            check!(error.to_string() == format!("{flag} needs {needs}"));
        }
    }

    #[test]
    fn sub_flags_of_the_selected_backend_are_accepted() {
        for argv in [
            &[
                "--archive-s3-bucket",
                "b",
                "--archive-s3-region",
                "eu-west-1",
                "--archive-s3-endpoint",
                "http://minio:9000",
                "--archive-s3-access-key-id",
                "key",
                "--archive-s3-secret-access-key",
                "secret",
                "--archive-s3-allow-http",
            ][..],
            &[
                "--archive-gcs-bucket",
                "g",
                "--archive-gcs-service-account-path",
                "/etc/sa.json",
                "--archive-gcs-endpoint",
                "http://fake-gcs:4443",
                "--archive-gcs-allow-http",
            ][..],
            &["--archive-local", "/a", "--archive-prefix", "tier"][..],
        ] {
            check!(parse(argv).expect("args").validate().is_ok(), "{argv:?}");
        }
    }

    #[test]
    fn a_local_archive_maps_to_the_local_backend() {
        let config = ArchiveArgs {
            local: Some(PathBuf::from("/archive")),
            ..ArchiveArgs::default()
        }
        .to_config()
        .expect("config");
        check!(
            matches!(config, ObjectStoreConfig::Local { root } if root == std::path::Path::new("/archive"))
        );
    }

    #[test]
    fn the_s3_flags_map_onto_the_s3_config() {
        let args = s3_archive_args("tier");
        let ObjectStoreConfig::S3(s3) = args.to_config().expect("config") else {
            panic!("the s3 bucket selects the S3 backend");
        };
        check!(s3 == expected_s3_config());
    }

    #[test]
    fn an_absent_s3_region_falls_back_to_the_placeholder() {
        let ObjectStoreConfig::S3(s3) = ArchiveArgs {
            s3_bucket: Some("backups".to_owned()),
            ..ArchiveArgs::default()
        }
        .to_config()
        .expect("config") else {
            panic!("the s3 bucket selects the S3 backend");
        };
        check!(s3.region == "us-east-1");
        check!(!s3.allow_http);
    }

    #[test]
    fn the_gcs_flags_map_onto_the_gcs_config() {
        let args = ArchiveArgs {
            gcs_bucket: Some("backups".to_owned()),
            gcs_service_account_path: Some("/etc/sa.json".to_owned()),
            gcs_endpoint: Some("http://fake-gcs:4443".to_owned()),
            gcs_allow_http: true,
            prefix: Some("tier".to_owned()),
            ..ArchiveArgs::default()
        };
        let ObjectStoreConfig::Gcs(gcs) = args.to_config().expect("config") else {
            panic!("the gcs bucket selects the GCS backend");
        };
        check!(
            gcs == GcsConfig {
                bucket: "backups".to_owned(),
                prefix: None,
                service_account_path: Some("/etc/sa.json".to_owned()),
                endpoint: Some("http://fake-gcs:4443".to_owned()),
                allow_http: true,
                ..GcsConfig::default()
            }
        );
    }

    #[test]
    fn a_hand_built_args_without_a_backend_names_the_three_that_would_do() {
        let error = ArchiveArgs::default()
            .to_config()
            .expect_err("no backend is not a default");
        check!(error == ArchiveArgsError::NoBackend);
        check!(
            error.to_string()
                == "no archive backend selected: pass one of --archive-local, \
                    --archive-s3-bucket, or --archive-gcs-bucket"
        );
    }

    #[test]
    fn prefixes_normalize_to_one_spelling() {
        let cases = [
            (None, None),
            (Some(""), None),
            (Some("/"), None),
            (Some("///"), None),
            (Some("   "), None),
            (Some("tier"), Some("tier")),
            (Some("/tier"), Some("tier")),
            (Some("tier/"), Some("tier")),
            (Some("/tier/"), Some("tier")),
            (Some(" /tier/ "), Some("tier")),
            (Some("prod/tier"), Some("prod/tier")),
        ];
        for (input, expected) in cases {
            check!(
                normalize_prefix(input).as_deref() == expected,
                "for {input:?}"
            );
        }
        let args = ArchiveArgs {
            prefix: Some("/prod/tier/".to_owned()),
            ..ArchiveArgs::default()
        };
        check!(args.normalized_prefix().as_deref() == Some("prod/tier"));
    }

    #[test]
    fn a_key_carries_the_prefix_when_there_is_one() {
        check!(
            prefixed_key(Some("prod/tier"), "restore-inputs/1/manifest.json")
                == Path::from("prod/tier/restore-inputs/1/manifest.json")
        );
        check!(
            prefixed_key(None, "restore-inputs/1/manifest.json")
                == Path::from("restore-inputs/1/manifest.json")
        );
    }
}
