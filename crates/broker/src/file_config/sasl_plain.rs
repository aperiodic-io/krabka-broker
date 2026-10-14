//! The `[sasl_plain]` TOML shape and how it reaches the broker config.
//!
//! Kafka reads PLAIN server credentials from the JAAS `PlainLoginModule`
//! entry, one `user_<name>="<password>"` option per user, and has no dynamic
//! path for them. This section is the file-config equivalent: a path to a
//! credential file, kept out of the TOML so the secret can be a mounted
//! `Secret` rather than a config-map value.

use {
    super::FileConfigError,
    crate::config::{BrokerConfig, PlainCredentials},
};

/// TOML shape of `[sasl_plain]`. Maps to
/// [`crate::BrokerConfig::plain_credentials`].
#[krabka_macros::config_table(strict)]
pub struct FileSaslPlainConfig {
    /// Filesystem path to the SASL/PLAIN credential file: one
    /// `username=password` per line, split at the first `=`, so a password may
    /// contain `=`. Line endings are stripped. A line whose trimmed content is
    /// empty or starts with `#` is ignored. The username is trimmed of
    /// surrounding whitespace, since it is matched against the SASL authcid
    /// and padding there is a typo rather than intent; the password is stored
    /// exactly as written, including any leading or trailing whitespace, so a
    /// secret is never silently rewritten. File-based (not literal) so no
    /// password sits in the TOML; the operator mounts a `Secret` and writes
    /// the mount path here.
    pub credentials_path: Option<std::path::PathBuf>,
}

/// Parse the credential file body into username → password pairs.
fn parse_credentials(
    path: &std::path::Path,
    body: &str,
) -> Result<PlainCredentials, FileConfigError> {
    body.lines()
        .enumerate()
        .filter(|(_, line)| {
            let trimmed = line.trim();
            !trimmed.is_empty() && !trimmed.starts_with('#')
        })
        .map(|(index, line)| {
            let (user, password) = line.split_once('=').ok_or_else(|| {
                FileConfigError::InvalidConfig(format!(
                    "[sasl_plain]: {}:{}: expected user=password",
                    path.display(),
                    index + 1
                ))
            })?;
            let user = user.trim();
            if user.is_empty() {
                return Err(FileConfigError::InvalidConfig(format!(
                    "[sasl_plain]: {}:{}: empty username",
                    path.display(),
                    index + 1
                )));
            }
            Ok((user.to_owned(), password.to_owned()))
        })
        .collect()
}

/// Load `[sasl_plain] credentials_path` into
/// [`crate::BrokerConfig::plain_credentials`].
///
/// # Errors
///
/// Returns [`FileConfigError::InvalidConfig`] when the file is unreadable or
/// a non-comment line is not `user=password`.
pub(super) fn apply_sasl_plain(
    sasl_plain: Option<&FileSaslPlainConfig>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    let Some(path) = sasl_plain.and_then(|section| section.credentials_path.as_deref()) else {
        return Ok(());
    };
    let body = std::fs::read_to_string(path).map_err(|error| {
        FileConfigError::InvalidConfig(format!(
            "[sasl_plain]: failed to read credentials_path {}: {error}",
            path.display()
        ))
    })?;
    cfg.plain_credentials = parse_credentials(path, body.trim_end_matches(['\n', '\r']))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::{BrokerConfig, InterBrokerCredentials},
            file_config::{FileConfig, test_support::configured_unwrap_parse},
        },
        assert2::assert,
    };

    fn apply_credential_file(body: &str) -> Result<BrokerConfig, String> {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain-credentials");
        std::fs::write(&path, body).unwrap();
        let toml = format!(
            "[sasl_plain]\ncredentials_path = '{}'\n",
            path.display().to_string().replace('\\', "/")
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg)
            .map(|()| cfg)
            .map_err(|error| error.to_string())
    }

    #[test]
    fn parses_users_comments_and_blank_lines() {
        let cfg = apply_credential_file(
            "# operators\nadmin=admin-secret\n\nalice=pa=ss\n# trailing comment\n",
        )
        .unwrap();

        assert!(
            *cfg.plain_credentials.as_map()
                == maplit::hashmap! {
                    "admin".to_string() => "admin-secret".to_string(),
                    "alice".to_string() => "pa=ss".to_string(),
                }
        );
    }

    #[yare::parameterized(
        padded_password = {
            "pad=  secret  ",
            maplit::hashmap! { "pad".to_string() => "  secret  ".to_string() }
        },
        tabbed_password = {
            "tabbed=\tsecret\t",
            maplit::hashmap! { "tabbed".to_string() => "\tsecret\t".to_string() }
        },
        password_with_equals = {
            "spacey=  pa=ss  ",
            maplit::hashmap! { "spacey".to_string() => "  pa=ss  ".to_string() }
        },
        padded_username = {
            "  admin  =  secret  ",
            maplit::hashmap! { "admin".to_string() => "  secret  ".to_string() }
        },
        empty_password = {
            "empty=",
            maplit::hashmap! { "empty".to_string() => String::new() }
        },
        whitespace_password = {
            "blank=   ",
            maplit::hashmap! { "blank".to_string() => "   ".to_string() }
        },
        comment_and_whitespace = {
            "   \t  \n\t# indented comment\n  admin=secret\n",
            maplit::hashmap! { "admin".to_string() => "secret".to_string() }
        },
    )]
    fn preserves_password_whitespace_and_trims_the_username(
        body: &str,
        expected: std::collections::HashMap<String, String>,
    ) {
        let cfg = apply_credential_file(body).unwrap();
        assert2::assert!(*cfg.plain_credentials.as_map() == expected, "{body:?}");
    }

    #[test]
    fn absent_section_leaves_the_table_empty() {
        let cfg = configured_unwrap_parse("").unwrap();

        assert!(cfg.plain_credentials.is_empty());
    }

    #[yare::parameterized(
        unpadded = { "admin=secret\nnonsense\n" },
        whitespace_padded = { "admin=secret\n  nonsense  \n" },
    )]
    fn rejects_a_malformed_line(body: &str) {
        let message = apply_credential_file(body).expect_err("a line without `=` is rejected");
        assert2::assert!(message.contains("expected user=password"), "{message}");
    }

    #[yare::parameterized(
        no_leading_whitespace = { "=secret\n" },
        leading_spaces = { "   =secret\n" },
        leading_tab_and_padded_password = { "\t=  secret  \n" },
    )]
    fn rejects_an_empty_username(body: &str) {
        let message = apply_credential_file(body).expect_err("an empty username is rejected");
        assert2::assert!(message.contains("empty username"), "{message}");
    }

    #[test]
    fn rejects_an_unreadable_path() {
        let file: FileConfig =
            toml::from_str("[sasl_plain]\ncredentials_path = '/no/such/krabka/plain'\n").unwrap();
        let mut cfg = BrokerConfig::default();

        let error = file
            .apply_to(&mut cfg)
            .expect_err("a missing credential file is rejected");
        assert!(
            error
                .to_string()
                .contains("failed to read credentials_path")
        );
    }

    #[test]
    fn debug_prints_usernames_but_no_passwords() {
        let mut cfg = apply_credential_file("admin=admin-secret\n").unwrap();
        cfg.inter_broker_credentials = Some(InterBrokerCredentials::Plain {
            username: "broker".to_string(),
            password: "broker-secret".to_string(),
        });

        let rendered = format!("{cfg:?}");

        assert!(rendered.contains("admin"));
        assert!(!rendered.contains("admin-secret"));
        assert!(rendered.contains("broker"));
        assert!(!rendered.contains("broker-secret"));
        assert!(rendered.contains("<redacted>"));
    }
}
