//! The `--add-scram` spec parser.
//!
//! One flag value carries a mechanism, a user, a password, and an optional
//! iteration count in a bracketed attribute list, which is the syntax
//! `kafka-storage format` uses. Parsing it is self-contained, so it sits apart
//! from the flag definitions and from the hashing the format run performs on
//! the parsed result.

use {
    super::args::ScramSpec,
    krabka_security::{SaslMechanism, scram::MIN_SCRAM_ITERATIONS},
};

pub(super) fn parse_scram_spec(s: &str) -> Result<ScramSpec, String> {
    let s = s.trim();
    let (mechanism, body) = if let Some(rest) = s.strip_prefix("SCRAM-SHA-512=[") {
        (SaslMechanism::ScramSha512, rest)
    } else if let Some(rest) = s.strip_prefix("SCRAM-SHA-256=[") {
        (SaslMechanism::ScramSha256, rest)
    } else {
        return Err("must start with SCRAM-SHA-256=[ or SCRAM-SHA-512=[".into());
    };
    let body = body.strip_suffix(']').ok_or("must end with ]")?;
    let mut name = None;
    let mut password = None;
    let mut iterations = u32::try_from(MIN_SCRAM_ITERATIONS).expect("SCRAM minimum is positive");
    for attr in body.split(',') {
        let (k, v) = attr
            .split_once('=')
            .ok_or_else(|| format!("malformed attr: {attr}"))?;
        match k.trim() {
            "name" => name = Some(v.trim().to_string()),
            "password" => password = Some(v.trim().to_string()),
            "iterations" => {
                iterations = v.trim().parse().map_err(|e| format!("iterations: {e}"))?;
            }
            other => return Err(format!("unknown attr: {other}")),
        }
    }
    Ok(ScramSpec {
        mechanism,
        name: name.ok_or("missing name")?,
        password: password.ok_or("missing password")?,
        iterations,
    })
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parse_scram_spec_happy_path() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert2::assert!(
            spec == ScramSpec {
                mechanism: SaslMechanism::ScramSha512,
                name: "alice".to_string(),
                password: "hunter2".to_string(),
                iterations: 8192,
            }
        );
    }

    #[test]
    fn parse_scram_spec_iterations_default() {
        let spec = parse_scram_spec("SCRAM-SHA-512=[name=bob,password=p]").unwrap();
        assert2::assert!(spec.iterations == 4096);
    }

    #[test]
    fn parse_scram_spec_sha256_prefix() {
        let spec = parse_scram_spec("SCRAM-SHA-256=[name=alice,password=hunter2,iterations=8192]")
            .unwrap();
        assert2::assert!(spec.name.as_str() == "alice");
        assert2::assert!(spec.mechanism == SaslMechanism::ScramSha256);
    }

    #[test]
    fn parse_scram_spec_rejects_missing_prefix() {
        assert2::assert!(parse_scram_spec("PLAIN=[name=a,password=b]").is_err());
    }

    #[test]
    fn parse_scram_spec_rejects_missing_name() {
        assert2::assert!(parse_scram_spec("SCRAM-SHA-512=[password=p,iterations=4096]").is_err());
    }

    #[test]
    fn parse_scram_spec_rejects_unknown_attr() {
        assert2::assert!(parse_scram_spec("SCRAM-SHA-512=[name=a,password=b,foo=bar]").is_err());
    }

    #[yare::parameterized(
        missing_closing_bracket = { "SCRAM-SHA-512=[name=a,password=b" },
        invalid_iterations = { "SCRAM-SHA-512=[name=a,password=b,iterations=xx]" },
        malformed_attribute = { "SCRAM-SHA-512=[name=a,badattr]" },
        missing_password = { "SCRAM-SHA-512=[name=a,iterations=4096]" },
    )]
    fn parse_scram_spec_error_branches(bad: &str) {
        assert2::assert!(parse_scram_spec(bad).is_err());
    }
}
