//! Kafka-compatible `--command-config` security for cluster connections.
//!
//! The file is parsed into the client library's [`ClientSecurity`] rather than
//! reimplementing TLS or SASL here. Its contents can include passwords, so this
//! module never logs or formats the parsed properties or resulting policy.

use {
    crate::BackupError,
    clap::Args,
    krabka_client_core::security::{
        ClientSecurity, KeyStore, SaslCredentials, TlsConnectorConfig, TrustStore,
    },
    krabka_security::{ListenerProtocol, SaslMechanism},
    std::{collections::BTreeMap, path::PathBuf},
};

/// Kafka client properties shared by capture and offset restoration.
#[derive(Debug, Args, Clone, Default)]
pub struct SecurityArgs {
    /// Kafka client properties containing `security.protocol`, TLS and SASL.
    #[arg(long, value_name = "FILE")]
    pub command_config: Option<PathBuf>,
}

impl SecurityArgs {
    /// Build the shared client security policy.
    ///
    /// # Errors
    ///
    /// Returns [`BackupError::Io`] when the file cannot be read and
    /// [`BackupError::InvalidArgument`] when its security properties conflict.
    pub async fn load(&self) -> Result<Option<ClientSecurity>, BackupError> {
        let Some(path) = &self.command_config else {
            return Ok(None);
        };
        let text = tokio::fs::read_to_string(path).await.map_err(|source| {
            BackupError::Io(std::io::Error::new(
                source.kind(),
                format!("read command config {}: {source}", path.display()),
            ))
        })?;
        let properties = parse_properties(&text)?;
        security(&properties)
    }
}

fn parse_properties(text: &str) -> Result<BTreeMap<String, String>, BackupError> {
    let mut logical = Vec::new();
    let mut pending = String::new();
    for line in text.lines() {
        let trailing = line
            .chars()
            .rev()
            .take_while(|character| *character == '\\')
            .count();
        pending.push_str(line.trim_start());
        if trailing % 2 == 1 {
            pending.pop();
        } else {
            logical.push(std::mem::take(&mut pending));
        }
    }
    if !pending.is_empty() {
        logical.push(pending);
    }

    let mut properties = BTreeMap::new();
    for line in logical {
        let trimmed = line.trim_start();
        if trimmed.is_empty() || trimmed.starts_with(['#', '!']) {
            continue;
        }
        let split = property_split(trimmed);
        let (key, value) = trimmed.split_at(split);
        let value = value.trim_start_matches(|character: char| {
            character.is_whitespace() || character == '=' || character == ':'
        });
        properties.insert(unescape(key.trim_end())?, unescape(value)?);
    }
    Ok(properties)
}

fn property_split(line: &str) -> usize {
    let mut escaped = false;
    for (index, character) in line.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '=' || character == ':' || character.is_whitespace() {
            return index;
        }
    }
    line.len()
}

fn unescape(value: &str) -> Result<String, BackupError> {
    let mut characters = value.chars();
    let mut unescaped = String::new();
    while let Some(character) = characters.next() {
        if character != '\\' {
            unescaped.push(character);
            continue;
        }
        match characters.next() {
            Some('t') => unescaped.push('\t'),
            Some('n') => unescaped.push('\n'),
            Some('r') => unescaped.push('\r'),
            Some('u') => {
                let digits: String = characters.by_ref().take(4).collect();
                let code = u32::from_str_radix(&digits, 16).map_err(|_| {
                    BackupError::InvalidArgument("invalid unicode escape in command config".into())
                })?;
                unescaped.push(char::from_u32(code).ok_or_else(|| {
                    BackupError::InvalidArgument(
                        "invalid unicode code point in command config".into(),
                    )
                })?);
            }
            Some(other) => unescaped.push(other),
            None => unescaped.push('\\'),
        }
    }
    Ok(unescaped)
}

fn security(properties: &BTreeMap<String, String>) -> Result<Option<ClientSecurity>, BackupError> {
    let protocol = match properties
        .get("security.protocol")
        .map_or("PLAINTEXT", String::as_str)
    {
        "PLAINTEXT" => ListenerProtocol::Plaintext,
        "SSL" => ListenerProtocol::Ssl,
        "SASL_PLAINTEXT" => ListenerProtocol::SaslPlaintext,
        "SASL_SSL" => ListenerProtocol::SaslSsl,
        value => return invalid(&format!("unsupported security.protocol {value}")),
    };
    if !protocol.requires_sasl()
        && let Some(key) = properties.keys().find(|key| key.starts_with("sasl."))
    {
        return invalid(&format!("{key} requires a SASL security.protocol"));
    }
    if !protocol.requires_tls()
        && let Some(key) = properties.keys().find(|key| key.starts_with("ssl."))
    {
        return invalid(&format!("{key} requires SSL or SASL_SSL"));
    }
    if protocol == ListenerProtocol::Plaintext {
        return Ok(None);
    }

    let tls = protocol
        .requires_tls()
        .then(|| tls_config(properties))
        .transpose()?;
    let sasl = protocol
        .requires_sasl()
        .then(|| sasl_credentials(properties))
        .transpose()?;
    Ok(Some(ClientSecurity {
        protocol,
        tls,
        sasl,
        sasl_host: properties.get("sasl.kerberos.service.host").cloned(),
    }))
}

fn tls_config(properties: &BTreeMap<String, String>) -> Result<TlsConnectorConfig, BackupError> {
    for key in ["ssl.truststore.type", "ssl.keystore.type"] {
        if let Some(kind) = properties.get(key)
            && kind != "PEM"
        {
            return invalid(&format!("{key}={kind} is unsupported; expected PEM"));
        }
    }
    let key_store = match (
        properties.get("ssl.keystore.location"),
        properties.get("ssl.key.location"),
    ) {
        (Some(cert), Some(key)) => Some(KeyStore::PemFiles {
            certificate_chain: PathBuf::from(cert),
            private_key: PathBuf::from(key),
            key_password: None,
        }),
        (None, None) => None,
        _ => {
            return invalid("ssl.keystore.location and ssl.key.location must be provided together");
        }
    };
    let mut tls = TlsConnectorConfig::default();
    if let Some(location) = properties.get("ssl.truststore.location") {
        tls.trust_store = TrustStore::PemFile(PathBuf::from(location));
    }
    tls.key_store = key_store;
    tls.server_name = properties
        .get("ssl.server.name")
        .cloned()
        .unwrap_or_default();
    Ok(tls)
}

fn sasl_credentials(properties: &BTreeMap<String, String>) -> Result<SaslCredentials, BackupError> {
    let mechanism = required(properties, "sasl.mechanism")?;
    let options = properties
        .get("sasl.jaas.config")
        .map_or_default(|value| jaas_options(value));
    match mechanism.as_str() {
        "PLAIN" => Ok(SaslCredentials::Plain {
            username: required(&options, "username")?,
            password: required(&options, "password")?,
        }),
        "SCRAM-SHA-256" | "SCRAM-SHA-512" => Ok(SaslCredentials::Scram {
            mechanism: if mechanism == "SCRAM-SHA-256" {
                SaslMechanism::ScramSha256
            } else {
                SaslMechanism::ScramSha512
            },
            username: required(&options, "username")?,
            password: required(&options, "password")?,
            // Kafka's `ScramLoginModule` reads `tokenauth` with
            // `Boolean.parseBoolean`: a delegation-token login.
            delegation_token: options
                .get("tokenauth")
                .is_some_and(|value| value.eq_ignore_ascii_case("true")),
        }),
        value => invalid(&format!("unsupported sasl.mechanism {value}")),
    }
}

fn jaas_options(value: &str) -> BTreeMap<String, String> {
    let mut options = BTreeMap::new();
    let mut characters = value.trim_end_matches(';').chars().peekable();
    while let Some(character) = characters.next() {
        if character.is_whitespace() {
            continue;
        }
        let mut key = String::from(character);
        while let Some(&character) = characters.peek() {
            if character == '=' || character.is_whitespace() {
                break;
            }
            key.push(character);
            characters.next();
        }
        while characters
            .peek()
            .is_some_and(|character| character.is_whitespace())
        {
            characters.next();
        }
        if characters.next() != Some('=') {
            while characters
                .peek()
                .is_some_and(|character| !character.is_whitespace())
            {
                characters.next();
            }
            continue;
        }
        while characters
            .peek()
            .is_some_and(|character| character.is_whitespace())
        {
            characters.next();
        }
        let quote = characters
            .peek()
            .copied()
            .filter(|character| *character == '"' || *character == '\'');
        if quote.is_some() {
            characters.next();
        }
        let mut parsed = String::new();
        let mut escaped = false;
        for character in characters.by_ref() {
            if escaped {
                parsed.push(character);
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if quote == Some(character) || (quote.is_none() && character.is_whitespace()) {
                break;
            } else {
                parsed.push(character);
            }
        }
        options.insert(key, parsed);
    }
    options
}

fn required(values: &BTreeMap<String, String>, key: &str) -> Result<String, BackupError> {
    values
        .get(key)
        .cloned()
        .ok_or_else(|| BackupError::InvalidArgument(format!("command config is missing {key}")))
}

fn invalid<T>(message: &str) -> Result<T, BackupError> {
    Err(BackupError::InvalidArgument(format!(
        "invalid command config: {message}"
    )))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    #[test]
    fn properties_support_comments_escapes_and_continuations() {
        let parsed = parse_properties("! ignored\na\\:b = one\\\n  two\\u0033\n")
            .expect("valid Java properties");
        check!(parsed == BTreeMap::from([("a:b".into(), "onetwo3".into())]));
    }

    #[test]
    fn sasl_ssl_maps_to_the_shared_client_policy() {
        let properties = parse_properties(
            "security.protocol=SASL_SSL\n\
             ssl.truststore.type=PEM\n\
             ssl.truststore.location=/etc/krabka/ca.pem\n\
             ssl.server.name=broker.example\n\
             sasl.mechanism=SCRAM-SHA-512\n\
             sasl.jaas.config=x required username=backup password=not-logged;\n",
        )
        .expect("valid properties");
        let policy = security(&properties)
            .expect("valid policy")
            .expect("secured policy");

        check!(policy.protocol == ListenerProtocol::SaslSsl);
        let tls = policy.tls.expect("TLS policy");
        check!(tls.trust_store == TrustStore::PemFile(PathBuf::from("/etc/krabka/ca.pem")));
        check!(tls.server_name == "broker.example");
        assert!(let Some(SaslCredentials::Scram {
            mechanism: SaslMechanism::ScramSha512,
            username,
            password,
            delegation_token: false,
        }) = policy.sasl);
        check!(username == "backup");
        check!(password == "not-logged");
    }

    /// `tokenauth` marks a delegation-token SCRAM login, read the way Kafka's
    /// `ScramLoginModule` reads it.
    #[yare::parameterized(
        absent = { "x required username=id password=hmac;", false },
        true_lowercase = { "x required username=id password=hmac tokenauth=true;", true },
        true_uppercase = { "x required username=id password=hmac tokenauth=TRUE;", true },
        false_value = { "x required username=id password=hmac tokenauth=false;", false },
    )]
    fn tokenauth_marks_a_delegation_token_login(jaas: &str, expected: bool) {
        let properties = parse_properties(&format!(
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism=SCRAM-SHA-256\n\
             sasl.jaas.config={jaas}\n"
        ))
        .expect("valid properties");
        let policy = security(&properties)
            .expect("valid policy")
            .expect("secured policy");
        assert2::assert!(
            let Some(SaslCredentials::Scram {
                delegation_token, ..
            }) = policy.sasl,
            "{jaas}"
        );
        check!(delegation_token == expected, "{jaas}");
    }

    #[test]
    fn invalid_config_errors_do_not_echo_credentials() {
        let properties = parse_properties(
            "security.protocol=SASL_PLAINTEXT\n\
             sasl.mechanism=SCRAM-SHA-256\n\
             sasl.jaas.config=x required password=hunter2;\n",
        )
        .expect("valid properties");
        let error = security(&properties)
            .expect_err("missing username must fail")
            .to_string();
        check!(!error.contains("hunter2"), "got: {error}");
        check!(error.contains("username"), "got: {error}");
    }

    #[test]
    fn tls_refuses_non_pem_stores() {
        let properties = parse_properties(
            "security.protocol=SSL\nssl.truststore.type=JKS\nssl.truststore.location=a.jks\n",
        )
        .expect("valid properties");
        let error = security(&properties)
            .expect_err("the shared client reads PEM, not JKS")
            .to_string();
        check!(error.contains("expected PEM"), "got: {error}");
    }

    #[yare::parameterized(
        sasl_mechanism = { "sasl.mechanism", "SCRAM-SHA-512" },
        tls_location = { "ssl.truststore.location", "/etc/krabka/ca.pem" },
    )]
    fn security_properties_cannot_be_silently_ignored(property: &str, value: &str) {
        let properties = BTreeMap::from([(property.into(), value.into())]);
        let error = security(&properties)
            .expect_err("PLAINTEXT must reject secured-client properties")
            .to_string();
        check!(error.contains(property), "got: {error}");
    }

    #[test]
    fn tls_sni_defaults_to_each_connection_target() {
        let properties =
            parse_properties("security.protocol=SSL\nssl.truststore.location=/etc/krabka/ca.pem\n")
                .expect("valid properties");
        let policy = security(&properties)
            .expect("valid policy")
            .expect("secured policy");
        check!(policy.tls.expect("TLS policy").server_name.is_empty());
    }
}
