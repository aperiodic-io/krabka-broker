//! Fixture-pinned TLS verification, with each suite's advertised signature schemes.
use tokio_rustls::rustls::{
    DigitallySignedStruct, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    pki_types::{CertificateDer, ServerName, UnixTime},
};
pub const DEV_CERT: &str = include_str!("../fixtures/security/dev_cert.pem");
pub const DEV_KEY: &str = include_str!("../fixtures/security/dev_key.pem");
pub const DEV_CLIENT_CA: &str = include_str!("../fixtures/security/dev_client_ca.pem");
pub const DEV_CLIENT_CERT: &str = include_str!("../fixtures/security/dev_client_cert.pem");
pub const DEV_CLIENT_KEY: &str = include_str!("../fixtures/security/dev_client_key.pem");

/// RFC 2253 Subject DN of the client fixture, as Kafka's DEFAULT mapping preserves it.
pub const CLIENT_PRINCIPAL: &str = r"CN=test-client\,OU\=integration\,O\=krabka";

#[derive(Debug)]
pub struct PinnedCertVerifier {
    pub pinned: CertificateDer<'static>,
    pub schemes: Vec<SignatureScheme>,
    pub mismatch: &'static str,
}

// Both protocol versions accept the signature for this already-pinned fixture certificate.
macro_rules! accept_pinned_signature {
    ($($name:ident),+ $(,)?) => {$(
        fn $name(
            &self,
            _message: &[u8],
            _cert: &CertificateDer<'_>,
            _dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, tokio_rustls::rustls::Error> {
            Ok(HandshakeSignatureValid::assertion())
        }
    )+};
}

impl ServerCertVerifier for PinnedCertVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, tokio_rustls::rustls::Error> {
        if end_entity.as_ref() == self.pinned.as_ref() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(tokio_rustls::rustls::Error::General(self.mismatch.into()))
        }
    }

    accept_pinned_signature!(verify_tls12_signature, verify_tls13_signature);

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.schemes.clone()
    }
}

pub fn fixture_signature_schemes() -> Vec<SignatureScheme> {
    vec![
        SignatureScheme::ED25519,
        SignatureScheme::ECDSA_NISTP256_SHA256,
        SignatureScheme::ECDSA_NISTP384_SHA384,
        SignatureScheme::RSA_PSS_SHA256,
        SignatureScheme::RSA_PSS_SHA384,
        SignatureScheme::RSA_PSS_SHA512,
        SignatureScheme::RSA_PKCS1_SHA256,
        SignatureScheme::RSA_PKCS1_SHA384,
        SignatureScheme::RSA_PKCS1_SHA512,
    ]
}

/// A loopback SSL listener with the suite's certificate and client-auth policy.
pub fn ssl_config(
    log_dir: std::path::PathBuf,
    tls: krabka_security::TlsConfig,
) -> krabka_broker::BrokerConfig {
    let mut config = krabka_broker::BrokerConfig::for_tests(log_dir);
    config.listeners = vec![crate::support::listeners::loopback_listener(
        "SSL",
        krabka_security::ListenerProtocol::Ssl,
    )];
    config.inter_broker_listener_name = "SSL".to_string();
    config.tls_config = Some(tls);
    config
}
