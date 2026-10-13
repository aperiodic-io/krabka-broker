//! KIP-368 scenarios shared by the PLAIN and SCRAM wire drivers.

use {
    crate::{
        harness,
        harness::common_imports::*,
        kafka_wire::{PlainAuthenticationSetup, ScramAuthenticationSetup, exchange},
        plain, scram,
        support::wire::{
            CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
        },
    },
    std::ops::RangeInclusive,
};

pub struct Session {
    broker: krabka_broker::BrokerHandle,
    stream: TcpStream,
    correlation: WireCorrelationId,
    mechanism: SaslMechanism,
    _dir: tempfile::TempDir,
}

impl Session {
    async fn start(mechanism: SaslMechanism, max_reauth: krabka_units::Time) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let broker = match mechanism {
            SaslMechanism::Plain | SaslMechanism::ScramSha512 => {
                harness::start_reauth_broker(dir.path(), max_reauth, mechanism).await
            }
            _ => panic!("unsupported fixture mechanism"),
        };
        let stream = TcpStream::connect(broker.listen_addr()).await.unwrap();
        Self {
            broker,
            stream,
            correlation: WireCorrelationId(0),
            mechanism,
            _dir: dir,
        }
    }

    async fn authenticate(&mut self, user: &str) -> std::io::Result<SaslAuthenticateResponse> {
        match self.mechanism {
            SaslMechanism::Plain => {
                plain::plain_authenticate(
                    &mut self.stream,
                    &mut self.correlation,
                    &PlainAuthenticationSetup {
                        client_id: "krabka-sasl-test",
                        user,
                        ..Default::default()
                    },
                )
                .await
            }
            SaslMechanism::ScramSha512 => {
                scram::scram_authenticate(
                    &mut self.stream,
                    &mut self.correlation,
                    &ScramAuthenticationSetup {
                        client_id: "krabka-sasl-test",
                        user,
                        mechanism: self.mechanism,
                        ..Default::default()
                    },
                )
                .await
            }
            _ => panic!("unsupported fixture mechanism"),
        }
    }

    async fn metadata(
        &mut self,
    ) -> std::io::Result<krabka_protocol::owned::metadata_response::MetadataResponse> {
        self.correlation.0 += 1;
        exchange()
            .stream(&mut self.stream)
            .request(&krabka_protocol::owned::metadata_request::MetadataRequest::default())
            .api_key(3)
            .version(12)
            .correlation(self.correlation.0)
            .client_id("krabka-sasl-test")
            .header(RequestHeaderEncoding::Flexible)
            .call()
            .await
    }
}

pub async fn capped_session_closes(mechanism: SaslMechanism) {
    let mut session = Session::start(mechanism, krabka_units::millis(300)).await;
    let response = session
        .authenticate("alice")
        .await
        .expect("authenticate round-trip");
    check!(response.error_code == 0);
    check!(
        (200..=300).contains(&response.session_lifetime_ms),
        "session_lifetime_ms = {}, expected the 300 ms cap",
        response.session_lifetime_ms
    );
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // A data-plane request, rather than a timer, closes the expired session.
    let _ = session.metadata().await;
    let n = harness::read_after_auth(&mut session.stream).await;
    check!(
        n == 0,
        "expected EOF after the re-auth window, got {n} bytes"
    );
    session.broker.shutdown().await;
}

pub async fn same_principal_reopens(mechanism: SaslMechanism) {
    let mut session = Session::authenticated(mechanism).await;
    let reauth = session
        .authenticate("alice")
        .await
        .expect("in-band re-auth round-trip");
    check!(reauth.error_code == 0, "in-band re-auth must succeed");
    check!(
        (29_000..=30_000).contains(&reauth.session_lifetime_ms),
        "re-auth must re-arm the window, got {}",
        reauth.session_lifetime_ms
    );
    let response = session
        .metadata()
        .await
        .expect("Metadata RPC after in-band re-auth");
    check!(!response.brokers.is_empty());
    session.broker.shutdown().await;
}

pub async fn different_principal_closes(mechanism: SaslMechanism) {
    let mut session = Session::authenticated(mechanism).await;
    let reauth = session
        .authenticate("bob")
        .await
        .expect("re-auth round-trip completes");
    check!(
        reauth.error_code == 58,
        "expected SASL_AUTHENTICATION_FAILED, got {}",
        reauth.error_code
    );
    let n = harness::read_after_auth(&mut session.stream).await;
    check!(n == 0, "expected EOF after a refused re-auth");
    session.broker.shutdown().await;
}

pub async fn repeated_expired_reauth(
    mechanism: SaslMechanism,
    max_reauth: krabka_units::Time,
    window: RangeInclusive<i64>,
) {
    let mut session = Session::start(mechanism, max_reauth).await;
    let initial = session
        .authenticate("alice")
        .await
        .expect("initial authenticate");
    check!(initial.error_code == 0);
    for round in 1..=3 {
        // Also cross Kafka's minimum one-second reauthentication interval.
        tokio::time::sleep(std::time::Duration::from_millis(1_050)).await;
        let reauth = session
            .authenticate("alice")
            .await
            .unwrap_or_else(|e| panic!("re-auth round {round} must round-trip: {e}"));
        check!(
            reauth.error_code == 0,
            "re-auth round {round} must succeed, got {} {:?}",
            reauth.error_code,
            reauth.error_message
        );
        check!(
            window.contains(&reauth.session_lifetime_ms),
            "re-auth round {round} must re-arm the window, got {}",
            reauth.session_lifetime_ms
        );
        let response = session
            .metadata()
            .await
            .unwrap_or_else(|e| panic!("Metadata after re-auth round {round}: {e}"));
        check!(!response.brokers.is_empty(), "round {round}");
    }
    session.broker.shutdown().await;
}

impl Session {
    async fn authenticated(mechanism: SaslMechanism) -> Self {
        let mut session = Self::start(mechanism, krabka_units::secs(30)).await;
        session
            .authenticate("alice")
            .await
            .expect("initial authenticate");
        session
    }
}
