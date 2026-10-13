//! Opaque builders retain evaluation order and every operator-chosen policy value.

use {
    assert2::assert,
    std::{cell::RefCell, time::Duration},
};

thread_local! { static EVENTS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) }; }

fn record(event: &'static str) { EVENTS.with_borrow_mut(|events| events.push(event)); }

struct Config {
    max_retries: usize,
    retry_timeout: Duration,
    allow_http: bool,
    request_timeout: Duration,
    connect_timeout: Duration,
}

#[derive(Debug, PartialEq, Eq)]
struct RetryConfig(usize, Duration);

fn retry_config(attempts: usize, timeout: Duration) -> RetryConfig {
    record("retry_config");
    RetryConfig(attempts, timeout)
}

#[derive(Debug, PartialEq, Eq)]
struct ClientOptions {
    allow_http: bool,
    request: Duration,
    connect: Duration,
}

impl ClientOptions {
    fn new() -> Self {
        record("client_new");
        Self {
            allow_http: false,
            request: Duration::ZERO,
            connect: Duration::ZERO,
        }
    }
}

fn client_options(
    mut base: ClientOptions,
    http: bool,
    request: Duration,
    connect: Duration,
) -> ClientOptions {
    record("client_options");
    base.allow_http = http;
    base.request = request;
    base.connect = connect;
    base
}

struct Builder {
    kind: &'static str,
    retry: Option<RetryConfig>,
    client: Option<ClientOptions>,
}

impl Builder {
    fn with_retry(mut self, retry: RetryConfig) -> Self {
        record("with_retry");
        self.retry = Some(retry);
        self
    }

    fn with_client_options(mut self, client: ClientOptions) -> Self {
        record("with_client_options");
        self.client = Some(client);
        self
    }
}

fn sdk_builder<const MAJOR: u8, const MINOR: u8>(kind: &'static str) -> Builder {
    record("builder");
    assert!((MAJOR, MINOR) == (1, 2));
    Builder {
        kind,
        retry: None,
        client: None,
    }
}

#[test]
fn connection_policy_preserves_opaque_builder_scope_and_method_order_for_both_sdks() {
    for kind in ["S3", "GCS"] {
        EVENTS.with_borrow_mut(Vec::clear);
        let config = Config {
            max_retries: 3,
            retry_timeout: Duration::from_secs(40),
            allow_http: true,
            request_timeout: Duration::from_secs(7),
            connect_timeout: Duration::from_secs(2),
        };
        let configured = krabka_macros::connection_policy!(
            {
                let label = if kind == "S3" { "S3" } else { "GCS" };
                sdk_builder::<1, 2>(label)
            },
            config
        );
        assert!(configured.kind == kind);
        assert!(configured.retry == Some(RetryConfig(3, Duration::from_secs(40))));
        assert!(
            configured.client
                == Some(ClientOptions {
                    allow_http: true,
                    request: Duration::from_secs(7),
                    connect: Duration::from_secs(2),
                })
        );
        EVENTS.with_borrow(|events| {
            assert!(
                *events
                    == [
                        "builder",
                        "retry_config",
                        "with_retry",
                        "client_new",
                        "client_options",
                        "with_client_options"
                    ]
            );
        });
    }
}
