//! Per-request connection metadata threaded through every inline-intercept
//! handler.

use {
    super::CorrelationId,
    crate::{
        broker::Broker,
        client_metrics::manager::ClientAttributes,
        metrics::QuotaCharge,
        quota::{
            ControllerMutationQuota, QuotaDelay, QuotaRequest, ThrottleSlot, consume_request_quota,
        },
    },
    krabka_security::Principal,
    std::net::SocketAddr,
};

/// Per-request connection metadata. Constructed once per frame in
/// `network::dispatch` from the authenticated `ConnectionAuth`, the
/// accept-time peer `SocketAddr`, and the frame's `client_id` header.
pub(crate) struct RequestContext<'a> {
    pub principal: &'a Principal,
    pub peer: &'a SocketAddr,
    /// Frame's `client_id` header: `None` when the wire field is null (`-1`
    /// length), `Some("")` when it is zero-length. Kafka's quota callback tells
    /// the two apart (a null client id resolves to no client-id quota at all,
    /// an empty one to the user levels), so the quota paths read this field
    /// as is. Every other reader treats a null id as an empty one.
    pub client_id: Option<&'a str>,
    /// The request header's `correlation_id`. A handler that sends a request
    /// on the client's behalf, such as a KIP-590 `Envelope` for topic
    /// auto-creation, names the client's request with it. It is zero unless
    /// the dispatch loop set it with [`RequestContext::with_correlation_id`].
    pub correlation_id: CorrelationId,
    /// Unique identifier for the live network connection. Share sessions use
    /// it to release acquisitions when the connection closes.
    pub connection_id: &'a str,
    /// `true` when the connection can serve the fetch records region with
    /// kernel `sendfile(2)`. That means a plaintext `TcpStream` on a
    /// SENDFILE-alias platform: Linux, Apple, FreeBSD, or `DragonFly`. The fetch
    /// handler uses this to emit the zero-copy `RecordsPayload::FileRegions`
    /// instead of `Raw` for large records runs (Increments D and E). It is
    /// `false` on TLS, on Windows, and for every non-fetch handler, which all
    /// ignore it.
    pub sendfile_capable: bool,
    /// Name of the [`crate::config::ListenerSpec`] serving this connection
    /// such as `"PLAINTEXT"`, `"SSL"`, or a configured listener name. This is
    /// the same string that self-registration writes as each
    /// [`krabka_metadata::BrokerEndpoint::name`]. Address-projecting
    /// handlers (`Metadata`, `FindCoordinator`, `DescribeCluster`) therefore
    /// advertise the endpoint that matches the listener the request arrived
    /// on, exactly as Apache Kafka does. Handlers that do not project broker
    /// addresses ignore this field.
    pub connection_listener_name: &'a str,
    /// KIP-219 throttle window for this request, filled in by whichever quota
    /// the handler charged. The dispatch loop drains it with
    /// [`RequestContext::take_throttle`] once the response is on the wire and
    /// mutes the connection for that long. Handlers that charge no quota leave
    /// it at zero.
    pub throttle: ThrottleSlot,
    /// Set by an `acks=0` `Produce` whose response carries an error on any
    /// partition. `KafkaApis.handleProduceRequest` closes the connection in
    /// that case instead of muting it, because a suppressed response is the
    /// only signal such a producer gets; the dispatch loop drains this with
    /// [`RequestContext::take_close_after_response`] once the (empty) response
    /// has been written and closes the connection instead of muting it.
    close_after_response: std::sync::atomic::AtomicBool,
    /// Size of the request as Kafka's `Request.sizeInBytes` counts it: the
    /// header and the body, without the four-byte length prefix. `Produce`
    /// charges it to `producer_byte_rate`. It is zero unless the dispatch loop
    /// set it with [`RequestContext::with_request_size`].
    pub request_size: u64,
    /// Whether the connection is still authenticating on a SASL listener.
    /// Kafka answers `ApiVersions` there from `SaslServerAuthenticator`, which
    /// runs none of the `KafkaApis` checks that need an authenticated session.
    /// It is `false` unless the dispatch loop set it with
    /// [`RequestContext::with_pre_authentication`].
    pub pre_authentication: bool,
    /// Set only by controller dispatch after authorizing and unwrapping a
    /// KIP-590 Envelope. Broker-local config preprocessing has already run.
    pub forwarded: bool,
}

/// Connection attributes a KIP-714 telemetry handler needs to match a
/// client to a subscription. Telemetry RPCs are unauthenticated, so this
/// carries no principal. It carries only the wire-derived and
/// connection-derived fields.
pub(crate) struct TelemetryContext<'a> {
    pub connection_id: &'a str,
    pub client_id: &'a str,
    pub peer: &'a std::net::SocketAddr,
    pub software_name: &'a str,
    pub software_version: &'a str,
}

impl<'a> RequestContext<'a> {
    pub(crate) fn new(
        principal: &'a Principal,
        peer: &'a SocketAddr,
        client_id: impl Into<Option<&'a str>>,
        connection_id: &'a str,
        sendfile_capable: bool,
        connection_listener_name: &'a str,
    ) -> Self {
        Self {
            principal,
            peer,
            client_id: client_id.into(),
            correlation_id: 0,
            connection_id,
            sendfile_capable,
            connection_listener_name,
            throttle: ThrottleSlot::default(),
            close_after_response: std::sync::atomic::AtomicBool::new(false),
            request_size: 0,
            pre_authentication: false,
            forwarded: false,
        }
    }

    /// Records the correlation id of the request this context serves.
    #[must_use]
    pub(crate) fn with_correlation_id(mut self, correlation_id: CorrelationId) -> Self {
        self.correlation_id = correlation_id;
        self
    }

    /// Marks the request as one that arrived before the connection finished
    /// authenticating.
    #[must_use]
    pub(crate) fn with_pre_authentication(mut self, pre_authentication: bool) -> Self {
        self.pre_authentication = pre_authentication;
        self
    }

    /// Records the size of the request frame this context serves, header and
    /// body, as Kafka's `Request.sizeInBytes` counts it.
    #[must_use]
    pub(crate) fn with_request_size(mut self, frame_len: usize) -> Self {
        self.request_size = u64::try_from(frame_len).unwrap_or(u64::MAX);
        self
    }

    /// Opens this caller's controller-mutation quota with the API version's strictness.
    pub(crate) fn controller_mutation_quota(
        &self,
        broker: &Broker,
        image: &krabka_metadata::MetadataImage,
        strict: bool,
    ) -> ControllerMutationQuota {
        ControllerMutationQuota::new(&QuotaRequest {
            image,
            buckets: &broker.quota_buckets,
            principal: self.principal.name.as_str(),
            client_id: self.client_id,
            window: broker.config.controller_mutation_quota_window,
            strict,
        })
    }

    /// Charges the request's elapsed handler time against its request quota.
    pub(crate) fn charge_request_quota(
        &self,
        broker: &Broker,
        image: &krabka_metadata::MetadataImage,
        start: std::time::Instant,
    ) -> QuotaDelay {
        let elapsed_micros = u64::try_from(start.elapsed().as_micros().min(u128::from(u64::MAX)))
            .expect("elapsed microseconds clamped to u64");
        consume_request_quota(
            image,
            &broker.quota_buckets,
            &self.principal.name,
            self.client_id,
            elapsed_micros,
            broker.config.quota_throttle_max,
        )
    }

    /// Records the KIP-219 window this request must be throttled for. The
    /// response still goes out immediately; the connection loop applies the
    /// window afterwards by muting the connection.
    pub(crate) fn record_throttle(&self, window: krabka_units::Time) {
        self.throttle.record(window);
    }

    /// Leaves a quota charge for the dispatch loop to resolve with the request
    /// quota in one metrics call, and records its window. See
    /// [`crate::quota::ThrottleSlot::defer`].
    pub(crate) fn defer_quota_charge(&self, charge: QuotaCharge) { self.throttle.defer(charge); }

    /// Drains the recorded KIP-219 window. It returns a zero extent when no
    /// quota charged this request.
    pub(crate) fn take_throttle(&self) -> krabka_units::Time { self.throttle.take() }

    /// Marks this request's connection to be closed once its (suppressed)
    /// response has been written. Only an `acks=0` `Produce` whose response
    /// carries an error on any partition calls this.
    pub(crate) fn mark_close_after_response(&self) {
        self.close_after_response
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Drains the close-after-response flag [`Self::mark_close_after_response`]
    /// set. `false` unless that call happened.
    pub(crate) fn take_close_after_response(&self) -> bool {
        self.close_after_response
            .swap(false, std::sync::atomic::Ordering::Relaxed)
    }

    /// Kafka's group coordinator stores `InetAddress::toString()`, which is
    /// the peer IP prefixed with `/` and does not include the connection port.
    pub(crate) fn client_host(&self) -> String {
        match self.peer {
            SocketAddr::V4(peer) => format!("/{}", peer.ip()),
            SocketAddr::V6(peer) => {
                let address = peer
                    .ip()
                    .segments()
                    .map(|segment| format!("{segment:x}"))
                    .join(":");
                let scope = match peer.scope_id() {
                    0 => String::new(),
                    scope_id => format!("%{scope_id}"),
                };
                format!("/{address}{scope}")
            }
        }
    }
}

impl<'a> TelemetryContext<'a> {
    pub(crate) fn new(
        connection_id: &'a str,
        peer: &'a SocketAddr,
        client_id: &'a str,
        software_name: &'a str,
        software_version: &'a str,
    ) -> Self {
        Self {
            connection_id,
            client_id,
            peer,
            software_name,
            software_version,
        }
    }

    /// The subscription identity and source fields shared by both telemetry APIs.
    pub(crate) fn client_attributes(&self, instance: uuid::Uuid) -> ClientAttributes {
        ClientAttributes {
            connection_id: self.connection_id.to_string(),
            client_instance_id: instance,
            client_id: self.client_id.to_string(),
            software_name: self.software_name.to_string(),
            software_version: self.software_version.to_string(),
            source_address: self.source_address(),
            source_port: self.peer.port(),
        }
    }

    /// The `client_source_address` a subscription matches against: Kafka's
    /// `ClientMetricsInstanceMetadata` reads `InetAddress.getHostAddress()` of
    /// the peer. That is the uncompressed hex groups of an IPv6 peer, with a
    /// `%scope` suffix when the address carries a scope, and the dotted form of
    /// an IPv4-mapped peer, which the JDK's socket layer hands out as an
    /// `Inet4Address`.
    pub(crate) fn source_address(&self) -> String {
        let host = krabka_authz::jdk_host_address(self.peer.ip());
        match self.peer {
            SocketAddr::V6(peer)
                if peer.scope_id() != 0 && peer.ip().to_ipv4_mapped().is_none() =>
            {
                format!("{host}%{}", peer.scope_id())
            }
            _ => host,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{handlers::test_support::operators_principal as principal, test_support::peer},
        assert2::assert,
    };

    #[test]
    fn request_context_new_preserves_connection_fields() {
        let principal = principal();
        let peer = peer();

        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            true,
            "SASL_SSL",
        );

        assert!(ctx.principal.name == "alice");
        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == Some("client-a"));
        assert!(ctx.connection_id == "connection-a");
        assert!(ctx.sendfile_capable);
        assert!(ctx.connection_listener_name == "SASL_SSL");
        assert!(ctx.client_host() == "/127.0.0.1");
        assert!(ctx.request_size == 0);
        assert!(ctx.correlation_id == 0);
        let ctx = ctx.with_request_size(8_300).with_correlation_id(42);
        assert!(ctx.request_size == 8_300);
        assert!(ctx.correlation_id == 42);
    }

    /// A null client id and an empty one are different requests to Kafka's
    /// quota callback (#1241), so the context keeps them apart.
    #[test]
    fn request_context_keeps_a_null_client_id_apart_from_an_empty_one() {
        let principal = principal();
        let peer = peer();

        let client_ids = [None, Some(""), Some("client-a")].map(|client_id| {
            RequestContext::new(&principal, &peer, client_id, "connection-a", false, "").client_id
        });

        assert!(client_ids == [None, Some(""), Some("client-a")]);
    }

    #[test]
    fn request_context_client_host_uses_java_ipv6_format() {
        let principal = principal();
        let peer = SocketAddr::V6(std::net::SocketAddrV6::new(
            std::net::Ipv6Addr::LOCALHOST,
            9092,
            0,
            4,
        ));

        let ctx = RequestContext::new(
            &principal,
            &peer,
            "client-a",
            "connection-a",
            false,
            "PLAINTEXT",
        );

        assert!(ctx.client_host() == "/0:0:0:0:0:0:0:1%4");
    }

    #[test]
    fn telemetry_context_new_preserves_client_identity_fields() {
        let peer = peer();

        let ctx = TelemetryContext::new("connection-a", &peer, "client-a", "krabka-test", "1.2.3");

        assert!(ctx.connection_id == "connection-a");
        assert!(ctx.peer == &peer);
        assert!(ctx.client_id == "client-a");
        assert!(ctx.software_name == "krabka-test");
        assert!(ctx.software_version == "1.2.3");
    }

    /// `client_source_address` is the JDK's `getHostAddress()` text (#1246).
    #[test]
    fn telemetry_source_address_uses_the_jdk_host_address() {
        use std::net::{Ipv4Addr, Ipv6Addr, SocketAddrV6};

        let v6 = |ip: Ipv6Addr, scope_id| SocketAddr::V6(SocketAddrV6::new(ip, 9092, 0, scope_id));
        let rows = [
            ("ipv4", SocketAddr::from(([10, 0, 0, 5], 9092)), "10.0.0.5"),
            (
                "ipv6 loopback",
                v6(Ipv6Addr::LOCALHOST, 0),
                "0:0:0:0:0:0:0:1",
            ),
            (
                "ipv6 is not zero-compressed",
                v6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 5), 0),
                "2001:db8:0:0:0:0:0:5",
            ),
            (
                "ipv6 with a scope",
                v6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1), 4),
                "fe80:0:0:0:0:0:0:1%4",
            ),
            (
                "ipv4-mapped ipv6 is dotted ipv4",
                v6(Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped(), 0),
                "1.2.3.4",
            ),
            (
                "ipv4-mapped ipv6 has no scope",
                v6(Ipv4Addr::new(1, 2, 3, 4).to_ipv6_mapped(), 4),
                "1.2.3.4",
            ),
        ];
        for (name, peer, expected) in rows {
            let ctx = TelemetryContext::new("connection-a", &peer, "client-a", "sw", "1");
            assert!(ctx.source_address() == expected, "row {name}");
        }
    }
}
