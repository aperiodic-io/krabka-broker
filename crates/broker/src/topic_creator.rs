//! Sends `CreateTopics` requests from a broker to the active controller.
//!
//! This is Apache Kafka 4.3.1's `KRaftTopicCreator`, together with the retry
//! behavior of the `NodeToControllerChannelManagerImpl` "forwarding" channel
//! that it sends through:
//!
//! - [`TopicCreator::create_topic_without_principal`] sends a plain
//!   `CreateTopics` request. The controller authorizes it against the
//!   identity of this broker's own controller connection.
//! - [`TopicCreator::create_topic_with_principal`] wraps the request in a
//!   KIP-590 `Envelope` that names the client identity. The controller checks
//!   `ClusterAction` for this broker, and then authorizes the embedded request
//!   against the client identity.
//!
//! Both requests go over the network to the controller listener of the
//! active controller, also on a combined node, as Kafka's `NetworkClient`
//! does. `NodeToControllerRequestThread` sends a request again when the
//! connection fails and when the answer carries `NOT_CONTROLLER`. It gives up
//! with a timeout when the request is older than the retry timeout of the
//! channel.

#[cfg(test)]
mod tests;

use {
    crate::{
        broker::Broker,
        codes,
        controller_endpoint::{ControllerDialer, leader_endpoint},
        envelope::{self, ForwardedPrincipal, ForwardedRequest},
        handlers::RequestContext,
        metadata_source::MetadataSource,
    },
    bytes::{Bytes, BytesMut},
    krabka_client_core::{ClientError, Connection, ConnectionOptions},
    krabka_protocol::{
        Decode as _, Encode as _, ProtocolError,
        owned::{
            create_topics_request::{self, CreateTopicsRequest},
            create_topics_response::CreateTopicsResponse,
            envelope_request::EnvelopeRequest,
        },
    },
    krabka_units::{Time, convert::TimeExt as _, millis, secs},
    std::{net::IpAddr, sync::Arc},
    tokio::time::Instant,
};

/// How long a request can wait for a response before it fails with
/// [`TopicCreatorError::Timeout`].
///
/// `BrokerServer` builds the "forwarding" channel as
/// `new NodeToControllerChannelManagerImpl(controllerNodeProvider, time,
/// metrics, config, "forwarding", s"broker-${config.nodeId}-", 60000)`, and
/// `NodeToControllerRequestThread.generateRequests` fails a queued request
/// with `onTimeout` once `currentTimeMs - createdTimeMs >= retryTimeoutMs`.
pub(crate) const RETRY_TIMEOUT: Time = secs(60);

/// The pause before the next attempt after a connection failure or a
/// `NOT_CONTROLLER` answer. This is the default of Kafka's
/// `reconnect.backoff.ms`, which the channel's `NetworkClient` gets as 50.
const RECONNECT_BACKOFF: Time = millis(50);

/// The pause before the next attempt when no controller is known.
/// `NodeToControllerRequestThread.doWork` polls with a 100 ms timeout when
/// the controller node provider names no controller.
const NO_CONTROLLER_BACKOFF: Time = millis(100);

/// The default of Kafka's `controller.socket.timeout.ms`. The channel's
/// `NetworkClient` uses the smaller of this and the retry timeout as its
/// request timeout.
const CONTROLLER_SOCKET_TIMEOUT: Time = secs(30);

/// The client identity that a `CreateTopics` request sent with a principal
/// carries to the controller in a KIP-590 Envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ForwardedIdentity {
    pub principal_name: String,
    pub client_address: IpAddr,
    pub client_id: String,
    pub correlation_id: i32,
}

impl ForwardedIdentity {
    /// The identity of the request that `ctx` serves.
    pub(crate) fn of(ctx: &RequestContext<'_>) -> Self {
        Self {
            principal_name: ctx.principal.name.clone(),
            client_address: ctx.peer.ip(),
            client_id: ctx.client_id.unwrap_or_default().to_owned(),
            correlation_id: ctx.correlation_id,
        }
    }
}

/// Why a `CreateTopics` request to the controller got no response.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TopicCreatorError {
    /// No response within the retry timeout. Kafka completes the future with
    /// `TimeoutException("CreateTopicsRequest to controller timed out")`.
    #[error("CreateTopicsRequest to controller timed out")]
    Timeout,
    /// The controller refused the Envelope with this error code. Kafka
    /// completes the future with `envelopeError.exception()`.
    #[error("{}", envelope_message(*.0))]
    Envelope(i16),
    /// The request or the response could not be encoded or decoded, or the
    /// connection failed in a way that is not retried.
    #[error("{0}")]
    Protocol(String),
}

/// Kafka's message for an Envelope the controller refused with `code`.
fn envelope_message(code: i16) -> String {
    if code == codes::CLUSTER_AUTHORIZATION_FAILED {
        "Cluster authorization failed.".to_owned()
    } else {
        format!("the controller refused the envelope with error code {code}")
    }
}

impl From<ProtocolError> for TopicCreatorError {
    fn from(error: ProtocolError) -> Self { Self::Protocol(error.to_string()) }
}

/// Sends `CreateTopics` requests to the active controller.
pub(crate) struct TopicCreator {
    controller: Arc<dyn MetadataSource>,
    dialer: ControllerDialer,
    client_id: String,
    retry_timeout: Time,
}

/// What one request to the controller carries.
enum Outbound<'a> {
    Plain(&'a CreateTopicsRequest),
    Enveloped(&'a ForwardedIdentity, &'a CreateTopicsRequest),
}

/// The result of one attempt.
enum Attempt {
    /// The request is complete, with a response or with an error that is not
    /// retried.
    Done(Result<CreateTopicsResponse, TopicCreatorError>),
    /// The request must go again after `backoff`.
    Retry { reason: String, backoff: Time },
}

impl Attempt {
    fn reconnect(reason: impl Into<String>) -> Self {
        Self::Retry {
            reason: reason.into(),
            backoff: RECONNECT_BACKOFF,
        }
    }

    fn no_controller(reason: impl Into<String>) -> Self {
        Self::Retry {
            reason: reason.into(),
            backoff: NO_CONTROLLER_BACKOFF,
        }
    }
}

impl TopicCreator {
    /// A creator that dials the controller listener of the active controller
    /// as `broker` does for its heartbeat.
    pub(crate) fn new(broker: &Broker) -> Self {
        let config = &broker.config;
        Self::from_parts(
            Arc::clone(&broker.controller),
            ControllerDialer {
                outbound_client: Arc::clone(&broker.inter_broker_client),
                listener_protocol: config.controller_listener_protocol,
                server_name: config
                    .controller_server_name
                    .clone()
                    .unwrap_or_else(|| "localhost".to_owned()),
                quorum_voters: config.controller_quorum_voters.clone(),
            },
            // Kafka's channel `NetworkClient` uses
            // `String.valueOf(config.brokerId())` as its client id.
            config.broker_id.to_string(),
        )
    }

    fn from_parts(
        controller: Arc<dyn MetadataSource>,
        dialer: ControllerDialer,
        client_id: String,
    ) -> Self {
        Self {
            controller,
            dialer,
            client_id,
            retry_timeout: RETRY_TIMEOUT,
        }
    }

    /// The same creator with a shorter retry timeout, so that a test can see
    /// the timeout without a wait of [`RETRY_TIMEOUT`].
    #[cfg(test)]
    fn with_retry_timeout(mut self, retry_timeout: Time) -> Self {
        self.retry_timeout = retry_timeout;
        self
    }

    /// Kafka's `createTopicWithoutPrincipal`: sends `request` as this broker's
    /// own identity.
    pub(crate) async fn create_topic_without_principal(
        &self,
        request: CreateTopicsRequest,
    ) -> Result<CreateTopicsResponse, TopicCreatorError> {
        self.send(&Outbound::Plain(&request)).await
    }

    /// Kafka's `createTopicWithPrincipal`: wraps `request` in an Envelope that
    /// names `identity`.
    pub(crate) async fn create_topic_with_principal(
        &self,
        identity: &ForwardedIdentity,
        request: CreateTopicsRequest,
    ) -> Result<CreateTopicsResponse, TopicCreatorError> {
        self.send(&Outbound::Enveloped(identity, &request)).await
    }

    /// Sends `outbound` until it gets a response, an error that is not
    /// retried, or the retry timeout.
    async fn send(
        &self,
        outbound: &Outbound<'_>,
    ) -> Result<CreateTopicsResponse, TopicCreatorError> {
        let deadline = Instant::now() + self.retry_timeout.to_std();
        loop {
            match self.attempt(outbound, deadline).await {
                Attempt::Done(result) => return result,
                Attempt::Retry { reason, backoff } => {
                    let now = Instant::now();
                    if now >= deadline {
                        tracing::warn!(%reason, "CreateTopicsRequest to controller timed out");
                        return Err(TopicCreatorError::Timeout);
                    }
                    tracing::debug!(%reason, "CreateTopicsRequest to controller: retry");
                    // intentional: NodeToControllerRequestThread has no
                    // signal for a new controller or a healed connection. It
                    // waits out the reconnect backoff and tries again.
                    tokio::time::sleep_until(deadline.min(now + backoff.to_std())).await;
                }
            }
        }
    }

    /// One attempt: find the controller, connect to it, send, and read the
    /// answer.
    async fn attempt(&self, outbound: &Outbound<'_>, deadline: Instant) -> Attempt {
        let leader = *self.controller.watch_leader().borrow();
        let Some(leader) = leader else {
            return Attempt::no_controller("no controller leader");
        };
        let image = self.controller.current_image();
        let Some((host, port)) = leader_endpoint(&image, &self.dialer.quorum_voters, leader) else {
            return Attempt::no_controller(format!(
                "controller leader {} has no known controller endpoint",
                leader.0
            ));
        };
        let setup_deadline = deadline.min(
            Instant::now() + krabka_client_core::DEFAULT_SOCKET_CONNECTION_SETUP_TIMEOUT.to_std(),
        );
        let connected = tokio::time::timeout_at(
            setup_deadline,
            self.dialer.outbound_client.connect_as_connection(
                &host,
                port,
                self.dialer.listener_protocol,
                &self.dialer.server_name,
                self.connection_options(),
            ),
        )
        .await;
        let connection = match connected {
            Ok(Ok(connection)) => connection,
            Ok(Err(error)) => return Attempt::reconnect(format!("connect: {error}")),
            Err(_) => return Attempt::reconnect("connect timed out"),
        };
        let attempt = match outbound {
            Outbound::Plain(request) => send_plain(&connection, request).await,
            Outbound::Enveloped(identity, request) => {
                send_enveloped(&connection, identity, request).await
            }
        };
        connection.close();
        attempt
    }

    fn connection_options(&self) -> ConnectionOptions {
        let request_timeout = if CONTROLLER_SOCKET_TIMEOUT < self.retry_timeout {
            CONTROLLER_SOCKET_TIMEOUT
        } else {
            self.retry_timeout
        };
        ConnectionOptions {
            client_id: self.client_id.clone(),
            request_timeout,
            ..Default::default()
        }
    }
}

/// Sends `request` as it is, and checks the answer for `NOT_CONTROLLER`.
async fn send_plain(connection: &Connection, request: &CreateTopicsRequest) -> Attempt {
    match connection.send(request.clone()).await {
        Ok(response) if names_another_controller(&response) => {
            Attempt::reconnect("CreateTopics answered NOT_CONTROLLER")
        }
        Ok(response) => Attempt::Done(Ok(response)),
        Err(error) => send_failure(error),
    }
}

/// Wraps `request` in an Envelope that names `identity`, sends it, and
/// unwraps the answer.
async fn send_enveloped(
    connection: &Connection,
    identity: &ForwardedIdentity,
    request: &CreateTopicsRequest,
) -> Attempt {
    let version = match create_topics_version(
        connection
            .advertised_api_range(create_topics_request::API_KEY)
            .map(|(min, max)| AdvertisedRange { min, max }),
    ) {
        Ok(version) => version,
        Err(error) => return Attempt::Done(Err(error)),
    };
    let envelope = match build_envelope(identity, request, version) {
        Ok(envelope) => envelope,
        Err(error) => return Attempt::Done(Err(error.into())),
    };
    let response = match connection.send(envelope).await {
        Ok(response) => response,
        Err(error) => return send_failure(error),
    };
    // Kafka's `RequestChannel` answers an Envelope whose embedded response
    // carries NOT_CONTROLLER with NOT_CONTROLLER on the Envelope itself, so
    // `NodeToControllerRequestThread` sees it in `errorCounts()`.
    if response.error_code == codes::NOT_CONTROLLER {
        return Attempt::reconnect("Envelope answered NOT_CONTROLLER");
    }
    if response.error_code != codes::NONE {
        return Attempt::Done(Err(TopicCreatorError::Envelope(response.error_code)));
    }
    let Some(response_data) = response.response_data else {
        return Attempt::Done(Err(TopicCreatorError::Protocol(
            "Got no response body for EnvelopeResponse".to_owned(),
        )));
    };
    match parse_embedded_response(&response_data, identity.correlation_id, version) {
        Ok(response) if names_another_controller(&response) => {
            Attempt::reconnect("embedded CreateTopics answered NOT_CONTROLLER")
        }
        Ok(response) => Attempt::Done(Ok(response)),
        Err(error) => Attempt::Done(Err(error)),
    }
}

/// Whether the controller said that it is not the active controller.
/// `CreateTopicsResponse.errorCounts()` counts the error code of each topic.
fn names_another_controller(response: &CreateTopicsResponse) -> bool {
    response
        .topics
        .iter()
        .any(|topic| topic.error_code == codes::NOT_CONTROLLER)
}

/// Sorts a send failure into one that goes again and one that does not.
///
/// `NodeToControllerRequestThread` sends the request again after a
/// disconnect, and the channel's `NetworkClient` reports a request timeout as
/// a disconnect. A version mismatch and an authentication failure complete
/// the request with that error.
fn send_failure(error: ClientError) -> Attempt {
    match error {
        ClientError::Disconnected | ClientError::Timeout(_) | ClientError::Io(_) => {
            Attempt::reconnect(format!("send: {error}"))
        }
        error => Attempt::Done(Err(TopicCreatorError::Protocol(error.to_string()))),
    }
}

/// The range of versions of an API that a controller advertised.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AdvertisedRange {
    min: i16,
    max: i16,
}

/// The `CreateTopics` version of an embedded request: Kafka's
/// `controllerApiVersions().latestUsableVersion(CREATE_TOPICS)`, which is the
/// highest stable version that this codec and the controller both support.
///
/// `controller` is the range the controller advertised, or `None` when it did
/// not advertise `CreateTopics`.
fn create_topics_version(controller: Option<AdvertisedRange>) -> Result<i16, TopicCreatorError> {
    let unsupported = || {
        TopicCreatorError::Protocol(format!(
            "The controller does not support CREATE_TOPICS at a version in {}..={}",
            create_topics_request::MIN_VERSION,
            create_topics_request::LATEST_STABLE_VERSION
        ))
    };
    let AdvertisedRange {
        min: controller_min,
        max: controller_max,
    } = controller.ok_or_else(unsupported)?;
    let version = create_topics_request::LATEST_STABLE_VERSION.min(controller_max);
    if version < create_topics_request::MIN_VERSION || version < controller_min {
        return Err(unsupported());
    }
    Ok(version)
}

/// Kafka's `ForwardingManagerUtil.buildEnvelopeRequest` over
/// `createTopicsRequest.build(version).serializeWithHeader(RequestHeader(
/// CREATE_TOPICS, version, clientId, correlationId))`.
fn build_envelope(
    identity: &ForwardedIdentity,
    request: &CreateTopicsRequest,
    version: i16,
) -> Result<EnvelopeRequest, ProtocolError> {
    let mut body = BytesMut::with_capacity(request.encoded_len(version));
    request.encode(&mut body, version)?;
    let request_data = envelope::wrap_request(&ForwardedRequest {
        api_key: create_topics_request::API_KEY,
        api_version: version,
        correlation_id: identity.correlation_id,
        client_id: Some(identity.client_id.clone()),
        body: body.freeze(),
        body_flexible: create_topics_request::is_flexible(version),
    });
    envelope::envelope_request(
        request_data,
        &ForwardedPrincipal {
            name: identity.principal_name.clone(),
            token_authenticated: false,
        },
        identity.client_address,
    )
}

/// Kafka's `AbstractResponse.parseResponse(responseData, requestHeader)`:
/// strip the embedded response header, check that it echoes the correlation
/// id of the embedded request, and decode the body at `version`.
fn parse_embedded_response(
    response_data: &Bytes,
    correlation_id: i32,
    version: i16,
) -> Result<CreateTopicsResponse, TopicCreatorError> {
    let (echoed, body) = envelope::unwrap_response(
        create_topics_request::API_KEY,
        create_topics_request::is_flexible(version),
        response_data,
    )?;
    if echoed != correlation_id {
        return Err(TopicCreatorError::Protocol(format!(
            "Correlation id for response ({echoed}) does not match request ({correlation_id})"
        )));
    }
    let mut cur = body.as_ref();
    Ok(CreateTopicsResponse::decode(&mut cur, version)?)
}
