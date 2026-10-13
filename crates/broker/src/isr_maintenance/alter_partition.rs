//! Delivery of an ISR proposal to the active controller. It finds the
//! CONTROLLER endpoint of the active controller, sends the request on a
//! short-lived connection, and classifies the response. The scan loop decides
//! what to propose. This module only reaches the controller.

use {
    super::request_builder::{IsrChange, build_alter_partition_request},
    crate::{
        codes::NOT_CONTROLLER,
        controller_endpoint::{ControllerDialer, LeaderEndpoint, current_leader_endpoint},
        host_port::HostPort,
        metadata_source::MetadataSource,
    },
    krabka_protocol::owned::{
        alter_partition_request::AlterPartitionRequest,
        alter_partition_response::AlterPartitionResponse,
    },
    std::sync::Arc,
    tracing::{debug, warn},
};

/// Who sends an ISR proposal, and how it reaches the active controller.
pub(super) struct ControllerLink<'a> {
    pub(super) controller: &'a Arc<dyn MetadataSource>,
    pub(super) broker_id: i32,
    pub(super) dialer: &'a ControllerDialer,
}

/// Sends one ISR proposal to the active controller.
///
/// Kafka's `AlterPartitionManager` sends `AlterPartition` only to the active
/// controller, on its CONTROLLER listener. This function finds that endpoint
/// with [`leader_endpoint`], and it dials the endpoint through the dialer of
/// `link`. The dialer runs the TLS and SASL that the listener is configured
/// for. A controller-only node has no broker registration, so
/// `image.broker()` cannot find it.
///
/// The function sends one request and does not try a different node. When
/// the controller answers `NOT_CONTROLLER`, or the connection fails, the next
/// scan proposes the change again to the active controller of that time.
///
/// It returns `Err` in these cases:
/// - the image holds no controller leader
/// - neither the voter set nor the configured quorum names the controller
///   endpoint of the leader
/// - the connection, the send, or the receive failed
/// - the response carries a non-zero error code
#[tracing::instrument(
    name = "isr_send_alter_partition",
    level = "info",
    skip_all,
    fields(
        topic = %change.topic,
        partition = change.partition,
        leader_epoch = change.leader_epoch,
        partition_epoch = change.partition_epoch,
        new_isr_len = change.new_isr.len(),
    ),
    err,
)]
pub(super) async fn send_alter_partition(
    link: &ControllerLink<'_>,
    change: &IsrChange<'_>,
) -> Result<(), String> {
    let ControllerLink {
        controller,
        broker_id,
        dialer,
    } = *link;
    let LeaderEndpoint {
        leader_id,
        image,
        endpoint: HostPort { host, port },
    } = current_leader_endpoint(controller.as_ref(), &dialer.quorum_voters)?;

    let req = build_alter_partition_request(&image, broker_id, change);
    match send_alter_partition_to(dialer, broker_id, &host, port, req).await {
        Ok(()) => {
            debug!(controller = leader_id.0, "AlterPartition proposed");
            Ok(())
        }
        Err(AlterPartitionSendError::NotController) => Err(format!(
            "controller {leader_id} is not the active controller"
        )),
        Err(AlterPartitionSendError::Rejected {
            global_err,
            part_err,
        }) => {
            warn!(
                controller = leader_id.0,
                global_error_code = global_err,
                partition_error_code = part_err,
                "AlterPartition rejected by controller"
            );
            Err(format!(
                "AlterPartition rejected: global={global_err} partition={part_err}"
            ))
        }
        Err(AlterPartitionSendError::Transport(error)) => {
            Err(format!("controller {leader_id} ({host}:{port}): {error}"))
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum AlterPartitionSendError {
    NotController,
    Rejected { global_err: i16, part_err: i16 },
    Transport(String),
}

/// Sends one `AlterPartition` to the controller listener at `host:port`, and
/// classifies the response.
///
/// A Kafka controller advertises `AlterPartition` in the `ApiVersions` table
/// of its controller listener, and so does a krabka controller. So
/// `Connection::send` negotiates the version from that table. A peer that
/// does not advertise the key fails the send with `IncompatibleVersion`, as a
/// Kafka broker fails it with `UnsupportedVersionException`.
async fn send_alter_partition_to(
    dialer: &ControllerDialer,
    broker_id: i32,
    host: &str,
    port: u16,
    req: AlterPartitionRequest,
) -> Result<(), AlterPartitionSendError> {
    let connection = dialer
        .outbound_client
        .connect_as_connection(
            host,
            port,
            dialer.listener_protocol,
            &dialer.server_name,
            krabka_client_core::ConnectionOptions {
                client_id: format!("krabka-broker-{broker_id}-isr"),
                ..krabka_client_core::ConnectionOptions::default()
            },
        )
        .await
        .map_err(|e| AlterPartitionSendError::Transport(format!("connect: {e}")))?;
    let sent = connection.send(req).await;
    connection.close();
    let resp = sent.map_err(|e| AlterPartitionSendError::Transport(format!("send: {e}")))?;
    let ResponseErrors {
        global_err,
        part_err,
    } = alter_partition_response_errors(&resp);
    classify_alter_partition_response(global_err, part_err)
}

/// The error codes an `AlterPartition` response carries: its own, and the
/// first partition's.
struct ResponseErrors {
    global_err: i16,
    part_err: i16,
}

fn alter_partition_response_errors(resp: &AlterPartitionResponse) -> ResponseErrors {
    let part_err = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(0, |p| p.error_code);
    ResponseErrors {
        global_err: resp.error_code,
        part_err,
    }
}

fn classify_alter_partition_response(
    global_err: i16,
    part_err: i16,
) -> Result<(), AlterPartitionSendError> {
    if is_not_controller_response(global_err, part_err) {
        return Err(AlterPartitionSendError::NotController);
    }
    if global_err != 0 || part_err != 0 {
        return Err(AlterPartitionSendError::Rejected {
            global_err,
            part_err,
        });
    }
    Ok(())
}

fn is_not_controller_response(global_err: i16, part_err: i16) -> bool {
    global_err == NOT_CONTROLLER || part_err == NOT_CONTROLLER
}

#[cfg(test)]
mod tests;
