use krabka_protocol::owned::api_versions_response::ApiVersion;

pub fn supported_client_apis() -> Vec<ApiVersion> {
    let mut apis = krabka_broker::api_catalog::supported_apis(
        krabka_broker::api_catalog::ListenerKind::Client,
        krabka_broker::api_catalog::ClientMetricsReceiver::Absent,
        krabka_broker::api_catalog::VersionGates::default(),
    );
    apis.sort_by_key(|api| api.api_key);
    apis
}
