use std::path::PathBuf;

pub(crate) fn broker_config(keytab_path: PathBuf, kdc: String) -> krabka_broker::GssapiConfig {
    krabka_broker::GssapiConfig {
        keytab_path,
        service_name: "kafka".to_string(),
        principal_to_local_rules: vec![krabka_broker::KerberosRule::Default],
        realm: Some("CRABKA.TEST".to_string()),
        kdc: Some(kdc),
        max_time_skew: krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW,
    }
}
