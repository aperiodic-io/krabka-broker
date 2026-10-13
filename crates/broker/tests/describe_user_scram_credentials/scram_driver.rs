//! The typed driver for `DescribeUserScramCredentials` (`api_key` 50): it
//! authenticates, encodes the request, and flattens the response into the
//! per-user rows the tests assert on.

use {
    crate::{
        CLIENT_ID,
        kafka_wire::{self, PlainAuthenticationSetup},
        support::wire::HeaderEncoding as RequestHeaderEncoding,
    },
    std::net::SocketAddr,
};

krabka_macros::scram_users_fixture!(describe_request);

/// Drives `DescribeUserScramCredentials` (`api_key=50`) over a SASL/PLAIN
/// connection.
///
/// Returns `(top_level_error, per_user_rows)` where each row is
/// `(user, error_code, credential_infos)` and each `credential_info` is
/// `(mechanism, iterations)`.
pub(crate) async fn drive_describe_user_scram_credentials_sasl(
    addr: SocketAddr,
    user: &str,
    pass: &str,
    users_filter: Option<Vec<String>>,
) -> (i16, Vec<(String, i16, Vec<(i8, i32)>)>) {
    use krabka_protocol::owned::describe_user_scram_credentials_response::DescribeUserScramCredentialsResponse;

    let req = describe_request(users_filter);

    let mut stream = kafka_wire::sasl_plain_authenticate(addr, &PlainAuthenticationSetup {
        client_id: CLIENT_ID,
        user,
        password: (pass.as_bytes()).to_vec(),
    })
    .await
    .expect("SASL authenticate for DescribeUserScramCredentials");

    let resp: DescribeUserScramCredentialsResponse = kafka_wire::exchange()
        .stream(&mut stream)
        .request(&req)
        .api_key(50)
        .client_id(CLIENT_ID)
        .header(RequestHeaderEncoding::Flexible)
        .call()
        .await
        .expect("DescribeUserScramCredentials round-trip");

    let per_user: Vec<_> = resp
        .results
        .into_iter()
        .map(|r| {
            let infos: Vec<(i8, i32)> = r
                .credential_infos
                .into_iter()
                .map(|c| (c.mechanism, c.iterations))
                .collect();
            (r.user, r.error_code, infos)
        })
        .collect();

    (resp.error_code, per_user)
}
