use {
    super::*,
    crate::{ClusterGrants, ClusterOperation},
    krabka_protocol::{Encode as _, owned::describe_quorum_request::DescribeQuorumRequest},
    std::sync::atomic::{AtomicI64, AtomicUsize, Ordering},
};

struct CountingGrants(Arc<AtomicUsize>);

impl ClusterGrants for CountingGrants {
    fn allows(&self, _: ClusterOperation) -> bool {
        self.0.fetch_add(1, Ordering::Relaxed);
        true
    }

    fn cluster_authorized_operations(&self) -> i32 {
        ClusterGrants::cluster_authorized_operations(&AllowAllGrants)
    }
}

fn describe_quorum_request(correlation: i32) -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    DescribeQuorumRequest::default()
        .encode(&mut body, 2)
        .unwrap();
    let mut frame = Vec::new();
    frame.extend_from_slice(&55_i16.to_be_bytes());
    frame.extend_from_slice(&2_i16.to_be_bytes());
    frame.extend_from_slice(&correlation.to_be_bytes());
    frame.extend_from_slice(&(-1_i16).to_be_bytes());
    frame.push(0); // Flexible request header tags.
    frame.extend_from_slice(&body);
    let mut request = i32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
    request.extend_from_slice(&frame);
    request
}

#[yare::parameterized(
    api_versions_exact_deadline = { 18, Some(1_000), 999, 1_000, true },
    quorum_exact_deadline = { 55, Some(1_000), 999, 1_000, true },
    api_versions_i64_limit = { 18, Some(i64::MAX), i64::MAX - 1, i64::MAX, true },
    quorum_i64_limit = { 55, Some(i64::MAX), i64::MAX - 1, i64::MAX, true },
    api_versions_without_deadline = { 18, None, 999, i64::MAX, false },
    quorum_without_deadline = { 55, None, 999, i64::MAX, false },
    api_versions_before_deadline = { 18, Some(1_000), 999, 999, false },
    quorum_before_deadline = { 55, Some(1_000), 999, 999, false },
)]
#[test_macro(tokio::test)]
async fn credential_deadlines_close_before_bootstrap_or_acl_dispatch(
    api: i16,
    deadline: Option<i64>,
    first: i64,
    second: i64,
    closes: bool,
) {
    let (engine, _dir) = single_voter_engine();
    let calls = Arc::new(AtomicUsize::new(0));
    let clock = Arc::new(AtomicI64::new(first));
    let server_clock = Arc::clone(&clock);
    let mut ctx = context(ListenerLimits::default());
    ctx.expires_at_ms = deadline;
    ctx.grants = Arc::new(CountingGrants(Arc::clone(&calls)));
    let (mut client, server) = tokio::io::duplex(1 << 16);
    let connection = tokio::spawn(handle_conn(
        server,
        engine,
        CancellationToken::new(),
        None,
        None,
        ctx,
        move || server_clock.load(Ordering::Acquire),
    ));
    let request = if api == 18 {
        api_versions_request
    } else {
        describe_quorum_request
    };
    client.write_all(&request(1)).await.unwrap();
    assert2::assert!(read_correlation_id(&mut client).await == 1);
    let first_calls = usize::from(api == 55);
    assert2::assert!(calls.load(Ordering::Relaxed) == first_calls);

    clock.store(second, Ordering::Release);
    client.write_all(&request(2)).await.unwrap();
    if closes {
        let mut size = [0_u8; 4];
        let read = tokio::time::timeout(Duration::from_secs(1), client.read(&mut size))
            .await
            .unwrap()
            .unwrap();
        assert2::assert!(read == 0, "api={api} deadline={deadline:?} now={second}");
        assert2::assert!(calls.load(Ordering::Relaxed) == first_calls);
        // Once closed, a later clock rollback cannot revive this session.
        clock.store(0, Ordering::Release);
        assert2::assert!(client.read(&mut size).await.unwrap() == 0);
    } else {
        assert2::assert!(read_correlation_id(&mut client).await == 2);
        assert2::assert!(calls.load(Ordering::Relaxed) == first_calls * 2);
    }
    drop(client);
    tokio::time::timeout(Duration::from_secs(1), connection)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
