//! Pure Produce fixtures that retain each caller's wire and transport handling.

krabka_macros::single_partition_produce_fixture!(single_partition_produce);

#[bon::builder]
pub fn replicated_topic_produce_setup(
    topic: String,
    topic_id: [u8; 16],
) -> SinglePartitionProduceSetup {
    SinglePartitionProduceSetup {
        topic,
        topic_id: krabka_protocol::primitives::uuid::Uuid(topic_id),
        ..SinglePartitionProduceSetup::replicated()
    }
}

/// The repeated one-topic form of the replicated Produce setup builder.
pub fn replicated_topic_produce_for(
    topic: &str,
    topic_id: [u8; 16],
) -> SinglePartitionProduceSetup {
    replicated_topic_produce_setup()
        .topic(topic.to_owned())
        .topic_id(topic_id)
        .call()
}

/// Verify the response cardinality before a scenario checks its partition row.
///
/// # Panics
/// Panics unless the response contains exactly one topic and one partition.
pub fn single_partition_response(
    response: &krabka_protocol::owned::produce_response::ProduceResponse,
) -> &krabka_protocol::owned::produce_response::PartitionProduceResponse {
    assert2::assert!(response.responses.len() == 1, "one topic in response");
    assert2::assert!(
        response.responses[0].partition_responses.len() == 1,
        "one partition row in response"
    );
    &response.responses[0].partition_responses[0]
}

/// The complete expected partition row after a successful non-duplicate append.
/// Kept independent from the broker's actual response construction.
pub fn expected_partition_success(
    base_offset: i64,
    log_start_offset: i64,
) -> krabka_protocol::owned::produce_response::PartitionProduceResponse {
    krabka_protocol::owned::produce_response::PartitionProduceResponse {
        index: 0,
        error_code: krabka_broker::codes::NONE,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset,
        record_errors: vec![],
        error_message: None,
        current_leader: krabka_protocol::owned::produce_response::LeaderIdAndEpoch::default(),
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    }
}

/// Build a request for one v2 batch while keeping transport and response checks with the caller.
pub fn batch_request(
    batch: krabka_protocol::records::RecordBatch,
    mut setup: SinglePartitionProduceSetup,
) -> krabka_protocol::owned::produce_request::ProduceRequest {
    setup.records = Some(batch.into());
    single_partition_produce(setup)
}

/// Build and encode one v2-batch produce request for a raw-wire test.
#[bon::builder]
pub fn encoded_batch_request(
    batch: krabka_protocol::records::RecordBatch,
    topic: String,
    #[builder(into, default)] partition: krabka_ids::PartitionIndex,
    version: i16,
) -> bytes::BytesMut {
    let request = batch_request(batch, SinglePartitionProduceSetup {
        topic,
        partition,
        ..Default::default()
    });
    let mut body = bytes::BytesMut::new();
    krabka_protocol::Encode::encode(&request, &mut body, version).expect("encode ProduceRequest");
    body
}

/// Encode one partition's Produce body for callers that do not need to adjust builder defaults.
pub fn encode_batch_request(
    batch: krabka_protocol::records::RecordBatch,
    topic: &str,
    partition: i32,
    version: i16,
) -> bytes::BytesMut {
    encoded_batch_request()
        .batch(batch)
        .topic(topic.to_owned())
        .partition(partition)
        .version(version)
        .call()
}

/// Both client transports retain their normal Produce routing and error handling.
pub trait BatchSender: Sync {
    fn send_produce_request(
        &self,
        request: krabka_protocol::owned::produce_request::ProduceRequest,
    ) -> impl std::future::Future<Output = krabka_protocol::owned::produce_response::ProduceResponse>
    + Send;
}

impl BatchSender for krabka_client_core::Client {
    async fn send_produce_request(
        &self,
        request: krabka_protocol::owned::produce_request::ProduceRequest,
    ) -> krabka_protocol::owned::produce_response::ProduceResponse {
        self.send(request).await.expect("Produce")
    }
}

impl BatchSender for krabka_client_core::Connection {
    async fn send_produce_request(
        &self,
        req: krabka_protocol::owned::produce_request::ProduceRequest,
    ) -> krabka_protocol::owned::produce_response::ProduceResponse {
        let resp = self.send(req).await;
        resp.expect("Connection Produce")
    }
}

impl<T: BatchSender + ?Sized> BatchSender for &T {
    async fn send_produce_request(
        &self,
        request: krabka_protocol::owned::produce_request::ProduceRequest,
    ) -> krabka_protocol::owned::produce_response::ProduceResponse {
        (**self).send_produce_request(request).await
    }
}

/// Send one batch with the default Produce diagnostic; callers keep their response oracles.
pub async fn send_batch(
    sender: &(impl BatchSender + ?Sized),
    batch: krabka_protocol::records::RecordBatch,
    setup: SinglePartitionProduceSetup,
) -> krabka_protocol::owned::produce_response::ProduceResponse {
    sender
        .send_produce_request(batch_request(batch, setup))
        .await
}

impl SinglePartitionProduceSetup {
    /// Replication scenarios allow thirty seconds for every replica to acknowledge.
    pub fn replicated_with_thirty_second_timeout() -> Self {
        Self {
            timeout: ProduceTimeoutMillis(30_000),
            ..Self::replicated()
        }
    }
}
