//! Share-state admission and global denial rows without nested macro expansion.

use {
    crate::meta::broker_handler_signature,
    moxy::{
        ast::{Parse, ParseError, Parser, Path, Token},
        token::{Ident, TokenStream},
    },
};

pub(crate) fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let request = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let response = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let result = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let partition = Path::parse(&parser)?;
    <Token![,]>::parse(&parser)?;
    let serve = Ident::parse(&parser)?;
    if !parser.is_empty() {
        return Err(parser.error("expected four wire types and one serving function name"));
    }
    let signature = broker_handler_signature(&request, &response);
    Ok(moxy::template! {
        /// Checks `ClusterAction` on the cluster, then serves the request.
        ///
        #[doc = concat!("Kafka's `KafkaApis` answers a denied principal with `", stringify!({{ response }}), ".toGlobalErrorResponse`: `CLUSTER_AUTHORIZATION_FAILED` on every requested partition, and the share coordinator does not run.")]
        {{ signature }} {
            Ok(if super::cluster_action_denied(broker, ctx) {
                {{ response }} {
                    results: req.topics.iter().map(|topic| {{ result }} {
                        topic_id: topic.topic_id,
                        partitions: topic.partitions.iter().map(|partition| {{ partition }} {
                            partition: partition.partition,
                            error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
                            error_message: Some(super::CLUSTER_AUTHORIZATION_FAILED_MESSAGE.to_string()),
                            ..Default::default()
                        }).collect(),
                        ..Default::default()
                    }).collect(),
                    ..Default::default()
                }
            } else {
                {{ serve }}(
                    &broker.share_coordinator,
                    &broker.controller.current_image(),
                    req,
                ).await
            })
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, assert2::assert};

    #[test]
    fn rejects_missing_arguments_invalid_types_and_extra_tokens() {
        for input in [
            "",
            "Request, Response, TopicRow, PartitionRow",
            "Request Response, TopicRow, PartitionRow, serve",
            "42, Response, TopicRow, PartitionRow, serve",
            "Request, Response, TopicRow, PartitionRow, 42",
            "Request, Response, TopicRow, PartitionRow, serve, extra",
        ] {
            assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
        }
    }

    #[test]
    fn emits_an_async_handler_with_direct_admission_and_operation_calls() {
        let output = expand(
            &"Request, protocol::Response, TopicRow, PartitionRow, serve"
                .parse()
                .unwrap(),
        )
        .unwrap();
        let rendered = output.to_string();
        assert!(rendered.contains("async fn handle"));
        assert!(!rendered.contains("macro_rules"));
        assert!(!rendered.contains("cluster_authorization_failed"));
        assert!(rendered.contains("CLUSTER_AUTHORIZATION_FAILED_MESSAGE"));
    }
}
