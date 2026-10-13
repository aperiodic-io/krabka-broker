//! The raw wire adapter used by Krabka's extension API handlers.

macro_rules! wire_handler {
    (
        $name:ident,
        $span:literal,
        $api:literal,
        $level:literal,
        $request:ty, |
        $broker:ident,
        $version:ident,
        $req:ident,
        $ctx:ident |
        $body:block
    ) => {
        #[tracing::instrument(name = $span, level = $level, skip_all, fields(api = $api), err)]
        pub(crate) async fn $name(
            $broker: &crate::broker::Broker,
            $version: i16,
            _correlation_id: i32,
            req_bytes: &[u8],
            $ctx: &crate::handlers::RequestContext<'_>,
        ) -> Result<::bytes::Bytes, crate::error::BrokerError> {
            let mut cur = req_bytes;
            let $req = <$request as ::krabka_protocol::Decode>::decode(&mut cur, $version)?;
            $body
        }
    };
}
