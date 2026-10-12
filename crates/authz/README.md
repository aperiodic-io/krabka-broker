# krabka-authz

[API documentation](https://krabka-io.github.io/krabka-broker/krabka-authz/krabka_authz/index.html)
[![CI](https://github.com/robot-head/crabka/actions/workflows/ci.yml/badge.svg)](https://github.com/robot-head/crabka/actions/workflows/ci.yml)

Shared Kafka-ACL authorization evaluator for the Krabka broker and gateway.

This crate is part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Kafka-compatible infrastructure and clients.

## Install

```sh
cargo add krabka-authz --path ../krabka-broker/crates/authz
```

This crate is not published by this repository. Adjust the path to your local checkout. Workspace development uses the path dependencies declared in this repository.

## Usage example

Authorize a topic operation against an ACL source:

```rust
use std::net::SocketAddr;
use krabka_authz::{AllowAllAuthorizer, AuthorizationRequest, Authorizer};
use krabka_metadata::{AclOperation, MetadataImage, ResourceType};
use krabka_security::{AuthMethod, Principal};
use uuid::Uuid;

let image = MetadataImage::new(Uuid::nil());
let host: SocketAddr = "127.0.0.1:9092".parse().unwrap();
let principal = Principal {
    name: "alice".into(),
    auth_method: AuthMethod::SaslPlain,
    groups: vec![],
};

let request = AuthorizationRequest {
    principal: &principal,
    host: &host,
    resource_type: ResourceType::Topic,
    resource_name: "orders",
    operation: AclOperation::Read,
};

let decision = AllowAllAuthorizer.authorize(&image, &request);
println!("authorization decision: {decision:?}");
```

## Documentation

Read the [krabka-authz API documentation](https://krabka-io.github.io/krabka-broker/krabka-authz/krabka_authz/index.html). The repository README contains the project-wide setup, development, and release notes.

## License

Apache-2.0. See the repository `LICENSE` and `NOTICE` files for details.
