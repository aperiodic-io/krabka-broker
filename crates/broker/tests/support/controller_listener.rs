//! Common imports for controller-listener wire and authorization suites.

#![allow(unused_imports)] // Each suite uses a different subset of this prelude.

pub(crate) use assert2::{assert, check};
pub(crate) use bytes::{BufMut, Bytes, BytesMut};
pub(crate) use krabka_broker::{
    BrokerConfig, BrokerHandle, authorizer::SimpleAclAuthorizer, config::InterBrokerCredentials,
};
pub(crate) use krabka_metadata::AclOperation;
pub(crate) use krabka_protocol::{Decode, Encode, UnknownTaggedFields};
