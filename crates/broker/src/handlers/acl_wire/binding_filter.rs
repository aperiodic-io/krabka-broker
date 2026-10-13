//! Kafka's `AclBindingFilter`, the filter that `DescribeAcls` and `DeleteAcls`
//! match stored ACL bindings against.
//!
//! The semantics follow apache/kafka trunk:
//!
//! - `ResourcePatternFilter.matches`: the `MATCH` pattern type (KIP-290)
//!   matches every binding that applies to the named resource. That is the
//!   `LITERAL` binding of the same name, the `LITERAL` wildcard `*` and every
//!   `PREFIXED` binding whose name is a prefix of the filter's name. `ANY` and
//!   a concrete pattern type compare the name exactly.
//! - `AccessControlEntryFilter.matches`: only a null principal or host is a
//!   wildcard. An empty string matches only an empty field.
//! - `DescribeAclsRequest.normalizeAndValidate` and
//!   `DeleteAclsRequest.normalizeAndValidate`: an `UNKNOWN` (0) byte on any
//!   enum axis fails the request at parse, which closes the connection.
//! - `ResourceType.fromCode` and its siblings: any other byte Kafka does not
//!   define becomes `UNKNOWN` after parsing and matches no binding.

use {
    super::{
        WIRE_ANY, WIRE_PATTERN_MATCH, WIRE_UNKNOWN, operation_concrete, pattern_type_concrete,
        permission_concrete, resource_type_concrete,
    },
    krabka_metadata::{AclEntry, AclOperation, PatternType, PermissionType, ResourceType},
};

/// Kafka's `ResourcePattern.WILDCARD_RESOURCE`, the `LITERAL` name that
/// applies to every resource of its type.
const WILDCARD_RESOURCE: &str = "*";

/// The error message Kafka's `AclControlManager.validateFilter` gives a
/// `DeleteAcls` filter whose resource or pattern type is `UNKNOWN`.
pub const UNKNOWN_PATTERN_FILTER_MESSAGE: &str = "Unknown patternFilter.";
/// The error message Kafka's `AclControlManager.validateFilter` gives a
/// `DeleteAcls` filter whose operation or permission type is `UNKNOWN`.
pub const UNKNOWN_ENTRY_FILTER_MESSAGE: &str = "Unknown entryFilter.";

/// One enum axis of a filter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AxisFilter<T> {
    /// `ANY`: every value matches.
    Any,
    /// A concrete value. Only that value matches.
    Exact(T),
    /// A byte that Kafka's `fromCode` maps to `UNKNOWN`. Nothing matches.
    Unknown,
}

impl<T: PartialEq> AxisFilter<T> {
    fn matches(&self, value: &T) -> bool {
        match self {
            Self::Any => true,
            Self::Exact(want) => want == value,
            Self::Unknown => false,
        }
    }

    fn is_unknown(&self) -> bool { matches!(self, Self::Unknown) }
}

/// The pattern-type axis of a filter, which has the extra `MATCH` value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternTypeFilter {
    /// `ANY`: every pattern type matches, and a name compares exactly.
    Any,
    /// `MATCH`: every binding that applies to the named resource matches.
    Match,
    /// `LITERAL` or `PREFIXED`: only that pattern type matches, and a name
    /// compares exactly.
    Exact(PatternType),
    /// A byte that Kafka's `PatternType.fromCode` maps to `UNKNOWN`.
    Unknown,
}

/// The seven wire fields that `DescribeAclsRequest` and each
/// `DeleteAclsFilter` carry, borrowed from the decoded request.
#[derive(Debug, Clone, Copy)]
pub struct WireAclBindingFilter<'a> {
    pub resource_type: i8,
    pub resource_name: Option<&'a str>,
    pub pattern_type: i8,
    pub principal: Option<&'a str>,
    pub host: Option<&'a str>,
    pub operation: i8,
    pub permission_type: i8,
}

/// A filter carried an `UNKNOWN` (0) byte on an enum axis. Kafka refuses the
/// whole request at parse, and the broker closes the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownElement;

/// Kafka's `AclBindingFilter`: a `ResourcePatternFilter` and an
/// `AccessControlEntryFilter` in one value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AclBindingFilter {
    pub resource_type: AxisFilter<ResourceType>,
    /// `None` (a null string on the wire) matches every name.
    pub resource_name: Option<String>,
    pub pattern_type: PatternTypeFilter,
    /// `None` (a null string on the wire) matches every principal.
    pub principal: Option<String>,
    /// `None` (a null string on the wire) matches every host.
    pub host: Option<String>,
    pub operation: AxisFilter<AclOperation>,
    pub permission_type: AxisFilter<PermissionType>,
}

impl AclBindingFilter {
    /// Decodes the wire fields of one filter.
    ///
    /// # Errors
    ///
    /// Returns [`UnknownElement`] when any enum axis carries the `UNKNOWN`
    /// (0) byte, which Kafka refuses at parse.
    pub fn from_wire(wire: WireAclBindingFilter<'_>) -> Result<Self, UnknownElement> {
        if [
            wire.resource_type,
            wire.pattern_type,
            wire.operation,
            wire.permission_type,
        ]
        .contains(&WIRE_UNKNOWN)
        {
            return Err(UnknownElement);
        }
        Ok(Self {
            resource_type: resource_type_axis(wire.resource_type),
            resource_name: wire.resource_name.map(str::to_owned),
            pattern_type: pattern_type_axis(wire.pattern_type),
            principal: wire.principal.map(str::to_owned),
            host: wire.host.map(str::to_owned),
            operation: operation_axis(wire.operation),
            permission_type: permission_axis(wire.permission_type),
        })
    }

    /// Kafka's `AclBindingFilter.matches`.
    #[must_use]
    pub fn matches(&self, entry: &AclEntry) -> bool {
        self.pattern_matches(entry) && self.entry_matches(entry)
    }

    /// The message Kafka's `AclControlManager.validateFilter` refuses a
    /// `DeleteAcls` filter with, or `None` when the filter is valid.
    #[must_use]
    pub fn unknown_message(&self) -> Option<&'static str> {
        if self.resource_type.is_unknown() || self.pattern_type == PatternTypeFilter::Unknown {
            Some(UNKNOWN_PATTERN_FILTER_MESSAGE)
        } else if self.operation.is_unknown() || self.permission_type.is_unknown() {
            Some(UNKNOWN_ENTRY_FILTER_MESSAGE)
        } else {
            None
        }
    }

    /// `ResourcePatternFilter.matches`.
    fn pattern_matches(&self, entry: &AclEntry) -> bool {
        if !self.resource_type.matches(&entry.resource_type) {
            return false;
        }
        let exact_name = match self.pattern_type {
            PatternTypeFilter::Unknown => return false,
            PatternTypeFilter::Exact(want) if want != entry.pattern_type => return false,
            PatternTypeFilter::Any | PatternTypeFilter::Exact(_) => true,
            PatternTypeFilter::Match => false,
        };
        let Some(name) = self.resource_name.as_deref() else {
            return true;
        };
        if exact_name {
            return name == entry.resource_name;
        }
        match entry.pattern_type {
            PatternType::Literal => {
                name == entry.resource_name || entry.resource_name == WILDCARD_RESOURCE
            }
            PatternType::Prefixed => name.starts_with(entry.resource_name.as_str()),
        }
    }

    /// `AccessControlEntryFilter.matches`.
    fn entry_matches(&self, entry: &AclEntry) -> bool {
        self.principal
            .as_deref()
            .is_none_or(|p| p == entry.principal)
            && self.host.as_deref().is_none_or(|h| h == entry.host)
            && self.operation.matches(&entry.operation)
            && self.permission_type.matches(&entry.permission_type)
    }
}

fn resource_type_axis(b: i8) -> AxisFilter<ResourceType> {
    match b {
        WIRE_ANY => AxisFilter::Any,
        _ => resource_type_concrete(b).map_or(AxisFilter::Unknown, AxisFilter::Exact),
    }
}

fn pattern_type_axis(b: i8) -> PatternTypeFilter {
    match b {
        WIRE_ANY => PatternTypeFilter::Any,
        WIRE_PATTERN_MATCH => PatternTypeFilter::Match,
        _ => pattern_type_concrete(b).map_or(PatternTypeFilter::Unknown, PatternTypeFilter::Exact),
    }
}

fn operation_axis(b: i8) -> AxisFilter<AclOperation> {
    match b {
        WIRE_ANY => AxisFilter::Any,
        _ => operation_concrete(b).map_or(AxisFilter::Unknown, AxisFilter::Exact),
    }
}

fn permission_axis(b: i8) -> AxisFilter<PermissionType> {
    match b {
        WIRE_ANY => AxisFilter::Any,
        _ => permission_concrete(b).map_or(AxisFilter::Unknown, AxisFilter::Exact),
    }
}

#[cfg(test)]
mod tests;
