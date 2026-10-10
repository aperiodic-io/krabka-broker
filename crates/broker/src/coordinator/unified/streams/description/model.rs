//! Kafka's `StreamsGroupTopologyDescription`, and its
//! `StreamsGroupTopologyDescriptionConverter` to and from the wire.
//!
//! A push carries every field on every node. The broker keeps only the fields
//! of the node's type, as Kafka's converter does: a source keeps its topics, a
//! processor its stores, a sink its topic. Each list keeps the first of
//! repeated names in wire order, as Kafka's `LinkedHashSet` keeps them, so a
//! describe answers the description Kafka would store, not the bytes the
//! client sent.

use krabka_protocol::owned::common::{
    streams_group_describe_response as describe,
    streams_group_topology_description_update_request as push,
};

/// Kafka's `NODE_TYPE_SOURCE`.
const NODE_TYPE_SOURCE: i8 = 1;
/// Kafka's `NODE_TYPE_PROCESSOR`.
const NODE_TYPE_PROCESSOR: i8 = 2;
/// Kafka's `NODE_TYPE_SINK`.
const NODE_TYPE_SINK: i8 = 3;

/// A Streams topology as the plugin stores it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyDescription {
    pub subtopologies: Vec<Subtopology>,
    pub global_stores: Vec<GlobalStore>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subtopology {
    pub id: String,
    pub nodes: Vec<Node>,
}

/// One processing node. The wire carries only the successors; a reader
/// rebuilds the predecessors from them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    pub name: String,
    pub kind: NodeKind,
    pub successors: Vec<String>,
}

/// What a [`Node`] keeps of its type: Kafka's `Source`, `Processor` and
/// `Sink` records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    Source { topics: Vec<String> },
    Processor { stores: Vec<String> },
    Sink { topic: Option<String> },
}

/// A global store: a source node that feeds a processor node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalStore {
    pub source: Node,
    pub processor: Node,
}

impl TopologyDescription {
    /// Kafka's `StreamsGroupTopologyDescriptionConverter.fromRequest`.
    ///
    /// # Errors
    ///
    /// Returns the message of Kafka's `InvalidRequestException` for a node of
    /// an unknown type, or for a global store that is not a source and a
    /// processor. The subtopologies are read before the global stores, so the
    /// first such node in that order names the error.
    pub fn from_push(
        wire: &push::topology_description::TopologyDescription,
    ) -> Result<Self, String> {
        let subtopologies = wire
            .subtopologies
            .iter()
            .map(|subtopology| {
                Ok(Subtopology {
                    id: subtopology.subtopology_id.clone(),
                    nodes: subtopology
                        .nodes
                        .iter()
                        .map(Node::from_push)
                        .collect::<Result<_, String>>()?,
                })
            })
            .collect::<Result<_, String>>()?;
        let global_stores = wire
            .global_stores
            .iter()
            .map(|store| {
                let source = Node::from_push(&store.source)?;
                let processor = Node::from_push(&store.processor)?;
                match (&source.kind, &processor.kind) {
                    (NodeKind::Source { .. }, NodeKind::Processor { .. }) => {
                        Ok(GlobalStore { source, processor })
                    }
                    _ => Err(
                        "Global store must be composed of a source and a processor node."
                            .to_owned(),
                    ),
                }
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            subtopologies,
            global_stores,
        })
    }

    /// Kafka's `StreamsGroupTopologyDescriptionConverter.toDescribeResponse`.
    #[must_use]
    pub fn to_describe(&self) -> describe::topology_description::TopologyDescription {
        describe::topology_description::TopologyDescription {
            subtopologies: self
                .subtopologies
                .iter()
                .map(|subtopology| {
                    describe::topology_description_subtopology::TopologyDescriptionSubtopology {
                        subtopology_id: subtopology.id.clone(),
                        nodes: subtopology.nodes.iter().map(Node::to_describe).collect(),
                        ..Default::default()
                    }
                })
                .collect(),
            global_stores: self
                .global_stores
                .iter()
                .map(|store| {
                    describe::topology_description_global_store::TopologyDescriptionGlobalStore {
                        source: store.source.to_describe(),
                        processor: store.processor.to_describe(),
                        ..Default::default()
                    }
                })
                .collect(),
            ..Default::default()
        }
    }
}

impl Node {
    fn from_push(
        wire: &push::topology_description_node::TopologyDescriptionNode,
    ) -> Result<Self, String> {
        let kind = match wire.node_type {
            NODE_TYPE_SOURCE => NodeKind::Source {
                topics: first_of_each(&wire.source_topics),
            },
            NODE_TYPE_PROCESSOR => NodeKind::Processor {
                stores: first_of_each(&wire.stores),
            },
            NODE_TYPE_SINK => NodeKind::Sink {
                topic: wire.sink_topic.clone(),
            },
            other => return Err(format!("Unknown topology node type: {other}")),
        };
        Ok(Self {
            name: wire.name.clone(),
            kind,
            successors: first_of_each(&wire.successors),
        })
    }

    fn to_describe(&self) -> describe::topology_description_node::TopologyDescriptionNode {
        let mut node = describe::topology_description_node::TopologyDescriptionNode {
            name: self.name.clone(),
            successors: self.successors.clone(),
            ..Default::default()
        };
        match &self.kind {
            NodeKind::Source { topics } => {
                node.node_type = NODE_TYPE_SOURCE;
                node.source_topics.clone_from(topics);
            }
            NodeKind::Processor { stores } => {
                node.node_type = NODE_TYPE_PROCESSOR;
                node.stores.clone_from(stores);
            }
            NodeKind::Sink { topic } => {
                node.node_type = NODE_TYPE_SINK;
                node.sink_topic.clone_from(topic);
            }
        }
        node
    }
}

/// `names` without repeats, each at its first position: the iteration order of
/// Kafka's `new LinkedHashSet<>(names)`.
fn first_of_each(names: &[String]) -> Vec<String> {
    let mut kept: Vec<String> = Vec::with_capacity(names.len());
    for name in names {
        if !kept.contains(name) {
            kept.push(name.clone());
        }
    }
    kept
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn push_node(
        name: &str,
        node_type: i8,
    ) -> push::topology_description_node::TopologyDescriptionNode {
        push::topology_description_node::TopologyDescriptionNode {
            name: name.into(),
            node_type,
            // Every field set, so the conversion shows what each type keeps.
            source_topics: vec!["in".into(), "in-b".into(), "in".into()],
            sink_topic: Some("out".into()),
            stores: vec!["store".into(), "store".into()],
            successors: vec!["next".into(), "next".into()],
            ..Default::default()
        }
    }

    #[derive(Clone, Copy, Default)]
    enum DescribedNodeKind {
        #[default]
        Source,
        Processor,
        Sink,
    }

    impl DescribedNodeKind {
        fn wire_code(self) -> i8 {
            match self {
                Self::Source => NODE_TYPE_SOURCE,
                Self::Processor => NODE_TYPE_PROCESSOR,
                Self::Sink => NODE_TYPE_SINK,
            }
        }
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct DescribedNodeSetup<'a> {
        #[default("node")]
        name: &'a str,
        kind: DescribedNodeKind,
        source_topics: &'a [&'a str],
        sink_topic: Option<&'a str>,
        stores: &'a [&'a str],
    }

    fn describe_node(
        setup: DescribedNodeSetup<'_>,
    ) -> describe::topology_description_node::TopologyDescriptionNode {
        let DescribedNodeSetup {
            name,
            kind,
            source_topics,
            sink_topic,
            stores,
        } = setup;
        describe::topology_description_node::TopologyDescriptionNode {
            name: name.into(),
            node_type: kind.wire_code(),
            source_topics: source_topics.iter().map(|&t| t.to_owned()).collect(),
            sink_topic: sink_topic.map(str::to_owned),
            stores: stores.iter().map(|&s| s.to_owned()).collect(),
            successors: vec!["next".into()],
            ..Default::default()
        }
    }

    /// A push round-trips to the describe response Kafka builds from the
    /// plugin's copy: each node keeps the fields of its type, and every list
    /// keeps its first occurrences in wire order.
    #[test]
    fn a_push_describes_as_kafka_stores_it() {
        let wire = push::topology_description::TopologyDescription {
            subtopologies: vec![
                push::topology_description_subtopology::TopologyDescriptionSubtopology {
                    subtopology_id: "0".into(),
                    nodes: vec![
                        push_node("KSTREAM-SOURCE-0000000000", NODE_TYPE_SOURCE),
                        push_node("KSTREAM-MAPVALUES-0000000001", NODE_TYPE_PROCESSOR),
                        push_node("KSTREAM-SINK-0000000002", NODE_TYPE_SINK),
                    ],
                    ..Default::default()
                },
            ],
            global_stores: vec![
                push::topology_description_global_store::TopologyDescriptionGlobalStore {
                    source: push_node("KSTREAM-SOURCE-0000000003", NODE_TYPE_SOURCE),
                    processor: push_node("KTABLE-SOURCE-0000000004", NODE_TYPE_PROCESSOR),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let described = TopologyDescription::from_push(&wire)
            .expect("a valid description")
            .to_describe();

        let expected = describe::topology_description::TopologyDescription {
            subtopologies: vec![
                describe::topology_description_subtopology::TopologyDescriptionSubtopology {
                    subtopology_id: "0".into(),
                    nodes: vec![
                        describe_node(DescribedNodeSetup {
                            name: "KSTREAM-SOURCE-0000000000",
                            source_topics: &["in", "in-b"],
                            ..Default::default()
                        }),
                        describe_node(DescribedNodeSetup {
                            name: "KSTREAM-MAPVALUES-0000000001",
                            kind: DescribedNodeKind::Processor,
                            stores: &["store"],
                            ..Default::default()
                        }),
                        describe_node(DescribedNodeSetup {
                            name: "KSTREAM-SINK-0000000002",
                            kind: DescribedNodeKind::Sink,
                            sink_topic: Some("out"),
                            ..Default::default()
                        }),
                    ],
                    ..Default::default()
                },
            ],
            global_stores: vec![
                describe::topology_description_global_store::TopologyDescriptionGlobalStore {
                    source: describe_node(DescribedNodeSetup {
                        name: "KSTREAM-SOURCE-0000000003",
                        source_topics: &["in", "in-b"],
                        ..Default::default()
                    }),
                    processor: describe_node(DescribedNodeSetup {
                        name: "KTABLE-SOURCE-0000000004",
                        kind: DescribedNodeKind::Processor,
                        stores: &["store"],
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert!(described == expected);
    }

    /// The pushes Kafka's converter refuses with `InvalidRequestException`,
    /// and its messages.
    #[test]
    fn malformed_pushes_are_refused_with_kafkas_message() {
        let subtopology = |node_type| push::topology_description::TopologyDescription {
            subtopologies: vec![
                push::topology_description_subtopology::TopologyDescriptionSubtopology {
                    subtopology_id: "0".into(),
                    nodes: vec![push_node("n", node_type)],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let global_store = |source, processor| push::topology_description::TopologyDescription {
            global_stores: vec![
                push::topology_description_global_store::TopologyDescriptionGlobalStore {
                    source: push_node("s", source),
                    processor: push_node("p", processor),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mixed = push::topology_description::TopologyDescription {
            global_stores: global_store(NODE_TYPE_SINK, NODE_TYPE_PROCESSOR).global_stores,
            ..subtopology(9)
        };
        let shape = "Global store must be composed of a source and a processor node.";
        let rows = [
            (
                "node type 0",
                subtopology(0),
                "Unknown topology node type: 0",
            ),
            (
                "node type 4",
                subtopology(4),
                "Unknown topology node type: 4",
            ),
            (
                "negative node type",
                subtopology(-1),
                "Unknown topology node type: -1",
            ),
            (
                "global store of a sink",
                global_store(NODE_TYPE_SINK, NODE_TYPE_PROCESSOR),
                shape,
            ),
            (
                "global store of two sources",
                global_store(NODE_TYPE_SOURCE, NODE_TYPE_SOURCE),
                shape,
            ),
            (
                "global store with an unknown processor type",
                global_store(NODE_TYPE_SOURCE, 7),
                "Unknown topology node type: 7",
            ),
            (
                "subtopologies are read first",
                mixed,
                "Unknown topology node type: 9",
            ),
        ];
        for (row, wire, message) in rows {
            assert!(
                TopologyDescription::from_push(&wire) == Err(message.to_owned()),
                "{row}"
            );
        }
    }
}
