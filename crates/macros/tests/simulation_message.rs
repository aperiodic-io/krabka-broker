//! The derived simulation adapter preserves endpoint filtering and equality order.

use {
    assert2::assert,
    std::{cell::Cell, rc::Rc},
};

trait PartitionMessage<Node> {
    fn touches(&self, node: Node) -> bool;
}

struct Node {
    id: u8,
    comparisons: Rc<Cell<usize>>,
}
impl PartialEq for Node {
    fn eq(&self, other: &Self) -> bool {
        self.comparisons.set(self.comparisons.get() + 1);
        self.id == other.id
    }
}

#[derive(krabka_macros::PartitionMessage)]
struct Message {
    src: Node,
    dst: Node,
}

#[test]
fn partition_membership_checks_both_endpoints_and_short_circuits_source_matches() {
    for source in 0..3 {
        for destination in 0..3 {
            for partition in 0..3 {
                let observed = Rc::new(Cell::new(0));
                let make_node = |id| Node {
                    id,
                    comparisons: Rc::clone(&observed),
                };
                let message = Message {
                    src: make_node(source),
                    dst: make_node(destination),
                };
                assert!(
                    message.touches(make_node(partition))
                        == (source == partition || destination == partition)
                );
                assert!(observed.get() == if source == partition { 1 } else { 2 });
            }
        }
    }
}
