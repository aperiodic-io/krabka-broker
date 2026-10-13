# KFCs

A KFC is a **Kall for Comment**: the krabka record of a change that alters observable broker behaviour where no Kafka KIP covers it.

Apache Kafka changes its public behaviour through a KIP. A KIP states the motivation, the public interfaces, the proposed change, the compatibility story, and the alternatives the authors rejected. krabka follows a KIP wherever one exists, and the KIP stays the specification for that feature.

Some krabka work has no KIP to follow. A capability that Kafka never accepted is one example. A config that changes what a consumer sees is another. Each of them changes what a client observes, and no upstream document explains it. A KFC is the document for that case. It carries the same sections as a KIP, in the same order, so a reader who knows KIPs can read one without a second explanation.

A KFC is a design record, not a manual. It explains why the broker behaves in a new way. It does not repeat the API reference that the rustdoc carries, and it does not repeat the test detail that the coverage report carries.

## When a Change Needs a KFC

Write a KFC before you merge a change that does one of these things:

- **A new client-visible semantic.** A consumer, a producer, or an admin tool sees an outcome that Kafka does not define. Delayed visibility of a produced record is one example.
- **A new topic config or broker config that changes delivery.** A setting that changes which records a fetch returns, in what order, or at what time is a change of contract for every client of that topic.
- **A deliberate divergence from Kafka.** krabka answers a request in a way that the JVM broker does not, and the difference is a decision rather than a defect.

The test is what a client can observe. If a stock Kafka client can tell the difference, and no KIP explains the difference, the change needs a KFC.

## When a Change Does Not Need a KFC

- **An internal refactor.** New module boundaries, a new data structure, or a new task layout change nothing that a client sees.
- **A bug fix.** A fix that moves krabka back towards Kafka's documented behaviour needs a test, not a KFC.
- **An implementation of an existing KIP.** The KIP is the specification. Track the work against the KIP and record the interpretation decisions in the subsystem design doc.

A change that needs a subsystem design doc does not automatically need a KFC. The two documents answer different questions. A [design doc](../style_guides/design_doc_style_guide.md) explains how a subsystem works. A KFC explains why the broker's contract with its clients changed.

## File Names

Each KFC is one file, named `KFC-<n>-<slug>.md`.

The number `<n>` is the next free integer. Numbers run in order of creation and a number is never reused. The slug is the title in lower case with hyphens between the words. `KFC-1-deliver-at-time-visibility.md` is the first one.

A KFC file stays in the tree after the decision, whatever the decision was. A rejected KFC is a useful record, because it stops the same proposal from coming back without new arguments.

## Required Sections

Every KFC carries these headings, in this order. The order is the Apache Kafka KIP order.

| Section | Content |
| :--- | :--- |
| Status | The current status value, and the pull request or release that carries the implementation. |
| Motivation | The problem, and why the broker is the right place to solve it. |
| Public Interfaces | Every config, error code, metric, and wire element that a client or an operator sees. |
| Proposed Changes | The design and the reasons behind it. This is the longest section. |
| Compatibility, Deprecation, and Migration Plan | What breaks, what does not, and what an operator has to do. |
| Test Plan | How the project proves the design is correct. Name the test tiers, do not copy them. |
| Rejected Alternatives | Each alternative in its own subsection, with the reason for the rejection. |

The Rejected Alternatives section is not decoration. Most readers arrive with an obvious design already in mind. They go to that section first, and the reason has to be there.

## Status Values

| Status | Meaning |
| :--- | :--- |
| `Draft` | The author still works on the document. The design can still change. |
| `Under discussion` | The document is open for comment. No code depends on it yet. |
| `Adopted` | The project accepted the design. The Status section names the pull request or the release that carries the implementation. |
| `Deferred` | The project neither accepted nor rejected the design, and nobody works on it. No code depends on it, and no part of the tree is held free for it. The Status section states what would have to change for the work to start. |
| `Rejected` | The project decided against the design. The document stays in the tree with the reason. |
| `Superseded` | A later KFC replaces this one. The Status section names it. |

## Index

| KFC | Title | Status |
| :--- | :--- | :--- |
| [KFC-1](KFC-1-deliver-at-time-visibility.md) | Deliver-at-time visibility | Adopted |
| [KFC-2](KFC-2-witness-broker-stretch-cluster.md) | Witness broker role and stretch cluster | Adopted |
| [KFC-3](KFC-3-point-in-time-restore.md) | Point-in-time restore from a tiered-storage archive | Adopted |
| [KFC-4](KFC-4-cross-topic-snapshots.md) | Consistent cross-topic snapshots | Adopted |
| [KFC-5](KFC-5-worm-archive-integrity-manifests.md) | WORM archive mode with integrity manifests | Adopted |
| [KFC-6](KFC-6-coordination-primitives-api.md) | Coordination primitives as a client API | Deferred |
| [KFC-7](KFC-7-broker-side-schema-validation.md) | Broker-side schema validation | Adopted |
| [KFC-8](KFC-8-clock-confidence-signal.md) | Clock confidence as a first-class signal | Adopted |
| [KFC-9](KFC-9-topic-write-freeze-and-break-glass-workflows.md) | Topic write-freeze and break-glass workflows | Adopted |
| [KFC-10](KFC-10-produce-time-record-deduplication.md) | Produce-time record deduplication | Draft |

## Style

A KFC follows the [design doc style guide](../style_guides/design_doc_style_guide.md) for content and the [prose style guide](../style_guides/prose_style_guide.md) for wording. The reader knows Kafka and distributed systems, and may know little Rust. Explain the decision, not the code.
