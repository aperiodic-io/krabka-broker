# KFC-1: Deliver-at-Time Visibility

Scheduled delivery for a Kafka topic, with no wire-protocol change and no client change.

## Status

**Adopted.** The implementation merged to `main` through pull request [#9](https://github.com/krabka-io/krabka-broker/pull/9) and ships in release `v0.5.0` and every later release.

No KIP defines scheduled delivery, so this document is the specification for it. Each section below states where the semantics sit next to Kafka's. The [Compatibility](#compatibility-deprecation-and-migration-plan) section names the one place where krabka overloads a Kafka meaning.

## Motivation

Delayed and scheduled delivery is one of the longest-requested Kafka capabilities. Users proposed it upstream many times over more than a decade, and no proposal landed. The capability is still missing, and the demand did not go away.

Three common systems sit on the same missing primitive.

- **Scheduled command execution.** A service writes a command now and wants a worker to receive it at a set time.
- **Retry queues with backoff.** A failed record goes back to a topic. It has to stay invisible until the backoff time ends.
- **SLA timers.** A record is a deadline. A completion before the deadline cancels it. No completion means the record comes due and a handler acts on it.

Each of the three is built today outside the broker. The pattern is always the same: an external scheduler plus a re-produce loop. The producer writes the record to a database or to a delay topic. A scheduler wakes up when the record comes due. A second produce then puts the record into the topic that the real consumer reads.

That loop is expensive in the ways that matter. It holds a second copy of the record data. It adds a second failure domain, with its own leader election and its own durable state. It drops the log's order and its exactly-once guarantees across the hop. An operator who wants a scheduled record to survive a crash now has two systems to reason about instead of one.

The broker already holds everything the feature needs. Records in the log are durable, replicated, ordered, and each one carries a timestamp. The missing part is small: a rule that says when a record becomes visible to a consumer. This KFC adds that rule and nothing else.

## Public Interfaces

The feature adds three topic configs, one broker config, and four metrics. It adds no API key, no new error code, and no field to any request or response.

### Topic Configs

| Config | Default | Values | Meaning |
| :--- | :--- | :--- | :--- |
| `delivery.mode` | `immediate` | `immediate`, `scheduled` | `scheduled` makes the record timestamp the delivery time. `immediate` is Kafka's behaviour. |
| `delivery.max.delay.ms` | `604800000` | a non-negative number of milliseconds, or `-1` | The largest delay the broker accepts, measured forward from produce time. The default is 7 days. `-1` removes the limit. |
| `delivery.schedule.monotonic` | `false` | `true`, `false` | `true` makes the broker reject a batch whose delivery time is before the largest delivery time already in the partition. |

### The Record Timestamp Is the Delivery Time

On a topic with `delivery.mode=scheduled`, the record's own timestamp is the time at which the record becomes visible to a consumer. There is no new field, and there is no new header.

This choice is what lets the stock JVM client work unchanged. The JVM producer has carried a timestamp argument since Kafka 0.10. It sits in the `ProducerRecord` constructor that takes a topic, a partition, a timestamp, a key, and a value. A producer that wants to schedule a record sets the timestamp it already knows how to set, and the broker does the rest. librdkafka exposes the same timestamp input, but the librdkafka container suite currently covers ordinary produce and consume, group membership, and version negotiation rather than future-timestamp scheduling. Until that suite schedules a record end to end, this KFC makes the no-client-change claim only for the JVM client.

### Rejections

The broker rejects a produce request in two cases, and it uses the existing Kafka error `INVALID_TIMESTAMP`, code 32, for both.

The first case is a delivery time further ahead than `delivery.max.delay.ms`. The second case is a delivery time that goes backwards on a partition with `delivery.schedule.monotonic=true`. The [head-of-line section](#why-visibility-is-offset-ordered-for-classic-groups) explains why the second rejection is worth having.

`INVALID_TIMESTAMP` is the right code because every client already knows it. Kafka has returned it for `message.timestamp.difference.max.ms` violations since 0.10, so client error tables, retry classifiers, and admin tools all handle it. A new error code would arrive at old clients as an unknown code, and an unknown code is where retry logic goes wrong.

### Metrics

- A per-partition gauge for the delivery watermark.
- A per-partition gauge for the count of pending records. A pending record is produced but not yet active.
- A histogram of activation lateness. Lateness is the time from the instant a batch became due, which is its delivery time plus the declared clock bound, to the first moment the record was visible. It therefore reads at or near zero on a healthy broker, and any positive tail is a delay the broker did not promise. Add `delivery_clock_uncertainty` to read the delay from the record's own delivery time.
- A counter of scheduler wakeups.

The lateness histogram is the one an operator watches. It measures the promise this design makes. A rising tail points at clock skew, or at a scheduler that does not get enough CPU.

## Proposed Changes

### Activation

The unit of activation is the record batch, not the record.

A batch activates at its `max_timestamp`. That field sits in the v2 batch header at a fixed position, in front of the compressed record data. The broker reads it without decompression and without a decode of the records inside, so both the produce path and the scheduler stay cheap. A producer that wants per-record precision should lower `linger.ms` or produce one record per batch.

A batch is active once `max_timestamp + delivery_clock_uncertainty` is at or before the broker's clock reading. The bound is what turns a local clock into a safe decision.

Call the broker's clock reading `c` and the declared bound `e`. The bound says true time lies somewhere in the interval from `c - e` to `c + e`. The broker activates a batch with delivery time `T` only when `c` is at or past `T + e`. In that case true time is at least `c - e`, and `c - e` is at least `T`. So true time is at or past the delivery time whenever the broker activates a batch.

Two properties follow. **Delivery is never early.** Delivery is late by at most `2e` plus one scheduler tick. The broker waits out the full bound before it acts, which costs `e` against its own clock, and true time can already sit a further `e` ahead of that reading. Early delivery is the failure that breaks a retry backoff or an SLA timer, and this design rules it out. Bounded lateness is the price, and an operator can measure that price on the lateness histogram.

### The Delivery Watermark

Each partition gets one new derived offset: the **delivery watermark**. It is the largest offset such that every batch below it is active. Equally, it is the base offset of the first pending batch, or the log end offset when no batch is pending.

The watermark is a read cap, and nothing else.

- A `read_uncommitted` consumer fetch stops at the lower of the high watermark and the delivery watermark.
- A `read_committed` consumer fetch stops at the lowest of the last stable offset, the high watermark, and the delivery watermark.
- A follower fetch is not capped. See [Followers Replicate Everything](#followers-replicate-everything).

The high watermark and the last stable offset that the fetch response reports do not change. The broker computes them exactly as it does today and sends the same values. Two things follow from that. Consumer lag stays honest, because a client measures lag against a high watermark that still describes what the leader holds. KIP-227 watermark monotonicity is untouched, because the two reported values are the same values that KIP-227 constrains, computed the same way.

```
offset  |   100   |   101   |   102   |   103   |   104   |   105   |
        +---------+---------+---------+---------+---------+---------+
batch   |  active |  active |  active | pending | pending |  active |
        +---------+---------+---------+---------+---------+---------+
                                      ^                             ^
                                      delivery watermark = 103      log end = 106
```

The figure shows the rule that matters. The batch at 105 is due, but it sits behind two pending batches, so the watermark stops at 103 and no consumer sees 105 yet. The next section explains why that is the contract and not a defect.

#### What Bounds the Pending Span

The gap between the delivery watermark and the log end is the set of durable records no consumer may read yet. `delivery.max.delay.ms` is what bounds it. A batch scheduled further ahead than that is rejected at produce time with `INVALID_TIMESTAMP`, so a record stamped at `i64::MAX` never reaches the log while the default 7 days is in force. Set the config to `-1` and the bound is gone: the span then grows as far as a producer cares to schedule.

The bound matters more than a read cap usually would, because a pending record also pins the log. A segment holding an undelivered record survives both retention paths, by design, and [Retention, Compaction, and DeleteRecords](#retention-compaction-and-deleterecords) explains why. The consequence is worth stating in one line: on a scheduled topic, **`delivery.max.delay.ms` is the disk bound, not `retention.bytes`**. A partition carrying a 7-day schedule carries 7 days of log whatever `retention.bytes` says.

`delivery_pending_records` is the gauge that shows this, and it is the one to alert on. It reports the log end minus the watermark per partition. A value that climbs and never falls is a schedule whose head-of-line record is further out than the operator intended.

### Why Visibility Is Offset-Ordered for Classic Groups

A classic Kafka consumer's position in a partition is a single offset. The group commits one number per partition, and the fetch loop reads forward from it. Everything to the left of the position is unreachable, forever, for that group.

That fact decides the design. Suppose the broker let a later record overtake an earlier pending one, so a consumer read offset 105 while 103 was still pending. The consumer's position is now 106. When 103 comes due, no fetch will ever ask for it again. The broker would have delivered a scheduled record to nobody, and it would have done so silently.

So head-of-line order is the contract. A scheduled topic is a schedule: the broker delivers its records in offset order, and the timestamps say when each one comes due. A record with an earlier offset and a later delivery time holds up the records behind it. That is by design, because the alternative is to lose them.

This is a sharp edge, and calling it the contract does not blunt it. One record scheduled a week out stops every record behind it in that partition, however soon those are due. The design cannot remove that for a classic group, so it does two things instead: it makes the stall loud rather than silent, and it offers a delivery model that does not have the problem at all.

A workload whose records carry independent, unordered delays does not belong on a classic group on one partition. Two shapes fit it. A share group tracks per-record delivery state and skips a pending record to reach a due one behind it, which is exactly this case, and [Share Groups Deliver Out of Order](#share-groups-deliver-out-of-order) describes it. Failing that, partition by delay class, so that records which share a deadline share a partition and head-of-line order is the order you wanted anyway.

`delivery.schedule.monotonic=true` is the guard for the operator who stays on a classic group and does not want a silent stall. A partition whose delivery times go backwards is a partition whose schedule stalls. A stall is hard to see from the outside. The topic looks healthy, the lag is real, and nothing is wrong with the broker. The config turns that silent stall into an `INVALID_TIMESTAMP` at produce time, where the producer that caused it can see it and fix it.

The guard is absolute, and an operator should know why. The test runs inside the log, under the same lock acquisition that writes the batch: `Log::append` compares the batch's delivery time against the partition's schedule and refuses it there. That is the only place where the test and the append it guards are one critical section. Two producers writing to one partition at the same time therefore cannot both pass and then append out of order, and neither can two batches the partition writer groups into a single append. Every batch that reaches the log is checked against the schedule the log holds at that instant, so a partition with `delivery.schedule.monotonic=true` never contains a delivery time earlier than one at a lower offset.

### Share Groups Deliver Out of Order

Share groups from KIP-932 do not have the constraint that forces offset order.

A share group tracks per-record delivery state instead of one position. The share-partition state machine already knows how to skip a record and come back to it later. An in-flight acquisition, a lock timeout, and a redelivery all need that. A record the group has not acknowledged stays acquirable.

The broker uses that. A range of offsets that is not yet active is marked `Deferred`, and acquisition skips a `Deferred` range. So a share consumer on a scheduled topic gets the records that are due now. It gets the pending ones on a later acquire pass, in delivery-time order rather than offset order.

The mechanism already exists in the state machine. Transaction control batches occupy offsets that hold no user record. The share-fetch path marks those ranges on every acquire pass, so acquisition steps over them. Deferral works the same way: the broker re-derives it from the log and the clock on each acquire pass. Nothing caches a decision that a later clock reading would change, and a `Deferred` range becomes acquirable on the first pass after the batch activates.

`Deferred` is a derived, in-memory refinement of `Available`, and it persists as `Available`. The `__share_group_state` record encoding does not change. A share coordinator that reloads its state after a leader change reads back `Available`. It then re-derives the deferral against its own clock, and not against the old leader's.

### Durability Needs No New State

The schedule needs no durable timer store, because the schedule is the replicated records themselves. Each delivery time arrives in the log, replicates to the ISR, and survives a crash exactly as the record data does.

The delivery watermark is a derived value, in the same sense that the high watermark is derived. A leader that starts, or a follower that takes over, recomputes the watermark from the log and the clock. A recomputation of a derived value cannot disagree with the log it comes from. So a restart and a leader change need no special case, and no fence around a separate store.

The recovery walk is bounded. Each segment carries a maximum timestamp. Recovery skips a whole segment whose records are all active, and starts its record-level work at the first segment that can hold a pending batch. The walk touches the tail of the log, not the log.

A truncation carries the watermark back down with the records it removes. The watermark moves forward while the records under it stay, but a truncation cuts a suffix away, and a later append fills those offsets with different records. A watermark that stayed above them would declare them visible without ever reading their delivery times, so the log lowers it to the truncation point and the next walk reads those records again. This is the same rule Kafka applies to the high watermark, and for the same reason.

### Followers Replicate Everything

Replication is not gated. A follower fetches, writes, and acknowledges a pending record as soon as the leader has it. The ISR, the high watermark, and durability all behave as they do on any other topic.

Only consumer delivery is gated. The split between the two is what makes the feature cheap. A scheduled record is a normal replicated record. Every guarantee the replication protocol gives applies to it before it is ever visible.

### Retention, Compaction, and DeleteRecords

A pending record that retention deletes is a record the broker promised to deliver and then dropped. Three paths can do that, and each one is closed.

Time retention closes itself. It compares a segment's maximum timestamp against the cutoff, and a record scheduled for the future raises that maximum above the cutoff by definition. So a segment that holds an undelivered record is never old enough to delete.

Size retention does not read timestamps, so it needs a guard. The guard stops size retention from deleting a segment that holds an undelivered record. An operator should know the consequence. A partition with a long schedule can hold more bytes than `retention.bytes`. The broker keeps a promised record and gives up the exact byte limit.

`DeleteRecords` closes the third path. The broker caps the trim at the delivery watermark, so an admin call cannot delete a record that no consumer could have read yet.

The cap applies to every resolved target, not only to the `-1` sentinel that means "delete up to the current end of the log". An explicit offset between the watermark and the end of the log destroys an undelivered record exactly as the sentinel would, so scoping the cap to the sentinel would leave the guarantee open to the more deliberate call. The cap only ever lowers a target: a trim at or below the watermark is untouched, a target past the end of the log is still the error it always was, and the response reports the offset the trim reached, so a capped call is visible to the caller rather than silent.

Compaction cannot be closed the same way, so the broker rejects the combination. `cleanup.policy=compact` and `delivery.mode=scheduled` together are a configuration error at topic creation and at config alter time. Compaction deletes a record when a later record carries the same key. On a scheduled topic that later record can arrive long before the earlier one comes due. The earlier record would then be deleted without a single delivery, which is the failure the whole design exists to prevent.

### Tiered Reads Check the Batch Itself

A read that the local log cannot serve is the one read path the watermark cannot cap.

The broker reaches the tiered path only after a local read answers `OFFSET_OUT_OF_RANGE`, which means the requested offset is below the local log start. The watermark never goes below the local log start, so every offset the tiered path can serve is already below it, and an offset cap there would be dead code. Tiered local retention also stops at the delivery watermark, so a pending batch stays local and `LATEST` continues to name it. Remote retention checks stored batch headers before removing a scheduled segment; a pending batch or unreadable activation evidence stops the deletion prefix, even when the byte budget is exceeded. Retention timestamps can fall back to file modification time, so they are not sufficient activation evidence.

The tiered fetch path also checks the batch instead of the offset. Each remote batch carries its own maximum timestamp, which is the same field the watermark is derived from, so the evidence survives the copy. A batch that is not due yet is not served.

The answer to that fetch is an empty partition and no error, which is the answer the local path gives for a pending batch. `OFFSET_OUT_OF_RANGE` would send the consumer to its `auto.offset.reset` policy, and the consumer would lose the record it is waiting for. The record is due later, not never.

### ListOffsets

On a scheduled topic, `LATEST` returns the delivery watermark. If older eviction behavior or a change from immediate to scheduled delivery has left pending batches only in the archive, the lookup derives that part of the watermark from their stored headers too. An unavailable archive frontier fails the lookup rather than returning an offset that skips waiting records.

The reason is the same one that forces offset order. A consumer that seeks to end sets its position to the value `LATEST` gives it. If that value were the log end offset, the consumer would step over every pending record in one move. Those records would then be unreachable for it forever. With the delivery watermark, seek-to-end lands the consumer on the first pending record, and the consumer receives it when it activates. Seek-to-end means "start with what is not yet delivered", which is what a caller on a scheduled topic wants.

Two honest notes belong with that.

First, this value can move backwards across a leader change, by up to twice the declared clock bound. The old leader and the new leader can read clocks that differ by up to `2e` and still be inside their bounds. The new leader can then compute a watermark up to `2e` behind the one a client saw a moment earlier. A client that calls `LATEST` on both sides of a leader change can see the smaller value second. The effect is bounded, and it gives a re-delivery and not a loss. An operator who wants a smaller bound lowers `delivery_clock_uncertainty`.

Second, an ordinary consumer's `LATEST` is capped at the high watermark, or at the last stable offset under `read_committed`, as in Kafka. On a scheduled topic the delivery watermark adds a cap to that bound. Replication requests still resolve against the log end.

### Cost on an Ordinary Topic Is Zero

A topic with `delivery.mode=immediate` pays nothing.

The watermark call returns the log end offset before it does any work. It reads no batch header, it walks no segment, and it takes no extra lock. So that cap never limits a fetch. The fetch path reaches `sendfile` on the same branch it reaches today. Zero-copy reads and byte-exact record passthrough are untouched for every topic that does not ask for scheduled delivery.

### Cancellation and Update Are Not in This KFC

A scheduled record cannot be cancelled, and its delivery time cannot be changed. Once a producer appends it, it will be delivered. This is the first question an operator asks about scheduled delivery, so it is worth saying why the answer is no rather than leaving it to be discovered.

The log is append-only, so nothing rewrites a record in place. That much is ordinary Kafka. What is specific to this design is that four separate mechanisms now protect a pending record from disappearing: time retention spares it, the size-retention guard spares it, `DeleteRecords` is capped at the delivery watermark, and `cleanup.policy=compact` is rejected outright. Each one exists to stop an undelivered record being lost by accident. Cancellation is that same removal performed on purpose, and the broker has no way to tell the two apart. The invariant that makes scheduled delivery trustworthy is the same invariant that forbids taking a record back.

It is also not a small addition, because three properties cannot hold together: no client change, zero-copy passthrough, and broker-enforced cancellation. Removing one record from a batch means re-encoding the batch, which abandons the `sendfile` path this design is built on. Kafka's own precedent for "durable, but this must not be delivered" is the aborted transaction, and it takes the third route: the broker ships the bytes untouched and returns an `aborted_transactions` list, and the *consumer* drops the records. A cancellation list would work the same way and would need the same thing — a client that knows to read it. The central claim of this KFC is that a stock client works unchanged, so that route is closed to it here.

Two patterns cover the need today.

**Short-hop rescheduling** is the better one. Schedule in bounded hops rather than one long delay: deliver in an hour, and on receipt decide whether to produce the next hop. Cancelling is not producing the next hop, and updating is producing a different one. It costs write amplification proportional to the delay divided by the hop, and it needs a consumer to be running. In exchange it gives exact cancel and update semantics with no broker change, and it keeps the pending span small, which is the same thing that keeps [the disk bound](#what-bounds-the-pending-span) low.

**Side-channel suppression** fits when hops do not. The producer stamps a schedule id in a record header, and the consumer checks a compacted cancellation topic keyed by that id before it acts. The record is still stored and still delivered, so this buys nothing back in disk or in delivery work; it only stops the effect.

A broker-side mechanism is possible, and it is a separate KFC rather than an extension of this one. It needs a cancellation record with its own durability and replication story, a rule for what a cancellation means when it arrives after activation, and then either a protocol addition that a client must opt into or batch-granularity suppression that gives up passthrough. Those are design decisions with their own rejected alternatives, and folding them in here would bury them.

## Compatibility, Deprecation, and Migration Plan

There is no wire change, no new API key, no new error code, and no client change. A stock JVM producer sets a timestamp it can already set. A stock JVM consumer polls a topic that behaves like any other topic with a slower leader.

One admin tool cannot set the new configs. `kafka-topics` validates every `--config` name against the `LogConfig.configNames` set compiled into the client, and it does that before it sends `CreateTopics`. It therefore answers `InvalidConfigurationException: Unknown topic config name: delivery.mode` without the request ever reaching the broker. This is not a version gap that a newer image closes. The KIP-405 keys behaved the same way until the Kafka that introduced them shipped, and a krabka extension never becomes known to Apache Kafka's client.

The check lives in `TopicCommand`, not in the protocol and not in `AdminClient`, which sends a config map it does not inspect and leaves the names for the broker to validate. So a client that speaks `CreateTopics` directly sets the key, and `jvm_deliver_at_time` creates its topics that way. This document does not claim the `AdminClient` path is proved: no test here drives it, and the suite that would is the one to extend if that guarantee is ever wanted in writing.

This is a limitation of the config surface, not of the data path, and it is the price of naming the feature at all: any key Apache Kafka does not know is a key `kafka-topics` will not send. The alternative is to overload a config name that Kafka already knows, which trades a tool error for a silent misreading of an operator's intent, and that is the worse of the two.

krabka is greenfield and undeployed, so there is no migration. No on-disk format changes. No record already in a log means something different after this change.

One semantic overload is deliberate and worth stating plainly. On a topic with `delivery.mode=scheduled`, the record timestamp means the delivery time, and every Kafka feature built on the record timestamp inherits that meaning. The `.timeindex` maps activation times to offsets. `ListOffsets` by timestamp finds the first record that activates at or after the given time. `MAX_TIMESTAMP` returns the latest activation time in the partition. Those are not accidents of the implementation. They follow from the choice to use the timestamp, and each one is the more useful answer on a schedule.

The overload applies only to topics that an operator configured for it. A topic with `delivery.mode=immediate` keeps every Kafka meaning exactly as it is.

## Test Plan

The design makes claims about a state space, so the strongest tests enumerate that state space rather than sample it.

**Model checks.** The fetch read path already carries an exhaustive `stateright` model over the visibility window and its watermarks. The model gains the delivery watermark. The checker then enumerates the extended window, and proves that the clamp contract and KIP-227 monotonicity still hold under the new cap. A second `stateright` model covers the share-partition state machine with `Deferred` added. That machine holds the most reachable interleavings in the design.

**Formal verification.** The visibility decision is a pure kernel in `krabka-verified`, called by the fetch handler. The kernel gains a Creusot contract for the new cap. The proof covers every input, and not only the inputs a test picks.

The checked-in proof under `verif/` is gated. The [`proofs` CI job](../../.github/workflows/ci.yml) installs the Creusot release in `.creusot-version`, checks that its proof list covers every proof-bearing workspace package, and re-runs those proof sessions. The job fails on an uncovered package or unproved goal. The [proof ledger](../verification.md) records the contract, caller preconditions, and proof-session path.

**Integration.** In-process broker tests drive a mock clock. They produce a scheduled batch, move the clock across the activation boundary, and assert what a fetch returns on each side of it. A mock clock is what turns the lateness bound into a deterministic assertion, and not a race against real time.

**Differential.** A Docker-gated test runs a stock JVM producer and a stock JVM consumer against krabka. It is the direct proof of this KFC's central compatibility claim: a scheduled topic needs no client change.

## Rejected Alternatives

### A `krabka.deliver.at` Record Header

An explicit header is the first design most readers propose. It is per-record, it is self-describing, and it does not overload the timestamp.

The produce hot path rules it out. A header lives inside the record data, under the batch's compression, so the broker would have to decompress and decode every batch to find it. Today the broker writes the producer's bytes through without a look inside, and that is a large part of why the write path is fast.

The fetch path rules it out a second time. Per-record delivery times do not land on batch boundaries, and the fetch path enforces its offset cap against a batch's base offset. A cap that falls inside a batch still ships that batch whole, so a mixed batch would leak records that are not yet due. To honour per-record times exactly, the broker would have to re-encode batches on fetch, which is the next rejected alternative.

The batch's `max_timestamp` gives the same capability from a field that is already in the header. That field sits outside the compressed data, and every producer already maintains it.

### A Broker-Side Byte Filter on Fetch

The broker could decode each batch on the read path, drop the records that are not yet due, re-encode the remainder, and recompute the CRC.

That abandons zero-copy `sendfile` and the byte-exact passthrough guarantee, which are two of the broker's most valuable properties. Every fetch on a scheduled topic would cost a decompress, a re-compress, and a checksum over the whole batch.

The cost gives nothing back, because the filter does not solve the problem. A consumer whose position has moved past an offset never asks for that offset again. A filter that drops a record from a response makes it invisible now and unreachable later. That is the loss described in [Why Visibility Is Offset-Ordered](#why-visibility-is-offset-ordered-for-classic-groups). The filter hides the failure and does not prevent it.

### Record Relocation at Activation Time

The broker could accept a scheduled record into a staging area and append it into the topic when it comes due. This is the design that gives true out-of-order delivery for classic consumers, with no record held up behind an earlier one.

It breaks the produce contract. A produce response carries the record's final offset, and the producer's idempotence and the transaction machinery are built on that offset. A staged record has no final offset yet, because the offset depends on what else is appended between the produce and the activation. The broker would have to answer the produce with an offset it cannot know, or with no offset at all.

A produce response without the offset loses idempotent produce, and it loses exactly-once semantics with it. That loss is much larger than head-of-line order. It also falls on every producer to the topic, and not on the schedule that caused it.

### A Separate Durable Timer Store

The broker could keep a persisted timer wheel or a checkpoint file that lists the pending activations, and consult it on restart.

It is redundant state. The activation times are already in the replicated log. A second copy of them can corrupt, can fall behind, and can survive a record that a leader change rolled back. Any disagreement between the store and the log is a bug. It produces either an early delivery or a lost one, and it gives the operator a new file to repair.

A derived watermark cannot disagree with the log, because the log is its only input. The design keeps one source of truth and recomputes from it, which is the same choice the broker already makes for the high watermark.
