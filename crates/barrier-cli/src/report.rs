//! Renders a barrier response as the lines an operator reads.
//!
//! Each report turns one response into stdout for what succeeded, stderr for
//! what did not, and the exit code the tool ends on. Keeping the rendering
//! apart from the sending is what lets the shape of the output be reasoned
//! about on its own: a tab-separated row per partition, so `cut` and `awk`
//! read it as well as a person does.

use crate::{EXIT_MISMATCH, EXIT_REFUSED, verify::VerifyOutcome};

/// Describe an error code.
///
/// `krabka-broker` has no code-to-name table, so this prints the number. The
/// one code an operator of this tool meets that a Kafka reference will not
/// list is the krabka-private one, so that gets a word.
fn code_name(code: i16) -> String {
    if code == krabka_broker::codes::BARRIER_INJECTION_IN_PROGRESS {
        return format!("error {code} (an injection is already in flight, retry)");
    }
    format!("error {code}")
}

/// An error code and the broker's message, if it sent one.
pub(crate) fn described_error(code: i16, message: Option<&str>) -> String {
    let name = code_name(code);
    match message {
        Some(m) => format!("{name}: {m}"),
        None => name,
    }
}

fn report_refused(code: i16, message: Option<&str>) -> i32 {
    eprintln!("{}", described_error(code, message));
    EXIT_REFUSED
}

/// One line per altered group.
pub(crate) fn report_alter(
    response: &krabka_protocol::krabka::barrier::AlterBarrierGroupsResponse,
) -> i32 {
    let mut exit = 0;
    for result in &response.results {
        if result.error_code == 0 {
            println!("{}\tok", result.group);
        } else {
            eprintln!(
                "{}\t{}",
                result.group,
                described_error(result.error_code, result.error_message.as_deref())
            );
            exit = EXIT_REFUSED;
        }
    }
    exit
}

/// One block per described group.
pub(crate) fn report_describe(
    response: &krabka_protocol::krabka::barrier::DescribeBarrierGroupsResponse,
) -> i32 {
    let mut exit = 0;
    for group in &response.groups {
        if group.error_code != 0 {
            eprintln!("{}\t{}", group.group, code_name(group.error_code));
            exit = EXIT_REFUSED;
            continue;
        }
        println!("group           {}", group.group);
        println!("topics          {}", group.topics.join(", "));
        println!(
            "interval        {}",
            if group.interval_ms < 0 {
                "on demand only".to_owned()
            } else {
                format!("{}ms", group.interval_ms)
            }
        );
        println!("retained cuts   {}", group.retained_cuts);
        // The broker spells "never injected" as 0 and allocates 1 for the
        // first cut, while the wire field's own default is -1. Either way no
        // real epoch is at or below zero, so both read as none.
        println!(
            "last epoch      {}",
            if group.last_epoch <= 0 {
                "none yet".to_owned()
            } else {
                group.last_epoch.to_string()
            }
        );
        println!("coordinator     {}", group.coordinator_id);
        println!();
    }
    exit
}

/// The epoch a trigger produced, and the offsets it took.
pub(crate) fn report_trigger(
    response: &krabka_protocol::krabka::barrier::TriggerBarrierResponse,
) -> i32 {
    if response.error_code != 0 {
        return report_refused(response.error_code, response.error_message.as_deref());
    }
    println!("epoch  {}", response.epoch);
    println!("status {}", status_name(response.status));
    for topic in &response.topics {
        for partition in &topic.partitions {
            println!(
                "{}\t{}\t{}",
                topic.topic, partition.partition, partition.offset
            );
        }
    }
    // A partial cut is a published outcome, not a failure: the epoch is
    // consumed and the markers that did land cannot be withdrawn. Naming the
    // gaps is what lets a reader skip the epoch deterministically.
    for missing in &response.missing {
        eprintln!("no marker: {}\t{}", missing.topic, missing.partition);
    }
    0
}

/// One line per partition of each retained cut.
pub(crate) fn report_list(
    response: &krabka_protocol::krabka::barrier::ListBarrierCutsResponse,
) -> i32 {
    if response.error_code != 0 {
        return report_refused(response.error_code, response.error_message.as_deref());
    }
    for cut in &response.cuts {
        println!(
            "epoch {} {} triggered {} completed {}",
            cut.epoch,
            status_name(cut.status),
            cut.triggered_at,
            cut.completed_at
        );
        for topic in &cut.topics {
            for partition in &topic.partitions {
                println!(
                    "  {}\t{}\t{}",
                    topic.topic, partition.partition, partition.offset
                );
            }
        }
        for missing in &cut.missing {
            println!("  {}\t{}\tno marker", missing.topic, missing.partition);
        }
    }
    0
}

/// What a verify found.
pub(crate) fn report_verify(outcome: &VerifyOutcome) -> i32 {
    for checked in &outcome.checked {
        println!("{}\t{}\t{}\tok", checked.0, checked.1, checked.2);
    }
    if outcome.mismatches.is_empty() {
        println!(
            "cut {} verified: {} markers are in the log",
            outcome.epoch,
            outcome.checked.len()
        );
        return 0;
    }
    for mismatch in &outcome.mismatches {
        eprintln!(
            "{}\t{}\t{}\t{}",
            mismatch.topic, mismatch.partition, mismatch.offset, mismatch.reason
        );
    }
    eprintln!(
        "cut {} does not match the log: {} of {} offsets are wrong",
        outcome.epoch,
        outcome.mismatches.len(),
        outcome.checked.len() + outcome.mismatches.len()
    );
    EXIT_MISMATCH
}

/// The name of a cut status code.
fn status_name(status: i8) -> &'static str {
    if status == krabka_protocol::krabka::barrier::CUT_STATUS_COMPLETE {
        "complete"
    } else {
        "partial"
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn a_status_code_reads_as_a_word() {
        check!(status_name(krabka_protocol::krabka::barrier::CUT_STATUS_COMPLETE) == "complete");
        check!(status_name(krabka_protocol::krabka::barrier::CUT_STATUS_PARTIAL) == "partial");
    }
}
