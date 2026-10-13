//! Serialisation of a captured `DescribeGroups` response to the JSON fixtures
//! under `tests/fixtures/describe_groups/`.
//!
//! String fields are written verbatim and byte fields as hex plus a UTF-8-lossy
//! rendering, so a fixture is both diffable and readable by a person.

use {crate::support::manifest_dir, std::path::PathBuf};

fn fixtures_dir() -> PathBuf {
    manifest_dir()
        .join("tests")
        .join("fixtures")
        .join("describe_groups")
}

pub(crate) fn write_fixture(name: &str, body: &str) {
    let dir = fixtures_dir();
    std::fs::create_dir_all(&dir).unwrap_or_else(|e| panic!("create dir {}: {e}", dir.display()));
    let path = dir.join(name);
    std::fs::write(&path, body).unwrap_or_else(|e| panic!("write fixture {}: {e}", path.display()));
    eprintln!("CAPTURE wrote {} ({} bytes)", path.display(), body.len());
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Convert one described group to JSON.
///
/// String fields stay verbatim. Member byte fields become hex plus UTF-8-lossy,
/// so the fixture is both diffable and readable.
pub(crate) fn group_json(
    g: &krabka_protocol::owned::describe_groups_response::DescribedGroup,
) -> serde_json::Value {
    let members: Vec<serde_json::Value> = g
        .members
        .iter()
        .map(|m| {
            serde_json::json!({
                "member_id": m.member_id,
                "client_id": m.client_id,
                "client_host": m.client_host,
                "member_metadata_len": m.member_metadata.len(),
                "member_metadata_hex": hex(&m.member_metadata),
                "member_metadata_lossy": String::from_utf8_lossy(&m.member_metadata),
                "member_assignment_len": m.member_assignment.len(),
                "member_assignment_hex": hex(&m.member_assignment),
                "member_assignment_lossy": String::from_utf8_lossy(&m.member_assignment),
            })
        })
        .collect();
    serde_json::json!({
        "group_id": g.group_id,
        "error_code": g.error_code,
        "group_state": g.group_state,
        "protocol_type": g.protocol_type,
        "protocol_data": g.protocol_data,
        "members": members,
    })
}
