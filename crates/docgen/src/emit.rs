//! Wrap generated markdown bodies in Zola front matter and write the
//! reference tree to disk.

use std::path::Path;

/// Front matter for a docs page. It uses the `AdiDoks` docs layout.
#[must_use]
pub fn page_front_matter(title: &str, weight: u32, body: &str) -> String {
    format!(
        "+++\ntitle = \"{title}\"\nweight = {weight}\ntemplate = \"docs/page.html\"\n+++\n\n{body}"
    )
}

/// Front matter for a docs page that also needs an `[extra]` table.
///
/// An example is a page that opts into Mermaid rendering with
/// `extra.mermaid = true`. The `extra_toml` is the raw body of the `[extra]`
/// table: one `key = value` per line, with no `[extra]` header around it.
#[must_use]
pub fn page_front_matter_with_extra(
    title: &str,
    weight: u32,
    extra_toml: &str,
    body: &str,
) -> String {
    format!(
        "+++\ntitle = \"{title}\"\nweight = {weight}\ntemplate = \"docs/page.html\"\n\n[extra]\n{extra_toml}\n+++\n\n{body}"
    )
}

/// Front matter for a docs section index. It uses the `AdiDoks` docs section
/// layout.
#[must_use]
pub fn section_front_matter(title: &str, weight: u32, body: &str) -> String {
    format!(
        "+++\ntitle = \"{title}\"\nweight = {weight}\nsort_by = \"weight\"\ntemplate = \"docs/section.html\"\n+++\n\n{body}"
    )
}

/// Write the full reference tree under `out_dir`.
///
/// The tree holds the broker pages, the concepts pages, and the section
/// indexes. This function overwrites existing files.
///
/// The operator CRD pages are not here. The operator crate lives in
/// `krabka-io/krabka-operator`, so its reference pages are rendered there.
///
/// # Errors
/// Returns an error if any directory cannot be created or any file cannot be
/// written.
pub fn write_reference_tree(out_dir: &Path) -> anyhow::Result<()> {
    use {
        crate::{broker, scenarios},
        std::fs,
    };
    let br_dir = out_dir.join("broker");
    let concepts_dir = out_dir.join("concepts");
    fs::create_dir_all(&br_dir)?;
    fs::create_dir_all(&concepts_dir)?;
    fs::write(
        out_dir.join("_index.md"),
        section_front_matter(
            "Reference",
            40,
            "Auto-generated API reference for the Krabka broker.",
        ),
    )?;
    fs::write(
        br_dir.join("_index.md"),
        section_front_matter(
            "Broker",
            20,
            "Broker server config, topic configs, and the Kafka protocol API surface.",
        ),
    )?;
    fs::write(
        br_dir.join("server-config.md"),
        page_front_matter("Server Configuration", 10, &broker::server_config_md()),
    )?;
    fs::write(
        br_dir.join("topic-configs.md"),
        page_front_matter("Topic Configs", 20, &broker::topic_configs_md()),
    )?;
    fs::write(
        br_dir.join("protocol-apis.md"),
        page_front_matter("Protocol APIs", 30, &broker::protocol_apis_md()),
    )?;
    fs::write(
        concepts_dir.join("_index.md"),
        section_front_matter(
            "Concepts",
            40,
            "How Krabka's consensus core behaves under failures, illustrated with \
             diagrams generated from the simulator itself.",
        ),
    )?;
    fs::write(
        concepts_dir.join("failure-scenarios.md"),
        page_front_matter_with_extra(
            "Failure Scenarios",
            10,
            "mermaid = true",
            &scenarios::failure_scenarios_md(),
        ),
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {

    use {super::*, tempfile::tempdir};
    #[test]
    fn page_front_matter_wraps_body() {
        let s = page_front_matter("Kafka", 10, "hello");
        assert2::assert!(s.starts_with(
            "+++\ntitle = \"Kafka\"\nweight = 10\ntemplate = \"docs/page.html\"\n+++\n"
        ));
        assert2::assert!(s.contains("hello"));
    }
    #[test]
    fn writes_full_tree() {
        let dir = tempdir().unwrap();
        write_reference_tree(dir.path()).unwrap();
        for page in [
            "broker/protocol-apis.md",
            "broker/server-config.md",
            "broker/topic-configs.md",
            "concepts/failure-scenarios.md",
        ] {
            assert2::assert!(dir.path().join(page).exists());
        }
        let apis = std::fs::read_to_string(dir.path().join("broker/protocol-apis.md")).unwrap();
        assert2::assert!(apis.contains("template = \"docs/page.html\""));
        let idx = std::fs::read_to_string(dir.path().join("_index.md")).unwrap();
        assert2::assert!(idx.contains("template = \"docs/section.html\""));
    }
}
