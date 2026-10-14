//! Sync fenced code blocks in website markdown from anchored regions of source
//! files.
//!
//! The published docs thus contain exactly the tested example code.

use std::path::Path;

/// Extract the lines between `// docs:begin <anchor>` and `// docs:end <anchor>`
/// in `source`.
///
/// The function removes the markers and trims the common leading indentation.
///
/// # Errors
/// Returns an error string if a marker is missing, repeated, or out of order.
pub fn extract(source: &str, anchor: &str) -> Result<String, String> {
    let begin = format!("// docs:begin {anchor}");
    let end = format!("// docs:end {anchor}");
    let mut lines = Vec::new();
    let mut inside = false;
    let mut completed = false;
    for line in source.lines() {
        if line.trim() == begin {
            if inside || completed {
                return Err(format!("anchor {anchor}: repeated begin marker"));
            }
            inside = true;
            continue;
        }
        if line.trim() == end {
            if !inside {
                return Err(format!("anchor {anchor}: end before begin"));
            }
            inside = false;
            completed = true;
            continue;
        }
        if inside {
            lines.push(line.to_string());
        }
    }
    if !completed || inside {
        return Err(format!("anchor {anchor}: markers not found in source"));
    }
    let indent = lines
        .iter()
        .filter(|line| !line.trim().is_empty())
        .map(|line| line.len() - line.trim_start_matches([' ', '\t']).len())
        .min()
        .unwrap_or(0);
    Ok(lines
        .iter()
        .map(|line| line.get(indent..).unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n"))
}

/// Rewrite every `<!-- snippet: <relpath>#<anchor> --> ... <!-- /snippet -->`
/// block in `markdown` with the current code from `crates_dir/<relpath>`.
///
/// The function returns the new markdown. It is idempotent.
///
/// # Errors
/// Returns an error if a referenced source file or anchor cannot be read.
pub fn sync_markdown(markdown: &str, crates_dir: &Path) -> Result<String, String> {
    let mut out = String::new();
    let mut rest = markdown;
    while let Some(start) = rest.find("<!-- snippet:") {
        let (head, after) = rest.split_at(start);
        out.push_str(head);
        let close = after.find("-->").ok_or("unterminated snippet directive")?;
        let directive = &after[..close];
        let spec = directive.trim_start_matches("<!-- snippet:").trim();
        let (relpath, anchor) = spec
            .split_once('#')
            .ok_or_else(|| format!("snippet directive missing '#': {spec}"))?;
        let end_marker = "<!-- /snippet -->";
        let body_start = &after[close + 3..];
        let body_end = body_start
            .find(end_marker)
            .ok_or("missing <!-- /snippet -->")?;
        let source = std::fs::read_to_string(crates_dir.join(relpath.trim()))
            .map_err(|e| format!("read {relpath}: {e}"))?;
        let code = extract(&source, anchor.trim())?;
        out.push_str(directive);
        out.push_str("-->\n");
        out.push_str("```rust\n");
        out.push_str(&code);
        out.push_str("\n```\n");
        out.push_str(end_marker);
        rest = &body_start[body_end + end_marker.len()..];
    }
    out.push_str(rest);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_trims_indent_and_markers() {
        let src = "fn main() {\n    // docs:begin foo\n    let x = 1;\n    // docs:end foo\n}\n";
        assert2::assert!(extract(src, "foo").unwrap() == "let x = 1;");
    }

    #[test]
    fn extract_uses_the_unique_exact_comment_marker_pair() {
        let source = "// docs:begin foobar\nwrong();\n// docs:end foobar\n\
                      // docs:begin foo\nright();\n// docs:end foo\n";
        assert2::assert!(extract(source, "foo").unwrap() == "right();");
    }

    #[yare::parameterized(
        wrong_marker_name = { "// docs:begin foobar\nwrong();\n// docs:end foobar\n" },
        marker_text_in_strings = {
            "let s = \"// docs:begin foo\";\nwrong();\nlet e = \"// docs:end foo\";\n",
        },
        duplicate_begin = { "// docs:begin foo\n// docs:begin foo\n// docs:end foo\n" },
        duplicate_pairs = {
            "// docs:begin foo\none();\n// docs:end foo\n// docs:begin foo\ntwo();\n// docs:end foo\n",
        },
        reversed_markers = { "// docs:end foo\n// docs:begin foo\n" },
    )]
    fn extract_requires_an_exact_comment_marker_and_unique_pair(invalid: &str) {
        assert2::assert!(extract(invalid, "foo").is_err());
    }

    #[test]
    fn sync_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("c/examples")).unwrap();
        std::fs::write(
            dir.path().join("c/examples/e.rs"),
            "// docs:begin a\nlet y = 2;\n// docs:end a\n",
        )
        .unwrap();
        let md = "intro\n<!-- snippet: c/examples/e.rs#a -->\nOLD\n<!-- /snippet -->\nend\n";
        let once = sync_markdown(md, dir.path()).unwrap();
        let twice = sync_markdown(&once, dir.path()).unwrap();
        assert2::assert!(once == twice);
        assert2::assert!(once.contains("```rust\nlet y = 2;\n```"));
        assert2::assert!(!once.contains("OLD"));
    }
}
