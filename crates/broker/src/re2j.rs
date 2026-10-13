//! The RE2J dialect that Kafka compiles client-supplied patterns with.
//!
//! Kafka uses `com.google.re2j` for the KIP-848 `SubscribedTopicRegex` and for
//! the KIP-1038 `TransactionIdPattern` of `ListTransactions`. RE2 has no
//! backtracking, so a pattern that a client sends cannot cost more than linear
//! time in the input. Rust's `regex` gives the same guarantee and the same
//! refusals for lookaround, backreferences and possessive quantifiers.
//!
//! The two grammars are not identical. We translate RE2J's `\Q...\E` quoting
//! and character-class syntax before compiling, and leave only `regex`'s
//! compiled-size limit as a known acceptance difference.
//!
//! The inline flags are screened, because that is the divergence a client is
//! likely to hit: `regex` takes `x`, `u` and `R`, and RE2J refuses them.

/// RE2J's `ERR_INVALID_PERL_OP`, the description Kafka formats into the
/// `INVALID_REGULAR_EXPRESSION` message when a pattern names a flag RE2J does
/// not have.
pub(crate) const INVALID_PERL_OP: &str = "invalid or unsupported Perl syntax";

/// The inline flags RE2J's `Parser.parsePerlFlags` accepts, plus the `-` that
/// negates the ones that follow it.
const INLINE_FLAGS: [char; 5] = ['i', 'm', 's', 'U', '-'];

/// The first inline flag in `pattern` that RE2J's `parsePerlFlags` does not
/// accept, if any.
///
/// The scan tracks escapes and character classes so a `(` that is a literal,
/// and everything inside `[...]`, is left alone, and it hands every other
/// `(?` form -- named captures, non-capturing groups, the lookarounds both
/// engines refuse -- to the regex parser rather than judging it here.
pub(crate) fn unsupported_inline_flag(pattern: &str) -> Option<char> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_class = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\\' {
            index += 2;
            continue;
        }
        if in_class {
            in_class = current != ']';
            index += 1;
            continue;
        }
        if current == '[' {
            in_class = true;
            index += 1;
            continue;
        }
        index += 1;
        if current != '(' || chars.get(index) != Some(&'?') {
            continue;
        }
        // `(?P<`, `(?<`, `(?'`, `(?:`, `(?=` and `(?!` are not flag groups.
        if matches!(
            chars.get(index + 1),
            Some('P' | ':' | '<' | '\'' | '=' | '!')
        ) {
            continue;
        }
        let mut flag = index + 1;
        while let Some(&candidate) = chars.get(flag) {
            if candidate == ')' || candidate == ':' {
                break;
            }
            if !INLINE_FLAGS.contains(&candidate) {
                return Some(candidate);
            }
            flag += 1;
        }
    }
    None
}

/// RE2J's `ERR_INVALID_REPEAT_OP`, its message for a possessive quantifier.
pub(crate) const INVALID_REPEAT_OP: &str = "invalid nested repetition operator";

/// RE2J's `ERR_INVALID_CHAR_RANGE`.
const INVALID_CHAR_RANGE: &str = "invalid character class range";

/// Kafka's `String` protocol type is bounded by a signed 16-bit byte length.
const MAX_PATTERN_BYTES: usize = i16::MAX as usize;

/// Translate RE2J's literal quoting and character-class behavior to syntax
/// accepted by Rust's non-backtracking `regex` engine.
fn translate(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len());
    let mut index = 0;
    let mut in_class = false;
    let mut class_first = false;
    let mut class_first_member = false;

    while index < chars.len() {
        let current = chars[index];
        if !in_class && current == '\\' && chars.get(index + 1) == Some(&'Q') {
            index += 2;
            let start = index;
            while index + 1 < chars.len() && !(chars[index] == '\\' && chars[index + 1] == 'E') {
                index += 1;
            }
            let has_terminator = index + 1 < chars.len();
            let end = if has_terminator { index } else { chars.len() };
            let quoted: String = chars[start..end].iter().collect();
            out.push_str(&regex::escape(&quoted));
            if has_terminator {
                index += 2; // closing \\E
            } else {
                index = chars.len(); // RE2J quotes through end of input
            }
            continue;
        }

        if current == '\\' {
            out.push(current);
            if let Some(&escaped) = chars.get(index + 1) {
                out.push(escaped);
                index += 2;
            } else {
                index += 1;
            }
            if in_class && class_first {
                class_first = false;
            }
            if in_class {
                class_first_member = false;
            }
            continue;
        }

        if in_class {
            if current == ']' && class_first_member {
                out.push(current);
                class_first = false;
                class_first_member = false;
                index += 1;
                continue;
            }
            if current == ']' && !class_first {
                in_class = false;
                class_first_member = false;
                out.push(current);
                index += 1;
                continue;
            }
            if current == '^' && class_first {
                out.push(current);
                class_first = false;
                index += 1;
                continue;
            }
            if current == '[' {
                // RE2J treats an unescaped '[' as a class member. Escaping it
                // keeps Rust from interpreting it as a nested set.
                out.push_str(r"\[");
            } else if current == '&' && chars.get(index + 1) == Some(&'&') {
                // RE2J's class grammar treats both ampersands literally.
                out.push_str(r"\&\&");
                index += 2;
                class_first = false;
                continue;
            } else if current == '-' && chars.get(index + 1) == Some(&'-') {
                if class_first_member {
                    // At the beginning RE2J accepts `--a` as the range `-` to
                    // `a`, and `[---]` as the range `-` to `-`.
                    out.push_str(r"\x2d-");
                    if chars.get(index + 2) == Some(&'-') && chars.get(index + 3) == Some(&']') {
                        out.push_str(r"\x2d");
                        index += 3;
                    } else {
                        index += 2;
                    }
                    class_first = false;
                    class_first_member = false;
                    continue;
                }
                // Rust accepts this as set subtraction; RE2J parses it as a
                // descending range and refuses it.
                return Err(INVALID_CHAR_RANGE.to_owned());
            } else {
                out.push(current);
            }
            class_first = false;
            class_first_member = false;
            index += 1;
            continue;
        }

        if current == '[' {
            in_class = true;
            class_first = true;
            class_first_member = true;
        }
        out.push(current);
        index += 1;
    }
    Ok(out)
}

/// Whether `pattern` carries a possessive quantifier (`a*+`, `a++`, `a?+`,
/// `a{2,3}+`).
///
/// RE2 has no possessive quantifiers and refuses them. Rust's `regex` reads
/// `a*+` as a repetition of a repetition and compiles it, so the scan is what
/// keeps the two engines on the same answer. It tracks escapes and character
/// classes, so a literal `+` is left alone.
pub(crate) fn possessive_quantifier(pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_class = false;
    let mut after_repeat = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\\' {
            index += 2;
            after_repeat = false;
            continue;
        }
        if in_class {
            in_class = current != ']';
            index += 1;
            after_repeat = false;
            continue;
        }
        match current {
            '[' => {
                in_class = true;
                after_repeat = false;
            }
            '*' | '?' | '}' => after_repeat = true,
            '+' => {
                if after_repeat {
                    return true;
                }
                after_repeat = true;
            }
            _ => after_repeat = false,
        }
        index += 1;
    }
    false
}

/// `regex`'s `Display` is a multi-line diagram. Flatten it so a response's
/// `error_message` stays one line.
pub(crate) fn flatten_error(error: &regex::Error) -> String {
    error
        .to_string()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `pattern` is one RE2J compiles.
///
/// # Errors
///
/// Returns the reason, in the wording Kafka puts in its
/// `INVALID_REGULAR_EXPRESSION` message.
pub(crate) fn check(pattern: &str) -> Result<(), String> { compile_full_match(pattern).map(|_| ()) }

/// Compiles `pattern` for Kafka's `Matcher.matches()`, which matches the whole
/// input.
///
/// The anchors are `\A` and `\z`, so an inline `(?m)` cannot turn them into
/// line anchors.
///
/// # Errors
///
/// Returns the reason, in the wording Kafka puts in its
/// `INVALID_REGULAR_EXPRESSION` message.
pub(crate) fn compile_full_match(pattern: &str) -> Result<regex::Regex, String> {
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(format!(
            "pattern exceeds Kafka string limit of {MAX_PATTERN_BYTES} bytes"
        ));
    }
    let translated = translate(pattern)?;
    if unsupported_inline_flag(&translated).is_some() {
        return Err(INVALID_PERL_OP.to_owned());
    }
    if possessive_quantifier(&translated) {
        return Err(INVALID_REPEAT_OP.to_owned());
    }
    // Compile the pattern on its own first, so the error text names what the
    // client sent rather than the anchors around it.
    regex::Regex::new(&translated).map_err(|error| flatten_error(&error))?;
    regex::Regex::new(&format!(r"\A(?:{translated})\z")).map_err(|error| flatten_error(&error))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    /// The patterns Kafka's RE2J takes, and the ones it refuses.
    #[test]
    fn the_dialect_follows_re2j() {
        // (pattern, compiles)
        let cases = [
            ("txn-.*", true),
            ("(a|b)+c", true),
            ("(?i)TXN", true),
            ("(?s).*", true),
            ("[a-z0-9_.-]+", true),
            // Lookaround, backreferences and possessive quantifiers are Perl
            // syntax that RE2 does not have.
            ("(?=txn-).*", false),
            ("(?!txn-).*", false),
            ("(txn)-\\1", false),
            ("a*+", false),
            ("a++", false),
            ("a?+", false),
            ("a{2,3}+", false),
            // A lazy quantifier is in both engines, and a literal `+` in a
            // class is not a quantifier.
            ("a+?", true),
            ("[+*]+", true),
            ("a\\++", true),
            // An inline flag RE2J does not know.
            ("(?x) txn", false),
            ("(?u)txn", false),
            // Malformed.
            ("[", false),
            ("(", false),
        ];
        let expected: Vec<(&str, bool)> = cases.to_vec();
        let actual: Vec<(&str, bool)> = cases
            .iter()
            .map(|(pattern, _)| (*pattern, compile_full_match(pattern).is_ok()))
            .collect();
        assert!(actual == expected);
    }

    #[test]
    fn literal_quoting_matches_re2j() {
        let quoted = compile_full_match(r"\Q[a.*]\E").expect("quoted literal compiles");
        check!(quoted.is_match("[a.*]"));
        check!(!quoted.is_match("aaa"));

        let through_end = compile_full_match(r"prefix\Q.+[").expect("unterminated quote compiles");
        check!(through_end.is_match("prefix.+["));

        // The first `\\E` ends quoting, and the trailing expression is parsed
        // normally.
        let ended = compile_full_match(r"\Q.\E+").expect("quote terminator compiles");
        check!(ended.is_match("."));
        check!(ended.is_match("..."));
        check!(!ended.is_match("+"));
    }

    #[test]
    fn character_class_operations_keep_re2j_meaning() {
        let literal_ampersands = compile_full_match("[a&&b]").expect("literal class compiles");
        for character in ["a", "&", "b"] {
            check!(literal_ampersands.is_match(character));
        }
        check!(!literal_ampersands.is_match("c"));

        let groups_are_unioned = compile_full_match(r"[\w&&\d]").expect("class compiles");
        for character in ["x", "7", "&"] {
            check!(groups_are_unioned.is_match(character));
        }

        // RE2J reads the leading pair as a range from '-' to 'a', not set
        // subtraction.
        let leading_hyphens = compile_full_match("[--a]").expect("leading range compiles");
        check!(leading_hyphens.is_match("-"));
        check!(leading_hyphens.is_match("0"));
        check!(leading_hyphens.is_match("a"));

        let negated_leading_hyphens =
            compile_full_match("[^--a]").expect("negated leading range compiles");
        check!(!negated_leading_hyphens.is_match("-"));
        check!(!negated_leading_hyphens.is_match("0"));
        check!(!negated_leading_hyphens.is_match("a"));
        check!(negated_leading_hyphens.is_match("z"));

        let hyphen_to_hyphen = compile_full_match("[---]").expect("hyphen range compiles");
        check!(hyphen_to_hyphen.is_match("-"));
        check!(!hyphen_to_hyphen.is_match("a"));

        check!(compile_full_match("[a-z--b]").err() == Some(INVALID_CHAR_RANGE.to_owned()));
        // An unescaped '[' inside a class is a member, not a nested set.
        let literal_open_bracket = compile_full_match("[[a]").expect("class compiles");
        check!(literal_open_bracket.is_match("["));
        check!(literal_open_bracket.is_match("a"));
    }

    #[test]
    fn negated_class_prefix_and_initial_close_bracket_are_preserved() {
        let pattern = compile_full_match("[^^][a]").expect("classes compile");
        check!(pattern.is_match("xa"));
        check!(!pattern.is_match("^a"));

        let initial_close = compile_full_match("[^]]").expect("initial close bracket compiles");
        check!(!initial_close.is_match("]"));
        check!(initial_close.is_match("a"));
    }

    #[test]
    fn rejects_patterns_over_kafka_string_bound_before_translation() {
        let pattern = format!("\\Q{}\\E", "x".repeat(MAX_PATTERN_BYTES));
        let error = compile_full_match(&pattern).expect_err("oversized pattern rejected");
        check!(error.contains("Kafka string limit"));
    }

    /// `Matcher.matches()` matches the whole input, and an inline `(?m)` does
    /// not change that.
    #[test]
    fn a_compiled_pattern_matches_the_whole_input() {
        let pattern = compile_full_match("txn-.*").expect("compiles");
        check!(pattern.is_match("txn-1"));
        check!(!pattern.is_match("my-txn-1"));
        check!(!pattern.is_match("txn-1\nother"));

        let multiline = compile_full_match("(?m)^txn$").expect("compiles");
        check!(multiline.is_match("txn"));
        check!(!multiline.is_match("other\ntxn"));
    }

    #[test]
    fn an_unsupported_flag_carries_kafkas_message() {
        check!(compile_full_match("(?x)txn").err() == Some(INVALID_PERL_OP.to_owned()));
        check!(unsupported_inline_flag("[(?x)]").is_none());
        check!(unsupported_inline_flag("\\(?x)").is_none());
    }
}
