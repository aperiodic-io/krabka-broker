//! `java.util.regex` behaviour for the two rule grammars that hand a
//! user-written regular expression to Java: `ssl.principal.mapping.rules`
//! and the Kerberos `auth_to_local` rules.
//!
//! The `regex` crate differs from Java in the two places these grammars lean
//! on. `Matcher.matches()` succeeds when *some* way of matching spans the
//! whole input, including through backtracking and lazy quantifiers, where a
//! leftmost-first search stops at the first match it finds. And
//! `Matcher.replaceAll` reads `$1_x` as group 1 followed by `_x`, where the
//! `regex` crate reads a group named `1_x`. [`JavaPattern`] gives both the
//! Java answer.
//!
//! The pattern itself is rewritten by `java_to_fancy` first, which also gives
//! Java's reading of `\w`, `.`, and `(?i)`: Java folds ASCII case only, unless
//! `(?u)` asks for Unicode case as well, so `(?i)service-` does not match
//! `ſervice-` (long s).

use {
    crate::client_metrics::config::java_to_fancy,
    fancy_regex::{Captures, Regex},
    std::{collections::BTreeMap, sync::Arc},
};

/// A `java.util.regex.Pattern` compiled for `matches()` and
/// `replaceAll`/`replaceFirst`.
#[derive(Debug, Clone)]
pub(super) struct JavaPattern(Arc<Compiled>);

/// The engines behind a [`JavaPattern`], shared so that a rule holding one is
/// small.
#[derive(Debug)]
struct Compiled {
    /// The pattern as written, for `replaceAll` and `replaceFirst`.
    search: Regex,
    /// The pattern pinned to the whole input, for `matches()`.
    whole: Regex,
    /// Group name to index, for `${name}` in a replacement.
    names: BTreeMap<String, usize>,
}

/// A replacement string Java's `Matcher.appendReplacement` refuses, which
/// Java reports as `IllegalArgumentException` or `IndexOutOfBoundsException`,
/// or a search the engine gave up on at its backtrack limit.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct ReplacementError(pub(super) String);

impl JavaPattern {
    /// Compiles `pattern`, or says why Java's `Pattern.compile` would not.
    pub(super) fn compile(pattern: &str) -> Result<Self, String> {
        let translated = java_to_fancy(pattern)
            .ok_or_else(|| format!("{pattern:?} is not a java.util.regex pattern"))?;
        let compile = |source: &str| Regex::new(source).map_err(|error| error.to_string());
        let search = compile(&translated)?;
        let whole = compile(&format!("\\A(?:{translated})\\z"))?;
        let names = search
            .capture_names()
            .enumerate()
            .filter_map(|(index, name)| Some((name?.to_owned(), index)))
            .collect();
        Ok(Self(Arc::new(Compiled {
            search,
            whole,
            names,
        })))
    }

    /// The number of capturing groups, `Matcher.groupCount()`.
    pub(super) fn group_count(&self) -> usize { self.0.search.captures_len() - 1 }

    /// `Matcher.matches()`.
    ///
    /// # Errors
    ///
    /// A search that runs past the engine's backtrack limit is an error and
    /// not a no match. Java answers such a pattern, however slowly, so
    /// reading the limit as "does not match" would let a rule that Java
    /// applies fall through to the next rule, or to `DEFAULT`, and map the
    /// principal to a name the operator never meant for it. A caller rejects
    /// the name instead.
    pub(super) fn matches(&self, input: &str) -> Result<bool, ReplacementError> {
        self.0
            .whole
            .is_match(input)
            .map_err(|error| ReplacementError(error.to_string()))
    }

    /// `input.replaceAll(pattern, replacement)` when `all`, and
    /// `input.replaceFirst(pattern, replacement)` when not.
    ///
    /// The matches are `Matcher.find`'s, not the `regex` crate's iterator's.
    /// After a match that is not empty, `find` may match the empty string at
    /// the very position the match ended, and the crate's iterator does not:
    /// `"alice".replaceAll("(.*)", "$1x")` is `alicexx` in Java. After an empty
    /// match `find` starts one character on.
    pub(super) fn replace(
        &self,
        input: &str,
        replacement: &str,
        all: bool,
    ) -> Result<String, ReplacementError> {
        let mut out = String::with_capacity(input.len());
        let mut copied = 0;
        let mut search_from = 0;
        while search_from <= input.len() {
            let captures = self
                .0
                .search
                .captures_from_pos(input, search_from)
                .map_err(|error| ReplacementError(error.to_string()))?;
            let Some(captures) = captures else { break };
            let Some(whole) = captures.get(0) else { break };
            out.push_str(&input[copied..whole.start()]);
            self.append_replacement(&mut out, replacement, &captures)?;
            copied = whole.end();
            if !all {
                break;
            }
            search_from = if whole.start() == whole.end() {
                input[whole.end()..]
                    .chars()
                    .next()
                    .map_or(input.len() + 1, |next| whole.end() + next.len_utf8())
            } else {
                whole.end()
            };
        }
        out.push_str(&input[copied..]);
        Ok(out)
    }

    /// `Matcher.appendReplacement`: `\x` is a literal `x`, `${name}` and `$n`
    /// are groups, and `$n` takes as many further digits as still name a
    /// group. A group that did not take part in the match adds nothing.
    fn append_replacement(
        &self,
        out: &mut String,
        replacement: &str,
        captures: &Captures<'_, str>,
    ) -> Result<(), ReplacementError> {
        let refuse = |message: &str| ReplacementError(message.to_owned());
        let group_count = self.group_count();
        let mut chars = replacement.chars().peekable();
        while let Some(character) = chars.next() {
            match character {
                '\\' => out.push(
                    chars
                        .next()
                        .ok_or_else(|| refuse("character to be escaped is missing"))?,
                ),
                '$' => {
                    let first = chars
                        .next()
                        .ok_or_else(|| refuse("illegal group reference: group index is missing"))?;
                    let group = if first == '{' {
                        let mut name = String::new();
                        while let Some(next) = chars.next_if(char::is_ascii_alphanumeric) {
                            name.push(next);
                        }
                        if name.is_empty() {
                            return Err(refuse("named capturing group has 0 length name"));
                        }
                        if chars.next() != Some('}') {
                            return Err(refuse("named capturing group is missing trailing '}'"));
                        }
                        *self
                            .0
                            .names
                            .get(&name)
                            .ok_or_else(|| ReplacementError(format!("no group with name {name}")))?
                    } else {
                        let mut group = first
                            .to_digit(10)
                            .ok_or_else(|| refuse("illegal group reference"))?
                            as usize;
                        if group > group_count {
                            return Err(ReplacementError(format!("no group {group}")));
                        }
                        while let Some(digit) = chars.peek().and_then(|next| next.to_digit(10)) {
                            let longer = group * 10 + digit as usize;
                            if longer > group_count {
                                break;
                            }
                            group = longer;
                            chars.next();
                        }
                        group
                    };
                    if let Some(text) = captures.get(group) {
                        out.push_str(text.as_str());
                    }
                }
                other => out.push(other),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    fn pattern(source: &str) -> JavaPattern {
        JavaPattern::compile(source).unwrap_or_else(|error| panic!("{source}: {error}"))
    }

    fn check_matches<'a>(
        cases: impl IntoIterator<Item = (&'a str, &'a str, bool)>,
        debug_names: bool,
    ) {
        for (source, input, expected) in cases {
            let context = if debug_names {
                format!("{source:?} against {input:?}")
            } else {
                format!("{source} against {input}")
            };
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{context}"
            );
        }
    }

    fn check_replacements<'a>(
        cases: impl IntoIterator<Item = (&'a str, &'a str, &'a str, &'a str)>,
        all: bool,
        include_replacement: bool,
    ) {
        for (source, input, replacement, expected) in cases {
            let context = if include_replacement {
                format!("{source} on {input} with {replacement}")
            } else {
                format!("{source} on {input}")
            };
            check!(
                pattern(source)
                    .replace(input, replacement, all)
                    .ok()
                    .as_deref()
                    == Some(expected),
                "{context}"
            );
        }
    }

    /// `Matcher.matches()` finds a whole-input match by backtracking, which a
    /// leftmost-first search does not.
    #[test]
    fn matches_backtracks_to_span_the_whole_input() {
        // (pattern, input, matches)
        let cases = [
            ("a|ab", "ab", true),
            ("a|ab", "abc", false),
            ("CN=(.*?)", "CN=abc", true),
            ("(a+)+b", "aaab", true),
            ("ali", "alice", false),
            ("ali.*", "alice", true),
        ];
        check_matches(cases, false);
    }

    /// A search that runs past the engine's backtrack limit is an error. Java
    /// answers the same pattern, slowly, so "no match" would send a rule that
    /// Java applies on to the next rule, or to `DEFAULT`.
    #[test]
    fn matches_reports_a_search_the_engine_gave_up_on() {
        let hostile = format!("{}b", "a".repeat(40));

        let result = pattern("^(a|aa)+\\1$").matches(&hostile);

        assert!(result.is_err());
    }

    /// `Matcher.find` may match the empty string at the position a non-empty
    /// match ended, which the `regex` crate's iterator does not, and starts
    /// one character on after an empty match. Each row is the result of
    /// `input.replaceAll(pattern, replacement)` on JDK 21.
    #[test]
    fn replace_all_finds_an_empty_match_after_a_non_empty_one() {
        // (pattern, input, replacement, result)
        let cases = [
            ("(.*)", "alice", "$1x", "alicexx"),
            ("a*", "baaac", "-", "-b--c-"),
            ("x*", "abc", "-", "-a-b-c-"),
            ("\\w*", "ab cd", "[$0]", "[ab][] [cd][]"),
            ("(?:)", "é!", "|", "|é|!|"),
            ("", "", "|", "|"),
            ("a", "", "|", ""),
            ("b*", "aé", "-", "-a-é-"),
        ];
        check_replacements(cases, true, false);
    }

    /// `replaceFirst` stops after the first match, an empty one included.
    #[test]
    fn replace_first_stops_after_the_first_match_even_when_it_is_empty() {
        // (pattern, input, replacement, result)
        check_replacements(
            [
                ("(.*)", "alice", "$1x", "alicex"),
                ("b*", "abc", "-", "-abc"),
            ],
            false,
            false,
        );
    }

    /// Java reads `\w`, `\d`, `\s` and `\b` as ASCII unless `(?U)` asks for
    /// Unicode, and `.` stops at every line terminator unless `(?s)` or `(?d)`
    /// says otherwise. Each row is `Pattern.matches` on JDK 21, except `\b`,
    /// which reads ASCII from JDK 19.
    #[test]
    fn matches_reads_classes_as_ascii_and_dot_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("\\w+", "abc_09", true),
            ("\\w+", "é", false),
            ("\\W", "é", true),
            ("\\d", "٣", false),
            ("\\D", "٣", true),
            ("\\s", " \t\u{0B}", false),
            ("\\s+", " \t\u{0B}", true),
            ("\\s", "\u{a0}", false),
            ("\\s", "\u{2003}", false),
            ("\\S", "\u{a0}", true),
            ("[\\w.]+", "a.b", true),
            ("[\\w.]+", "é", false),
            ("[^\\w]", "é", true),
            ("[\\W]+", "é!", true),
            ("[\\d-]+", "1-2", true),
            ("[\\s]", "\u{a0}", false),
            ("(?U)\\w+", "é", true),
            ("(?U)\\d", "٣", true),
            ("(?U)\\s", "\u{a0}", true),
            ("(?U:\\w)\\w", "éa", true),
            ("(?U:\\w)\\w", "éé", false),
            ("(?:(?U)\\w)\\w", "éé", false),
            ("(?U)(?-U)\\w", "é", false),
            ("\\bfoo", "foo", true),
            ("a\\b", "a", true),
            ("\\Bfoo", "foo", false),
            ("a\\bé", "aé", true),
            ("\\bé", "é", false),
            ("a\\Bé", "aé", false),
            ("a.b", "axb", true),
            ("a.b", "a\nb", false),
            ("a.b", "a\rb", false),
            ("a.b", "a\u{85}b", false),
            ("a.b", "a\u{2028}b", false),
            ("a.b", "a\u{2029}b", false),
            (".*", "abc\rdef", false),
            ("(?s)a.b", "a\rb", true),
            ("(?s)a.b", "a\nb", true),
            ("(?d)a.b", "a\rb", true),
            ("(?d)a.b", "a\u{85}b", true),
            ("(?d)a.b", "a\nb", false),
            ("(?s:a.)b.", "a\nbx", true),
            ("(?s:a.)b.", "a\nb\r", false),
            ("(?s)(?-s).", "\n", false),
            ("a[.]b", "a.b", true),
            ("a\\.b", "axb", false),
        ];
        check_matches(cases, true);
    }

    /// `(?i)` folds ASCII case only, and `(?u)`, or `(?U)`, adds Unicode case.
    /// `fancy_regex` folds Unicode whenever its `i` is on, so the translation
    /// writes both cases itself. Each row is `Pattern.matches` on JDK 17, from
    /// the `java.util.regex` documentation of `CASE_INSENSITIVE` and
    /// `UNICODE_CASE`: long s (U+017F) and the Kelvin sign (U+212A) are the
    /// letters that fold to `s` and `k` in Unicode and not in ASCII.
    #[test]
    fn matches_folds_case_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?i)service-", "SERVICE-", true),
            ("(?i)service-", "service-", true),
            ("(?i)service-", "\u{17f}ervice-", false),
            ("(?iu)service-", "\u{17f}ervice-", true),
            ("(?iu)service-", "SERVICE-", true),
            ("(?i)k", "\u{212a}", false),
            ("(?iu)k", "\u{212a}", true),
            ("(?ui)k", "\u{212a}", true),
            ("(?u)k", "K", false),
            ("(?i)é", "É", false),
            ("(?i)é", "é", true),
            ("(?iu)é", "É", true),
            ("(?U)(?i)k", "\u{212a}", true),
            ("(?iU)k", "\u{212a}", true),
            ("(?iU)(?-U)k", "\u{212a}", false),
            ("(?iU)(?-U)k", "K", true),
            ("(?iU)(?-u)k", "\u{212a}", false),
            ("(?iU)(?-u)\\w", "\u{e9}", true),
            ("(?i)a(?u)k", "A\u{212a}", true),
            ("(?i)a(?u)k", "AK", true),
            ("(?iu)a(?-u)k", "A\u{212a}", false),
            ("(?iu)a(?-u)k", "AK", true),
            ("(?i)ss", "\u{df}", false),
            ("(?iu)ss", "\u{df}", false),
            ("(?i)i", "\u{130}", false),
            ("(?i)i", "\u{131}", false),
            ("(?i)\\u00E5", "\u{212b}", false),
            ("(?iu)\\u00E5", "\u{212b}", true),
            ("(?i)[a-f]+", "AbCdEf", true),
            ("(?i)[a-f]", "\u{212a}", false),
            ("(?i)[k]", "\u{212a}", false),
            ("(?iu)[k]", "\u{212a}", true),
            ("(?i)[^a]", "A", false),
            ("(?i)[^a]", "b", true),
            ("(?i)[^a]", "\u{e9}", true),
            ("(?i)[^k]", "\u{212a}", true),
            ("(?iu)[^k]", "\u{212a}", false),
            ("(?i)[X-c]", "x", true),
            ("(?i)[X-c]", "A", true),
            ("(?i)[X-c]", "[", true),
            ("(?i)[X-c]", "d", false),
            ("(?i)[+-z]", "Q", true),
            ("(?i)[a-c&&[^b]]", "B", false),
            ("(?i)[a-c&&[^b]]", "C", true),
            ("(?i)[a-c&&[^b]]", "A", true),
            ("(?i)[a-c[^x-z]]", "Y", false),
            ("(?i)[a-c[^x-z]]", "B", true),
            ("(?i)[\\x41-\\x43]", "b", true),
            ("(?i)[\\u0041-\\u0043]", "b", true),
            ("(?i)[\\Qab\\E]", "B", true),
            ("(?i)[a-]", "A", true),
            ("(?i)[-a]", "A", true),
            ("(?i)[]a]", "]", true),
            ("(?i)[]a]", "A", true),
            ("(?i)[a-c-e]", "E", true),
            ("(?i)[a-c-e]", "-", true),
            ("(?i)[\\w&&[^a]]", "A", false),
            ("(?i)[\\w&&[^a]]", "B", true),
            ("(?i)[a\\-c]", "C", true),
            ("(?i)[a\\-c]", "-", true),
            ("(?i)[\\0101-\\0103]", "b", true),
            ("(?i)[\\0101-\\0103]", "d", false),
            ("(?i)[\\x41-c]", "b", true),
            ("(?i)[a-c[d-f]]", "E", true),
            ("(?i)[\u{e9}-\u{eb}]", "\u{ca}", false),
            ("(?ix) [ a - c ]", "B", true),
            ("(?ix) [ a - c ] # x", "d", false),
            ("(?i)a\\.b", "A.B", true),
            ("(?i)\\[a\\]", "[A]", true),
            ("(?i)a{1,2}b", "aAB", true),
            ("(?i)\u{e9}+", "\u{e9}\u{e9}", true),
            ("(?i)\\0101\\0102", "aB", true),
            ("(?i)\\x41", "a", true),
            ("(?i)\\u0041", "a", true),
            ("(?i)\\x{41}", "a", true),
            ("(?i)\\0101", "a", true),
            ("(?i)\\c!", "A", true),
            ("(?i)\\Qab\\E", "AB", true),
            ("(?i)\\Q.b\\E", ".B", true),
            ("(?i)\\Q.b\\E", "xB", false),
            ("(?i)\\Qk\\E", "\u{212a}", false),
            ("(?i)\\p{Lower}", "A", true),
            ("(?i)\\p{Lower}", "\u{e9}", false),
            ("(?i)\\p{Upper}", "a", true),
            ("(?i)\\p{Upper}", "\u{c9}", false),
            ("\\p{Lower}", "a", true),
            ("\\p{Lower}", "A", false),
            ("\\p{Lower}", "\u{e9}", false),
            ("\\p{Upper}", "A", true),
            ("\\p{Upper}", "\u{c9}", false),
            ("(?i)\\p{Lu}", "a", true),
            ("(?i)\\p{Lu}", "\u{e9}", true),
            ("(?i)\\p{Lu}", "1", false),
            ("(?i)\\P{Lu}", "a", false),
            ("(?i)\\P{Lu}", "1", true),
            ("\\p{Lu}", "a", false),
            ("\\p{Lu}", "A", true),
            ("(?i)[\\p{Lower}]", "A", true),
            ("(?i)[\\P{Lower}]", "A", false),
            ("(?i)[\\P{Lower}]", "1", true),
            ("(?i)\\p{IsLowercase}", "A", true),
            ("(?i)\\p{gc=Lu}", "a", true),
            ("(?i)\\p{IsLu}", "a", true),
            ("(?i:a)b", "Ab", true),
            ("(?i:a)b", "AB", false),
            ("a(?i)b", "aB", true),
            ("a(?i)b", "Ab", false),
            ("a(?i:b)c", "aBc", true),
            ("a(?i:b)c", "aBC", false),
            ("(?i)a(?-i)b", "Ab", true),
            ("(?i)a(?-i)b", "AB", false),
            ("((?i)a)b", "Ab", true),
            ("((?i)a)b", "AB", false),
            ("(?i)a|b", "B", true),
            ("(?i)(?i)(?-i)a", "A", false),
            ("(?i)(?i)(?-i)a", "a", true),
            ("(?i)a{2}", "aA", true),
            ("(?i)(?:a|b)+", "AbBa", true),
            ("(?i)^ab$", "AB", true),
            ("(?i)\\bab", "AB", true),
            ("(?i)\\w", "A", true),
            ("(?i)a.c", "AxC", true),
            ("(?i)(?<Name>a)", "A", true),
            ("(?i)(?=a)A", "A", true),
            ("(?i)(?!a)b", "B", true),
            ("(?ix) a b # comment", "AB", true),
            ("(?x)a b # Comment here", "ab", true),
            ("(?x)a b # Comment here", "AB", false),
            ("(?iu)(a)\\1", "aA", true),
            ("(?iu)(a)\\1", "ab", false),
            ("(?i)(a)(?-i)\\1", "aA", false),
            ("(?i)(a)(?-i)\\1", "aa", true),
            ("(?i)(a)(?-i:\\1)", "aA", false),
            ("(a)\\1", "aa", true),
            ("(a)\\1", "aA", false),
        ];
        check_matches(cases, true);
    }

    /// A `\Q...\E` is read out before the pattern is, as Java's `RemoveQEQuoting`
    /// does, so a quoted member takes part in a range. A `]` first in a class is a
    /// member that may start a range, a backslash before `<` or `>` is the
    /// character, `\w`, `\p{Lower}` and `\p{Upper}` are the ASCII letters under
    /// `(?iu)`, and a backreference compares ASCII case-insensitively under
    /// `(?i)`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_quotes_classes_and_references_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // \Q...\E is expanded before the pattern is read, so a quoted member takes part in a range
            ("(?i)[\\Qa\\E-c]", "B", true),
            ("(?i)[\\Qa\\E-c]", "b", true),
            ("(?i)[\\Qa\\E-c]", "A", true),
            ("(?i)[\\Qa\\E-c]", "X", false),
            ("(?i)[a-\\Qc\\E]", "B", true),
            ("(?i)[a-\\Qc\\E]", "X", false),
            ("(?i)[\\Qa\\E-\\Qc\\E]", "B", true),
            ("(?i)[\\Qa\\E-\\Qc\\E]", "X", false),
            ("(?i)[\\Qa\\Eb-d]", "C", true),
            ("(?i)[\\Qa\\Eb-d]", "A", true),
            ("(?i)[\\Qa\\Eb-d]", "X", false),
            ("(?i)[\\Q\\Ea-c]", "B", true),
            ("(?i)[a-\\Q\\Ec]", "B", true),
            ("(?i)[a-\\Q\\Ec]", "X", false),
            ("[\\Qa\\E-c]", "b", true),
            ("[\\Qa\\E-c]", "B", false),
            ("[\\Qa\\E-c]", "X", false),
            ("(?i)[\\Q]\\E-c]", "A", true),
            ("(?i)[\\Q]\\E-c]", "X", false),
            ("(?i)[\\Q1\\E-3]", "2", true),
            // a quoted dash is not a range
            ("(?i)[\\Qa-c\\E]", "B", false),
            ("(?i)[\\Qa-c\\E]", "-", true),
            ("(?i)[\\Qa-c\\E]", "C", true),
            ("(?i)[a\\Q-\\Ec]", "B", false),
            ("(?i)[a\\Q-\\Ec]", "-", true),
            ("(?i)[a\\Q-\\Ec]", "C", true),
            // a quote with no \E runs to the end of the pattern
            ("(?i)x\\Qa", "XA", true),
            ("(?i)x\\Qa\\E", "XA", true),
            ("(?i)a\\Q\\E+", "AA", true),
            ("\\Q1\\E2", "12", true),
            ("\\Qa\\\\E", "a\\", true),
            ("\\Q\\\\E", "\\", true),
            ("\\Qa.b\\E", "a.b", true),
            ("\\Qa.b\\E", "axb", false),
            ("(?i)\\Qk\\E", "\u{212a}", false),
            ("(?iu)\\Qk\\E", "\u{212a}", true),
            // a backslash before < or > is the character
            ("\\<", "<", true),
            ("\\>", ">", true),
            ("[\\<]", "<", true),
            ("[\\>]", ">", true),
            ("\\Q<\\E", "<", true),
            ("[\\Q<\\E]", "<", true),
            ("\\Q>\\E", ">", true),
            // a ] first in a class is a member, and starts a range
            ("(?i)[]-c]", "A", true),
            ("(?i)[]-c]", "B", true),
            ("(?i)[]-c]", "_", true),
            ("(?i)[]-c]", "]", true),
            ("(?i)[]-c]", "X", false),
            ("(?i)[]-c]", "d", false),
            ("(?i)[]-c]", "D", false),
            ("(?i)[^]-c]", "A", false),
            ("(?i)[^]-c]", "X", true),
            ("(?i)[^]-c]", "d", true),
            ("(?i)[]-cx]", "X", true),
            ("(?i)[]-]", "-", true),
            ("(?i)[]-]", "]", true),
            ("(?iu)[]-c]", "A", true),
            ("(?ix)[ ]-c ]", "A", true),
            ("[]-c]", "a", true),
            ("[]-c]", "A", false),
            ("[]-c]", "]", true),
            // \p{Lower} and \p{Upper} are the ASCII letters under (?iu), whatever fancy_regex's i would fold
            ("(?iu)\\p{Lower}", "\u{17f}", false),
            ("(?iu)\\p{Lower}", "\u{212a}", false),
            ("(?iu)\\p{Lower}", "A", true),
            ("(?iu)\\p{Lower}", "a", true),
            ("(?iu)\\p{Upper}", "\u{17f}", false),
            ("(?iu)\\p{Upper}", "\u{212a}", false),
            ("(?iu)\\p{Upper}", "a", true),
            ("(?iu)\\p{Upper}", "\u{e9}", false),
            ("(?iu)\\P{Lower}", "\u{17f}", true),
            ("(?iu)\\P{Lower}", "A", false),
            ("(?iu)\\P{Lower}", "1", true),
            ("(?iu)\\P{Upper}", "\u{212a}", true),
            ("(?u)\\p{Lower}", "\u{17f}", false),
            ("(?u)\\p{Lower}", "A", false),
            // \w and \W are ASCII under (?iu), whatever fancy_regex's i would fold
            ("(?iu)\\w", "\u{17f}", false),
            ("(?iu)\\w", "\u{212a}", false),
            ("(?iu)\\w", "a", true),
            ("(?iu)\\w", "_", true),
            ("(?iu)\\W", "\u{17f}", true),
            ("(?iu)\\W", "\u{212a}", true),
            ("(?iu)\\W", "a", false),
            ("(?iu)\\W", "-", true),
            ("(?iu)\\w+", "k\u{212a}", false),
            ("(?iu)k\\w", "k\u{212a}", false),
            ("(?iu)\\w\\p{Lower}", "k\u{212a}", false),
            ("(?i)\\w", "\u{17f}", false),
            ("(?u)\\w", "\u{17f}", false),
            ("(?iU)\\w", "\u{17f}", true),
            // a backreference compares ASCII case-insensitively under (?i)
            ("(?i)(a)\\1", "aA", true),
            ("(?i)(a)\\1", "aa", true),
            ("(?i)(a)\\1", "ab", false),
            ("(?i)(\\d+)-\\1", "12-12", true),
            ("(?i)(\\d+)-\\1", "12-13", false),
            ("(?i)(?<x>a)\\k<x>", "aA", true),
            ("(?i)(?<x>a)\\k<x>", "ab", false),
            ("(?i)(a+)\\1", "aaAA", true),
            ("(?i)(a)\\1+", "aAAa", true),
            ("(?i)(a)(\\1)", "aA", true),
            ("(?i)(k)\\1", "kK", true),
            ("(?i)(k)\\1", "k\u{212a}", false),
            ("(?i:(a)\\1)", "aA", true),
            ("a(?i:(b)\\1)", "aBb", true),
            ("a(?i:(b)\\1)", "aBB", true),
            ("a(?i:(b)\\1)", "AbB", false),
            ("(?i)(a)((b))\\3\\2\\1", "aBBBA", true),
            ("(?i)(a)((b))\\3\\2\\1", "aBbBa", true),
        ];
        check_matches(cases, true);
    }

    /// Under `(?iu)` a class member is the set that Java's predicates accept, and not what
    /// `fancy_regex`'s `i` folds a whole class to: `\w`, `\W`, `\p{Lower}` and `\p{Upper}` in a
    /// class are ASCII, a range that has `K` and not `k` leaves out the Kelvin sign, and `İ`
    /// and `ı` are members of a range that has `i`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_folds_unicode_case_in_a_class_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // \w, \W, \p{Lower}, \p{Upper} in a class are ASCII under (?iu), so the letters that fold to s and k are not members
            ("(?iu)[\\w]", "\u{17f}", false),
            ("(?iu)[\\w]", "\u{212a}", false),
            ("(?iu)[\\w]", "a", true),
            ("(?iu)[\\w]", "K", true),
            ("(?iu)[\\w]", "_", true),
            ("(?iu)[\\w]", "\u{e9}", false),
            ("(?iu)[\\w-]", "\u{17f}", false),
            ("(?iu)[\\w-]", "\u{212a}", false),
            ("(?iu)[\\w-]", "-", true),
            ("(?iu)[\\w-]", "z", true),
            ("(?iu)[\\W]", "\u{17f}", true),
            ("(?iu)[\\W]", "\u{212a}", true),
            ("(?iu)[\\W]", "a", false),
            ("(?iu)[\\W]", "-", true),
            ("(?iu)[^\\w]", "\u{17f}", true),
            ("(?iu)[^\\w]", "k", false),
            ("(?iu)[\\p{Lower}]", "\u{17f}", false),
            ("(?iu)[\\p{Lower}]", "\u{212a}", false),
            ("(?iu)[\\p{Lower}]", "A", true),
            ("(?iu)[\\p{Lower}]", "a", true),
            ("(?iu)[\\p{Lower}]", "\u{e9}", false),
            ("(?iu)[\\p{Upper}]", "\u{17f}", false),
            ("(?iu)[\\p{Upper}]", "\u{212a}", false),
            ("(?iu)[\\p{Upper}]", "a", true),
            ("(?iu)[\\p{Upper}]", "\u{c9}", false),
            ("(?iu)[\\P{Lower}]", "\u{17f}", true),
            ("(?iu)[\\P{Lower}]", "A", false),
            ("(?iu)[\\P{Lower}]", "1", true),
            ("(?iu)[\\P{Upper}]", "\u{212a}", true),
            ("(?iu)[\\p{Lower}k]", "\u{17f}", false),
            ("(?iu)[\\p{Lower}k]", "\u{212a}", true),
            ("(?iu)[\\p{Lower}s]", "\u{17f}", true),
            ("(?iu)[\\w&&[^a]]", "A", false),
            ("(?iu)[\\w&&[^a]]", "b", true),
            ("(?iu)[\\w&&[^k]]", "\u{212a}", false),
            ("(?iu)[\\w&&[^k]]", "K", false),
            ("(?iu)[^\\p{Lower}]", "\u{17f}", true),
            ("(?iu)[^\\p{Lower}]", "a", false),
            ("(?iu)[^\\p{Lower}]", "1", true),
            // a range that holds K and not k, or s and not S, compares the upper case and the lower case of the upper case of the char
            ("(?iu)[a-z]", "\u{212a}", true),
            ("(?iu)[a-z]", "\u{17f}", true),
            ("(?iu)[a-z]", "\u{130}", true),
            ("(?iu)[a-z]", "\u{131}", true),
            ("(?iu)[a-z]", "K", true),
            ("(?iu)[a-z]", "\u{df}", false),
            ("(?iu)[A-Z]", "\u{212a}", false),
            ("(?iu)[A-Z]", "\u{17f}", true),
            ("(?iu)[A-Z]", "\u{130}", false),
            ("(?iu)[A-Z]", "\u{131}", true),
            ("(?iu)[A-Z]", "k", true),
            ("(?iu)[A-K]", "\u{212a}", false),
            ("(?iu)[A-K]", "k", true),
            ("(?iu)[A-K]", "\u{17f}", false),
            ("(?iu)[A-c]", "\u{212a}", false),
            ("(?iu)[A-c]", "k", true),
            ("(?iu)[A-c]", "C", true),
            ("(?iu)[A-c]", "d", true),
            ("(?iu)[A-k]", "\u{212a}", true),
            ("(?iu)[k-k]", "\u{212a}", true),
            ("(?iu)[K-K]", "\u{212a}", false),
            ("(?iu)[K-K]", "k", true),
            ("(?iu)[s-s]", "\u{17f}", true),
            ("(?iu)[S-S]", "\u{17f}", true),
            ("(?iu)[i-i]", "\u{130}", true),
            ("(?iu)[I-I]", "\u{131}", true),
            ("(?iu)[^A-Z]", "\u{212a}", true),
            ("(?iu)[^A-Z]", "\u{17f}", false),
            ("(?iu)[^a-z]", "\u{212a}", false),
            ("(?iu)[^A-K]", "\u{212a}", true),
            ("(?iu)[^A-K]", "k", false),
            ("(?iu)[\\x{E0}-\\x{FE}]", "\u{c9}", true),
            ("(?iu)[\\x{E0}-\\x{FE}]", "\u{178}", false),
            ("(?iu)[\\x{C0}-\\x{DE}]", "\u{ff}", false),
            ("(?iu)[\\x{C0}-\\x{DE}]", "\u{178}", false),
            ("(?iu)[\\x{391}-\\x{3A9}]", "\u{3c3}", true),
            ("(?iu)[\\x{391}-\\x{3A9}]", "\u{3c2}", true),
            ("(?iu)[\\x{3B1}-\\x{3C9}]", "\u{3a3}", true),
            ("(?iu)[\\x{410}-\\x{42F}]", "\u{44f}", true),
            ("(?iu)[\\x{410}-\\x{42F}]", "\u{436}", true),
            ("(?iu)[\\x{DF}-\\x{DF}]", "\u{1e9e}", true),
            ("(?iu)[\\x{1E9E}-\\x{1E9E}]", "\u{df}", false),
            // a single member: Latin-1 members go in a bit class, the other members compare keys
            ("(?iu)[k]", "\u{212a}", true),
            ("(?iu)[K]", "\u{212a}", true),
            ("(?iu)[s]", "\u{17f}", true),
            ("(?iu)[S]", "\u{17f}", true),
            ("(?iu)[i]", "\u{130}", true),
            ("(?iu)[i]", "\u{131}", true),
            ("(?iu)[I]", "\u{130}", true),
            ("(?iu)[I]", "\u{131}", true),
            ("(?iu)[\\x{E5}]", "\u{212b}", true),
            ("(?iu)[\\x{C5}]", "\u{212b}", true),
            ("(?iu)[\\x{FF}]", "\u{178}", true),
            ("(?iu)[\\x{B5}]", "\u{3bc}", true),
            ("(?iu)[\\x{B5}]", "\u{39c}", true),
            ("(?iu)[\\x{3BC}]", "\u{b5}", true),
            ("(?iu)[\\x{E9}]", "\u{c9}", true),
            ("(?iu)[\\x{C9}]", "\u{e9}", true),
            ("(?iu)[\\x{DF}]", "\u{1e9e}", false),
            ("(?iu)[\\x{1E9E}]", "\u{df}", true),
            ("(?iu)[\\x{3C3}]", "\u{3c2}", true),
            ("(?iu)[\\x{3C2}]", "\u{3a3}", true),
            ("(?iu)[\\x{3A3}]", "\u{3c2}", true),
            ("(?iu)[\\x{1C6}]", "\u{1c5}", true),
            ("(?iu)[\\x{1C5}]", "\u{1c4}", true),
            ("(?iu)[\\x{1F88}]", "\u{1f80}", true),
            ("(?iu)[\\x{1F80}]", "\u{1f88}", true),
            ("(?iu)[\\x{10428}]", "\u{10400}", true),
            ("(?iu)[\\x{10400}-\\x{1044F}]", "\u{10428}", true),
            ("(?iu)[z]", "Z", true),
            ("(?iu)[1]", "1", true),
            // with && and a nested class
            ("(?iu)[a-z&&[^k]]", "\u{212a}", false),
            ("(?iu)[a-z&&[^k]]", "K", false),
            ("(?iu)[a-z&&[^k]]", "j", true),
            ("(?iu)[a-z&&k]", "\u{212a}", true),
            ("(?iu)[a-z&&k]", "K", true),
            ("(?iu)[a-c[x-z]]", "X", true),
            ("(?iu)[a-c[^x-z]]", "Y", false),
            ("(?iu)[a-c[^x-z]]", "B", true),
            ("(?iu)[^a-z&&[^k]]", "k", true),
            ("(?iu)[^a-z&&[^k]]", "\u{212a}", true),
            ("(?iu)[a&&-1]", "-", false),
            ("(?iu)[a&&-1]", "a", false),
            ("(?iu)[a-c&&b-d]", "B", true),
            ("(?iu)[a-c&&b-d]", "A", false),
            // under (?iU) the classes are Unicode and the members fold the same
            ("(?iU)[\\w]", "\u{17f}", true),
            ("(?iU)[\\w]", "\u{e9}", true),
            ("(?iU)[a-z]", "\u{212a}", true),
            ("(?iU)[A-Z]", "\u{212a}", false),
            ("(?iU)[\\p{Lower}]", "A", true),
            ("(?iU)[\\p{Upper}]", "a", true),
            ("(?iU)[\\p{Lower}]", "\u{1c5}", true),
            ("(?iU)[\\p{Lu}]", "a", true),
            ("(?iU)[\\p{Lu}]", "1", false),
            // only the ASCII fold, which does not reach these
            ("(?i)[a-z]", "\u{212a}", false),
            ("(?i)[a-z]", "\u{17f}", false),
            ("(?i)[A-K]", "k", true),
            ("(?i)[k]", "\u{212a}", false),
            ("(?i)[\\w]", "\u{17f}", false),
            ("(?u)[a-z]", "A", false),
            ("(?u)[k]", "\u{212a}", false),
            ("[a-z]", "K", false),
            // the fold ends where the flag does
            ("(?iu)(?-i)[a-z]", "A", false),
            ("(?iu)(?-i)[a-z]", "\u{212a}", false),
            ("(?iu)(?-u)[a-z]", "A", true),
            ("(?iu)(?-u)[a-z]", "\u{212a}", false),
            ("(?iu)(?-u)[k]", "\u{212a}", false),
            ("(?iu:[a-z])A", "AA", true),
            ("(?iu:[a-z])A", "Aa", false),
            ("(?iu:[a-z])[A-Z]", "aa", false),
            ("(?iu:[a-z])[A-Z]", "\u{212a}a", false),
        ];
        check_matches(cases, true);
    }

    /// The surrogates are not chars, so a range that has them in the middle, or at an end, holds
    /// the chars on either side, and a negated one leaves out the chars on either side. Each row
    /// is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_a_class_across_the_surrogates_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{e000}", false),
            ("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{d7ff}", false),
            ("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{e001}", false),
            ("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{10976}", true),
            ("(?iu)[^\\x{3c2}-\\x{10975}]", "a", true),
            ("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{d7ff}", true),
            ("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{e000}", true),
            ("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{e001}", false),
            ("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{d7ff}", false),
            ("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{e000}", false),
            ("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{d7fe}", true),
            ("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{e001}", true),
            ("(?iu)[^\\x{D800}-\\x{E001}]", "\u{e000}", false),
            ("(?iu)[^\\x{D800}-\\x{E001}]", "\u{e002}", true),
            ("(?iu)[^\\x{41}-\\x{DBFF}]", "\u{d7ff}", false),
            ("(?iu)[^\\x{41}-\\x{DBFF}]", "\u{e000}", true),
            ("(?iu)[\\x{D800}-\\x{DFFF}]", "a", false),
            ("(?iu)[^\\x{D800}-\\x{DFFF}]", "a", true),
            ("(?iu)[^\\x{D800}-\\x{DFFF}]", "\u{e000}", true),
            ("(?iu)[a\\x{D800}-\\x{DFFF}]", "a", true),
            ("(?iu)[a\\x{D800}-\\x{DFFF}]", "\u{d7ff}", false),
            ("(?iu)[^a\\x{D800}-\\x{DFFF}]", "a", false),
            ("(?iu)[^a\\x{D800}-\\x{DFFF}]", "b", true),
        ];
        check_matches(cases, true);
    }

    /// The POSIX classes are ASCII unless `(?U)` is on, when they are the Unicode properties, and
    /// no case flag folds them or the other properties, except that `Lower`, `Upper`, `Lu`, `Ll`
    /// and `Lt` take in every cased letter under `(?i)`. `fancy_regex` reads the names as Unicode
    /// and folds a property under its `i`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_the_posix_classes_and_properties_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // the POSIX classes are ASCII without (?U), where fancy_regex reads them as Unicode
            ("\\p{Alpha}", "a", true),
            ("\\p{Alpha}", "Z", true),
            ("\\p{Alpha}", "\u{e9}", false),
            ("\\p{Alpha}", "\u{3b1}", false),
            ("\\p{Alpha}", "1", false),
            ("\\p{Alpha}", "_", false),
            ("\\P{Alpha}", "\u{e9}", true),
            ("\\P{Alpha}", "a", false),
            ("\\p{Alnum}", "5", true),
            ("\\p{Alnum}", "q", true),
            ("\\p{Alnum}", "_", false),
            ("\\p{Alnum}", "\u{e9}", false),
            ("\\p{Alnum}", "\u{663}", false),
            ("\\p{Digit}", "7", true),
            ("\\p{Digit}", "\u{663}", false),
            ("\\p{Digit}", "\u{ff17}", false),
            ("\\p{Punct}", "!", true),
            ("\\p{Punct}", "$", true),
            ("\\p{Punct}", "_", true),
            ("\\p{Punct}", "~", true),
            ("\\p{Punct}", "`", true),
            ("\\p{Punct}", "@", true),
            ("\\p{Punct}", "a", false),
            ("\\p{Punct}", "\u{a1}", false),
            ("\\p{Punct}", "\u{2014}", false),
            ("\\P{Punct}", "\u{a1}", true),
            ("\\P{Punct}", "!", false),
            ("\\p{ASCII}", "a", true),
            ("\\p{ASCII}", "\u{7f}", true),
            ("\\p{ASCII}", "\u{0}", true),
            ("\\p{ASCII}", "\u{80}", false),
            ("\\p{ASCII}", "\u{e9}", false),
            ("\\P{ASCII}", "\u{e9}", true),
            ("\\p{XDigit}", "f", true),
            ("\\p{XDigit}", "F", true),
            ("\\p{XDigit}", "9", true),
            ("\\p{XDigit}", "g", false),
            ("\\p{XDigit}", "\u{ff11}", false),
            ("\\p{Blank}", " ", true),
            ("\\p{Blank}", "\t", true),
            ("\\p{Blank}", "\n", false),
            ("\\p{Blank}", "\u{a0}", false),
            ("\\p{Cntrl}", "\u{1f}", true),
            ("\\p{Cntrl}", "\u{7f}", true),
            ("\\p{Cntrl}", "\u{0}", true),
            ("\\p{Cntrl}", " ", false),
            ("\\p{Cntrl}", "\u{80}", false),
            ("\\p{Graph}", "!", true),
            ("\\p{Graph}", "~", true),
            ("\\p{Graph}", " ", false),
            ("\\p{Graph}", "\u{e9}", false),
            ("\\p{Print}", " ", true),
            ("\\p{Print}", "~", true),
            ("\\p{Print}", "\u{7f}", false),
            ("\\p{Print}", "\u{e9}", false),
            ("\\p{Space}", " ", true),
            ("\\p{Space}", "\u{b}", true),
            ("\\p{Space}", "\u{a0}", false),
            ("\\p{Space}", "\u{2003}", false),
            ("[\\p{Alpha}]", "\u{e9}", false),
            ("[\\p{Alpha}]", "b", true),
            ("[\\p{Alpha}\\d]", "7", true),
            ("[\\p{Alnum}_]", "_", true),
            ("[\\p{Alnum}_]", "\u{e9}", false),
            ("[\\p{Punct}]", "\u{a1}", false),
            ("[\\p{Punct}]", ";", true),
            ("[^\\p{Punct}]", ";", false),
            ("[^\\p{Punct}]", "\u{a1}", true),
            ("[\\P{Punct}]", ";", false),
            ("[\\p{ASCII}]", "\u{e9}", false),
            ("[\\p{ASCII}-]", "-", true),
            ("[\\p{XDigit}]", "\u{ff11}", false),
            ("[\\p{Blank}]", "\u{a0}", false),
            ("[\\p{Space}x]", "\u{a0}", false),
            ("[\\p{Cntrl}]", "\u{80}", false),
            ("[\\p{Graph}]", "\u{e9}", false),
            ("[\\p{Print}]", "\u{e9}", false),
            ("[\\p{Alpha}&&[^a]]", "a", false),
            ("[\\p{Alpha}&&[^a]]", "b", true),
            // the case flags do not fold them, and under (?i) Lower and Upper are the ASCII letters of both cases
            ("(?i)\\p{Alpha}", "\u{17f}", false),
            ("(?i)\\p{Alpha}", "\u{212a}", false),
            ("(?iu)\\p{Alpha}", "\u{17f}", false),
            ("(?iu)\\p{Alpha}", "\u{212a}", false),
            ("(?iu)\\p{ASCII}", "\u{17f}", false),
            ("(?iu)\\p{ASCII}", "\u{212a}", false),
            ("(?iu)\\p{ASCII}", "K", true),
            ("(?iu)\\p{Punct}", "\u{212a}", false),
            ("(?iu)\\p{Digit}", "1", true),
            ("(?iu)[\\p{Alnum}]", "\u{17f}", false),
            ("(?iu)[\\p{Alnum}]", "\u{212a}", false),
            ("(?iu)[\\p{ASCII}]", "\u{212a}", false),
            ("(?iu)[\\p{XDigit}]", "\u{212a}", false),
            ("(?iu)[\\p{Punct}k]", "\u{212a}", true),
            ("(?i)\\p{Lower}", "A", true),
            ("(?i)\\p{Upper}", "a", true),
            ("(?i)\\p{Lower}", "\u{e9}", false),
            ("(?iu)\\p{Lower}", "\u{17f}", false),
            ("(?iu)\\p{Upper}", "\u{212a}", false),
            // the properties that are not the ASCII ones are not folded either
            ("(?iu)\\p{IsGreek}", "\u{345}", false),
            ("(?iu)\\p{IsLatin}", "\u{212a}", true),
            ("(?iu)\\p{IsLatin}", "\u{17f}", true),
            ("(?iu)\\p{L}", "\u{17f}", true),
            ("(?iu)\\p{Nd}", "\u{663}", true),
            ("(?iu)[\\p{IsGreek}]", "\u{345}", false),
            ("(?iu)\\p{Lu}", "a", true),
            ("(?iu)\\p{Lu}", "\u{df}", true),
            ("(?iu)\\p{Lu}", "\u{138}", true),
            ("(?iu)\\p{Lu}", "1", false),
            ("(?iu)\\p{Ll}", "A", true),
            ("(?iu)\\p{Lt}", "a", true),
            ("(?iu)\\P{Lu}", "a", false),
            ("(?iu)\\P{Lu}", "1", true),
            ("(?iu)[\\p{Lu}]", "\u{df}", true),
            ("(?iu)[\\p{Lu}]", "\u{138}", true),
            ("(?iu)[^\\p{Lu}]", "\u{df}", false),
            ("(?iu)[^\\p{Lu}]", "1", true),
            ("(?iu)\\p{IsLowercase}", "A", true),
            ("(?iu)\\p{IsUppercase}", "a", true),
            ("(?iu)\\p{IsLowercase}", "\u{2102}", true),
            ("(?iu)\\p{gc=Lu}", "\u{138}", true),
            ("(?iu)\\p{IsLu}", "\u{138}", true),
            ("(?iu)\\p{Lower}", "A", true),
            // under (?U) the POSIX classes are the Unicode properties
            ("(?U)\\p{Alpha}", "\u{e9}", true),
            ("(?U)\\p{Alpha}", "1", false),
            ("(?U)\\p{Alnum}", "\u{e9}", true),
            ("(?U)\\p{Alnum}", "\u{663}", true),
            ("(?U)\\p{Alnum}", "_", false),
            ("(?U)\\p{Digit}", "\u{663}", true),
            ("(?U)\\p{Digit}", "a", false),
            ("(?U)\\p{Punct}", "\u{a1}", true),
            ("(?U)\\p{Punct}", "$", false),
            ("(?U)\\p{Punct}", "!", true),
            ("(?U)\\p{Space}", "\u{a0}", true),
            ("(?U)\\p{Space}", "\u{85}", true),
            ("(?U)\\p{Space}", "\u{2028}", true),
            ("(?U)\\p{Space}", "a", false),
            ("(?U)\\p{XDigit}", "\u{ff11}", true),
            ("(?U)\\p{XDigit}", "\u{663}", true),
            ("(?U)\\p{XDigit}", "g", false),
            ("(?U)\\p{Blank}", "\u{a0}", true),
            ("(?U)\\p{Blank}", "\t", true),
            ("(?U)\\p{Blank}", "\n", false),
            ("(?U)\\p{Cntrl}", "\u{80}", true),
            ("(?U)\\p{Cntrl}", " ", false),
            ("(?U)\\p{Graph}", "\u{e9}", true),
            ("(?U)\\p{Graph}", " ", false),
            ("(?U)\\p{Graph}", "\u{a0}", false),
            ("(?U)\\p{Graph}", "\u{2028}", false),
            ("(?U)\\p{Print}", "\u{e9}", true),
            ("(?U)\\p{Print}", " ", true),
            ("(?U)\\p{Print}", "\u{a0}", true),
            ("(?U)\\p{Print}", "\n", false),
            ("(?U)\\p{Print}", "\u{2028}", false),
            ("(?U)\\p{Lower}", "\u{e9}", true),
            ("(?U)\\p{Lower}", "A", false),
            ("(?U)\\p{Upper}", "\u{c9}", true),
            ("(?U)\\p{alpha}", "\u{e9}", true),
            ("(?U)\\P{Graph}", " ", true),
            ("(?U)\\P{Graph}", "a", false),
            ("(?U)[\\p{Alpha}]", "\u{e9}", true),
            ("(?U)[\\p{Graph}]", "\u{e9}", true),
            ("(?U)[\\P{Graph}x]", " ", true),
            ("(?U)[\\P{Graph}x]", "x", true),
            ("(?U)[\\P{Graph}x]", "a", false),
            ("(?iU)\\p{Lower}", "A", true),
            ("(?iU)\\p{Lower}", "\u{1c5}", true),
            ("(?iU)\\p{Upper}", "a", true),
            ("(?iU)\\p{Lower}", "\u{2102}", true),
            ("(?iU)\\p{Alpha}", "\u{212a}", true),
            ("(?iU)\\p{Punct}", "\u{a1}", true),
            ("(?iU)\\p{Graph}", "\u{212a}", true),
        ];
        check_matches(cases, true);
    }

    /// Under `(?iu)` a literal matches the code points with its key, `Character.toLowerCase` of
    /// `Character.toUpperCase`, which is not the simple case folding that `fancy_regex`'s `i`
    /// compares: `İ` and `ı` match `i`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_folds_unicode_case_of_a_literal_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // a literal compares Character.toLowerCase(Character.toUpperCase(ch)), not the simple case folding of fancy_regex
            ("(?iu)i", "\u{130}", true),
            ("(?iu)i", "\u{131}", true),
            ("(?iu)I", "\u{130}", true),
            ("(?iu)I", "\u{131}", true),
            ("(?iu)\u{130}", "i", true),
            ("(?iu)\u{130}", "\u{131}", true),
            ("(?iu)\u{131}", "I", true),
            ("(?iu)\u{131}", "\u{130}", true),
            ("(?iu)i", "I", true),
            ("(?iu)id", "\u{130}D", true),
            ("(?iu)k", "\u{212a}", true),
            ("(?iu)K", "\u{212a}", true),
            ("(?iu)\u{212a}", "k", true),
            ("(?iu)s", "\u{17f}", true),
            ("(?iu)\u{17f}", "S", true),
            ("(?iu)\u{e5}", "\u{212b}", true),
            ("(?iu)\u{212b}", "\u{c5}", true),
            ("(?iu)\u{3c3}", "\u{3c2}", true),
            ("(?iu)\u{3c2}", "\u{3a3}", true),
            ("(?iu)\u{3a3}", "\u{3c3}", true),
            ("(?iu)\u{b5}", "\u{39c}", true),
            ("(?iu)\u{b5}", "\u{3bc}", true),
            ("(?iu)\u{1c5}", "\u{1c6}", true),
            ("(?iu)\u{1f80}", "\u{1f88}", true),
            ("(?iu)\u{1f88}", "\u{1f80}", true),
            ("(?iu)\u{1f80}", "\u{1f80}", true),
            ("(?iu)\u{ff}", "\u{178}", true),
            ("(?iu)\u{10428}", "\u{10400}", true),
            ("(?iu)\u{e9}", "\u{c9}", true),
            ("(?iu)\u{436}", "\u{416}", true),
            ("(?iu)1", "1", true),
            ("(?iu)ss", "\u{df}", false),
            ("(?iu)SS", "\u{1e9e}", false),
            ("(?iu)\u{df}", "\u{df}", true),
            // with the fold off, in ASCII, or ended, the letters stay apart
            ("(?i)i", "\u{130}", false),
            ("(?i)i", "\u{131}", false),
            ("(?i)k", "\u{212a}", false),
            ("(?u)i", "I", false),
            ("(?u)i", "\u{130}", false),
            ("(?iu)(?-i)i", "I", false),
            ("(?iu)(?-i)i", "\u{130}", false),
            ("(?iu)(?-u)i", "\u{130}", false),
            ("(?iu)(?-u)i", "I", true),
            ("(?iu:i)", "\u{130}", true),
            ("(?iu:i)i", "\u{130}i", true),
            ("(?iu:i)i", "\u{130}I", false),
        ];
        check_matches(cases, true);
    }

    /// Java reads a literal in a run of two or more as a slice, which also matches `ẞ` for `ß`,
    /// and a lone literal, or one that a quantifier follows, on its own. Each row is
    /// `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_a_run_of_literals_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // a lone ß matches only itself, and one in a run of two or more also matches ẞ
            ("(?iu)\u{df}", "\u{1e9e}", false),
            ("(?iu)\u{df}", "\u{df}", true),
            ("(?iu)\u{df}\u{df}", "\u{1e9e}\u{1e9e}", true),
            ("(?iu)\u{df}a", "\u{1e9e}a", true),
            ("(?iu)a\u{df}", "a\u{1e9e}", true),
            ("(?iu)\u{df}*", "\u{1e9e}", false),
            ("(?iu)\u{df}a*", "\u{1e9e}", false),
            ("(?iu)\u{df}a*", "\u{1e9e}a", false),
            ("(?iu)\u{df}ab*", "\u{1e9e}ab", true),
            ("(?iu)\u{df}ab*", "\u{1e9e}a", true),
            ("(?iu)a\u{df}*", "a\u{1e9e}", false),
            ("(?iu)a\u{df}*", "a", true),
            ("(?iu)a{2}\u{df}", "aa\u{1e9e}", false),
            ("(?iu)a{2}\u{df}\u{df}", "aa\u{1e9e}\u{1e9e}", true),
            ("(?iu)(\u{df})", "\u{1e9e}", false),
            ("(?iu)(\u{df}\u{df})", "\u{1e9e}\u{1e9e}", true),
            ("(?iu)\u{df}|\u{df}", "\u{1e9e}", false),
            ("(?iu)\u{df}.", "\u{1e9e}x", false),
            ("(?iu)\u{df}\\.", "\u{1e9e}.", true),
            ("(?iu)\u{df}\\w", "\u{1e9e}x", false),
            ("(?iu)\u{df}{1,2}", "\u{1e9e}", false),
            ("(?iu)\u{df}\u{df}?", "\u{1e9e}", false),
            ("(?iu)\u{df}\u{df}?", "\u{1e9e}\u{1e9e}", false),
            ("(?iu)x\u{df}", "x\u{1e9e}", true),
            ("(?iu)\u{df}+x", "\u{1e9e}x", false),
            ("(?iu)\u{df}x", "\u{1e9e}x", true),
            ("(?iu)(?:\u{df})\u{df}", "\u{1e9e}\u{1e9e}", false),
            ("(?iu)(?:\u{df})", "\u{1e9e}", false),
            ("(?iu)\u{df}$", "\u{1e9e}", false),
            ("(?iu)^\u{df}", "\u{1e9e}", false),
            ("(?iu)\u{df}[a]", "\u{1e9e}a", false),
            ("(?iu)\u{df}\\x{DF}", "\u{1e9e}\u{1e9e}", true),
            ("(?iu)\\x{DF}", "\u{1e9e}", false),
            ("(?iu)\\Q\u{df}\\E", "\u{1e9e}", false),
            ("(?iu)\\Q\u{df}\u{df}\\E", "\u{1e9e}\u{1e9e}", true),
            ("(?iux)\u{df} \u{df}", "\u{1e9e}\u{1e9e}", true),
            ("(?iux)\u{df} #c\n\u{df}", "\u{1e9e}\u{1e9e}", true),
            ("(?iux)\u{df} #c\n", "\u{1e9e}", false),
            ("(?iu)\u{1e9e}", "\u{df}", true),
            ("(?iu)\u{1e9e}\u{1e9e}", "\u{df}\u{df}", true),
        ];
        check_matches(cases, true);
    }

    /// The flag `x` ends with the group it is set in, and the whitespace and the `#` after that
    /// are text. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_ends_flag_x_with_its_group_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // the flag x ends with the group it is set in
            ("(?u)(?<n>.(?x))A(?i)I ", "&A\u{131} ", true),
            ("(?u)(?<n>.(?x))A(?i)I ", "&A\u{131}", false),
            ("(?u)(?<n>.(?x))A(?i)I", "&A\u{131}", true),
            ("(?u)(.(?x))A#", "xA#", true),
            ("(?u)(.(?x))A#", "xA", false),
            ("(?x)(.(?x))A#c\nB", "xAB", true),
            ("(?x:a )b", "ab", true),
            ("(?x:a )b", "a b", false),
            ("(?x:a)#b", "a#b", true),
            ("(?x:a)#b", "a", false),
        ];
        check_matches(cases, true);
    }

    /// A `^` that Java reads as a class member stays one under `(?x)`, where
    /// the white space and comments between it and the `[` are dropped, and a
    /// `-` that does not join two members is the character, whatever member
    /// comes before it, `\w` and `\s` included. Each row is `Pattern.matches`
    /// on JDK 17.
    #[test]
    fn matches_reads_a_literal_caret_and_dash_in_a_class_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // `^` is a negation only right after the `[`
            ("(?x)[ ^a]", "^", true),
            ("(?x)[ ^a]", "a", true),
            ("(?x)[ ^a]", "b", false),
            ("(?ix)[ ^a]", "^", true),
            ("(?ix)[ ^a]", "A", true),
            ("(?ix)[ ^a]", "b", false),
            ("(?ix)[#c\n^a]", "^", true),
            ("(?ix)[#c\n^a]", "A", true),
            ("(?ix)[#c\n^a]", "b", false),
            ("(?x)[ ^-b]", "^", true),
            ("(?x)[ ^-b]", "a", true),
            ("(?x)[ ^-b]", "c", false),
            ("(?x)[a[ ^b]]", "^", true),
            ("(?x)[a[ ^b]]", "b", true),
            ("(?x)[a[ ^b]]", "c", false),
            ("(?x)[^ a]", "a", false),
            ("(?x)[^ a]", "b", true),
            ("(?x)[^ ^a]", "^", false),
            ("(?x)[^ ^a]", "b", true),
            ("[\\[^a]", "^", true),
            ("[\\[^a]", "b", false),
            // outside a class it is an anchor, after a `\[` too
            ("\\[^a", "[a", false),
            ("\\[^a", "[^a", false),
            ("(?x)\\[ ^a", "[^a", false),
            // a `-` that is not a range operator, after `\w`, `\s`, a folded
            // letter, a property, and a range
            ("[\\w-.]", "-", true),
            ("[\\w-.]", ".", true),
            ("[\\w-.]", "a", true),
            ("[\\w-.]", ",", false),
            ("[\\w-a]", "-", true),
            ("[\\w-a]", "a", true),
            ("[\\w-a]", "`", false),
            ("[\\s-.]", "-", true),
            ("[\\s-.]", ".", true),
            ("[\\s-.]", " ", true),
            ("[\\s-.]", ",", false),
            ("(?i)[a-[b]]", "-", true),
            ("(?i)[a-[b]]", "A", true),
            ("(?i)[a-[b]]", "B", true),
            ("(?i)[a-[b]]", "X", false),
            ("(?i)[a-[b]]", "[", false),
            ("[\\p{Lower}-z]", "-", true),
            ("[\\p{Lower}-z]", ".", false),
            ("[\\p{L}-z]", "-", true),
            ("[\\p{L}-z]", "\u{e9}", true),
            ("[\\p{L}-z]", ".", false),
            ("[\\w-\\d]", "-", true),
            ("[\\d-a]", "-", true),
            ("[\\d-a]", "a", true),
            ("[\\d-a]", "b", false),
            ("[a-b-c]", "-", true),
            ("[a-b-c]", "c", true),
            ("[a-b-c]", "d", false),
            ("(?i)[a-b-c]", "-", true),
            ("(?i)[a-b-c]", "C", true),
            ("(?i)[a-b-c]", "D", false),
            ("[a-c-e]", "-", true),
            ("[a-c-e]", "b", true),
            ("[a-c-e]", "d", false),
            ("[a-c-e]", "e", true),
            // first, last and negated
            ("[-a]", "-", true),
            ("[^-a]", "-", false),
            ("[^-a]", "b", true),
            ("[a-]", "-", true),
            // a `-` as either end of a range
            ("[+--]", ",", true),
            ("[+--]", "-", true),
            ("[+--]", ".", false),
            ("(?i)[+--]", "-", true),
            ("[--/]", ".", true),
            ("[--/]", "0", false),
            ("[!--]", ",", true),
            ("[!--]", ".", false),
            // outside a class it is a plain character
            ("a-b", "a-b", true),
            ("(?i)a-b", "A-B", true),
        ];
        check_matches(cases, true);
    }

    /// Under `(?x)` Java tells a `-` that ends a class from one that starts a
    /// range by the char right after it, without skipping white space or a
    /// comment: `(?x)[+- ]]` is the range from `+` to the first `]`, then the
    /// class ends at the second. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_a_range_that_ends_past_x_flag_white_space_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?x)[+- ]]", ",", true),
            ("(?x)[+- ]]", "a", false),
            ("(?x)[+- ]]", "]", true),
            ("(?x)[+- ]]", "-", true),
            ("(?x)[+- [b]]", "X]", true),
            ("(?x)[+- [b]]", "b]", true),
            ("(?x)[+- [b]]", "c]", false),
            ("(?x)[+- [b]]", "-]", true),
            ("(?x)[+-#c\n]]", "a", false),
            ("(?x)[+-#c\n]]", ",", true),
            ("(?x)[+-#c\n]]", "]", true),
            ("(?x)[a-b- ]", "-", true),
            ("(?x)[a-b- ]", "b", true),
            ("(?x)[a-b- ]", "c", false),
            ("(?x)[a - c]", "b", true),
            ("(?x)[a - c]", "-", false),
            ("(?x)[a - c]", " ", false),
            ("(?x)[a-\n b]", "a", true),
            ("(?x)[a-\n b]", "b", true),
            ("(?x)[a-\n b]", "-", false),
            ("(?x)[a-#c\nb]", "b", true),
            ("(?x)[a-#c\nb]", "-", false),
            ("(?x)[a-]", "-", true),
            ("(?x)[a-[b]]", "-", true),
            ("(?x)[a-[b]]", "b", true),
            ("(?x)[a-[b]]", "c", false),
        ];
        check_matches(cases, true);
        // The range ends at the `]` or the `[` past the white space, which
        // Java refuses as a reversed range or an unclosed class.
        for source in [
            "(?x)[.- ]",
            "(?x)[a- ]",
            "(?x)[a-\n]",
            "(?x)[.-\n]",
            "(?x)[_- [b]]",
            "(?x)[a-#c\n]]",
        ] {
            check!(JavaPattern::compile(source).is_err(), "{source:?}");
        }
    }

    /// A `)` with no `(` before it is `Unmatched closing ')'` in Java, which
    /// the `\A(?:...)\z` wrapper would otherwise read as the end of a group,
    /// and a range that ends in `\w`, `\s` or `\p{..}` is `Illegal character
    /// range` there. Each row is refused by `Pattern.compile` on JDK 17.
    #[test]
    fn a_stray_closing_paren_or_a_range_to_a_shorthand_does_not_compile() {
        for source in [
            "a)|(b",
            "a)",
            "(a))",
            "a(?i))",
            ")",
            "(?x)a )",
            "(?:a))",
            "(?=a))",
            "(?i:a))",
            "[a--b]",
            "[a-\\w]",
            "[a-\\d]",
            "[!-\\d]",
            "[+-\\w]",
            "[a-\\p{L}]",
            "[a-\\s]",
            "[!-\\W]",
            "[--\\w]",
            "(?i)[a-\\w]",
            "(?x)[a- \\w]",
        ] {
            check!(JavaPattern::compile(source).is_err(), "{source:?}");
        }
    }

    /// A `)` that Java reads as a member, or as part of a comment, is not
    /// stray. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn a_closing_paren_that_closes_no_group_is_still_a_character_where_java_reads_one() {
        // (pattern, input, whole input matches)
        let cases = [
            ("[)]", ")", true),
            ("\\)", ")", true),
            ("\\Q)\\E", ")", true),
            ("(?x)a #c)\n", "a", true),
            ("(a)|(b)", "b", true),
            ("(?i)(a)|b", "B", true),
        ];
        check_matches(cases, true);
    }

    /// A backreference under an ASCII fold is written `(?i:\1)`, and
    /// `fancy_regex`'s `i` compares the text of the group Unicode-insensitively
    /// when either text is not ASCII. So `(?i)(é)\1` matches `éÉ` here, where
    /// Java's `CIBackRef` does not, and the two agree on every ASCII text. The
    /// divergence is documented in `java_fold`; this test pins its extent, so
    /// that a change to it is noticed.
    #[test]
    fn a_backreference_under_an_ascii_fold_is_unicode_insensitive_for_non_ascii_text() {
        // (pattern, input, whole input matches here)
        for (source, input, expected) in [
            ("(?i)(\u{e9})\\1", "\u{e9}\u{c9}", true),
            ("(?i)(\u{e9})\\1", "\u{e9}\u{e9}", true),
            ("(?i)(\u{3c3})\\1", "\u{3c3}\u{3c2}", true),
        ] {
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{source:?} against {input:?}"
            );
        }
    }

    /// Under `(?iu)` Java's `CIBackRef` compares the keys of the chars, and
    /// `fancy_regex`'s `(?i:\1)` compares the simple case foldings, and only
    /// when the two texts are as long in bytes. So a reference to `i` does not
    /// match `İ` or `ı` here, and a reference to `s` does not match `ſ`, `k` the
    /// Kelvin sign, `ß` `ẞ`, or `å` the angstrom sign, which Java's does. The
    /// divergence is documented in `java_fold`; this test pins its extent, so
    /// that a change to it is noticed. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn a_backreference_under_a_unicode_fold_compares_what_fancy_regex_does() {
        // (pattern, input, whole input matches in Java)
        let differs = [
            ("(?iu)(i)\\1", "i\u{130}"),
            ("(?iu)(i)\\1", "i\u{131}"),
            ("(?iu)(\u{130})\\1", "\u{130}i"),
            ("(?iu)(s)\\1", "s\u{17f}"),
            ("(?iu)(k)\\1", "k\u{212a}"),
            ("(?iu)(\u{df})\\1", "\u{df}\u{1e9e}"),
            ("(?iu)(\u{e5})\\1", "\u{e5}\u{212b}"),
        ];
        for (source, input) in differs {
            check!(
                pattern(source).matches(input).ok() == Some(false),
                "{source:?} against {input:?}"
            );
        }
        // (pattern, input, whole input matches, in Java and here)
        let agrees = [
            ("(?iu)(a)\\1", "aA", true),
            ("(?iu)(a)\\1", "ab", false),
            ("(?iu)(k)\\1", "kK", true),
            ("(?iu)(\u{e9})\\1", "\u{e9}\u{c9}", true),
            ("(?iu)(\u{3c3})\\1", "\u{3c3}\u{3c2}", true),
            ("(?iu)(\u{3c3})\\1", "\u{3c3}\u{3a3}", true),
            ("(?iu)(\u{1c6})\\1", "\u{1c6}\u{1c5}", true),
            ("(?iu)(\\p{L}+)\\1", "\u{e9}\u{c9}", true),
        ];
        for (source, input, expected) in agrees {
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{source:?} against {input:?}"
            );
        }
    }

    /// Java reads `(?i){2}` as a repeat of the empty match, and `(?i)*` as a
    /// dangling quantifier. A flag group that leaves no text in the rewrite
    /// would hand its quantifier to the text before it, or make the braces
    /// text, so a quantifier after one is refused.
    #[test]
    fn a_quantifier_after_a_flag_that_leaves_no_text_is_refused() {
        for source in [
            "(?i){2}",
            "(?iu){2}a",
            "(?iu)*",
            "(?i)+a",
            "(?d)?",
            "(?ix) {2}a",
            "(?ix) *",
        ] {
            check!(JavaPattern::compile(source).is_err(), "{source:?}");
        }
    }

    /// `\N{name}` is a character in Java, which `fancy_regex` reads as `\N` and
    /// the text of the name, so a rule that has one is refused and not misread.
    #[test]
    fn a_named_character_is_refused() {
        for source in ["\\N{LATIN SMALL LETTER A}", "(?i)\\N{LATIN SMALL LETTER A}"] {
            check!(JavaPattern::compile(source).is_err(), "{source:?}");
        }
    }

    /// `Matcher.appendReplacement` semantics for `replaceAll`.
    #[test]
    fn replace_all_expands_a_replacement_as_java_does() {
        // (pattern, input, replacement, result)
        let cases = [
            // `$1_$2` is group 1, an underscore and group 2, not a group named `1_`.
            ("(a)-(b)", "a-b", "$1_$2", "a_b"),
            ("(a)", "a", "$1suffix", "asuffix"),
            // Digits after `$n` join the group number only while it names a group.
            ("(a)", "a", "$10", "a0"),
            ("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)", "abcdefghij", "$10", "j"),
            ("(a)", "a", "$0!", "a!"),
            ("(?<who>a)", "a", "${who}-$1", "a-a"),
            // A backslash makes the next character literal.
            ("(a)", "a", "\\$1", "$1"),
            ("a", "aa", "b", "bb"),
            // A group that took no part in the match adds nothing.
            ("(a)|(b)", "b", "[$1]", "[]"),
            ("x", "abc", "$9", "abc"),
        ];
        check_replacements(cases, true, true);
    }

    /// The replacements Java throws on.
    #[test]
    fn replace_all_refuses_what_java_throws_on() {
        for (source, input, replacement) in [
            ("(a)", "a", "$2"),
            ("a", "a", "$1"),
            ("(a)", "a", "$"),
            ("(a)", "a", "$x"),
            ("(a)", "a", "\\"),
            ("(a)", "a", "${who}"),
            ("(a)", "a", "${}"),
            ("(a)", "a", "${who"),
        ] {
            check!(
                pattern(source).replace(input, replacement, true).is_err(),
                "{source} on {input} with {replacement}"
            );
        }
    }

    #[test]
    fn replace_first_stops_after_one_match() {
        let result = pattern("a").replace("aaa", "b", false);
        assert!(result.ok().as_deref() == Some("baa"));
    }

    #[test]
    fn a_pattern_java_refuses_does_not_compile() {
        for source in ["(?P<x>a)", "[a-", "(a"] {
            check!(JavaPattern::compile(source).is_err(), "{source}");
        }
    }
}
