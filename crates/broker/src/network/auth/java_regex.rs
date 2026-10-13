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

    /// One whole-input match: whether `source` matches all of `input`.
    struct MatchCase<'a> {
        source: &'a str,
        input: &'a str,
        expected: bool,
    }

    /// A [`MatchCase`] of `source` against `input`.
    const fn row<'a>(source: &'a str, input: &'a str, expected: bool) -> MatchCase<'a> {
        MatchCase {
            source,
            input,
            expected,
        }
    }

    /// One replacement: `source` on `input` with `replacement` gives `expected`.
    struct ReplaceCase<'a> {
        source: &'a str,
        input: &'a str,
        replacement: &'a str,
        expected: &'a str,
    }

    /// A [`ReplaceCase`] of `source` on `input` with `replacement`.
    const fn replace_row<'a>(
        source: &'a str,
        input: &'a str,
        replacement: &'a str,
        expected: &'a str,
    ) -> ReplaceCase<'a> {
        ReplaceCase {
            source,
            input,
            replacement,
            expected,
        }
    }

    fn check_matches<'a>(cases: impl IntoIterator<Item = MatchCase<'a>>, debug_names: bool) {
        for MatchCase {
            source,
            input,
            expected,
        } in cases
        {
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
        cases: impl IntoIterator<Item = ReplaceCase<'a>>,
        all: bool,
        include_replacement: bool,
    ) {
        for ReplaceCase {
            source,
            input,
            replacement,
            expected,
        } in cases
        {
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
            row("a|ab", "ab", true),
            row("a|ab", "abc", false),
            row("CN=(.*?)", "CN=abc", true),
            row("(a+)+b", "aaab", true),
            row("ali", "alice", false),
            row("ali.*", "alice", true),
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
            replace_row("(.*)", "alice", "$1x", "alicexx"),
            replace_row("a*", "baaac", "-", "-b--c-"),
            replace_row("x*", "abc", "-", "-a-b-c-"),
            replace_row("\\w*", "ab cd", "[$0]", "[ab][] [cd][]"),
            replace_row("(?:)", "é!", "|", "|é|!|"),
            replace_row("", "", "|", "|"),
            replace_row("a", "", "|", ""),
            replace_row("b*", "aé", "-", "-a-é-"),
        ];
        check_replacements(cases, true, false);
    }

    /// `replaceFirst` stops after the first match, an empty one included.
    #[test]
    fn replace_first_stops_after_the_first_match_even_when_it_is_empty() {
        // (pattern, input, replacement, result)
        check_replacements(
            [
                replace_row("(.*)", "alice", "$1x", "alicex"),
                replace_row("b*", "abc", "-", "-abc"),
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
            row("\\w+", "abc_09", true),
            row("\\w+", "é", false),
            row("\\W", "é", true),
            row("\\d", "٣", false),
            row("\\D", "٣", true),
            row("\\s", " \t\u{0B}", false),
            row("\\s+", " \t\u{0B}", true),
            row("\\s", "\u{a0}", false),
            row("\\s", "\u{2003}", false),
            row("\\S", "\u{a0}", true),
            row("[\\w.]+", "a.b", true),
            row("[\\w.]+", "é", false),
            row("[^\\w]", "é", true),
            row("[\\W]+", "é!", true),
            row("[\\d-]+", "1-2", true),
            row("[\\s]", "\u{a0}", false),
            row("(?U)\\w+", "é", true),
            row("(?U)\\d", "٣", true),
            row("(?U)\\s", "\u{a0}", true),
            row("(?U:\\w)\\w", "éa", true),
            row("(?U:\\w)\\w", "éé", false),
            row("(?:(?U)\\w)\\w", "éé", false),
            row("(?U)(?-U)\\w", "é", false),
            row("\\bfoo", "foo", true),
            row("a\\b", "a", true),
            row("\\Bfoo", "foo", false),
            row("a\\bé", "aé", true),
            row("\\bé", "é", false),
            row("a\\Bé", "aé", false),
            row("a.b", "axb", true),
            row("a.b", "a\nb", false),
            row("a.b", "a\rb", false),
            row("a.b", "a\u{85}b", false),
            row("a.b", "a\u{2028}b", false),
            row("a.b", "a\u{2029}b", false),
            row(".*", "abc\rdef", false),
            row("(?s)a.b", "a\rb", true),
            row("(?s)a.b", "a\nb", true),
            row("(?d)a.b", "a\rb", true),
            row("(?d)a.b", "a\u{85}b", true),
            row("(?d)a.b", "a\nb", false),
            row("(?s:a.)b.", "a\nbx", true),
            row("(?s:a.)b.", "a\nb\r", false),
            row("(?s)(?-s).", "\n", false),
            row("a[.]b", "a.b", true),
            row("a\\.b", "axb", false),
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
            row("(?i)service-", "SERVICE-", true),
            row("(?i)service-", "service-", true),
            row("(?i)service-", "\u{17f}ervice-", false),
            row("(?iu)service-", "\u{17f}ervice-", true),
            row("(?iu)service-", "SERVICE-", true),
            row("(?i)k", "\u{212a}", false),
            row("(?iu)k", "\u{212a}", true),
            row("(?ui)k", "\u{212a}", true),
            row("(?u)k", "K", false),
            row("(?i)é", "É", false),
            row("(?i)é", "é", true),
            row("(?iu)é", "É", true),
            row("(?U)(?i)k", "\u{212a}", true),
            row("(?iU)k", "\u{212a}", true),
            row("(?iU)(?-U)k", "\u{212a}", false),
            row("(?iU)(?-U)k", "K", true),
            row("(?iU)(?-u)k", "\u{212a}", false),
            row("(?iU)(?-u)\\w", "\u{e9}", true),
            row("(?i)a(?u)k", "A\u{212a}", true),
            row("(?i)a(?u)k", "AK", true),
            row("(?iu)a(?-u)k", "A\u{212a}", false),
            row("(?iu)a(?-u)k", "AK", true),
            row("(?i)ss", "\u{df}", false),
            row("(?iu)ss", "\u{df}", false),
            row("(?i)i", "\u{130}", false),
            row("(?i)i", "\u{131}", false),
            row("(?i)\\u00E5", "\u{212b}", false),
            row("(?iu)\\u00E5", "\u{212b}", true),
            row("(?i)[a-f]+", "AbCdEf", true),
            row("(?i)[a-f]", "\u{212a}", false),
            row("(?i)[k]", "\u{212a}", false),
            row("(?iu)[k]", "\u{212a}", true),
            row("(?i)[^a]", "A", false),
            row("(?i)[^a]", "b", true),
            row("(?i)[^a]", "\u{e9}", true),
            row("(?i)[^k]", "\u{212a}", true),
            row("(?iu)[^k]", "\u{212a}", false),
            row("(?i)[X-c]", "x", true),
            row("(?i)[X-c]", "A", true),
            row("(?i)[X-c]", "[", true),
            row("(?i)[X-c]", "d", false),
            row("(?i)[+-z]", "Q", true),
            row("(?i)[a-c&&[^b]]", "B", false),
            row("(?i)[a-c&&[^b]]", "C", true),
            row("(?i)[a-c&&[^b]]", "A", true),
            row("(?i)[a-c[^x-z]]", "Y", false),
            row("(?i)[a-c[^x-z]]", "B", true),
            row("(?i)[\\x41-\\x43]", "b", true),
            row("(?i)[\\u0041-\\u0043]", "b", true),
            row("(?i)[\\Qab\\E]", "B", true),
            row("(?i)[a-]", "A", true),
            row("(?i)[-a]", "A", true),
            row("(?i)[]a]", "]", true),
            row("(?i)[]a]", "A", true),
            row("(?i)[a-c-e]", "E", true),
            row("(?i)[a-c-e]", "-", true),
            row("(?i)[\\w&&[^a]]", "A", false),
            row("(?i)[\\w&&[^a]]", "B", true),
            row("(?i)[a\\-c]", "C", true),
            row("(?i)[a\\-c]", "-", true),
            row("(?i)[\\0101-\\0103]", "b", true),
            row("(?i)[\\0101-\\0103]", "d", false),
            row("(?i)[\\x41-c]", "b", true),
            row("(?i)[a-c[d-f]]", "E", true),
            row("(?i)[\u{e9}-\u{eb}]", "\u{ca}", false),
            row("(?ix) [ a - c ]", "B", true),
            row("(?ix) [ a - c ] # x", "d", false),
            row("(?i)a\\.b", "A.B", true),
            row("(?i)\\[a\\]", "[A]", true),
            row("(?i)a{1,2}b", "aAB", true),
            row("(?i)\u{e9}+", "\u{e9}\u{e9}", true),
            row("(?i)\\0101\\0102", "aB", true),
            row("(?i)\\x41", "a", true),
            row("(?i)\\u0041", "a", true),
            row("(?i)\\x{41}", "a", true),
            row("(?i)\\0101", "a", true),
            row("(?i)\\c!", "A", true),
            row("(?i)\\Qab\\E", "AB", true),
            row("(?i)\\Q.b\\E", ".B", true),
            row("(?i)\\Q.b\\E", "xB", false),
            row("(?i)\\Qk\\E", "\u{212a}", false),
            row("(?i)\\p{Lower}", "A", true),
            row("(?i)\\p{Lower}", "\u{e9}", false),
            row("(?i)\\p{Upper}", "a", true),
            row("(?i)\\p{Upper}", "\u{c9}", false),
            row("\\p{Lower}", "a", true),
            row("\\p{Lower}", "A", false),
            row("\\p{Lower}", "\u{e9}", false),
            row("\\p{Upper}", "A", true),
            row("\\p{Upper}", "\u{c9}", false),
            row("(?i)\\p{Lu}", "a", true),
            row("(?i)\\p{Lu}", "\u{e9}", true),
            row("(?i)\\p{Lu}", "1", false),
            row("(?i)\\P{Lu}", "a", false),
            row("(?i)\\P{Lu}", "1", true),
            row("\\p{Lu}", "a", false),
            row("\\p{Lu}", "A", true),
            row("(?i)[\\p{Lower}]", "A", true),
            row("(?i)[\\P{Lower}]", "A", false),
            row("(?i)[\\P{Lower}]", "1", true),
            row("(?i)\\p{IsLowercase}", "A", true),
            row("(?i)\\p{gc=Lu}", "a", true),
            row("(?i)\\p{IsLu}", "a", true),
            row("(?i:a)b", "Ab", true),
            row("(?i:a)b", "AB", false),
            row("a(?i)b", "aB", true),
            row("a(?i)b", "Ab", false),
            row("a(?i:b)c", "aBc", true),
            row("a(?i:b)c", "aBC", false),
            row("(?i)a(?-i)b", "Ab", true),
            row("(?i)a(?-i)b", "AB", false),
            row("((?i)a)b", "Ab", true),
            row("((?i)a)b", "AB", false),
            row("(?i)a|b", "B", true),
            row("(?i)(?i)(?-i)a", "A", false),
            row("(?i)(?i)(?-i)a", "a", true),
            row("(?i)a{2}", "aA", true),
            row("(?i)(?:a|b)+", "AbBa", true),
            row("(?i)^ab$", "AB", true),
            row("(?i)\\bab", "AB", true),
            row("(?i)\\w", "A", true),
            row("(?i)a.c", "AxC", true),
            row("(?i)(?<Name>a)", "A", true),
            row("(?i)(?=a)A", "A", true),
            row("(?i)(?!a)b", "B", true),
            row("(?ix) a b # comment", "AB", true),
            row("(?x)a b # Comment here", "ab", true),
            row("(?x)a b # Comment here", "AB", false),
            row("(?iu)(a)\\1", "aA", true),
            row("(?iu)(a)\\1", "ab", false),
            row("(?i)(a)(?-i)\\1", "aA", false),
            row("(?i)(a)(?-i)\\1", "aa", true),
            row("(?i)(a)(?-i:\\1)", "aA", false),
            row("(a)\\1", "aa", true),
            row("(a)\\1", "aA", false),
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
            row("(?i)[\\Qa\\E-c]", "B", true),
            row("(?i)[\\Qa\\E-c]", "b", true),
            row("(?i)[\\Qa\\E-c]", "A", true),
            row("(?i)[\\Qa\\E-c]", "X", false),
            row("(?i)[a-\\Qc\\E]", "B", true),
            row("(?i)[a-\\Qc\\E]", "X", false),
            row("(?i)[\\Qa\\E-\\Qc\\E]", "B", true),
            row("(?i)[\\Qa\\E-\\Qc\\E]", "X", false),
            row("(?i)[\\Qa\\Eb-d]", "C", true),
            row("(?i)[\\Qa\\Eb-d]", "A", true),
            row("(?i)[\\Qa\\Eb-d]", "X", false),
            row("(?i)[\\Q\\Ea-c]", "B", true),
            row("(?i)[a-\\Q\\Ec]", "B", true),
            row("(?i)[a-\\Q\\Ec]", "X", false),
            row("[\\Qa\\E-c]", "b", true),
            row("[\\Qa\\E-c]", "B", false),
            row("[\\Qa\\E-c]", "X", false),
            row("(?i)[\\Q]\\E-c]", "A", true),
            row("(?i)[\\Q]\\E-c]", "X", false),
            row("(?i)[\\Q1\\E-3]", "2", true),
            // a quoted dash is not a range
            row("(?i)[\\Qa-c\\E]", "B", false),
            row("(?i)[\\Qa-c\\E]", "-", true),
            row("(?i)[\\Qa-c\\E]", "C", true),
            row("(?i)[a\\Q-\\Ec]", "B", false),
            row("(?i)[a\\Q-\\Ec]", "-", true),
            row("(?i)[a\\Q-\\Ec]", "C", true),
            // a quote with no \E runs to the end of the pattern
            row("(?i)x\\Qa", "XA", true),
            row("(?i)x\\Qa\\E", "XA", true),
            row("(?i)a\\Q\\E+", "AA", true),
            row("\\Q1\\E2", "12", true),
            row("\\Qa\\\\E", "a\\", true),
            row("\\Q\\\\E", "\\", true),
            row("\\Qa.b\\E", "a.b", true),
            row("\\Qa.b\\E", "axb", false),
            row("(?i)\\Qk\\E", "\u{212a}", false),
            row("(?iu)\\Qk\\E", "\u{212a}", true),
            // a backslash before < or > is the character
            row("\\<", "<", true),
            row("\\>", ">", true),
            row("[\\<]", "<", true),
            row("[\\>]", ">", true),
            row("\\Q<\\E", "<", true),
            row("[\\Q<\\E]", "<", true),
            row("\\Q>\\E", ">", true),
            // a ] first in a class is a member, and starts a range
            row("(?i)[]-c]", "A", true),
            row("(?i)[]-c]", "B", true),
            row("(?i)[]-c]", "_", true),
            row("(?i)[]-c]", "]", true),
            row("(?i)[]-c]", "X", false),
            row("(?i)[]-c]", "d", false),
            row("(?i)[]-c]", "D", false),
            row("(?i)[^]-c]", "A", false),
            row("(?i)[^]-c]", "X", true),
            row("(?i)[^]-c]", "d", true),
            row("(?i)[]-cx]", "X", true),
            row("(?i)[]-]", "-", true),
            row("(?i)[]-]", "]", true),
            row("(?iu)[]-c]", "A", true),
            row("(?ix)[ ]-c ]", "A", true),
            row("[]-c]", "a", true),
            row("[]-c]", "A", false),
            row("[]-c]", "]", true),
            // \p{Lower} and \p{Upper} are the ASCII letters under (?iu), whatever fancy_regex's i would fold
            row("(?iu)\\p{Lower}", "\u{17f}", false),
            row("(?iu)\\p{Lower}", "\u{212a}", false),
            row("(?iu)\\p{Lower}", "A", true),
            row("(?iu)\\p{Lower}", "a", true),
            row("(?iu)\\p{Upper}", "\u{17f}", false),
            row("(?iu)\\p{Upper}", "\u{212a}", false),
            row("(?iu)\\p{Upper}", "a", true),
            row("(?iu)\\p{Upper}", "\u{e9}", false),
            row("(?iu)\\P{Lower}", "\u{17f}", true),
            row("(?iu)\\P{Lower}", "A", false),
            row("(?iu)\\P{Lower}", "1", true),
            row("(?iu)\\P{Upper}", "\u{212a}", true),
            row("(?u)\\p{Lower}", "\u{17f}", false),
            row("(?u)\\p{Lower}", "A", false),
            // \w and \W are ASCII under (?iu), whatever fancy_regex's i would fold
            row("(?iu)\\w", "\u{17f}", false),
            row("(?iu)\\w", "\u{212a}", false),
            row("(?iu)\\w", "a", true),
            row("(?iu)\\w", "_", true),
            row("(?iu)\\W", "\u{17f}", true),
            row("(?iu)\\W", "\u{212a}", true),
            row("(?iu)\\W", "a", false),
            row("(?iu)\\W", "-", true),
            row("(?iu)\\w+", "k\u{212a}", false),
            row("(?iu)k\\w", "k\u{212a}", false),
            row("(?iu)\\w\\p{Lower}", "k\u{212a}", false),
            row("(?i)\\w", "\u{17f}", false),
            row("(?u)\\w", "\u{17f}", false),
            row("(?iU)\\w", "\u{17f}", true),
            // a backreference compares ASCII case-insensitively under (?i)
            row("(?i)(a)\\1", "aA", true),
            row("(?i)(a)\\1", "aa", true),
            row("(?i)(a)\\1", "ab", false),
            row("(?i)(\\d+)-\\1", "12-12", true),
            row("(?i)(\\d+)-\\1", "12-13", false),
            row("(?i)(?<x>a)\\k<x>", "aA", true),
            row("(?i)(?<x>a)\\k<x>", "ab", false),
            row("(?i)(a+)\\1", "aaAA", true),
            row("(?i)(a)\\1+", "aAAa", true),
            row("(?i)(a)(\\1)", "aA", true),
            row("(?i)(k)\\1", "kK", true),
            row("(?i)(k)\\1", "k\u{212a}", false),
            row("(?i:(a)\\1)", "aA", true),
            row("a(?i:(b)\\1)", "aBb", true),
            row("a(?i:(b)\\1)", "aBB", true),
            row("a(?i:(b)\\1)", "AbB", false),
            row("(?i)(a)((b))\\3\\2\\1", "aBBBA", true),
            row("(?i)(a)((b))\\3\\2\\1", "aBbBa", true),
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
            row("(?iu)[\\w]", "\u{17f}", false),
            row("(?iu)[\\w]", "\u{212a}", false),
            row("(?iu)[\\w]", "a", true),
            row("(?iu)[\\w]", "K", true),
            row("(?iu)[\\w]", "_", true),
            row("(?iu)[\\w]", "\u{e9}", false),
            row("(?iu)[\\w-]", "\u{17f}", false),
            row("(?iu)[\\w-]", "\u{212a}", false),
            row("(?iu)[\\w-]", "-", true),
            row("(?iu)[\\w-]", "z", true),
            row("(?iu)[\\W]", "\u{17f}", true),
            row("(?iu)[\\W]", "\u{212a}", true),
            row("(?iu)[\\W]", "a", false),
            row("(?iu)[\\W]", "-", true),
            row("(?iu)[^\\w]", "\u{17f}", true),
            row("(?iu)[^\\w]", "k", false),
            row("(?iu)[\\p{Lower}]", "\u{17f}", false),
            row("(?iu)[\\p{Lower}]", "\u{212a}", false),
            row("(?iu)[\\p{Lower}]", "A", true),
            row("(?iu)[\\p{Lower}]", "a", true),
            row("(?iu)[\\p{Lower}]", "\u{e9}", false),
            row("(?iu)[\\p{Upper}]", "\u{17f}", false),
            row("(?iu)[\\p{Upper}]", "\u{212a}", false),
            row("(?iu)[\\p{Upper}]", "a", true),
            row("(?iu)[\\p{Upper}]", "\u{c9}", false),
            row("(?iu)[\\P{Lower}]", "\u{17f}", true),
            row("(?iu)[\\P{Lower}]", "A", false),
            row("(?iu)[\\P{Lower}]", "1", true),
            row("(?iu)[\\P{Upper}]", "\u{212a}", true),
            row("(?iu)[\\p{Lower}k]", "\u{17f}", false),
            row("(?iu)[\\p{Lower}k]", "\u{212a}", true),
            row("(?iu)[\\p{Lower}s]", "\u{17f}", true),
            row("(?iu)[\\w&&[^a]]", "A", false),
            row("(?iu)[\\w&&[^a]]", "b", true),
            row("(?iu)[\\w&&[^k]]", "\u{212a}", false),
            row("(?iu)[\\w&&[^k]]", "K", false),
            row("(?iu)[^\\p{Lower}]", "\u{17f}", true),
            row("(?iu)[^\\p{Lower}]", "a", false),
            row("(?iu)[^\\p{Lower}]", "1", true),
            // a range that holds K and not k, or s and not S, compares the upper case and the lower case of the upper case of the char
            row("(?iu)[a-z]", "\u{212a}", true),
            row("(?iu)[a-z]", "\u{17f}", true),
            row("(?iu)[a-z]", "\u{130}", true),
            row("(?iu)[a-z]", "\u{131}", true),
            row("(?iu)[a-z]", "K", true),
            row("(?iu)[a-z]", "\u{df}", false),
            row("(?iu)[A-Z]", "\u{212a}", false),
            row("(?iu)[A-Z]", "\u{17f}", true),
            row("(?iu)[A-Z]", "\u{130}", false),
            row("(?iu)[A-Z]", "\u{131}", true),
            row("(?iu)[A-Z]", "k", true),
            row("(?iu)[A-K]", "\u{212a}", false),
            row("(?iu)[A-K]", "k", true),
            row("(?iu)[A-K]", "\u{17f}", false),
            row("(?iu)[A-c]", "\u{212a}", false),
            row("(?iu)[A-c]", "k", true),
            row("(?iu)[A-c]", "C", true),
            row("(?iu)[A-c]", "d", true),
            row("(?iu)[A-k]", "\u{212a}", true),
            row("(?iu)[k-k]", "\u{212a}", true),
            row("(?iu)[K-K]", "\u{212a}", false),
            row("(?iu)[K-K]", "k", true),
            row("(?iu)[s-s]", "\u{17f}", true),
            row("(?iu)[S-S]", "\u{17f}", true),
            row("(?iu)[i-i]", "\u{130}", true),
            row("(?iu)[I-I]", "\u{131}", true),
            row("(?iu)[^A-Z]", "\u{212a}", true),
            row("(?iu)[^A-Z]", "\u{17f}", false),
            row("(?iu)[^a-z]", "\u{212a}", false),
            row("(?iu)[^A-K]", "\u{212a}", true),
            row("(?iu)[^A-K]", "k", false),
            row("(?iu)[\\x{E0}-\\x{FE}]", "\u{c9}", true),
            row("(?iu)[\\x{E0}-\\x{FE}]", "\u{178}", false),
            row("(?iu)[\\x{C0}-\\x{DE}]", "\u{ff}", false),
            row("(?iu)[\\x{C0}-\\x{DE}]", "\u{178}", false),
            row("(?iu)[\\x{391}-\\x{3A9}]", "\u{3c3}", true),
            row("(?iu)[\\x{391}-\\x{3A9}]", "\u{3c2}", true),
            row("(?iu)[\\x{3B1}-\\x{3C9}]", "\u{3a3}", true),
            row("(?iu)[\\x{410}-\\x{42F}]", "\u{44f}", true),
            row("(?iu)[\\x{410}-\\x{42F}]", "\u{436}", true),
            row("(?iu)[\\x{DF}-\\x{DF}]", "\u{1e9e}", true),
            row("(?iu)[\\x{1E9E}-\\x{1E9E}]", "\u{df}", false),
            // a single member: Latin-1 members go in a bit class, the other members compare keys
            row("(?iu)[k]", "\u{212a}", true),
            row("(?iu)[K]", "\u{212a}", true),
            row("(?iu)[s]", "\u{17f}", true),
            row("(?iu)[S]", "\u{17f}", true),
            row("(?iu)[i]", "\u{130}", true),
            row("(?iu)[i]", "\u{131}", true),
            row("(?iu)[I]", "\u{130}", true),
            row("(?iu)[I]", "\u{131}", true),
            row("(?iu)[\\x{E5}]", "\u{212b}", true),
            row("(?iu)[\\x{C5}]", "\u{212b}", true),
            row("(?iu)[\\x{FF}]", "\u{178}", true),
            row("(?iu)[\\x{B5}]", "\u{3bc}", true),
            row("(?iu)[\\x{B5}]", "\u{39c}", true),
            row("(?iu)[\\x{3BC}]", "\u{b5}", true),
            row("(?iu)[\\x{E9}]", "\u{c9}", true),
            row("(?iu)[\\x{C9}]", "\u{e9}", true),
            row("(?iu)[\\x{DF}]", "\u{1e9e}", false),
            row("(?iu)[\\x{1E9E}]", "\u{df}", true),
            row("(?iu)[\\x{3C3}]", "\u{3c2}", true),
            row("(?iu)[\\x{3C2}]", "\u{3a3}", true),
            row("(?iu)[\\x{3A3}]", "\u{3c2}", true),
            row("(?iu)[\\x{1C6}]", "\u{1c5}", true),
            row("(?iu)[\\x{1C5}]", "\u{1c4}", true),
            row("(?iu)[\\x{1F88}]", "\u{1f80}", true),
            row("(?iu)[\\x{1F80}]", "\u{1f88}", true),
            row("(?iu)[\\x{10428}]", "\u{10400}", true),
            row("(?iu)[\\x{10400}-\\x{1044F}]", "\u{10428}", true),
            row("(?iu)[z]", "Z", true),
            row("(?iu)[1]", "1", true),
            // with && and a nested class
            row("(?iu)[a-z&&[^k]]", "\u{212a}", false),
            row("(?iu)[a-z&&[^k]]", "K", false),
            row("(?iu)[a-z&&[^k]]", "j", true),
            row("(?iu)[a-z&&k]", "\u{212a}", true),
            row("(?iu)[a-z&&k]", "K", true),
            row("(?iu)[a-c[x-z]]", "X", true),
            row("(?iu)[a-c[^x-z]]", "Y", false),
            row("(?iu)[a-c[^x-z]]", "B", true),
            row("(?iu)[^a-z&&[^k]]", "k", true),
            row("(?iu)[^a-z&&[^k]]", "\u{212a}", true),
            row("(?iu)[a&&-1]", "-", false),
            row("(?iu)[a&&-1]", "a", false),
            row("(?iu)[a-c&&b-d]", "B", true),
            row("(?iu)[a-c&&b-d]", "A", false),
            // under (?iU) the classes are Unicode and the members fold the same
            row("(?iU)[\\w]", "\u{17f}", true),
            row("(?iU)[\\w]", "\u{e9}", true),
            row("(?iU)[a-z]", "\u{212a}", true),
            row("(?iU)[A-Z]", "\u{212a}", false),
            row("(?iU)[\\p{Lower}]", "A", true),
            row("(?iU)[\\p{Upper}]", "a", true),
            row("(?iU)[\\p{Lower}]", "\u{1c5}", true),
            row("(?iU)[\\p{Lu}]", "a", true),
            row("(?iU)[\\p{Lu}]", "1", false),
            // only the ASCII fold, which does not reach these
            row("(?i)[a-z]", "\u{212a}", false),
            row("(?i)[a-z]", "\u{17f}", false),
            row("(?i)[A-K]", "k", true),
            row("(?i)[k]", "\u{212a}", false),
            row("(?i)[\\w]", "\u{17f}", false),
            row("(?u)[a-z]", "A", false),
            row("(?u)[k]", "\u{212a}", false),
            row("[a-z]", "K", false),
            // the fold ends where the flag does
            row("(?iu)(?-i)[a-z]", "A", false),
            row("(?iu)(?-i)[a-z]", "\u{212a}", false),
            row("(?iu)(?-u)[a-z]", "A", true),
            row("(?iu)(?-u)[a-z]", "\u{212a}", false),
            row("(?iu)(?-u)[k]", "\u{212a}", false),
            row("(?iu:[a-z])A", "AA", true),
            row("(?iu:[a-z])A", "Aa", false),
            row("(?iu:[a-z])[A-Z]", "aa", false),
            row("(?iu:[a-z])[A-Z]", "\u{212a}a", false),
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
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{e000}", false),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{d7ff}", false),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{e001}", false),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{10976}", true),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "a", true),
            row("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{d7ff}", true),
            row("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{e000}", true),
            row("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{e001}", false),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{d7ff}", false),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{e000}", false),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{d7fe}", true),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{e001}", true),
            row("(?iu)[^\\x{D800}-\\x{E001}]", "\u{e000}", false),
            row("(?iu)[^\\x{D800}-\\x{E001}]", "\u{e002}", true),
            row("(?iu)[^\\x{41}-\\x{DBFF}]", "\u{d7ff}", false),
            row("(?iu)[^\\x{41}-\\x{DBFF}]", "\u{e000}", true),
            row("(?iu)[\\x{D800}-\\x{DFFF}]", "a", false),
            row("(?iu)[^\\x{D800}-\\x{DFFF}]", "a", true),
            row("(?iu)[^\\x{D800}-\\x{DFFF}]", "\u{e000}", true),
            row("(?iu)[a\\x{D800}-\\x{DFFF}]", "a", true),
            row("(?iu)[a\\x{D800}-\\x{DFFF}]", "\u{d7ff}", false),
            row("(?iu)[^a\\x{D800}-\\x{DFFF}]", "a", false),
            row("(?iu)[^a\\x{D800}-\\x{DFFF}]", "b", true),
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
            row("\\p{Alpha}", "a", true),
            row("\\p{Alpha}", "Z", true),
            row("\\p{Alpha}", "\u{e9}", false),
            row("\\p{Alpha}", "\u{3b1}", false),
            row("\\p{Alpha}", "1", false),
            row("\\p{Alpha}", "_", false),
            row("\\P{Alpha}", "\u{e9}", true),
            row("\\P{Alpha}", "a", false),
            row("\\p{Alnum}", "5", true),
            row("\\p{Alnum}", "q", true),
            row("\\p{Alnum}", "_", false),
            row("\\p{Alnum}", "\u{e9}", false),
            row("\\p{Alnum}", "\u{663}", false),
            row("\\p{Digit}", "7", true),
            row("\\p{Digit}", "\u{663}", false),
            row("\\p{Digit}", "\u{ff17}", false),
            row("\\p{Punct}", "!", true),
            row("\\p{Punct}", "$", true),
            row("\\p{Punct}", "_", true),
            row("\\p{Punct}", "~", true),
            row("\\p{Punct}", "`", true),
            row("\\p{Punct}", "@", true),
            row("\\p{Punct}", "a", false),
            row("\\p{Punct}", "\u{a1}", false),
            row("\\p{Punct}", "\u{2014}", false),
            row("\\P{Punct}", "\u{a1}", true),
            row("\\P{Punct}", "!", false),
            row("\\p{ASCII}", "a", true),
            row("\\p{ASCII}", "\u{7f}", true),
            row("\\p{ASCII}", "\u{0}", true),
            row("\\p{ASCII}", "\u{80}", false),
            row("\\p{ASCII}", "\u{e9}", false),
            row("\\P{ASCII}", "\u{e9}", true),
            row("\\p{XDigit}", "f", true),
            row("\\p{XDigit}", "F", true),
            row("\\p{XDigit}", "9", true),
            row("\\p{XDigit}", "g", false),
            row("\\p{XDigit}", "\u{ff11}", false),
            row("\\p{Blank}", " ", true),
            row("\\p{Blank}", "\t", true),
            row("\\p{Blank}", "\n", false),
            row("\\p{Blank}", "\u{a0}", false),
            row("\\p{Cntrl}", "\u{1f}", true),
            row("\\p{Cntrl}", "\u{7f}", true),
            row("\\p{Cntrl}", "\u{0}", true),
            row("\\p{Cntrl}", " ", false),
            row("\\p{Cntrl}", "\u{80}", false),
            row("\\p{Graph}", "!", true),
            row("\\p{Graph}", "~", true),
            row("\\p{Graph}", " ", false),
            row("\\p{Graph}", "\u{e9}", false),
            row("\\p{Print}", " ", true),
            row("\\p{Print}", "~", true),
            row("\\p{Print}", "\u{7f}", false),
            row("\\p{Print}", "\u{e9}", false),
            row("\\p{Space}", " ", true),
            row("\\p{Space}", "\u{b}", true),
            row("\\p{Space}", "\u{a0}", false),
            row("\\p{Space}", "\u{2003}", false),
            row("[\\p{Alpha}]", "\u{e9}", false),
            row("[\\p{Alpha}]", "b", true),
            row("[\\p{Alpha}\\d]", "7", true),
            row("[\\p{Alnum}_]", "_", true),
            row("[\\p{Alnum}_]", "\u{e9}", false),
            row("[\\p{Punct}]", "\u{a1}", false),
            row("[\\p{Punct}]", ";", true),
            row("[^\\p{Punct}]", ";", false),
            row("[^\\p{Punct}]", "\u{a1}", true),
            row("[\\P{Punct}]", ";", false),
            row("[\\p{ASCII}]", "\u{e9}", false),
            row("[\\p{ASCII}-]", "-", true),
            row("[\\p{XDigit}]", "\u{ff11}", false),
            row("[\\p{Blank}]", "\u{a0}", false),
            row("[\\p{Space}x]", "\u{a0}", false),
            row("[\\p{Cntrl}]", "\u{80}", false),
            row("[\\p{Graph}]", "\u{e9}", false),
            row("[\\p{Print}]", "\u{e9}", false),
            row("[\\p{Alpha}&&[^a]]", "a", false),
            row("[\\p{Alpha}&&[^a]]", "b", true),
            // the case flags do not fold them, and under (?i) Lower and Upper are the ASCII letters of both cases
            row("(?i)\\p{Alpha}", "\u{17f}", false),
            row("(?i)\\p{Alpha}", "\u{212a}", false),
            row("(?iu)\\p{Alpha}", "\u{17f}", false),
            row("(?iu)\\p{Alpha}", "\u{212a}", false),
            row("(?iu)\\p{ASCII}", "\u{17f}", false),
            row("(?iu)\\p{ASCII}", "\u{212a}", false),
            row("(?iu)\\p{ASCII}", "K", true),
            row("(?iu)\\p{Punct}", "\u{212a}", false),
            row("(?iu)\\p{Digit}", "1", true),
            row("(?iu)[\\p{Alnum}]", "\u{17f}", false),
            row("(?iu)[\\p{Alnum}]", "\u{212a}", false),
            row("(?iu)[\\p{ASCII}]", "\u{212a}", false),
            row("(?iu)[\\p{XDigit}]", "\u{212a}", false),
            row("(?iu)[\\p{Punct}k]", "\u{212a}", true),
            row("(?i)\\p{Lower}", "A", true),
            row("(?i)\\p{Upper}", "a", true),
            row("(?i)\\p{Lower}", "\u{e9}", false),
            row("(?iu)\\p{Lower}", "\u{17f}", false),
            row("(?iu)\\p{Upper}", "\u{212a}", false),
            // the properties that are not the ASCII ones are not folded either
            row("(?iu)\\p{IsGreek}", "\u{345}", false),
            row("(?iu)\\p{IsLatin}", "\u{212a}", true),
            row("(?iu)\\p{IsLatin}", "\u{17f}", true),
            row("(?iu)\\p{L}", "\u{17f}", true),
            row("(?iu)\\p{Nd}", "\u{663}", true),
            row("(?iu)[\\p{IsGreek}]", "\u{345}", false),
            row("(?iu)\\p{Lu}", "a", true),
            row("(?iu)\\p{Lu}", "\u{df}", true),
            row("(?iu)\\p{Lu}", "\u{138}", true),
            row("(?iu)\\p{Lu}", "1", false),
            row("(?iu)\\p{Ll}", "A", true),
            row("(?iu)\\p{Lt}", "a", true),
            row("(?iu)\\P{Lu}", "a", false),
            row("(?iu)\\P{Lu}", "1", true),
            row("(?iu)[\\p{Lu}]", "\u{df}", true),
            row("(?iu)[\\p{Lu}]", "\u{138}", true),
            row("(?iu)[^\\p{Lu}]", "\u{df}", false),
            row("(?iu)[^\\p{Lu}]", "1", true),
            row("(?iu)\\p{IsLowercase}", "A", true),
            row("(?iu)\\p{IsUppercase}", "a", true),
            row("(?iu)\\p{IsLowercase}", "\u{2102}", true),
            row("(?iu)\\p{gc=Lu}", "\u{138}", true),
            row("(?iu)\\p{IsLu}", "\u{138}", true),
            row("(?iu)\\p{Lower}", "A", true),
            // under (?U) the POSIX classes are the Unicode properties
            row("(?U)\\p{Alpha}", "\u{e9}", true),
            row("(?U)\\p{Alpha}", "1", false),
            row("(?U)\\p{Alnum}", "\u{e9}", true),
            row("(?U)\\p{Alnum}", "\u{663}", true),
            row("(?U)\\p{Alnum}", "_", false),
            row("(?U)\\p{Digit}", "\u{663}", true),
            row("(?U)\\p{Digit}", "a", false),
            row("(?U)\\p{Punct}", "\u{a1}", true),
            row("(?U)\\p{Punct}", "$", false),
            row("(?U)\\p{Punct}", "!", true),
            row("(?U)\\p{Space}", "\u{a0}", true),
            row("(?U)\\p{Space}", "\u{85}", true),
            row("(?U)\\p{Space}", "\u{2028}", true),
            row("(?U)\\p{Space}", "a", false),
            row("(?U)\\p{XDigit}", "\u{ff11}", true),
            row("(?U)\\p{XDigit}", "\u{663}", true),
            row("(?U)\\p{XDigit}", "g", false),
            row("(?U)\\p{Blank}", "\u{a0}", true),
            row("(?U)\\p{Blank}", "\t", true),
            row("(?U)\\p{Blank}", "\n", false),
            row("(?U)\\p{Cntrl}", "\u{80}", true),
            row("(?U)\\p{Cntrl}", " ", false),
            row("(?U)\\p{Graph}", "\u{e9}", true),
            row("(?U)\\p{Graph}", " ", false),
            row("(?U)\\p{Graph}", "\u{a0}", false),
            row("(?U)\\p{Graph}", "\u{2028}", false),
            row("(?U)\\p{Print}", "\u{e9}", true),
            row("(?U)\\p{Print}", " ", true),
            row("(?U)\\p{Print}", "\u{a0}", true),
            row("(?U)\\p{Print}", "\n", false),
            row("(?U)\\p{Print}", "\u{2028}", false),
            row("(?U)\\p{Lower}", "\u{e9}", true),
            row("(?U)\\p{Lower}", "A", false),
            row("(?U)\\p{Upper}", "\u{c9}", true),
            row("(?U)\\p{alpha}", "\u{e9}", true),
            row("(?U)\\P{Graph}", " ", true),
            row("(?U)\\P{Graph}", "a", false),
            row("(?U)[\\p{Alpha}]", "\u{e9}", true),
            row("(?U)[\\p{Graph}]", "\u{e9}", true),
            row("(?U)[\\P{Graph}x]", " ", true),
            row("(?U)[\\P{Graph}x]", "x", true),
            row("(?U)[\\P{Graph}x]", "a", false),
            row("(?iU)\\p{Lower}", "A", true),
            row("(?iU)\\p{Lower}", "\u{1c5}", true),
            row("(?iU)\\p{Upper}", "a", true),
            row("(?iU)\\p{Lower}", "\u{2102}", true),
            row("(?iU)\\p{Alpha}", "\u{212a}", true),
            row("(?iU)\\p{Punct}", "\u{a1}", true),
            row("(?iU)\\p{Graph}", "\u{212a}", true),
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
            row("(?iu)i", "\u{130}", true),
            row("(?iu)i", "\u{131}", true),
            row("(?iu)I", "\u{130}", true),
            row("(?iu)I", "\u{131}", true),
            row("(?iu)\u{130}", "i", true),
            row("(?iu)\u{130}", "\u{131}", true),
            row("(?iu)\u{131}", "I", true),
            row("(?iu)\u{131}", "\u{130}", true),
            row("(?iu)i", "I", true),
            row("(?iu)id", "\u{130}D", true),
            row("(?iu)k", "\u{212a}", true),
            row("(?iu)K", "\u{212a}", true),
            row("(?iu)\u{212a}", "k", true),
            row("(?iu)s", "\u{17f}", true),
            row("(?iu)\u{17f}", "S", true),
            row("(?iu)\u{e5}", "\u{212b}", true),
            row("(?iu)\u{212b}", "\u{c5}", true),
            row("(?iu)\u{3c3}", "\u{3c2}", true),
            row("(?iu)\u{3c2}", "\u{3a3}", true),
            row("(?iu)\u{3a3}", "\u{3c3}", true),
            row("(?iu)\u{b5}", "\u{39c}", true),
            row("(?iu)\u{b5}", "\u{3bc}", true),
            row("(?iu)\u{1c5}", "\u{1c6}", true),
            row("(?iu)\u{1f80}", "\u{1f88}", true),
            row("(?iu)\u{1f88}", "\u{1f80}", true),
            row("(?iu)\u{1f80}", "\u{1f80}", true),
            row("(?iu)\u{ff}", "\u{178}", true),
            row("(?iu)\u{10428}", "\u{10400}", true),
            row("(?iu)\u{e9}", "\u{c9}", true),
            row("(?iu)\u{436}", "\u{416}", true),
            row("(?iu)1", "1", true),
            row("(?iu)ss", "\u{df}", false),
            row("(?iu)SS", "\u{1e9e}", false),
            row("(?iu)\u{df}", "\u{df}", true),
            // with the fold off, in ASCII, or ended, the letters stay apart
            row("(?i)i", "\u{130}", false),
            row("(?i)i", "\u{131}", false),
            row("(?i)k", "\u{212a}", false),
            row("(?u)i", "I", false),
            row("(?u)i", "\u{130}", false),
            row("(?iu)(?-i)i", "I", false),
            row("(?iu)(?-i)i", "\u{130}", false),
            row("(?iu)(?-u)i", "\u{130}", false),
            row("(?iu)(?-u)i", "I", true),
            row("(?iu:i)", "\u{130}", true),
            row("(?iu:i)i", "\u{130}i", true),
            row("(?iu:i)i", "\u{130}I", false),
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
            row("(?iu)\u{df}", "\u{1e9e}", false),
            row("(?iu)\u{df}", "\u{df}", true),
            row("(?iu)\u{df}\u{df}", "\u{1e9e}\u{1e9e}", true),
            row("(?iu)\u{df}a", "\u{1e9e}a", true),
            row("(?iu)a\u{df}", "a\u{1e9e}", true),
            row("(?iu)\u{df}*", "\u{1e9e}", false),
            row("(?iu)\u{df}a*", "\u{1e9e}", false),
            row("(?iu)\u{df}a*", "\u{1e9e}a", false),
            row("(?iu)\u{df}ab*", "\u{1e9e}ab", true),
            row("(?iu)\u{df}ab*", "\u{1e9e}a", true),
            row("(?iu)a\u{df}*", "a\u{1e9e}", false),
            row("(?iu)a\u{df}*", "a", true),
            row("(?iu)a{2}\u{df}", "aa\u{1e9e}", false),
            row("(?iu)a{2}\u{df}\u{df}", "aa\u{1e9e}\u{1e9e}", true),
            row("(?iu)(\u{df})", "\u{1e9e}", false),
            row("(?iu)(\u{df}\u{df})", "\u{1e9e}\u{1e9e}", true),
            row("(?iu)\u{df}|\u{df}", "\u{1e9e}", false),
            row("(?iu)\u{df}.", "\u{1e9e}x", false),
            row("(?iu)\u{df}\\.", "\u{1e9e}.", true),
            row("(?iu)\u{df}\\w", "\u{1e9e}x", false),
            row("(?iu)\u{df}{1,2}", "\u{1e9e}", false),
            row("(?iu)\u{df}\u{df}?", "\u{1e9e}", false),
            row("(?iu)\u{df}\u{df}?", "\u{1e9e}\u{1e9e}", false),
            row("(?iu)x\u{df}", "x\u{1e9e}", true),
            row("(?iu)\u{df}+x", "\u{1e9e}x", false),
            row("(?iu)\u{df}x", "\u{1e9e}x", true),
            row("(?iu)(?:\u{df})\u{df}", "\u{1e9e}\u{1e9e}", false),
            row("(?iu)(?:\u{df})", "\u{1e9e}", false),
            row("(?iu)\u{df}$", "\u{1e9e}", false),
            row("(?iu)^\u{df}", "\u{1e9e}", false),
            row("(?iu)\u{df}[a]", "\u{1e9e}a", false),
            row("(?iu)\u{df}\\x{DF}", "\u{1e9e}\u{1e9e}", true),
            row("(?iu)\\x{DF}", "\u{1e9e}", false),
            row("(?iu)\\Q\u{df}\\E", "\u{1e9e}", false),
            row("(?iu)\\Q\u{df}\u{df}\\E", "\u{1e9e}\u{1e9e}", true),
            row("(?iux)\u{df} \u{df}", "\u{1e9e}\u{1e9e}", true),
            row("(?iux)\u{df} #c\n\u{df}", "\u{1e9e}\u{1e9e}", true),
            row("(?iux)\u{df} #c\n", "\u{1e9e}", false),
            row("(?iu)\u{1e9e}", "\u{df}", true),
            row("(?iu)\u{1e9e}\u{1e9e}", "\u{df}\u{df}", true),
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
            row("(?u)(?<n>.(?x))A(?i)I ", "&A\u{131} ", true),
            row("(?u)(?<n>.(?x))A(?i)I ", "&A\u{131}", false),
            row("(?u)(?<n>.(?x))A(?i)I", "&A\u{131}", true),
            row("(?u)(.(?x))A#", "xA#", true),
            row("(?u)(.(?x))A#", "xA", false),
            row("(?x)(.(?x))A#c\nB", "xAB", true),
            row("(?x:a )b", "ab", true),
            row("(?x:a )b", "a b", false),
            row("(?x:a)#b", "a#b", true),
            row("(?x:a)#b", "a", false),
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
            row("(?x)[ ^a]", "^", true),
            row("(?x)[ ^a]", "a", true),
            row("(?x)[ ^a]", "b", false),
            row("(?ix)[ ^a]", "^", true),
            row("(?ix)[ ^a]", "A", true),
            row("(?ix)[ ^a]", "b", false),
            row("(?ix)[#c\n^a]", "^", true),
            row("(?ix)[#c\n^a]", "A", true),
            row("(?ix)[#c\n^a]", "b", false),
            row("(?x)[ ^-b]", "^", true),
            row("(?x)[ ^-b]", "a", true),
            row("(?x)[ ^-b]", "c", false),
            row("(?x)[a[ ^b]]", "^", true),
            row("(?x)[a[ ^b]]", "b", true),
            row("(?x)[a[ ^b]]", "c", false),
            row("(?x)[^ a]", "a", false),
            row("(?x)[^ a]", "b", true),
            row("(?x)[^ ^a]", "^", false),
            row("(?x)[^ ^a]", "b", true),
            row("[\\[^a]", "^", true),
            row("[\\[^a]", "b", false),
            // outside a class it is an anchor, after a `\[` too
            row("\\[^a", "[a", false),
            row("\\[^a", "[^a", false),
            row("(?x)\\[ ^a", "[^a", false),
            // a `-` that is not a range operator, after `\w`, `\s`, a folded
            // letter, a property, and a range
            row("[\\w-.]", "-", true),
            row("[\\w-.]", ".", true),
            row("[\\w-.]", "a", true),
            row("[\\w-.]", ",", false),
            row("[\\w-a]", "-", true),
            row("[\\w-a]", "a", true),
            row("[\\w-a]", "`", false),
            row("[\\s-.]", "-", true),
            row("[\\s-.]", ".", true),
            row("[\\s-.]", " ", true),
            row("[\\s-.]", ",", false),
            row("(?i)[a-[b]]", "-", true),
            row("(?i)[a-[b]]", "A", true),
            row("(?i)[a-[b]]", "B", true),
            row("(?i)[a-[b]]", "X", false),
            row("(?i)[a-[b]]", "[", false),
            row("[\\p{Lower}-z]", "-", true),
            row("[\\p{Lower}-z]", ".", false),
            row("[\\p{L}-z]", "-", true),
            row("[\\p{L}-z]", "\u{e9}", true),
            row("[\\p{L}-z]", ".", false),
            row("[\\w-\\d]", "-", true),
            row("[\\d-a]", "-", true),
            row("[\\d-a]", "a", true),
            row("[\\d-a]", "b", false),
            row("[a-b-c]", "-", true),
            row("[a-b-c]", "c", true),
            row("[a-b-c]", "d", false),
            row("(?i)[a-b-c]", "-", true),
            row("(?i)[a-b-c]", "C", true),
            row("(?i)[a-b-c]", "D", false),
            row("[a-c-e]", "-", true),
            row("[a-c-e]", "b", true),
            row("[a-c-e]", "d", false),
            row("[a-c-e]", "e", true),
            // first, last and negated
            row("[-a]", "-", true),
            row("[^-a]", "-", false),
            row("[^-a]", "b", true),
            row("[a-]", "-", true),
            // a `-` as either end of a range
            row("[+--]", ",", true),
            row("[+--]", "-", true),
            row("[+--]", ".", false),
            row("(?i)[+--]", "-", true),
            row("[--/]", ".", true),
            row("[--/]", "0", false),
            row("[!--]", ",", true),
            row("[!--]", ".", false),
            // outside a class it is a plain character
            row("a-b", "a-b", true),
            row("(?i)a-b", "A-B", true),
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
            row("(?x)[+- ]]", ",", true),
            row("(?x)[+- ]]", "a", false),
            row("(?x)[+- ]]", "]", true),
            row("(?x)[+- ]]", "-", true),
            row("(?x)[+- [b]]", "X]", true),
            row("(?x)[+- [b]]", "b]", true),
            row("(?x)[+- [b]]", "c]", false),
            row("(?x)[+- [b]]", "-]", true),
            row("(?x)[+-#c\n]]", "a", false),
            row("(?x)[+-#c\n]]", ",", true),
            row("(?x)[+-#c\n]]", "]", true),
            row("(?x)[a-b- ]", "-", true),
            row("(?x)[a-b- ]", "b", true),
            row("(?x)[a-b- ]", "c", false),
            row("(?x)[a - c]", "b", true),
            row("(?x)[a - c]", "-", false),
            row("(?x)[a - c]", " ", false),
            row("(?x)[a-\n b]", "a", true),
            row("(?x)[a-\n b]", "b", true),
            row("(?x)[a-\n b]", "-", false),
            row("(?x)[a-#c\nb]", "b", true),
            row("(?x)[a-#c\nb]", "-", false),
            row("(?x)[a-]", "-", true),
            row("(?x)[a-[b]]", "-", true),
            row("(?x)[a-[b]]", "b", true),
            row("(?x)[a-[b]]", "c", false),
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
            row("[)]", ")", true),
            row("\\)", ")", true),
            row("\\Q)\\E", ")", true),
            row("(?x)a #c)\n", "a", true),
            row("(a)|(b)", "b", true),
            row("(?i)(a)|b", "B", true),
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
            replace_row("(a)-(b)", "a-b", "$1_$2", "a_b"),
            replace_row("(a)", "a", "$1suffix", "asuffix"),
            // Digits after `$n` join the group number only while it names a group.
            replace_row("(a)", "a", "$10", "a0"),
            replace_row("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)", "abcdefghij", "$10", "j"),
            replace_row("(a)", "a", "$0!", "a!"),
            replace_row("(?<who>a)", "a", "${who}-$1", "a-a"),
            // A backslash makes the next character literal.
            replace_row("(a)", "a", "\\$1", "$1"),
            replace_row("a", "aa", "b", "bb"),
            // A group that took no part in the match adds nothing.
            replace_row("(a)|(b)", "b", "[$1]", "[]"),
            replace_row("x", "abc", "$9", "abc"),
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
