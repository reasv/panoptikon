//! Indexable prefix matching.
//!
//! `col LIKE ? || '%'` is the obvious way to write a prefix match and the
//! wrong one. SQLite applies the LIKE optimisation only when the pattern is a
//! string literal or a *bare* bound parameter, and only when the column's
//! collation agrees with LIKE's case-sensitivity. A concatenation expression
//! against a BINARY column satisfies neither, so the predicate is invisible to
//! the planner and every prefix lookup becomes a table scan.
//!
//! An explicit half-open range says the same thing in a form the planner can
//! seek. It also drops LIKE's two accidental behaviours: `%` and `_` in the
//! bound value stop acting as wildcards, and matching stops folding case —
//! both of which make distinct stored values collide.

/// Exclusive upper bound for the range of strings starting with `prefix`, or
/// `None` when the prefix has no upper bound and callers should emit `>= ?`
/// alone.
///
/// `None` means *unbounded above*, never *no match*: it occurs only when the
/// prefix is empty or consists entirely of `char::MAX`, and in both cases
/// every string at or above the lower bound starts with the prefix.
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    // Stepping the final character bounds the prefix: anything continuing past
    // `prefix` sorts below the stepped value, anything else at or above it.
    // A character already at `char::MAX` cannot be stepped, so it carries.
    while let Some(last) = chars.pop() {
        let stepped = match char::from_u32(last as u32 + 1) {
            Some(next) => Some(next),
            // The only gap in the scalar range is the surrogate block; the
            // next valid scalar after it is U+E000.
            None if (last as u32) < char::MAX as u32 => char::from_u32(0xE000),
            None => None,
        };
        if let Some(next) = stepped {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// Escapes `%`, `_` and the escape character itself so a value can be used as
/// a literal inside a LIKE pattern. The pattern must be paired with
/// `ESCAPE '\'` in the SQL.
///
/// For prefix matches prefer [`prefix_upper_bound`]; this exists for the cases
/// where LIKE's case-folding is load-bearing and cannot become a binary range.
pub(crate) fn escape_like_literal(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            escaped.push('\\');
        }
        escaped.push(ch);
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    // Ensures the bound covers exactly the strings starting with the prefix,
    // including longer strings that extend it.
    #[test]
    fn upper_bound_covers_the_prefix_and_its_extensions() {
        // Stepping the final 'r' to 's' — note this still contains the longer
        // "characters", which sorts below it at the stepped position.
        let upper = prefix_upper_bound("character").unwrap();
        assert_eq!(upper, "charactes");
        for inside in [
            "character",
            "characters",
            "character_name",
            "characterZ",
            "character\u{10FFFF}",
        ] {
            assert!(
                inside >= "character" && inside < upper.as_str(),
                "{inside} should be inside the range"
            );
        }
        for outside in ["charactes", "charactz", "chb", "general", "charact"] {
            assert!(
                !(outside >= "character" && outside < upper.as_str()),
                "{outside} should be outside the range"
            );
        }
    }

    // Ensures stepping carries past characters that cannot be incremented, and
    // that an absent bound only ever means "unbounded above".
    #[test]
    fn upper_bound_carries_and_reports_unbounded() {
        // '9' + 1 is ':', 'f' + 1 is 'g' — both stay ASCII.
        assert_eq!(prefix_upper_bound("a9").as_deref(), Some("a:"));
        assert_eq!(prefix_upper_bound("ff").as_deref(), Some("fg"));
        // The surrogate block is skipped rather than producing an invalid char.
        assert_eq!(prefix_upper_bound("\u{D7FF}").as_deref(), Some("\u{E000}"));
        // A trailing char::MAX carries into the previous character.
        assert_eq!(prefix_upper_bound("a\u{10FFFF}").as_deref(), Some("b"));
        // Nothing left to step: unbounded above, not "no match".
        assert_eq!(prefix_upper_bound(""), None);
        assert_eq!(prefix_upper_bound("\u{10FFFF}"), None);
    }

    // Ensures LIKE metacharacters survive as literals once escaped.
    #[test]
    fn like_escaping_neutralises_wildcards() {
        assert_eq!(escape_like_literal(r"D:\Photos_2024"), r"D:\\Photos\_2024");
        assert_eq!(escape_like_literal("100%"), r"100\%");
        assert_eq!(escape_like_literal("plain"), "plain");
    }
}
