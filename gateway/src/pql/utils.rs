use shell_words::split as shell_split;

pub(crate) fn parse_and_escape_query(user_input: &str) -> String {
    let mut working = user_input.replace("\\\"", "\"\"");
    if working.matches('"').count() % 2 != 0 {
        working.push('"');
    }
    working = working.replace('\'', "\\'");
    working = working.replace("\"\"", "\\\"");

    let tokens = shell_split(&working).unwrap_or_else(|_| {
        working
            .split_whitespace()
            .map(|token| token.to_string())
            .collect()
    });
    let escaped_tokens = tokens
        .into_iter()
        .map(|token| token.replace('"', "\"\""));
    let quoted_tokens = escaped_tokens.map(|token| format!("\"{}\"", token));
    quoted_tokens.collect::<Vec<String>>().join(" ")
}
