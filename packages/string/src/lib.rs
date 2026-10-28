// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! String manipulation streams, ported from `@datastream/string`.
//!
//! Sizes (chunk sizes, buffer limits) are in bytes; `string_length_stream`
//! counts chars.

use async_stream::try_stream;
use datastream_core::{
    create_pass_through_stream, create_transform_stream, noop, noop_flush, DataStream, Result,
    StreamExt, StreamResult,
};
use std::fmt;
use std::sync::Arc;

use regex::Regex;
use serde_json::json;

const MAX_BUFFER_SIZE: usize = 16_777_216; // 16MB

fn fail(message: String) -> Result<()> {
    Err(message.into())
}

#[derive(Default, Clone, Debug)]
pub struct StringLengthOptions {
    pub result_key: Option<String>,
}

/// Sum the length of every chunk. Result key defaults to "length".
pub fn string_length_stream<T: AsRef<str> + Send + 'static>(
    input: DataStream<T>,
    options: StringLengthOptions,
) -> (DataStream<T>, StreamResult) {
    let result = StreamResult::new(
        options.result_key.unwrap_or_else(|| "length".into()),
        json!(0),
    );
    let r = result.clone();
    let mut length: u64 = 0;
    let stream = create_pass_through_stream(
        input,
        move |chunk| {
            let chunk: &str = chunk.as_ref();
            length += chunk.chars().count() as u64;
            r.set(json!(length));
            Ok(())
        },
        noop,
    );
    (stream, result)
}

#[derive(Default, Clone, Debug)]
pub struct StringCountOptions {
    pub substr: String,
    pub result_key: Option<String>,
}

/// Count (overlapping) occurrences of `substr`, including across chunk
/// boundaries. Result key defaults to "stringCount".
pub fn string_count_stream<T: AsRef<str> + Send + 'static>(
    input: DataStream<T>,
    options: StringCountOptions,
) -> Result<(DataStream<T>, StreamResult)> {
    let StringCountOptions { substr, result_key } = options;
    let Some(first) = substr.chars().next() else {
        return Err("stringCountStream requires a non-empty substr".into());
    };
    let step = first.len_utf8();
    let result = StreamResult::new(result_key.unwrap_or_else(|| "stringCount".into()), json!(0));
    let r = result.clone();
    let mut count: u64 = 0;
    let mut carry = String::new();
    let stream = create_pass_through_stream(
        input,
        move |chunk| {
            let chunk: &str = chunk.as_ref();
            let combined = std::mem::take(&mut carry) + chunk;
            let mut start = 0;
            while let Some(i) = combined[start..].find(substr.as_str()) {
                count += 1;
                start += i + step;
            }
            // Keep just enough of the tail to complete a match in the next chunk.
            let mut cut = combined.len().saturating_sub(substr.len() - 1);
            while !combined.is_char_boundary(cut) {
                cut += 1;
            }
            carry = combined[cut..].to_string();
            r.set(json!(count));
            Ok(())
        },
        noop,
    );
    Ok((stream, result))
}

#[derive(Default, Clone, Debug)]
pub struct StringMinimumChunkSizeOptions {
    /// Defaults to 1024 bytes.
    pub chunk_size: Option<usize>,
}

/// Buffer until the first chunk is at least `chunk_size`, then pass through.
pub fn string_minimum_first_chunk_size_stream(
    mut input: DataStream<String>,
    options: StringMinimumChunkSizeOptions,
) -> DataStream<String> {
    let chunk_size = options.chunk_size.unwrap_or(1024);
    Box::pin(try_stream! {
        let mut buffer = String::new();
        while buffer.len() < chunk_size {
            let Some(chunk) = input.next().await else { break };
            let chunk = chunk?;
            buffer.push_str(&chunk);
        }
        if !buffer.is_empty() {
            yield buffer;
        }
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            yield chunk;
        }
    })
}

/// Buffer every output chunk until it is at least `chunk_size`.
pub fn string_minimum_chunk_size_stream(
    mut input: DataStream<String>,
    options: StringMinimumChunkSizeOptions,
) -> DataStream<String> {
    let chunk_size = options.chunk_size.unwrap_or(1024);
    Box::pin(try_stream! {
        let mut buffer = String::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            buffer.push_str(&chunk);
            if buffer.len() >= chunk_size {
                yield std::mem::take(&mut buffer);
            }
        }
        if !buffer.is_empty() {
            yield buffer;
        }
    })
}

/// Drop chunks equal to the previous chunk.
pub fn string_skip_consecutive_duplicates_stream(input: DataStream<String>) -> DataStream<String> {
    let mut previous: Option<String> = None;
    create_transform_stream(
        input,
        move |chunk, enqueue| {
            if previous.as_ref() != Some(&chunk) {
                previous = Some(chunk.clone());
                enqueue.push(chunk);
            }
            Ok(())
        },
        noop_flush,
    )
}

/// What [`string_replace_stream`] looks for. Every match is replaced.
#[derive(Clone, Debug)]
pub enum Pattern {
    String(String),
    Regex(Regex),
}

impl Default for Pattern {
    fn default() -> Self {
        Self::String(String::new())
    }
}

impl From<&str> for Pattern {
    fn from(pattern: &str) -> Self {
        Self::String(pattern.into())
    }
}

impl From<String> for Pattern {
    fn from(pattern: String) -> Self {
        Self::String(pattern)
    }
}

impl From<Regex> for Pattern {
    fn from(pattern: Regex) -> Self {
        Self::Regex(pattern)
    }
}

/// A match passed to a [`Replacement::Function`], like the arguments of a JS
/// replacer function.
#[derive(Debug)]
pub struct ReplaceMatch<'a> {
    /// The whole match, then each capture group (`None` if it didn't take part).
    pub groups: Vec<Option<&'a str>>,
    /// Named capture groups.
    pub named: Vec<(&'a str, Option<&'a str>)>,
    /// Stream (byte) offset of the match.
    pub position: usize,
    /// The text the match was found in: the held-back input plus up to
    /// `lookbehind` bytes before it (the whole stream for `` $` `` / `$'`).
    pub input: &'a str,
    /// Byte offset of the match in `input`.
    pub index: usize,
}

pub type ReplaceFn = Arc<dyn Fn(&ReplaceMatch<'_>) -> String + Send + Sync>;

/// What [`string_replace_stream`] replaces each match with.
#[derive(Clone)]
pub enum Replacement {
    /// A `String.prototype.replace` template: `$$`, `$&`, `` $` ``, `$'`,
    /// `$n` / `$nn` and `$<name>`. `` $` `` and `$'` need the whole stream,
    /// so such a template buffers everything and replaces at the end.
    Template(String),
    Function(ReplaceFn),
}

impl Default for Replacement {
    fn default() -> Self {
        Self::Template(String::new())
    }
}

impl fmt::Debug for Replacement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Template(template) => f.debug_tuple("Template").field(template).finish(),
            Self::Function(_) => f.write_str("Function(..)"),
        }
    }
}

impl From<&str> for Replacement {
    fn from(template: &str) -> Self {
        Self::Template(template.into())
    }
}

impl From<String> for Replacement {
    fn from(template: String) -> Self {
        Self::Template(template)
    }
}

impl<F: Fn(&ReplaceMatch<'_>) -> String + Send + Sync + 'static> From<F> for Replacement {
    fn from(function: F) -> Self {
        Self::Function(Arc::new(function))
    }
}

#[derive(Default, Clone, Debug)]
pub struct StringReplaceOptions {
    pub pattern: Pattern,
    pub replacement: Replacement,
    /// Longest possible regex match, in bytes. Unknown by default, in which
    /// case the latest chunk is held back instead (so a match must not span
    /// more than two chunks). Ignored for a string pattern.
    pub max_match_length: Option<usize>,
    /// Bytes of already-emitted text kept so `^`, `$` and `\b` see what
    /// preceded the held-back input. Defaults to 16.
    pub lookbehind: Option<usize>,
    /// Defaults to 16MB; `usize::MAX` for unlimited.
    pub max_buffer_size: Option<usize>,
}

/// Largest char boundary of `text` at or before `index`.
fn floor_char_boundary(text: &str, index: usize) -> usize {
    let mut index = index.min(text.len());
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// `String.prototype.replace`'s template expansion, per match.
fn expand(template: &str, m: &ReplaceMatch<'_>, has_named: bool, output: &mut String) {
    let group = |i: usize| m.groups.get(i).copied().flatten().unwrap_or_default();
    let whole = group(0);
    let mut rest = template;
    while let Some(i) = rest.find('$') {
        output.push_str(&rest[..i]);
        let after = &rest[i + 1..];
        let bytes = after.as_bytes();
        rest = match bytes.first() {
            Some(b'$') => {
                output.push('$');
                &after[1..]
            }
            Some(b'&') => {
                output.push_str(whole);
                &after[1..]
            }
            Some(b'`') => {
                output.push_str(&m.input[..m.index]);
                &after[1..]
            }
            Some(b'\'') => {
                output.push_str(&m.input[m.index + whole.len()..]);
                &after[1..]
            }
            Some(b'0'..=b'9') => {
                let len = if bytes.get(1).is_some_and(u8::is_ascii_digit) {
                    2
                } else {
                    1
                };
                let digits = &after[..len];
                let index: usize = digits.parse().expect("ascii digits");
                let first = usize::from(bytes[0] - b'0');
                if index > 0 && index < m.groups.len() {
                    output.push_str(group(index));
                } else if first > 0 && first < m.groups.len() {
                    // "$nn" that isn't a group falls back to "$n" and a literal digit
                    output.push_str(group(first));
                    output.push_str(&digits[1..]);
                } else {
                    output.push('$');
                    output.push_str(digits);
                }
                &after[len..]
            }
            Some(b'<') if after.contains('>') => {
                let end = after.find('>').expect("checked above");
                let name = &after[1..end];
                if has_named {
                    let value = m.named.iter().find(|(n, _)| *n == name);
                    output.push_str(value.and_then(|(_, v)| *v).unwrap_or_default());
                } else {
                    // Without named groups "$<" is literal and the rest still expands
                    output.push_str("$<");
                    expand(&format!("{name}>"), m, has_named, output);
                }
                &after[end + 1..]
            }
            _ => {
                output.push('$');
                after
            }
        };
    }
    output.push_str(rest);
}

/// Raw (not yet replaced) input and the state to scan it as one string.
struct Replacer {
    pattern: Pattern,
    replacement: Replacement,
    has_named: bool,
    lookbehind: usize,
    buffer: String,
    /// Tail of already-emitted raw text, so `^`, `$` and `\b` see what
    /// preceded the buffer instead of treating the buffer start as stream start.
    context: String,
    /// Stream position of `buffer[0]`, for function replacement offsets.
    offset: usize,
}

impl Replacer {
    /// The next match at or after `from` in `text`.
    fn find<'t>(&self, text: &'t str, from: usize) -> Option<(usize, Vec<Option<&'t str>>)> {
        if from > text.len() {
            return None;
        }
        match &self.pattern {
            Pattern::String(p) => {
                let start = from + text[from..].find(p.as_str())?;
                Some((start, vec![Some(&text[start..start + p.len()])]))
            }
            Pattern::Regex(r) => {
                let caps = r.captures_at(text, from)?;
                let start = caps.get(0).expect("group 0 is the whole match").start();
                Some((start, caps.iter().map(|g| g.map(|g| g.as_str())).collect()))
            }
        }
    }

    fn substitute(&self, groups: Vec<Option<&str>>, index: usize, text: &str, output: &mut String) {
        let named = match &self.pattern {
            Pattern::Regex(r) if self.has_named => r
                .capture_names()
                .zip(&groups)
                .filter_map(|(name, value)| Some((name?, *value)))
                .collect(),
            _ => Vec::new(),
        };
        let m = ReplaceMatch {
            groups,
            named,
            position: self.offset + index - (text.len() - self.buffer.len()),
            input: text,
            index,
        };
        match &self.replacement {
            Replacement::Template(template) => expand(template, &m, self.has_named, output),
            Replacement::Function(function) => output.push_str(&function(&m)),
        }
    }

    // Why: replacing each emitted slice on its own let the regex see neither
    // side of the slice (`^`/`$`/`\b`, greedy matches split at the buffer end).
    // Instead run one pass over context + buffer, emitting matches that start
    // before `safe` and don't end at `hold_end` (they could still grow); the
    // scan always resumes where emitting stopped.
    fn scan(&mut self, safe: usize, hold_end: usize) -> String {
        let text = format!("{}{}", self.context, self.buffer);
        let base = self.context.len();
        let mut output = String::new();
        let mut cut = 0;
        let mut search = base;
        loop {
            let start = search - base;
            let found = self.find(&text, search);
            let (index, len) = match &found {
                Some((at, groups)) => (at - base, groups[0].unwrap_or_default().len()),
                None => (usize::MAX, 0),
            };
            if index >= safe || index.saturating_add(len) >= hold_end {
                let emit_to = start.max(index.min(safe)).min(self.buffer.len());
                output.push_str(&self.buffer[cut..emit_to]);
                let end = base + emit_to;
                let from = floor_char_boundary(&text, end.saturating_sub(self.lookbehind));
                self.context = text[from..end].to_string();
                self.buffer.drain(..emit_to);
                self.offset += emit_to;
                return output;
            }
            let (at, groups) = found.expect("index < safe means a match");
            output.push_str(&self.buffer[cut..index]);
            self.substitute(groups, at, &text, &mut output);
            cut = index + len;
            search = at + len;
            if len == 0 {
                // Step past an empty match by one char, as a JS unicode RegExp does.
                search += text[search..].chars().next().map_or(1, char::len_utf8);
            }
        }
    }
}

/// Replace every `pattern` match with `replacement`, as if the whole stream
/// were one string, including matches that span chunks. Input is held back
/// until no match can still span it: `pattern.len() - 1` bytes for a string,
/// `max_match_length - 1` bytes or else the latest chunk for a regex. Empty
/// output chunks are not emitted.
pub fn string_replace_stream(
    mut input: DataStream<String>,
    options: StringReplaceOptions,
) -> Result<DataStream<String>> {
    let StringReplaceOptions {
        pattern,
        replacement,
        max_match_length,
        lookbehind,
        max_buffer_size,
    } = options;
    if matches!(&pattern, Pattern::String(p) if p.is_empty()) {
        return Err("stringReplaceStream requires a non-empty pattern".into());
    }
    let max = max_buffer_size.unwrap_or(MAX_BUFFER_SIZE);
    // Longest possible match; unknown for a regex unless given, in which case
    // the latest chunk is held back instead.
    let hold = match &pattern {
        Pattern::String(p) => Some(p.len()),
        Pattern::Regex(_) => max_match_length,
    };
    // $` and $' need the whole stream, so such a template is applied at flush.
    let whole_stream =
        matches!(&replacement, Replacement::Template(t) if t.contains("$`") || t.contains("$'"));
    let has_named = matches!(&pattern, Pattern::Regex(r) if r.capture_names().any(|n| n.is_some()));
    let mut replacer = Replacer {
        pattern,
        replacement,
        has_named,
        lookbehind: lookbehind.unwrap_or(16),
        buffer: String::new(),
        context: String::new(),
        offset: 0,
    };
    Ok(Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            replacer.buffer.push_str(&chunk);
            let length = replacer.buffer.len();
            if length > max {
                fail(format!(
                    "stringReplaceStream buffer ({length}) exceeds maxBufferSize ({max})"
                ))?;
            }
            if whole_stream {
                continue;
            }
            let safe = match hold {
                Some(hold) => (length + 1).saturating_sub(hold),
                None => length - chunk.len(),
            };
            let output = replacer.scan(floor_char_boundary(&replacer.buffer, safe), length);
            if !output.is_empty() {
                yield output;
            }
        }
        // With $` or $' nothing was scanned yet, so the text is the whole stream.
        let output = replacer.scan(usize::MAX, usize::MAX);
        if !output.is_empty() {
            yield output;
        }
    }))
}

#[derive(Default, Clone, Debug)]
pub struct StringSplitOptions {
    pub separator: String,
    /// Defaults to 16MB.
    pub max_buffer_size: Option<usize>,
}

/// Split the text on `separator`, across chunk boundaries.
pub fn string_split_stream(
    mut input: DataStream<String>,
    options: StringSplitOptions,
) -> Result<DataStream<String>> {
    let StringSplitOptions {
        separator,
        max_buffer_size,
    } = options;
    if separator.is_empty() {
        return Err("stringSplitStream requires a non-empty separator".into());
    }
    let max = max_buffer_size.unwrap_or(MAX_BUFFER_SIZE);
    Ok(Box::pin(try_stream! {
        let mut previous = String::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            previous.push_str(&chunk);
            let mut pos = 0;
            while let Some(i) = previous[pos..].find(separator.as_str()) {
                yield previous[pos..pos + i].to_string();
                pos += i + separator.len();
            }
            previous.drain(..pos);
            if previous.len() > max {
                fail(format!(
                    "stringSplitStream buffer ({}) exceeds maxBufferSize ({max}), separator not found",
                    previous.len()
                ))?;
            }
        }
        yield previous;
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline, stream_to_array};

    fn from(input: &[&str]) -> DataStream<String> {
        create_readable_stream(input.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    async fn collect(stream: DataStream<String>) -> Vec<String> {
        stream_to_array(stream, None).await.unwrap()
    }

    async fn count(input: &[&str], substr: &str) -> serde_json::Value {
        let options = StringCountOptions {
            substr: substr.into(),
            ..Default::default()
        };
        let (stream, result) = string_count_stream(from(input), options).unwrap();
        let output = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(result.key(), "stringCount");
        output["stringCount"].clone()
    }

    fn replace(
        pattern: impl Into<Pattern>,
        replacement: impl Into<Replacement>,
        max: Option<usize>,
    ) -> StringReplaceOptions {
        StringReplaceOptions {
            pattern: pattern.into(),
            replacement: replacement.into(),
            max_buffer_size: max,
            ..Default::default()
        }
    }

    async fn replace_chunks(input: &[&str], options: StringReplaceOptions) -> Vec<String> {
        collect(string_replace_stream(from(input), options).unwrap()).await
    }

    fn regex(pattern: &str) -> Regex {
        Regex::new(pattern).unwrap()
    }

    fn split(separator: &str, max: Option<usize>) -> StringSplitOptions {
        StringSplitOptions {
            separator: separator.into(),
            max_buffer_size: max,
        }
    }

    fn min(chunk_size: usize) -> StringMinimumChunkSizeOptions {
        StringMinimumChunkSizeOptions {
            chunk_size: Some(chunk_size),
        }
    }

    #[tokio::test]
    async fn length_sums_chunks() {
        let (stream, length) = string_length_stream(from(&["1", "2", "3"]), Default::default());
        let output = pipeline(stream, &[&length]).await.unwrap();
        assert_eq!(length.key(), "length");
        assert_eq!(output["length"], json!(3));

        let options = StringLengthOptions {
            result_key: Some("string".into()),
        };
        let (stream, length) = string_length_stream(create_readable_stream(["1", "é"]), options);
        let output = pipeline(stream, &[&length]).await.unwrap();
        assert_eq!(output["string"], json!(2));
    }

    #[tokio::test]
    async fn skip_consecutive_duplicates() {
        let output = collect(string_skip_consecutive_duplicates_stream(from(&[
            "1", "2", "2", "3", "1",
        ])))
        .await;
        assert_eq!(output, ["1", "2", "3", "1"]);
    }

    #[tokio::test]
    async fn split_into_empty_strings() {
        let output =
            collect(string_split_stream(from(&[",,", ",,"]), split(",", None)).unwrap()).await;
        assert_eq!(output, ["", "", "", "", ""]);
    }

    #[tokio::test]
    async fn split_across_chunks() {
        let output =
            collect(string_split_stream(from(&["a,b", "c,d"]), split(",", None)).unwrap()).await;
        assert_eq!(output, ["a", "bc", "d"]);
        let output =
            collect(string_split_stream(from(&["a<>b<", ">c"]), split("<>", None)).unwrap()).await;
        assert_eq!(output, ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn split_buffer_limit() {
        let output =
            collect(string_split_stream(from(&["aaaa"]), split("zzz", Some(4))).unwrap()).await;
        assert_eq!(output, ["aaaa"]);
        let stream = string_split_stream(from(&["aaaaa"]), split("zzz", Some(4))).unwrap();
        let e = pipeline(stream, &[]).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "stringSplitStream buffer (5) exceeds maxBufferSize (4), separator not found"
        );
        let stream = string_split_stream(
            from(&["aaaaaa", "bbbbbb", "cccccc"]),
            split("zzz", Some(10)),
        )
        .unwrap();
        assert!(pipeline(stream, &[])
            .await
            .unwrap_err()
            .to_string()
            .contains("maxBufferSize"));
    }

    #[test]
    fn split_requires_separator() {
        let e = string_split_stream(from(&[]), split("", None))
            .err()
            .unwrap();
        assert_eq!(
            e.to_string(),
            "stringSplitStream requires a non-empty separator"
        );
    }

    #[tokio::test]
    async fn count_occurrences() {
        assert_eq!(
            count(&["hello world", "hello universe"], "hello").await,
            json!(2)
        );
        assert_eq!(count(&["aaa aaa aaa"], "a").await, json!(9));
        assert_eq!(count(&["xab"], "ab").await, json!(1));
        assert_eq!(count(&["aaa"], "aa").await, json!(2));
    }

    #[tokio::test]
    async fn count_across_chunks() {
        assert_eq!(count(&["hel", "lo"], "hello").await, json!(1));
        assert_eq!(count(&["a", "b", "ab"], "ab").await, json!(2));
        assert_eq!(count(&["aab", "c"], "ab").await, json!(1));
        assert_eq!(count(&["abc", "abc"], "abc").await, json!(2));
        assert_eq!(count(&["a", "a", "a"], "a").await, json!(3));
        assert_eq!(count(&["xé", "éx"], "éé").await, json!(1));
        assert_eq!(count(&["a", "b", "c"], "a").await, json!(1));
        assert_eq!(count(&["xa", "bx", "ab"], "ab").await, json!(2));
    }

    #[tokio::test]
    async fn count_custom_key() {
        let options = StringCountOptions {
            substr: "test".into(),
            result_key: Some("matches".into()),
        };
        let (stream, result) = string_count_stream(from(&["test test"]), options).unwrap();
        let output = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(result.key(), "matches");
        assert_eq!(output["matches"], json!(2));
    }

    #[test]
    fn count_requires_substr() {
        let e = string_count_stream(from(&[]), Default::default())
            .err()
            .unwrap();
        assert_eq!(
            e.to_string(),
            "stringCountStream requires a non-empty substr"
        );
    }

    #[tokio::test]
    async fn replace_across_chunks() {
        let output = replace_chunks(
            &["hello world", "hello universe"],
            replace(regex("hello"), "hi", None),
        )
        .await;
        assert_eq!(output.concat(), "hi worldhi universe");
        let output =
            replace_chunks(&["hel", "lo world"], replace(regex("hello"), "hi", None)).await;
        assert_eq!(output.concat(), "hi world");
    }

    #[tokio::test]
    async fn replace_all_occurrences() {
        let output = replace_chunks(&["hello world"], replace("hello", "hi", None)).await;
        // Text is released as soon as no match can still span it.
        assert_eq!(output, ["hi w", "orld"]);
        let output = replace_chunks(&["aaa"], replace("a", "b", None)).await;
        assert_eq!(output, ["bb", "b"]);
        let output = replace_chunks(&["aaa"], replace(regex("a"), "b", None)).await;
        assert_eq!(output, ["bbb"]);
        let output = replace_chunks(&["hello"], replace(regex("(l+)"), "[$1]", None)).await;
        assert_eq!(output, ["he[ll]o"]);
    }

    #[tokio::test]
    async fn replace_matches_spanning_many_chunks() {
        let output = replace_chunks(&["b", "é", "é"], replace("béé", "x", None)).await;
        assert_eq!(output.concat(), "x");
        let output = replace_chunks(&["xhe", "l", "l", "ox"], replace("hello", "hi", None)).await;
        assert_eq!(output.concat(), "xhix");
    }

    #[tokio::test]
    async fn replace_does_not_rescan_replacements() {
        // The replacement contains the pattern; it must not be replaced again.
        let output = replace_chunks(&["ab", ""], replace("a", "aé", None)).await;
        assert_eq!(output.concat(), "aéb");
        let output = replace_chunks(&["a", "b"], replace("a", "aa", None)).await;
        assert_eq!(output.concat(), "aab");
        let output = replace_chunks(&["a", "b"], replace(regex("a"), "aa", None)).await;
        assert_eq!(output.concat(), "aab");
        let output = replace_chunks(&["a", "a", ""], replace(regex("(a)"), "$1$1", None)).await;
        assert_eq!(output.concat(), "aaaa");
    }

    #[tokio::test]
    async fn replace_keeps_char_boundaries() {
        // A match spanning the held-back chunk is released whole, never split.
        let output = replace_chunks(&["x", "a"], replace("xa", "é", None)).await;
        assert_eq!(output, ["é"]);
    }

    #[tokio::test]
    async fn replace_zero_length_matches() {
        let output = replace_chunks(&["ab", "c"], replace(regex(r"\b"), "-", None)).await;
        assert_eq!(output.concat(), "-abc-");
        // Empty matches advance by a whole char.
        let input = ["\u{1F600}x", "\u{FFFF}y"];
        let output = replace_chunks(&input, replace(regex(""), "-", None)).await;
        assert_eq!(output.concat(), "-\u{1F600}-x-\u{FFFF}-y-");
    }

    #[test]
    fn replace_requires_pattern() {
        let e = string_replace_stream(from(&[]), replace("", "x", None))
            .err()
            .unwrap();
        assert_eq!(
            e.to_string(),
            "stringReplaceStream requires a non-empty pattern"
        );
    }

    #[tokio::test]
    async fn replace_sees_context_around_the_emitted_slice() {
        // ^ and $ only match at the start / end of the stream.
        let output = replace_chunks(&["x", "y"], replace(regex("^"), ">", None)).await;
        assert_eq!(output.concat(), ">xy");
        let output = replace_chunks(&["x", "y"], replace(regex("$"), "<", None)).await;
        assert_eq!(output.concat(), "xy<");
        // \b sees the char before the buffer.
        let output = replace_chunks(&["ab", "cd"], replace(regex(r"\bc"), "X", None)).await;
        assert_eq!(output.concat(), "abcd");
        // ... unless lookbehind keeps none of it.
        let options = StringReplaceOptions {
            lookbehind: Some(0),
            ..replace(regex(r"\bc"), "X", None)
        };
        assert_eq!(
            replace_chunks(&["ab", "cd"], options).await.concat(),
            "abXd"
        );
    }

    #[tokio::test]
    async fn replace_holds_matches_that_could_grow() {
        let output = replace_chunks(&["xa", "a", "a"], replace(regex("a+"), "X", None)).await;
        assert_eq!(output, ["x", "X"]);
        let output = replace_chunks(&["a", "a"], replace(regex("a+"), "X", None)).await;
        assert_eq!(output, ["X"]);
        let output = replace_chunks(&["x", "abc", "d"], replace(regex("abcd|ab"), "X", None)).await;
        assert_eq!(output.concat(), "xX");
        let output = replace_chunks(&["xab", "c"], replace(regex("abc|b"), "X", None)).await;
        assert_eq!(output.concat(), "xX");
        let output = replace_chunks(&["aba", "b"], replace(regex("ab"), "X", None)).await;
        assert_eq!(output, ["X", "X"]);
        let output = replace_chunks(&["ab", "cd", "ef", "a"], replace(regex("a"), "X", None)).await;
        assert_eq!(output.concat(), "XbcdefX");
    }

    #[tokio::test]
    async fn replace_whole_stream_templates() {
        // $' and $` need the rest of the stream, so they are applied at the end.
        let output = replace_chunks(&["ab", "cd"], replace(regex("b"), "[$']", None)).await;
        assert_eq!(output, ["a[cd]cd"]);
        let output = replace_chunks(&["ab", "cd"], replace("b", "[$']", None)).await;
        assert_eq!(output, ["a[cd]cd"]);
        let output = replace_chunks(&["ab", "cd"], replace(regex("c"), "[$`]", None)).await;
        assert_eq!(output, ["ab[ab]d"]);
        // A bare quote or backtick still streams.
        let output = replace_chunks(&["ab", "cd"], replace(regex("b"), "'`", None)).await;
        assert_eq!(output, ["a'`", "cd"]);
        let output = replace_chunks(&["ab", "cd"], replace(regex("(b)"), "[$$$&$1]", None)).await;
        assert_eq!(output, ["a[$bb]", "cd"]);
        // A function's output is never a template.
        let function = |_: &ReplaceMatch<'_>| "$'".to_string();
        let output = replace_chunks(&["ab", "cd"], replace(regex("b"), function, None)).await;
        assert_eq!(output, ["a$'", "cd"]);
    }

    #[tokio::test]
    async fn replace_expands_tokens_like_js() {
        // Expected values from String.prototype.replace on "zabcdefghijza".
        let cases = [
            (
                Pattern::from(regex("(a)(b)?")),
                "[$$|$&|$1|$01|$2|$0|$00|$3|$05|$20|$12|$<x>|$<$&>|$<]",
                "z[$|ab|a|a|b|$0|$00|$3|$05|b0|a2|$<x>|$<ab>|$<]cdefghijz[$|a|a|a||$0|$00|$3|$05|0|a2|$<x>|$<a>|$<]",
            ),
            (
                Pattern::from(regex("(?<x>a)(?<y>b)?")),
                "[$<x>|$<y>|$<z>|$<>|$<x]",
                "z[a|b|||$<x]cdefghijz[a||||$<x]",
            ),
            (
                Pattern::from(regex("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)?")),
                "[$10|$11|$1x]",
                "z[j||ax]za",
            ),
            (Pattern::from("a"), "[$1|$&|$<x>]", "z[$1|a|$<x>]bcdefghijz[$1|a|$<x>]"),
        ];
        for (pattern, template, expected) in cases {
            let output =
                replace_chunks(&["zabcdefghijz", "a"], replace(pattern, template, None)).await;
            assert_eq!(output.concat(), expected, "{template}");
        }
    }

    #[tokio::test]
    async fn replace_function_gets_stream_offsets() {
        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = calls.clone();
        let function = move |m: &ReplaceMatch<'_>| {
            let groups: Vec<_> = m.groups.iter().map(|g| g.map(String::from)).collect();
            seen.lock()
                .unwrap()
                .push((groups, m.position, m.input.to_string(), m.index));
            "X".to_string()
        };
        let output = replace_chunks(&["ab", "cab"], replace(regex("a(b)"), function, None)).await;
        assert_eq!(output.concat(), "XcX");
        let ab = || vec![Some("ab".to_string()), Some("b".to_string())];
        assert_eq!(
            *calls.lock().unwrap(),
            [
                (ab(), 0, "abcab".to_string(), 0),
                (ab(), 3, "abcab".to_string(), 3)
            ]
        );
        // Named groups are passed too.
        let function = |m: &ReplaceMatch<'_>| format!("{:?}", m.named);
        let output = replace_chunks(&["ab"], replace(regex("(?<x>a)(b)"), function, None)).await;
        assert_eq!(output, [r#"[("x", Some("a"))]"#]);
    }

    #[tokio::test]
    async fn replace_emits_safe_text_before_the_end() {
        let output = replace_chunks(&["ab", "cd"], replace(regex("x"), "X", None)).await;
        assert_eq!(output, ["ab", "cd"]);
        let output = replace_chunks(&["xa", "b"], replace(regex("a"), "c", None)).await;
        assert_eq!(output, ["xc", "b"]);
        // Empty output chunks are dropped.
        let output = replace_chunks(&["a", "b"], replace(regex("a"), "", None)).await;
        assert_eq!(output, ["b"]);
        assert!(replace_chunks(&["a"], replace(regex("a"), "", None))
            .await
            .is_empty());
        let output =
            replace_chunks(&["aaaa", "bbbb", "cccc"], replace(regex("a"), "X", Some(8))).await;
        assert_eq!(output, ["XXXX", "bbbb", "cccc"]);
    }

    #[tokio::test]
    async fn replace_max_match_length() {
        // A regex's longest match is unknown, so by default only the latest
        // chunk is held back and longer matches are missed.
        let output = replace_chunks(&["a", "b", "c"], replace(regex("abc"), "X", None)).await;
        assert_eq!(output.concat(), "abc");
        let options = StringReplaceOptions {
            max_match_length: Some(3),
            ..replace(regex("abc"), "X", None)
        };
        assert_eq!(
            replace_chunks(&["a", "b", "c"], options).await.concat(),
            "X"
        );
        let options = StringReplaceOptions {
            max_match_length: Some(2),
            ..replace(regex("x"), "X", None)
        };
        assert_eq!(
            replace_chunks(&["abcd", "ef"], options).await,
            ["abc", "de", "f"]
        );
    }

    #[tokio::test]
    async fn replace_buffer_limit() {
        // A string pattern only holds back `pattern.len() - 1` bytes.
        let output = replace_chunks(&["aaaa", "a"], replace("zzz", "y", Some(4))).await;
        assert_eq!(output, ["aa", "a", "aa"]);
        let output = replace_chunks(&["aaaa"], replace("zzz", "yyy", Some(4))).await;
        assert_eq!(output.concat(), "aaaa");
        // The limit applies to the buffer before anything is released.
        for pattern in [Pattern::from("zzz"), Pattern::from(regex("zzz"))] {
            let stream =
                string_replace_stream(from(&["aaaaa"]), replace(pattern, "yyy", Some(4))).unwrap();
            let e = pipeline(stream, &[]).await.unwrap_err();
            assert_eq!(
                e.to_string(),
                "stringReplaceStream buffer (5) exceeds maxBufferSize (4)"
            );
        }
        let stream = string_replace_stream(
            from(&["aaaa", "bbbb", "cccc"]),
            replace("zzz", "yyy", Some(3)),
        )
        .unwrap();
        assert!(pipeline(stream, &[])
            .await
            .unwrap_err()
            .to_string()
            .contains("maxBufferSize"));
    }

    #[tokio::test]
    async fn minimum_first_chunk_size() {
        assert_eq!(
            collect(string_minimum_first_chunk_size_stream(
                from(&["ab", "cd", "ef"]),
                min(4)
            ))
            .await,
            ["abcd", "ef"]
        );
        assert_eq!(
            collect(string_minimum_first_chunk_size_stream(
                from(&["ab"]),
                min(100)
            ))
            .await,
            ["ab"]
        );
        assert_eq!(
            collect(string_minimum_first_chunk_size_stream(
                from(&["abcdef", "gh", "ij"]),
                min(4)
            ))
            .await,
            ["abcdef", "gh", "ij"]
        );
        assert_eq!(
            collect(string_minimum_first_chunk_size_stream(
                from(&["abcd"]),
                min(4)
            ))
            .await,
            ["abcd"]
        );
        assert!(
            collect(string_minimum_first_chunk_size_stream(from(&[""]), min(4)))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn minimum_chunk_size() {
        assert_eq!(
            collect(string_minimum_chunk_size_stream(
                from(&["ab", "cd", "ef"]),
                min(4)
            ))
            .await,
            ["abcd", "ef"]
        );
        assert_eq!(
            collect(string_minimum_chunk_size_stream(from(&["ab"]), min(100))).await,
            ["ab"]
        );
        assert_eq!(
            collect(string_minimum_chunk_size_stream(
                from(&["abcdef", "gh", "ij"]),
                min(4)
            ))
            .await,
            ["abcdef", "ghij"]
        );
        assert!(collect(string_minimum_chunk_size_stream(
            from(&[""]),
            Default::default()
        ))
        .await
        .is_empty());
    }
}
