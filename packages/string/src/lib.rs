// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! String manipulation streams, ported from `@datastream/string`.
//!
//! Sizes (chunk sizes, buffer limits) are in bytes; `string_length_stream`
//! counts chars.

use async_stream::try_stream;
use datastream_core::{
    create_pass_through_stream, create_readable_stream_from_string, create_transform_stream, noop,
    noop_flush, DataStream, Result, StreamExt, StreamResult,
};
use regex::Regex;
use serde_json::json;

const MAX_BUFFER_SIZE: usize = 16_777_216; // 16MB

fn fail(message: String) -> Result<()> {
    Err(message.into())
}

/// Readable that emits `input` in 16KB chunks.
pub fn string_readable_stream(input: impl Into<String>) -> DataStream<String> {
    create_readable_stream_from_string(input, None).expect("default chunk size is positive")
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
/// boundaries. Result key defaults to "count".
pub fn string_count_stream<T: AsRef<str> + Send + 'static>(
    input: DataStream<T>,
    options: StringCountOptions,
) -> Result<(DataStream<T>, StreamResult)> {
    let StringCountOptions { substr, result_key } = options;
    let Some(first) = substr.chars().next() else {
        return Err("stringCountStream requires a non-empty substr".into());
    };
    let step = first.len_utf8();
    let result = StreamResult::new(result_key.unwrap_or_else(|| "count".into()), json!(0));
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
pub fn string_minimum_first_chunk_size(
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
pub fn string_minimum_chunk_size(
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
pub fn string_skip_consecutive_duplicates(input: DataStream<String>) -> DataStream<String> {
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
    /// `replacement` may use `$1` / `${name}` capture references.
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

#[derive(Default, Clone, Debug)]
pub struct StringReplaceOptions {
    pub pattern: Pattern,
    pub replacement: String,
    /// Defaults to 16MB.
    pub max_buffer_size: Option<usize>,
}

/// Replace `pattern` with `replacement`, including matches that span chunks.
/// Input is held back until no match can still span it: `pattern.len() - 1`
/// bytes for a string, the latest chunk for a regex (so a regex match must not
/// span more than two chunks).
pub fn string_replace_stream(
    mut input: DataStream<String>,
    options: StringReplaceOptions,
) -> DataStream<String> {
    let StringReplaceOptions {
        pattern,
        replacement,
        max_buffer_size,
    } = options;
    let max = max_buffer_size.unwrap_or(MAX_BUFFER_SIZE);
    // A string match is at most `pattern.len()` bytes, so holding back one byte
    // less catches matches spanning any number of chunks. A regex's longest
    // match is unknown, so the latest chunk is held back instead.
    let hold = match &pattern {
        Pattern::String(p) => Some(p.len().saturating_sub(1)),
        Pattern::Regex(_) => None,
    };
    // Replace the matches that start before `safe` in `text`, returning the
    // replaced output and how much of `text` it consumed (at least `safe`, or
    // further when the last match runs past it).
    let replace_before = move |text: &str, safe: usize| -> (String, usize) {
        let mut output = String::new();
        let mut last = 0;
        match &pattern {
            Pattern::String(p) => {
                for (start, matched) in text.match_indices(p.as_str()) {
                    if start >= safe {
                        break;
                    }
                    output.push_str(&text[last..start]);
                    output.push_str(&replacement);
                    last = start + matched.len();
                }
            }
            Pattern::Regex(r) => {
                for caps in r.captures_iter(text) {
                    let whole = caps.get(0).expect("group 0 is the whole match");
                    if whole.start() >= safe {
                        break;
                    }
                    output.push_str(&text[last..whole.start()]);
                    caps.expand(&replacement, &mut output);
                    last = whole.end();
                }
            }
        }
        let cut = safe.max(last);
        output.push_str(&text[last..cut]);
        (output, cut)
    };
    Box::pin(try_stream! {
        // Raw, not yet replaced input: never scan replaced output again.
        let mut previous = String::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let held = previous.len();
            let combined = std::mem::take(&mut previous) + &chunk;
            let safe = match hold {
                Some(hold) => {
                    let mut safe = combined.len().saturating_sub(hold);
                    while !combined.is_char_boundary(safe) {
                        safe -= 1;
                    }
                    safe
                }
                None => held,
            };
            let (output, cut) = replace_before(&combined, safe);
            previous = combined[cut..].to_string();
            yield output;
            if previous.len() > max {
                fail(format!(
                    "stringReplaceStream buffer ({}) exceeds maxBufferSize ({max})",
                    previous.len()
                ))?;
            }
        }
        yield replace_before(&previous, previous.len()).0;
    })
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
        pipeline(stream, &[&result]).await.unwrap();
        result.get()
    }

    fn replace(
        pattern: impl Into<Pattern>,
        replacement: &str,
        max: Option<usize>,
    ) -> StringReplaceOptions {
        StringReplaceOptions {
            pattern: pattern.into(),
            replacement: replacement.into(),
            max_buffer_size: max,
        }
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
    async fn readable_reads_input() {
        assert_eq!(collect(string_readable_stream("abc")).await, ["abc"]);
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
        let output = collect(string_skip_consecutive_duplicates(from(&[
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
        let regex = Regex::new("hello").unwrap();
        let output = collect(string_replace_stream(
            from(&["hello world", "hello universe"]),
            replace(regex.clone(), "hi", None),
        ))
        .await;
        assert_eq!(output, ["", "hi world", "hi universe"]);
        let output = collect(string_replace_stream(
            from(&["hel", "lo world"]),
            replace(regex, "hi", None),
        ))
        .await;
        assert_eq!(output, ["", "hi", " world"]);
    }

    #[tokio::test]
    async fn replace_all_occurrences() {
        let output = collect(string_replace_stream(
            from(&["hello world"]),
            replace("hello", "hi", None),
        ))
        .await;
        // Text is released as soon as no match can still span it.
        assert_eq!(output, ["hi w", "orld"]);
        let output = collect(string_replace_stream(
            from(&["aaa"]),
            replace("a", "b", None),
        ))
        .await;
        assert_eq!(output, ["bbb", ""]);
        let regex = Regex::new("a").unwrap();
        let output = collect(string_replace_stream(
            from(&["aaa"]),
            replace(regex, "b", None),
        ))
        .await;
        assert_eq!(output, ["", "bbb"]);
        let regex = Regex::new("(l+)").unwrap();
        let output = collect(string_replace_stream(
            from(&["hello"]),
            replace(regex, "[$1]", None),
        ))
        .await;
        assert_eq!(output, ["", "he[ll]o"]);
    }

    #[tokio::test]
    async fn replace_matches_spanning_many_chunks() {
        let output = collect(string_replace_stream(
            from(&["b", "é", "é"]),
            replace("béé", "x", None),
        ))
        .await;
        assert_eq!(output.concat(), "x");
    }

    #[tokio::test]
    async fn replace_does_not_rescan_replacements() {
        // The replacement contains the pattern; it must not be replaced again.
        let output = collect(string_replace_stream(
            from(&["ab", ""]),
            replace("a", "aé", None),
        ))
        .await;
        assert_eq!(output.concat(), "aéb");
        let output = collect(string_replace_stream(
            from(&["a", "a", ""]),
            replace(Regex::new("(a)").unwrap(), "$1$1", None),
        ))
        .await;
        assert_eq!(output.concat(), "aaaa");
    }

    #[tokio::test]
    async fn replace_keeps_char_boundaries() {
        // A match spanning the held-back chunk is released whole, never split.
        let output = collect(string_replace_stream(
            from(&["x", "a"]),
            replace("xa", "é", None),
        ))
        .await;
        assert_eq!(output, ["", "é", ""]);
    }

    #[tokio::test]
    async fn replace_buffer_limit() {
        // A regex's longest match is unknown, so a whole chunk is held back.
        let zzz = || Regex::new("zzz").unwrap();
        let output = collect(string_replace_stream(
            from(&["aaaa"]),
            replace(zzz(), "yyy", Some(4)),
        ))
        .await;
        assert_eq!(output, ["", "aaaa"]);
        let e = pipeline(
            string_replace_stream(from(&["aaaaa"]), replace(zzz(), "yyy", Some(4))),
            &[],
        )
        .await
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            "stringReplaceStream buffer (5) exceeds maxBufferSize (4)"
        );
        // A string pattern only holds back `pattern.len() - 1` bytes.
        let output = collect(string_replace_stream(
            from(&["aaaaa"]),
            replace("zzz", "yyy", Some(4)),
        ))
        .await;
        assert_eq!(output.concat(), "aaaaa");
    }

    #[tokio::test]
    async fn minimum_first_chunk_size() {
        assert_eq!(
            collect(string_minimum_first_chunk_size(
                from(&["ab", "cd", "ef"]),
                min(4)
            ))
            .await,
            ["abcd", "ef"]
        );
        assert_eq!(
            collect(string_minimum_first_chunk_size(from(&["ab"]), min(100))).await,
            ["ab"]
        );
        assert_eq!(
            collect(string_minimum_first_chunk_size(
                from(&["abcdef", "gh", "ij"]),
                min(4)
            ))
            .await,
            ["abcdef", "gh", "ij"]
        );
        assert_eq!(
            collect(string_minimum_first_chunk_size(from(&["abcd"]), min(4))).await,
            ["abcd"]
        );
        assert!(
            collect(string_minimum_first_chunk_size(from(&[""]), min(4)))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn minimum_chunk_size() {
        assert_eq!(
            collect(string_minimum_chunk_size(from(&["ab", "cd", "ef"]), min(4))).await,
            ["abcd", "ef"]
        );
        assert_eq!(
            collect(string_minimum_chunk_size(from(&["ab"]), min(100))).await,
            ["ab"]
        );
        assert_eq!(
            collect(string_minimum_chunk_size(
                from(&["abcdef", "gh", "ij"]),
                min(4)
            ))
            .await,
            ["abcdef", "ghij"]
        );
        assert!(
            collect(string_minimum_chunk_size(from(&[""]), Default::default()))
                .await
                .is_empty()
        );
    }
}
