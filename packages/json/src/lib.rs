// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! JSON and NDJSON (JSON Lines) parsing and formatting streams, ported from
//! `@datastream/json`. Sizes are measured in bytes (JS counts UTF-16 units).

use async_stream::try_stream;
use datastream_core::{DataStream, Error, Result, StreamExt, StreamResult, Value};
use serde::Serialize;
use serde_json::json;

const DEFAULT_MAX_SIZE: usize = 16_777_216;
const DEFAULT_MAX_ERROR_ROWS: usize = 1_000;

/// Error map entry per id (csv parity): `idx` keeps at most `max_error_rows`
/// row indexes so a stream of bad rows can't grow memory without bound, while
/// `count` records the true total.
fn track_error(errors: &StreamResult, id: &str, message: &str, idx: usize, max_error_rows: usize) {
    errors.update(|value| {
        if let Some(map) = value.as_object_mut() {
            let entry = map
                .entry(id)
                .or_insert_with(|| json!({ "id": id, "message": message, "idx": [], "count": 0 }));
            entry["count"] = (entry["count"].as_u64().unwrap_or(0) + 1).into();
            if let Some(list) = entry["idx"].as_array_mut() {
                if list.len() < max_error_rows {
                    list.push(idx.into());
                }
            }
        }
    });
}

/// Drops a leading byte-order mark from the first non-empty chunk only:
/// serde_json rejects U+FEFF, and a BOM before `[` would be NoArrayStart.
/// Empty chunks don't count as "first", or a leading "" would let the BOM
/// through. Later U+FEFF chars are data.
#[derive(Default)]
struct BomStripper {
    done: bool,
}

impl BomStripper {
    fn strip<'a>(&mut self, text: &'a str) -> &'a str {
        if self.done || text.is_empty() {
            return text;
        }
        self.done = true;
        text.strip_prefix('\u{FEFF}').unwrap_or(text)
    }
}

fn is_blank(s: &str) -> bool {
    s.trim().is_empty()
}

/// `JSON.stringify(value, null, space)`; `space` is capped at 10 like JS.
fn stringify(value: &Value, space: Option<usize>) -> String {
    match space {
        Some(n) if n > 0 => {
            let indent = " ".repeat(n.min(10));
            let mut buf = Vec::new();
            let formatter = serde_json::ser::PrettyFormatter::with_indent(indent.as_bytes());
            let mut ser = serde_json::Serializer::with_formatter(&mut buf, formatter);
            // Serializing a `Value` into memory cannot fail.
            value.serialize(&mut ser).expect("serialize JSON value");
            String::from_utf8(buf).expect("JSON is UTF-8")
        }
        _ => value.to_string(),
    }
}

// *** NDJSON *** //

#[derive(Default, Clone, Debug)]
pub struct NdjsonParseOptions {
    /// Largest incomplete trailing line in bytes (default 16MiB; `usize::MAX`
    /// for unlimited).
    pub max_buffer_size: Option<usize>,
    /// Row indexes kept per error id (default 1,000; `usize::MAX` for
    /// unlimited). `count` always records the true total.
    pub max_error_rows: Option<usize>,
    pub result_key: Option<String>,
}

/// Parse newline-delimited JSON. Invalid lines are skipped and recorded in
/// the result (default key `ndjsonErrors`) as
/// `{ParseError: {id, message, idx, count}}`. A leading BOM is stripped.
pub fn ndjson_parse_stream(
    mut input: DataStream<String>,
    options: NdjsonParseOptions,
) -> (DataStream<Value>, StreamResult) {
    let max = options.max_buffer_size.unwrap_or(DEFAULT_MAX_SIZE);
    let max_error_rows = options.max_error_rows.unwrap_or(DEFAULT_MAX_ERROR_ROWS);
    let key = options.result_key.unwrap_or_else(|| "ndjsonErrors".into());
    let errors = StreamResult::new(key, json!({}));
    let result = errors.clone();
    let stream = Box::pin(try_stream! {
        let mut buffer = String::new();
        let mut idx = 0;
        let mut bom = BomStripper::default();
        while let Some(chunk) = input.next().await {
            buffer.push_str(bom.strip(&chunk?));
            let mut pos = 0;
            while let Some(nl) = buffer[pos..].find('\n') {
                let line = &buffer[pos..pos + nl];
                pos += nl + 1;
                // serde_json tolerates surrounding whitespace (incl. a trailing \r).
                if is_blank(line) {
                    continue;
                }
                let parsed = serde_json::from_str::<Value>(line);
                match parsed {
                    Ok(value) => {
                        yield value;
                    }
                    Err(_) => track_error(&errors, "ParseError", "Invalid JSON", idx, max_error_rows),
                }
                idx += 1;
            }
            buffer.drain(..pos);
            // maxBufferSize bounds the incomplete trailing line only (csv
            // parity), so one large chunk of many short lines is accepted.
            if buffer.len() > max {
                Err::<(), Error>(
                    format!(
                        "ndjsonParseStream buffer ({}) exceeds maxBufferSize ({max})",
                        buffer.len()
                    )
                    .into(),
                )?;
            }
        }
        if !is_blank(&buffer) {
            match serde_json::from_str::<Value>(&buffer) {
                Ok(value) => {
                    yield value;
                }
                Err(_) => track_error(&errors, "ParseError", "Invalid JSON", idx, max_error_rows),
            }
        }
    });
    (stream, result)
}

#[derive(Default, Clone, Debug)]
pub struct JsonFormatOptions {
    /// Indentation width, like `JSON.stringify`'s `space`. Ignored by
    /// [`ndjson_format_stream`].
    pub space: Option<usize>,
}

/// Format values as NDJSON, emitting batches of up to 64 lines.
/// `space` is ignored: NDJSON is one record per line, and pretty-printing
/// would split a record across lines.
pub fn ndjson_format_stream(
    mut input: DataStream<Value>,
    _options: JsonFormatOptions,
) -> DataStream<String> {
    Box::pin(try_stream! {
        let mut batch = Vec::new();
        while let Some(chunk) = input.next().await {
            batch.push(stringify(&chunk?, None));
            if batch.len() >= 64 {
                yield format!("{}\n", batch.join("\n"));
                batch.clear();
            }
        }
        if !batch.is_empty() {
            yield format!("{}\n", batch.join("\n"));
        }
    })
}

// *** JSON Array *** //

#[derive(Default, Clone, Debug)]
pub struct JsonParseOptions {
    /// Largest pending (incomplete) element in bytes (default 16MiB;
    /// `usize::MAX` for unlimited).
    pub max_buffer_size: Option<usize>,
    /// Largest raw element text in bytes (default 16MiB; `usize::MAX` for
    /// unlimited).
    pub max_value_size: Option<usize>,
    /// Row indexes kept per error id (default 1,000; `usize::MAX` for
    /// unlimited). `count` always records the true total.
    pub max_error_rows: Option<usize>,
    pub result_key: Option<String>,
}

fn is_whitespace(ch: u8) -> bool {
    matches!(ch, b' ' | b'\t' | b'\n' | b'\r')
}

/// Incremental scanner that splits a top-level JSON array into elements.
struct JsonScanner {
    buffer: String,
    scan_pos: usize,
    depth: usize,
    in_string: bool,
    escaped: bool,
    started: bool,
    /// Non-whitespace preceded the top-level `[`: not a JSON array.
    rejected: bool,
    /// The top-level `]` was seen.
    ended: bool,
    /// Non-whitespace followed the top-level `]`.
    trailing: bool,
    element_start: Option<usize>,
    idx: usize,
    max_value_size: usize,
    max_error_rows: usize,
    errors: StreamResult,
}

impl JsonScanner {
    fn track_error(&self, id: &str, message: &str) {
        track_error(&self.errors, id, message, self.idx, self.max_error_rows);
    }

    fn emit(&mut self, start: usize, end: usize, out: &mut Vec<Value>) -> Result<()> {
        let text = &self.buffer[start..end];
        if text.len() > self.max_value_size {
            return Err(format!(
                "jsonParseStream value size ({}) exceeds maxValueSize ({})",
                text.len(),
                self.max_value_size
            )
            .into());
        }
        match serde_json::from_str(text) {
            Ok(value) => out.push(value),
            Err(_) => self.track_error("ParseError", "Invalid JSON"),
        }
        self.idx += 1;
        Ok(())
    }

    fn scan(&mut self, out: &mut Vec<Value>) -> Result<()> {
        while self.scan_pos < self.buffer.len() {
            let ch = self.buffer.as_bytes()[self.scan_pos];
            let pos = self.scan_pos;
            self.scan_pos += 1;
            // Only whitespace may precede the top-level `[`. Skipping ahead to
            // the first `[` anywhere would stream a nested array out of e.g.
            // {"a":[1,2]}; anything else means the input is not a JSON array,
            // so stop scanning for good.
            if !self.started {
                if ch == b'[' {
                    self.started = true;
                } else if !is_whitespace(ch) {
                    self.rejected = true;
                    // The buffer is never read again, so it is left untrimmed.
                    return Ok(());
                }
                continue;
            }
            // Only whitespace may follow the top-level `]`. Anything else (e.g.
            // `[1]2` or `[1][2]`) is recorded once and the rest is ignored.
            if self.ended {
                if !is_whitespace(ch) {
                    self.trailing = true;
                    self.track_error("TrailingData", "Unexpected content after top-level array");
                    // The buffer is never read again, so it is left untrimmed.
                    return Ok(());
                }
                continue;
            }
            if self.escaped {
                self.escaped = false;
                continue;
            }
            if self.in_string {
                match ch {
                    b'\\' => self.escaped = true,
                    b'"' => self.in_string = false,
                    _ => {}
                }
                continue;
            }
            match ch {
                b'"' => {
                    self.in_string = true;
                    self.element_start.get_or_insert(pos);
                }
                b'[' | b'{' => {
                    self.element_start.get_or_insert(pos);
                    self.depth += 1;
                }
                b']' | b'}' if self.depth > 0 => {
                    self.depth -= 1;
                    if self.depth == 0 {
                        if let Some(start) = self.element_start.take() {
                            self.emit(start, pos + 1, out)?;
                        }
                    }
                }
                // Closing `]` of the top-level array (a stray `}` ends it too).
                b']' | b'}' => {
                    if let Some(start) = self.element_start.take() {
                        self.emit(start, pos, out)?;
                    }
                    self.ended = true;
                }
                // Top-level separator.
                b',' if self.depth == 0 => {
                    if let Some(start) = self.element_start.take() {
                        self.emit(start, pos, out)?;
                    }
                }
                _ if is_whitespace(ch) => {}
                _ => {
                    self.element_start.get_or_insert(pos);
                }
            }
        }
        // Drop the processed prefix; an in-progress element moves to 0.
        let trim = self.element_start.unwrap_or(self.scan_pos);
        self.buffer.drain(..trim);
        self.scan_pos -= trim;
        if self.element_start.is_some() {
            self.element_start = Some(0);
        }
        Ok(())
    }
}

/// Parse a top-level JSON array, emitting each element. Only whitespace (and a
/// leading BOM) may precede the `[`. Errors are recorded in the result
/// (default key `jsonErrors`) as `{id, message, idx, count}`: invalid elements
/// as `ParseError`; input that does not start with `[` as `NoArrayStart` (the
/// rest is ignored); non-whitespace after the closing `]` as `TrailingData`
/// (the rest is ignored); an array that never closes as `UnexpectedEnd`.
pub fn json_parse_stream(
    mut input: DataStream<String>,
    options: JsonParseOptions,
) -> (DataStream<Value>, StreamResult) {
    let max_buffer_size = options.max_buffer_size.unwrap_or(DEFAULT_MAX_SIZE);
    let key = options.result_key.unwrap_or_else(|| "jsonErrors".into());
    let errors = StreamResult::new(key, json!({}));
    let mut scanner = JsonScanner {
        buffer: String::new(),
        scan_pos: 0,
        depth: 0,
        in_string: false,
        escaped: false,
        started: false,
        rejected: false,
        ended: false,
        trailing: false,
        element_start: None,
        idx: 0,
        max_value_size: options.max_value_size.unwrap_or(DEFAULT_MAX_SIZE),
        max_error_rows: options.max_error_rows.unwrap_or(DEFAULT_MAX_ERROR_ROWS),
        errors: errors.clone(),
    };
    let stream = Box::pin(try_stream! {
        let mut bom = BomStripper::default();
        let mut out = Vec::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            // Not a JSON array, or content after it: the rest is ignored.
            if scanner.rejected || scanner.trailing {
                continue;
            }
            scanner.buffer.push_str(bom.strip(&chunk));
            scanner.scan(&mut out)?;
            for value in out.drain(..) {
                yield value;
            }
            // A stopped scan leaves the buffer untrimmed, but it is never read
            // again.
            if scanner.rejected || scanner.trailing {
                continue;
            }
            // After scan() the buffer holds only the incomplete trailing element,
            // so maxBufferSize bounds that (csv parity), not the whole chunk.
            let size = scanner.buffer.len();
            if size > max_buffer_size {
                Err::<(), Error>(
                    format!("jsonParseStream buffer ({size}) exceeds maxBufferSize ({max_buffer_size})")
                        .into(),
                )?;
            }
        }
        if scanner.rejected {
            scanner.track_error("NoArrayStart", "Input did not contain a top-level array");
        }
        // Empty/whitespace-only input, a rejected one, or a complete array
        // (with or without trailing content) has nothing pending.
        if scanner.started && !scanner.ended {
            if !is_blank(&scanner.buffer) {
                let end = scanner.buffer.len();
                scanner.emit(0, end, &mut out)?;
                for value in out.drain(..) {
                    yield value;
                }
            }
            // The top-level array was opened but never closed (truncated input).
            scanner.track_error(
                "UnexpectedEnd",
                "Input ended before the top-level array closed",
            );
        }
    });
    (stream, errors)
}

/// Format values as a JSON array: `[a,\nb\n]`, or `[]` when empty.
pub fn json_format_stream(
    mut input: DataStream<Value>,
    options: JsonFormatOptions,
) -> DataStream<String> {
    Box::pin(try_stream! {
        let mut first = true;
        while let Some(chunk) = input.next().await {
            let json = stringify(&chunk?, options.space);
            yield if first { format!("[{json}") } else { format!(",\n{json}") };
            first = false;
        }
        yield if first { "[]".to_string() } else { "\n]".to_string() };
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline, stream_to_array};

    fn strings(chunks: &[&str]) -> DataStream<String> {
        create_readable_stream(chunks.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    async fn ndjson(chunks: &[&str]) -> (Vec<Value>, Value) {
        let (stream, errors) = ndjson_parse_stream(strings(chunks), Default::default());
        (stream_to_array(stream, None).await.unwrap(), errors.get())
    }

    async fn parse(chunks: &[&str]) -> (Vec<Value>, Value) {
        let (stream, errors) = json_parse_stream(strings(chunks), Default::default());
        (stream_to_array(stream, None).await.unwrap(), errors.get())
    }

    async fn parse_err(input: &str, options: JsonParseOptions) -> String {
        let (stream, _) = json_parse_stream(strings(&[input]), options);
        pipeline(stream, &[]).await.unwrap_err().to_string()
    }

    async fn format(input: Vec<Value>, space: Option<usize>, ndjson: bool) -> Vec<String> {
        let options = JsonFormatOptions { space };
        let readable = create_readable_stream(input);
        let stream = if ndjson {
            ndjson_format_stream(readable, options)
        } else {
            json_format_stream(readable, options)
        };
        stream_to_array(stream, None).await.unwrap()
    }

    // *** ndjsonParseStream *** //
    #[tokio::test]
    async fn ndjson_parses_lines() {
        assert_eq!(ndjson(&["{\"a\":1}\n"]).await.0, [json!({"a": 1})]);
        let (out, errors) = ndjson(&["{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n"]).await;
        assert_eq!(out, [json!({"a": 1}), json!({"b": 2}), json!({"c": 3})]);
        assert_eq!(errors, json!({}));
    }

    #[tokio::test]
    async fn ndjson_chunk_boundaries_blank_lines_and_line_endings() {
        let expected = [json!({"a": 1}), json!({"b": 2})];
        assert_eq!(
            ndjson(&["{\"a\":", "1}\n", "{\"b\":2}\n"]).await.0,
            expected
        );
        assert_eq!(ndjson(&["{\"a\":1}\n\n \n{\"b\":2}\n"]).await.0, expected);
        assert_eq!(ndjson(&["{\"a\":1}\n{\"b\":2}"]).await.0, expected);
        assert_eq!(ndjson(&["{\"a\":1}\r\n{\"b\":2}\r\n"]).await.0, expected);
        assert_eq!(ndjson(&["{\"a\":1}\r"]).await.0, [json!({"a": 1})]);
    }

    #[tokio::test]
    async fn ndjson_tracks_parse_errors() {
        let (out, errors) = ndjson(&["{\"a\":1}\nnot json\n{\"b\":2}\n"]).await;
        assert_eq!(out, [json!({"a": 1}), json!({"b": 2})]);
        assert_eq!(
            errors,
            json!({"ParseError": {"id": "ParseError", "message": "Invalid JSON", "idx": [1], "count": 1}})
        );
        assert_eq!(
            ndjson(&["bad1\nbad2\nbad3\n"]).await.1["ParseError"]["idx"],
            json!([0, 1, 2])
        );
        // Trailing line parsed in flush.
        let (out, errors) = ndjson(&["not-json"]).await;
        assert!(out.is_empty());
        assert_eq!(errors["ParseError"]["idx"], json!([0]));
        let (out, errors) = ndjson(&["{\"a\":1}\nbad"]).await;
        assert_eq!(out, [json!({"a": 1})]);
        assert_eq!(errors["ParseError"]["idx"], json!([1]));
    }

    #[tokio::test]
    async fn ndjson_max_buffer_size() {
        let run = |chunks: Vec<String>, max: usize| async move {
            let options = NdjsonParseOptions {
                max_buffer_size: Some(max),
                ..Default::default()
            };
            let (stream, _) = ndjson_parse_stream(create_readable_stream(chunks), options);
            pipeline(stream, &[]).await
        };
        let e = run(vec!["a".repeat(200)], 100)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            e,
            "ndjsonParseStream buffer (200) exceeds maxBufferSize (100)"
        );
        // Equal to the limit is fine (the invalid line only records an error).
        assert!(run(vec!["a".repeat(10)], 10).await.is_ok());
        assert!(run(vec!["a".repeat(10), "b".into()], 10).await.is_err());
        let e = run(vec!["a".repeat(10), "b".repeat(5)], 12)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(
            e,
            "ndjsonParseStream buffer (15) exceeds maxBufferSize (12)"
        );
        // Only the incomplete trailing line counts, so one large chunk of many
        // short lines is accepted.
        let options = NdjsonParseOptions {
            max_buffer_size: Some(10),
            ..Default::default()
        };
        let (stream, _) = ndjson_parse_stream(strings(&[&"{\"a\":1}\n".repeat(50)]), options);
        assert_eq!(stream_to_array(stream, None).await.unwrap().len(), 50);
        let input = format!("{}\n{}", "1".repeat(19), "2".repeat(11));
        let e = run(vec![input], 10).await.unwrap_err().to_string();
        assert_eq!(
            e,
            "ndjsonParseStream buffer (11) exceeds maxBufferSize (10)"
        );
    }

    #[tokio::test]
    async fn ndjson_result_key() {
        let options = NdjsonParseOptions {
            result_key: Some("myErrors".into()),
            ..Default::default()
        };
        let (_, errors) = ndjson_parse_stream(strings(&[]), options);
        assert_eq!(errors.key(), "myErrors");
        let (_, errors) = ndjson_parse_stream(strings(&[]), Default::default());
        assert_eq!(errors.key(), "ndjsonErrors");
    }

    // *** ndjsonFormatStream *** //
    #[tokio::test]
    async fn ndjson_format() {
        let out = format(vec![json!({"a": 1}), json!({"b": 2})], None, true).await;
        assert_eq!(out.concat(), "{\"a\":1}\n{\"b\":2}\n");
        assert!(format(vec![], None, true).await.is_empty());
        // `space` is ignored: one record per line.
        assert_eq!(
            format(vec![json!({"a": 1}), json!({"b": [2]})], Some(2), true)
                .await
                .concat(),
            "{\"a\":1}\n{\"b\":[2]}\n"
        );
    }

    #[tokio::test]
    async fn ndjson_format_batches_64() {
        let items = |n: usize| (0..n).map(|i| json!({ "i": i })).collect::<Vec<_>>();
        let out = format(items(64), None, true).await;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trim().split('\n').count(), 64);
        let out = format(items(65), None, true).await;
        assert_eq!(out.len(), 2);
        assert_eq!(out[1], "{\"i\":64}\n");
    }

    #[tokio::test]
    async fn ndjson_round_trip() {
        let input = vec![
            json!({"a": 1}),
            json!({"b": "hello"}),
            json!({"c": [1, 2, 3]}),
        ];
        let formatted =
            ndjson_format_stream(create_readable_stream(input.clone()), Default::default());
        let (stream, _) = ndjson_parse_stream(formatted, Default::default());
        assert_eq!(stream_to_array(stream, None).await.unwrap(), input);
    }

    // *** jsonParseStream *** //
    #[tokio::test]
    async fn json_parses_arrays() {
        let expected = [json!({"a": 1}), json!({"b": 2})];
        assert_eq!(parse(&["[{\"a\":1},{\"b\":2}]"]).await.0, expected);
        assert_eq!(parse(&["[ {\"a\":1} , \n {\"b\":2} \n]"]).await.0, expected);
        assert_eq!(
            parse(&["[{\"a\":{\"b\":1}},{\"c\":[1,2,3]}]"]).await.0,
            [json!({"a": {"b": 1}}), json!({"c": [1, 2, 3]})]
        );
        assert_eq!(
            parse(&["[{\"a\":{\"b\":{\"c\":1}}}]"]).await.0,
            [json!({"a": {"b": {"c": 1}}})]
        );
        assert_eq!(
            parse(&["[1, \"hello\", true, null, false]"]).await.0,
            [
                json!(1),
                json!("hello"),
                json!(true),
                Value::Null,
                json!(false)
            ]
        );
        assert!(parse(&["[]"]).await.0.is_empty());
        assert!(parse(&["[   ]"]).await.0.is_empty());
        assert_eq!(
            parse(&["[1,\t2,\t3]"]).await.0,
            [json!(1), json!(2), json!(3)]
        );
        assert_eq!(
            parse(&["[1,\r2,\r3]"]).await.0,
            [json!(1), json!(2), json!(3)]
        );
    }

    #[tokio::test]
    async fn json_strings_and_escapes() {
        assert_eq!(
            parse(&["[{\"a\":\"{not json}\"},{\"b\":\"[1,2]\"}]"])
                .await
                .0,
            [json!({"a": "{not json}"}), json!({"b": "[1,2]"})]
        );
        assert_eq!(
            parse(&[r#"[{"a":"hello \"world\""},{"b":"test"}]"#])
                .await
                .0,
            [json!({"a": "hello \"world\""}), json!({"b": "test"})]
        );
        assert_eq!(parse(&[r#"[{"a":"\\"}]"#]).await.0, [json!({"a": "\\"})]);
        assert_eq!(
            parse(&[r#"[{"a":"[{nested}]"}]"#]).await.0,
            [json!({"a": "[{nested}]"})]
        );
        assert_eq!(
            parse(&[r#"["hello","world"]"#]).await.0,
            [json!("hello"), json!("world")]
        );
        assert_eq!(parse(&[r#"["é😀",1]"#]).await.0, [json!("é😀"), json!(1)]);
    }

    #[tokio::test]
    async fn json_chunk_boundaries() {
        let expected = [json!({"a": 1}), json!({"b": 2})];
        assert_eq!(parse(&["[{\"a\":", "1},", "{\"b\":2}]"]).await.0, expected);
        assert_eq!(
            parse(&["[", "{\"a\":", "1}", ",", "{\"b\":", "2}", "]"])
                .await
                .0,
            expected
        );
        assert_eq!(
            parse(&["[{\"a\":1},", "{\"b\":2},", "{\"c\":3}]"]).await.0,
            [json!({"a": 1}), json!({"b": 2}), json!({"c": 3})]
        );
        assert_eq!(parse(&["[12", "34]"]).await.0, [json!(1234)]);
    }

    #[tokio::test]
    async fn json_tracks_errors() {
        let (out, errors) = parse(&["[{\"a\":1},{invalid},{\"b\":2}]"]).await;
        assert_eq!(out, [json!({"a": 1}), json!({"b": 2})]);
        assert_eq!(
            errors,
            json!({"ParseError": {"id": "ParseError", "message": "Invalid JSON", "idx": [1], "count": 1}})
        );
        assert_eq!(
            parse(&["[bad1,bad2,bad3]"]).await.1["ParseError"]["idx"],
            json!([0, 1, 2])
        );
        let (out, errors) = parse(&["[{\"a\":1},bad,{\"b\":2},worse,{\"c\":3}]"]).await;
        assert_eq!(out.len(), 3);
        assert_eq!(errors["ParseError"]["idx"], json!([1, 3]));
    }

    #[tokio::test]
    async fn json_no_array_start() {
        let (out, errors) = parse(&["{\"a\":1}"]).await;
        assert!(out.is_empty());
        assert_eq!(
            errors,
            json!({"NoArrayStart": {
                "id": "NoArrayStart",
                "message": "Input did not contain a top-level array",
                "idx": [0],
                "count": 1
            }})
        );
        assert_eq!(
            parse(&["abc"]).await.1["NoArrayStart"]["id"],
            "NoArrayStart"
        );
        assert_eq!(parse(&[""]).await.1, json!({}));
        assert_eq!(parse(&["   \n\t  "]).await.1, json!({}));
    }

    #[tokio::test]
    async fn json_limits() {
        let max_buffer = |n| JsonParseOptions {
            max_buffer_size: Some(n),
            ..Default::default()
        };
        let max_value = |n| JsonParseOptions {
            max_value_size: Some(n),
            ..Default::default()
        };
        // Unterminated element: it stays pending in the buffer.
        let input = format!("[{{\"a\":\"{}", "x".repeat(200));
        let e = parse_err(&input, max_buffer(100)).await;
        assert_eq!(
            e,
            "jsonParseStream buffer (206) exceeds maxBufferSize (100)"
        );
        // "[1234" leaves "1234" pending; "5678" grows it to 8 chars.
        let (stream, _) = json_parse_stream(strings(&["[1234", "5678"]), max_buffer(6));
        assert_eq!(
            pipeline(stream, &[]).await.unwrap_err().to_string(),
            "jsonParseStream buffer (8) exceeds maxBufferSize (6)"
        );
        // Exactly the limit is fine.
        let (stream, _) = json_parse_stream(strings(&["[12", "]"]), max_buffer(2));
        assert_eq!(stream_to_array(stream, None).await.unwrap(), [json!(12)]);
        let (stream, _) = json_parse_stream(strings(&["[12", "3"]), max_buffer(2));
        assert!(pipeline(stream, &[]).await.is_err());
        // Only the pending element counts, so one large chunk of many small
        // elements is accepted.
        let input = format!("[{}]", vec!["{\"a\":1}"; 50].join(","));
        let (stream, _) = json_parse_stream(strings(&[&input]), max_buffer(10));
        assert_eq!(stream_to_array(stream, None).await.unwrap().len(), 50);
        // Rejected or trailing input is not buffered.
        for (input, id) in [
            ("{\"a\":\"xxxxxxxx\"}", "NoArrayStart"),
            ("[1]{\"a\":\"xxxxxxxx\"}", "TrailingData"),
        ] {
            let (stream, errors) = json_parse_stream(strings(&[input]), max_buffer(2));
            pipeline(stream, &[]).await.unwrap();
            let errors = errors.get();
            let keys: Vec<&String> = errors.as_object().unwrap().keys().collect();
            assert_eq!(keys, [id], "{input}");
        }
        // Whitespace after the array is dropped, not accumulated.
        let spaces = " ".repeat(8);
        let (stream, errors) =
            json_parse_stream(strings(&["[1]", &spaces, &spaces]), max_buffer(10));
        assert_eq!(stream_to_array(stream, None).await.unwrap(), [json!(1)]);
        assert_eq!(errors.get(), json!({}));
        // Raw (untrimmed) element text counts toward maxValueSize.
        let e = parse_err("[  1111  ]", max_value(5)).await;
        assert_eq!(e, "jsonParseStream value size (6) exceeds maxValueSize (5)");
        assert!(parse_err("[ 123456 ]", max_value(5))
            .await
            .contains("maxValueSize"));
        let (stream, _) = json_parse_stream(strings(&["[12345]"]), max_value(5));
        assert_eq!(stream_to_array(stream, None).await.unwrap(), [json!(12345)]);
    }

    #[tokio::test]
    async fn json_trailing_data() {
        let (out, errors) = parse(&["[1,2]3,{\"x\":1}"]).await;
        assert_eq!(out, [json!(1), json!(2)]);
        assert_eq!(
            errors,
            json!({"TrailingData": {
                "id": "TrailingData",
                "message": "Unexpected content after top-level array",
                "idx": [2],
                "count": 1
            }})
        );
        // A second top-level array.
        let (out, errors) = parse(&["[1][2]"]).await;
        assert_eq!(out, [json!(1)]);
        assert_eq!(errors["TrailingData"]["idx"], json!([1]));
        assert!(errors.get("UnexpectedEnd").is_none());
        // Recorded once; later chunks are ignored.
        let (out, errors) = parse(&["[1] ", " 2", "[3]", "4"]).await;
        assert_eq!(out, [json!(1)]);
        assert_eq!(errors["TrailingData"]["idx"], json!([1]));
        // Whitespace is allowed.
        let (out, errors) = parse(&["[1]", " \t\r\n", "\n"]).await;
        assert_eq!(out, [json!(1)]);
        assert_eq!(errors, json!({}));
    }

    #[tokio::test]
    async fn json_unexpected_end() {
        let (out, errors) = parse(&["[1,2"]).await;
        assert_eq!(out, [json!(1), json!(2)]);
        assert_eq!(
            errors,
            json!({"UnexpectedEnd": {
                "id": "UnexpectedEnd",
                "message": "Input ended before the top-level array closed",
                "idx": [2],
                "count": 1
            }})
        );
        // No pending element.
        for input in ["[", "[1,", "[1, \n"] {
            let errors = parse(&[input]).await.1;
            let keys: Vec<&String> = errors.as_object().unwrap().keys().collect();
            assert_eq!(keys, ["UnexpectedEnd"], "{input:?}");
        }
        // Empty or complete input.
        for input in ["", "  ", "[]", "[1]", "[1] "] {
            assert_eq!(parse(&[input]).await.1, json!({}), "{input:?}");
        }
    }

    #[tokio::test]
    async fn json_rejects_non_array_start() {
        // Only whitespace may precede the top-level `[`: a top-level object's
        // nested array must not leak its elements.
        let (out, errors) = parse(&[" {\"a\":[1,2]}"]).await;
        assert!(out.is_empty());
        assert_eq!(
            errors,
            json!({"NoArrayStart": {
                "id": "NoArrayStart",
                "message": "Input did not contain a top-level array",
                "idx": [0],
                "count": 1
            }})
        );
        // A string before the `[` is rejected too.
        let (out, errors) = parse(&["\"x\"[1]"]).await;
        assert!(out.is_empty());
        assert_eq!(errors["NoArrayStart"]["count"], 1);
        // Once rejected, later chunks are ignored, not buffered: a `[` arriving
        // afterwards neither starts the array nor counts towards maxBufferSize.
        let options = JsonParseOptions {
            max_buffer_size: Some(2),
            ..Default::default()
        };
        let (stream, errors) = json_parse_stream(strings(&["x", "[1]"]), options);
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
        assert_eq!(errors.get()["NoArrayStart"]["idx"], json!([0]));
    }

    #[tokio::test]
    async fn bom_is_stripped_once() {
        // A leading BOM is stripped from the first non-empty chunk only.
        for chunks in [&["\u{FEFF}[1,2]"][..], &["", "\u{FEFF}[1,2]"]] {
            let (out, errors) = parse(chunks).await;
            assert_eq!(out, [json!(1), json!(2)], "{chunks:?}");
            assert_eq!(errors, json!({}));
        }
        for chunks in [&["\u{FEFF}1\n2\n"][..], &["", "\u{FEFF}1\n2\n"]] {
            let (out, errors) = ndjson(chunks).await;
            assert_eq!(out, [json!(1), json!(2)], "{chunks:?}");
            assert_eq!(errors, json!({}));
        }
        // A U+FEFF that starts a later chunk is data.
        assert_eq!(
            parse(&["[\"a", "\u{FEFF}b\"]"]).await.0,
            [json!("a\u{FEFF}b")]
        );
        assert_eq!(
            ndjson(&["\"a", "\u{FEFF}b\"\n"]).await.0,
            [json!("a\u{FEFF}b")]
        );
    }

    #[tokio::test]
    async fn max_error_rows() {
        type Run = fn(Vec<String>, Option<usize>) -> (DataStream<Value>, StreamResult);
        type Wrap = fn(&[&str]) -> String;
        let runs: [(Run, Wrap); 2] = [
            (
                |chunks, max| {
                    let options = NdjsonParseOptions {
                        max_error_rows: max,
                        ..Default::default()
                    };
                    ndjson_parse_stream(create_readable_stream(chunks), options)
                },
                |rows| format!("{}\n", rows.join("\n")),
            ),
            (
                |chunks, max| {
                    let options = JsonParseOptions {
                        max_error_rows: max,
                        ..Default::default()
                    };
                    json_parse_stream(create_readable_stream(chunks), options)
                },
                |rows| format!("[{}]", rows.join(",")),
            ),
        ];
        for (run, wrap) in runs {
            // idx is capped at maxErrorRows; count keeps the true total.
            let (stream, errors) = run(vec![wrap(&["x", "x", "x"])], Some(2));
            pipeline(stream, &[]).await.unwrap();
            assert_eq!(
                errors.get(),
                json!({"ParseError": {
                    "id": "ParseError",
                    "message": "Invalid JSON",
                    "idx": [0, 1],
                    "count": 3
                }})
            );
            // Defaults to 1000; usize::MAX is unlimited.
            let rows = vec!["x"; 1001];
            let (stream, errors) = run(vec![wrap(&rows)], None);
            pipeline(stream, &[]).await.unwrap();
            let error = &errors.get()["ParseError"];
            let idx = error["idx"].as_array().unwrap();
            assert_eq!((idx.len(), &idx[999]), (1000, &json!(999)));
            assert_eq!(error["count"], 1001);
            let (stream, errors) = run(vec![wrap(&rows)], Some(usize::MAX));
            pipeline(stream, &[]).await.unwrap();
            assert_eq!(
                errors.get()["ParseError"]["idx"].as_array().unwrap().len(),
                1001
            );
        }
    }

    #[tokio::test]
    async fn json_result_key() {
        let options = JsonParseOptions {
            result_key: Some("myErrors".into()),
            ..Default::default()
        };
        assert_eq!(json_parse_stream(strings(&[]), options).1.key(), "myErrors");
        assert_eq!(
            json_parse_stream(strings(&[]), Default::default()).1.key(),
            "jsonErrors"
        );
    }

    // *** jsonFormatStream *** //
    #[tokio::test]
    async fn json_format() {
        let out = format(vec![json!({"a": 1}), json!({"b": 2})], None, false).await;
        assert_eq!(out.concat(), "[{\"a\":1},\n{\"b\":2}\n]");
        assert_eq!(format(vec![], None, false).await.concat(), "[]");
        assert_eq!(
            format(vec![json!({"a": 1})], Some(2), false).await.concat(),
            "[{\n  \"a\": 1\n}\n]"
        );
    }

    #[tokio::test]
    async fn json_round_trip() {
        let input = vec![
            json!({"a": 1}),
            json!({"b": "hello"}),
            json!({"c": [1, 2, 3]}),
        ];
        let formatted =
            json_format_stream(create_readable_stream(input.clone()), Default::default());
        let (stream, _) = json_parse_stream(formatted, Default::default());
        assert_eq!(stream_to_array(stream, None).await.unwrap(), input);
    }
}
