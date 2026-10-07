// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! JSON and NDJSON (JSON Lines) parsing and formatting streams, ported from
//! `@datastream/json`. Sizes are measured in bytes (JS counts UTF-16 units).

use async_stream::try_stream;
use datastream_core::{DataStream, Error, Result, StreamExt, StreamResult, Value};
use serde::Serialize;
use serde_json::json;

const DEFAULT_MAX_SIZE: usize = 16_777_216;

fn track_error(errors: &StreamResult, id: &str, message: &str, idx: usize) {
    errors.update(|value| {
        if let Some(map) = value.as_object_mut() {
            let entry = map
                .entry(id)
                .or_insert_with(|| json!({ "id": id, "message": message, "idx": [] }));
            if let Some(list) = entry["idx"].as_array_mut() {
                list.push(idx.into());
            }
        }
    });
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
    pub max_buffer_size: Option<usize>,
    pub result_key: Option<String>,
}

/// Parse newline-delimited JSON. Invalid lines are skipped and recorded in
/// the result (default key `jsonErrors`) as `{ParseError: {id, message, idx}}`.
pub fn ndjson_parse_stream(
    mut input: DataStream<String>,
    options: NdjsonParseOptions,
) -> (DataStream<Value>, StreamResult) {
    let max = options.max_buffer_size.unwrap_or(DEFAULT_MAX_SIZE);
    let key = options.result_key.unwrap_or_else(|| "jsonErrors".into());
    let errors = StreamResult::new(key, json!({}));
    let result = errors.clone();
    let stream = Box::pin(try_stream! {
        let mut buffer = String::new();
        let mut idx = 0;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            if buffer.len() + chunk.len() > max {
                Err::<(), Error>(
                    format!(
                        "ndjsonParseStream buffer ({}) exceeds maxBufferSize ({max})",
                        buffer.len() + chunk.len()
                    )
                    .into(),
                )?;
            }
            buffer.push_str(&chunk);
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
                    Err(_) => track_error(&errors, "ParseError", "Invalid JSON", idx),
                }
                idx += 1;
            }
            buffer.drain(..pos);
        }
        if !is_blank(&buffer) {
            match serde_json::from_str::<Value>(&buffer) {
                Ok(value) => {
                    yield value;
                }
                Err(_) => track_error(&errors, "ParseError", "Invalid JSON", idx),
            }
        }
    });
    (stream, result)
}

#[derive(Default, Clone, Debug)]
pub struct JsonFormatOptions {
    /// Indentation width, like `JSON.stringify`'s `space`.
    pub space: Option<usize>,
}

/// Format values as NDJSON, emitting batches of up to 64 lines.
pub fn ndjson_format_stream(
    mut input: DataStream<Value>,
    options: JsonFormatOptions,
) -> DataStream<String> {
    Box::pin(try_stream! {
        let mut batch = Vec::new();
        while let Some(chunk) = input.next().await {
            batch.push(stringify(&chunk?, options.space));
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
    pub max_buffer_size: Option<usize>,
    pub max_value_size: Option<usize>,
    pub result_key: Option<String>,
}

/// Incremental scanner that splits a top-level JSON array into elements.
struct JsonScanner {
    buffer: String,
    scan_pos: usize,
    depth: usize,
    in_string: bool,
    escaped: bool,
    started: bool,
    element_start: Option<usize>,
    idx: usize,
    max_value_size: usize,
    errors: StreamResult,
}

impl JsonScanner {
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
            Err(_) => track_error(&self.errors, "ParseError", "Invalid JSON", self.idx),
        }
        self.idx += 1;
        Ok(())
    }

    fn scan(&mut self, out: &mut Vec<Value>) -> Result<()> {
        while self.scan_pos < self.buffer.len() {
            let ch = self.buffer.as_bytes()[self.scan_pos];
            let pos = self.scan_pos;
            self.scan_pos += 1;
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
            if ch == b'"' {
                self.in_string = true;
                if self.started {
                    self.element_start.get_or_insert(pos);
                }
                continue;
            }
            if !self.started {
                self.started = ch == b'[';
                continue;
            }
            match ch {
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
                // Closing `]` of the top-level array, or a top-level separator.
                b']' | b'}' | b',' if self.depth == 0 => {
                    if let Some(start) = self.element_start.take() {
                        self.emit(start, pos, out)?;
                    }
                }
                b' ' | b'\t' | b'\n' | b'\r' => {}
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

/// Parse a top-level JSON array, emitting each element. Invalid elements are
/// recorded as `ParseError`; input without a `[` records `NoArrayStart`.
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
        element_start: None,
        idx: 0,
        max_value_size: options.max_value_size.unwrap_or(DEFAULT_MAX_SIZE),
        errors: errors.clone(),
    };
    let stream = Box::pin(try_stream! {
        let mut saw_non_whitespace = false;
        let mut out = Vec::new();
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let size = scanner.buffer.len() + chunk.len();
            if size > max_buffer_size {
                Err::<(), Error>(
                    format!("jsonParseStream buffer ({size}) exceeds maxBufferSize ({max_buffer_size})")
                        .into(),
                )?;
            }
            saw_non_whitespace |= !is_blank(&chunk);
            scanner.buffer.push_str(&chunk);
            scanner.scan(&mut out)?;
            for value in out.drain(..) {
                yield value;
            }
        }
        if !is_blank(&scanner.buffer) {
            let end = scanner.buffer.len();
            scanner.emit(0, end, &mut out)?;
            for value in out.drain(..) {
                yield value;
            }
        }
        if !scanner.started && saw_non_whitespace {
            track_error(
                &scanner.errors,
                "NoArrayStart",
                "Input did not contain a top-level array",
                scanner.idx,
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
            json!({"ParseError": {"id": "ParseError", "message": "Invalid JSON", "idx": [1]}})
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
        assert_eq!(errors.key(), "jsonErrors");
    }

    // *** ndjsonFormatStream *** //
    #[tokio::test]
    async fn ndjson_format() {
        let out = format(vec![json!({"a": 1}), json!({"b": 2})], None, true).await;
        assert_eq!(out.concat(), "{\"a\":1}\n{\"b\":2}\n");
        assert!(format(vec![], None, true).await.is_empty());
        assert_eq!(
            format(vec![json!({"a": 1})], Some(2), true).await.concat(),
            "{\n  \"a\": 1\n}\n"
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
            json!({"ParseError": {"id": "ParseError", "message": "Invalid JSON", "idx": [1]}})
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
                "idx": [0]
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
        let input = format!("[{{\"a\":\"{}\"}}]", "x".repeat(200));
        let e = parse_err(&input, max_buffer(100)).await;
        assert_eq!(
            e,
            "jsonParseStream buffer (210) exceeds maxBufferSize (100)"
        );
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
