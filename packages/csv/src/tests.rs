// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
// 3.14 is CSV test data, not an approximation of PI.
#![allow(clippy::approx_constant, clippy::type_complexity)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use super::*;
use datastream_core::{
    create_readable_stream, create_readable_stream_from_string, pipeline, stream_to_array,
    stream_to_string, DataStream, StreamResult, Value,
};
use serde_json::json;

fn text(input: &str) -> DataStream<String> {
    create_readable_stream_from_string(input, None).unwrap()
}

fn chunks(input: &[&str]) -> DataStream<String> {
    create_readable_stream(input.iter().map(|s| s.to_string()).collect::<Vec<_>>())
}

fn values(input: Value) -> DataStream<Value> {
    match input {
        Value::Array(items) => create_readable_stream(items),
        _ => panic!("expected array"),
    }
}

async fn collect(stream: DataStream<Value>) -> Value {
    Value::Array(stream_to_array(stream, None).await.unwrap())
}

async fn collect_strings(stream: DataStream<String>) -> Vec<String> {
    stream_to_array(stream, None).await.unwrap()
}

fn chars(delimiter: Option<&str>, newline: Option<&str>, escape: Option<&str>) -> CsvParseOptions {
    CsvParseOptions {
        delimiter_char: delimiter.map(LazyString::from),
        newline_char: newline.map(LazyString::from),
        escape_char: escape.map(LazyString::from),
        ..Default::default()
    }
}

async fn parse(input: DataStream<String>, options: CsvParseOptions) -> Value {
    collect(csv_parse_stream(input, options).0).await
}

async fn parse_errors(input: DataStream<String>, options: CsvParseOptions) -> Value {
    let (stream, result) = csv_parse_stream(input, options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    output[result.key()].clone()
}

async fn parse_error(input: DataStream<String>, options: CsvParseOptions) -> String {
    let (stream, _) = csv_parse_stream(input, options);
    stream_to_array(stream, None).await.unwrap_err().to_string()
}

fn quoted(input: &str, options: ParserOptions, flushing: bool) -> ParseResult {
    csv_quoted_parser(input, &options, flushing).unwrap()
}

fn unquoted(input: &str, options: ParserOptions, flushing: bool) -> ParseResult {
    csv_unquoted_parser(input, &options, flushing).unwrap()
}

fn nl(newline: &str) -> ParserOptions {
    ParserOptions {
        newline_char: newline.into(),
        ..Default::default()
    }
}

fn esc() -> ParserOptions {
    ParserOptions {
        escape_char: Some("\\".into()),
        ..Default::default()
    }
}

fn rows(result: &ParseResult) -> Value {
    json!(result.rows)
}

fn lazy(detect: &StreamResult, pointer: &str) -> Option<LazyString> {
    Some(LazyString::from_result(detect, pointer))
}

fn lazy_parse(detect: &StreamResult) -> CsvParseOptions {
    CsvParseOptions {
        delimiter_char: lazy(detect, "/delimiterChar"),
        newline_char: lazy(detect, "/newlineChar"),
        quote_char: lazy(detect, "/quoteChar"),
        ..Default::default()
    }
}

fn lazy_header(detect: &StreamResult) -> CsvDetectHeaderOptions {
    CsvDetectHeaderOptions {
        delimiter_char: lazy(detect, "/delimiterChar"),
        newline_char: lazy(detect, "/newlineChar"),
        quote_char: lazy(detect, "/quoteChar"),
        ..Default::default()
    }
}

fn header_keys(header: &StreamResult) -> Keys {
    let header = header.clone();
    Keys::lazy(move || serde_json::from_value(header.get()["header"].clone()).unwrap_or_default())
}

// *** csvParseStream *** //

#[tokio::test]
async fn parse_stream_cases() {
    let none = || chars(None, None, None);
    let cases: Vec<(DataStream<String>, CsvParseOptions, Value)> = vec![
        (
            text("1,2,3,4\r\n5,6,7,8\r\n"),
            none(),
            json!([["1", "2", "3", "4"], ["5", "6", "7", "8"]]),
        ),
        (
            text("1\t2\t3\r\n4\t5\t6\r\n"),
            chars(Some("\t"), None, None),
            json!([["1", "2", "3"], ["4", "5", "6"]]),
        ),
        (
            chunks(&["1,2,", "3,4\r\n5,6,", "7,8\r\n"]),
            none(),
            json!([["1", "2", "3", "4"], ["5", "6", "7", "8"]]),
        ),
        (
            text("\"hello, world\",42\r\n\"foo \"\"bar\"\"\",99\r\n"),
            none(),
            json!([["hello, world", "42"], ["foo \"bar\"", "99"]]),
        ),
        (text("1,2\r\n3,4"), none(), json!([["1", "2"], ["3", "4"]])),
        (
            text("\"line1\r\nline2\",val\r\n"),
            none(),
            json!([["line1\r\nline2", "val"]]),
        ),
        (text("\"a,b\",c\r\n"), none(), json!([["a,b", "c"]])),
        (text(""), none(), json!([])),
        (
            text("a\r\nb\r\nc\r\n"),
            none(),
            json!([["a"], ["b"], ["c"]]),
        ),
        (text("a\r\n\r\nb\r\n"), none(), json!([["a"], [""], ["b"]])),
        (
            chunks(&["\"hello,", " world\",42\r\n"]),
            none(),
            json!([["hello, world", "42"]]),
        ),
        (
            text("=1+2,+1,-1,@SUM\r\n"),
            none(),
            json!([["=1+2", "+1", "-1", "@SUM"]]),
        ),
        (
            text("\"he\"\"llo\",world\r\n"),
            none(),
            json!([["he\"llo", "world"]]),
        ),
        (text("a,\"b\""), none(), json!([["a", "b"]])),
        (text("\"a\"x,b\r\n"), none(), json!([["a", "x", "b"]])),
        (
            text("\"a\"x,b\r\n"),
            chars(None, None, Some("\\")),
            json!([["a", "x", "b"]]),
        ),
        (text("\"he\"\"llo"), none(), json!([["he\"llo"]])),
        (
            text("a,\"he\\\"llo\",world\r\nc,d\r\n"),
            chars(None, None, Some("\\")),
            json!([["a", "he\"llo", "world"], ["c", "d"]]),
        ),
        (
            text("\"val\\\"\""),
            chars(None, None, Some("\\")),
            json!([["val\""]]),
        ),
        (
            text("\"he\\\"llo"),
            chars(None, None, Some("\\")),
            json!([["he\"llo"]]),
        ),
        (
            text("\"a\"::b\r\nc::d\r\n"),
            chars(Some("::"), None, None),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\",b\nc,d\n"),
            chars(None, Some("\n"), None),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (text("a,b,"), none(), json!([["a", "b", ""]])),
        (
            text("a,\"b\"\nc,d\n"),
            chars(None, Some("\n"), None),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("a,\"b\"\nc,d\n"),
            chars(None, Some("\n"), Some("\\")),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("a,\"b\"\"c\"\nc,d\n"),
            chars(None, Some("\n"), None),
            json!([["a", "b\"c"], ["c", "d"]]),
        ),
        (
            text("a,\"b\\\"c\"\nc,d\n"),
            chars(None, Some("\n"), Some("\\")),
            json!([["a", "b\"c"], ["c", "d"]]),
        ),
        (
            text("\"a\",b---c,d---"),
            chars(None, Some("---"), None),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\",b---c,d---"),
            chars(None, Some("---"), Some("\\")),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("a,b\r\n\"c\",d\r\n"),
            none(),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("a,\"b\"\r\nc,\"d\"\r\n"),
            none(),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (text("a,b,c\r\nd"), none(), json!([["a", "b", "c"], ["d"]])),
        (text("a,b\r\nc,"), none(), json!([["a", "b"], ["c", ""]])),
        (
            text("a,b,c\r\nd\r\ne,f,g\r\n"),
            none(),
            json!([["a", "b", "c"], ["d"], ["e", "f", "g"]]),
        ),
        (text("a,b,c\r\n"), none(), json!([["a", "b", "c"]])),
        (
            text("\"a\"---\"b\"---"),
            chars(None, Some("---"), None),
            json!([["a"], ["b"]]),
        ),
        (
            text("\"a\"---\"b\"---"),
            chars(None, Some("---"), Some("\\")),
            json!([["a"], ["b"]]),
        ),
        (
            text("\"a\"::\"b\"\r\n\"c\"::\"d\"\r\n"),
            chars(Some("::"), None, Some("\\")),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("a,b,c\r\n\"d\"\r\n"),
            none(),
            json!([["a", "b", "c"], ["d"]]),
        ),
        (
            text("a,b,c\r\n\"d\"\r\n"),
            chars(None, None, Some("\\")),
            json!([["a", "b", "c"], ["d"]]),
        ),
        (
            text("a,b,c\r\n1,2,3,4,5\r\n"),
            none(),
            json!([["a", "b", "c"], ["1", "2", "3", "4", "5"]]),
        ),
        (
            text("a,b,c\r\n1,2,3,4,5\r\n\"x\",y,z\r\n"),
            none(),
            json!([["a", "b", "c"], ["1", "2", "3", "4", "5"], ["x", "y", "z"]]),
        ),
        (
            text("\"a\\\\\",b\r\nc,d\r\n"),
            chars(None, None, Some("\\")),
            json!([["a\\", "b"], ["c", "d"]]),
        ),
        (
            text("a,b,c\r\nd,e\r\nf,g,h\r\n"),
            none(),
            json!([["a", "b", "c"], ["d", "e"], ["f", "g", "h"]]),
        ),
        (
            text("a,b\r\n1,2\r\n3,4\r\n5,6\r\n"),
            none(),
            json!([["a", "b"], ["1", "2"], ["3", "4"], ["5", "6"]]),
        ),
        (
            text("a,b\r\n1,2\r\n3,4"),
            none(),
            json!([["a", "b"], ["1", "2"], ["3", "4"]]),
        ),
        (
            text("a,b~|1,2~|"),
            chars(None, Some("~|"), None),
            json!([["a", "b"], ["1", "2"]]),
        ),
        (
            text("\"a~b\",c~|d,e~|"),
            chars(None, Some("~|"), None),
            json!([["a~b", "c"], ["d", "e"]]),
        ),
        (
            text("\"\\\\x\\\"y\",z\r\n"),
            chars(None, None, Some("\\")),
            json!([["\\x\"y", "z"]]),
        ),
        (
            text("\"a\\\"b\\\"c\",d\r\n"),
            chars(None, None, Some("\\")),
            json!([["a\"b\"c", "d"]]),
        ),
        (
            text("a,b\r\n\"x,y\",z\r\n"),
            none(),
            json!([["a", "b"], ["x,y", "z"]]),
        ),
        (
            text("\"a\",b,c\r\nd,e,f\r\n"),
            none(),
            json!([["a", "b", "c"], ["d", "e", "f"]]),
        ),
        (
            text("\"a\"::\"b\"::c\r\n"),
            chars(Some("::"), None, None),
            json!([["a", "b", "c"]]),
        ),
        (
            text("\"a\",b\r\n\"c\",d\r\n"),
            chars(None, Some("\r\n"), None),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\",b\r\n\"c\",d\r\n"),
            chars(None, Some("\r\n"), Some("\\")),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\",b\n\"c\",d\n"),
            chars(None, Some("\n"), Some("\\")),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\",b###\"c\",d"),
            chars(None, Some("###"), None),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\",b###\"c\",d"),
            chars(None, Some("###"), Some("\\")),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            text("\"a\"##b"),
            chars(None, Some("###"), None),
            json!([["a", "##b"]]),
        ),
        (
            text("\"a\"##b"),
            chars(None, Some("###"), Some("\\")),
            json!([["a", "##b"]]),
        ),
        (
            chunks(&["\"hel", "lo\",x\r\n"]),
            none(),
            json!([["hello", "x"]]),
        ),
        (
            chunks(&["\"hel", "lo\",x\r\n"]),
            chars(None, None, Some("\\")),
            json!([["hello", "x"]]),
        ),
        (
            chunks(&["1,2\r\n", "3,4\r\n"]),
            none(),
            json!([["1", "2"], ["3", "4"]]),
        ),
        (
            chunks(&["a,", "b\r\n", "c,d\r\n"]),
            none(),
            json!([["a", "b"], ["c", "d"]]),
        ),
        (
            chunks(&["a,", "b\r\n", "1,", "2\r\n"]),
            none(),
            json!([["a", "b"], ["1", "2"]]),
        ),
        (
            chunks(&["a,b\r\n1,", "2\r\n"]),
            none(),
            json!([["a", "b"], ["1", "2"]]),
        ),
        (
            chunks(&["a,b,c,d,e\r\n", "1,2,3,4,5\r\n"]),
            none(),
            json!([["a", "b", "c", "d", "e"], ["1", "2", "3", "4", "5"]]),
        ),
    ];
    for (i, (input, options, expected)) in cases.into_iter().enumerate() {
        assert_eq!(parse(input, options).await, expected, "case {i}");
    }
}

#[tokio::test]
async fn parse_stream_preserves_injection_payloads() {
    let payloads = [
        "=cmd|'/C calc'!A0",
        "+cmd|'/C calc'!A0",
        "-cmd|'/C calc'!A0",
        "@SUM(1+1)*cmd|'/C calc'!A0",
        "=HYPERLINK(\"http://evil.com\",\"Click\")",
    ];
    let csv: String = payloads
        .iter()
        .map(|p| format!("\"{}\",safe\r\n", p.replace('"', "\"\"")))
        .collect();
    let output = parse(text(&csv), Default::default()).await;
    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(output[i], json!([p, "safe"]));
    }
}

#[tokio::test]
async fn parse_stream_many_rows_across_chunks() {
    let first = "a,b,c\r\n".repeat(50);
    let output = parse(chunks(&[first.as_str(), "x,y,z\r\n"]), Default::default()).await;
    let output = output.as_array().unwrap();
    assert_eq!(output.len(), 51);
    assert_eq!(output[50], json!(["x", "y", "z"]));
}

#[tokio::test]
async fn parse_stream_lazy_options() {
    let (detect, d) = csv_detect_delimiters_stream(text("a\tb\tc\n1\t2\t3\n"), Default::default());
    let output = parse(detect, lazy_parse(&d)).await;
    assert_eq!(output, json!([["a", "b", "c"], ["1", "2", "3"]]));
}

#[tokio::test]
async fn parse_stream_apostrophes_after_autodetect() {
    let input = "quote,author\n'twas the night,Moore\nhello,World\n";
    let (detect, d) = csv_detect_delimiters_stream(text(input), Default::default());
    let output = parse(detect, lazy_parse(&d)).await;
    assert_eq!(
        output,
        json!([
            ["quote", "author"],
            ["'twas the night", "Moore"],
            ["hello", "World"]
        ])
    );
}

#[tokio::test]
async fn parse_stream_errors_result() {
    let (stream, result) = csv_parse_stream(text("\"unterminated\r\n"), Default::default());
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(result.key(), "csvErrors");
    assert_eq!(result.get()["UnterminatedQuote"]["idx"], json!([0]));
    assert_eq!(
        output["csvErrors"]["UnterminatedQuote"],
        json!({"id": "UnterminatedQuote", "message": "Unterminated quoted field", "idx": [0]})
    );

    let errors = parse_errors(text("1,2\r\n3,4\r\n"), Default::default()).await;
    assert_eq!(errors, json!({}));

    let options = CsvParseOptions {
        result_key: Some("parseErrors".into()),
        ..Default::default()
    };
    let (stream, result) = csv_parse_stream(text("1,2\r\n"), options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(Value::Object(output), json!({"parseErrors": {}}));

    let (stream, result) = csv_parse_stream(text("\"a,b\",c\r\nd,e\r\n"), Default::default());
    assert_eq!(collect(stream).await, json!([["a,b", "c"], ["d", "e"]]));
    assert_eq!(result.get(), json!({}));
}

#[tokio::test]
async fn parse_stream_unterminated_quote_idx() {
    let cases = [
        ("\"ok\",val\r\n\"unterminated", None, 1),
        ("a,b\r\n\"unterminated", Some("\\"), 1),
        ("a,b\r\n\"unterminated", None, 1),
        ("a,b\r\nc,d\r\ne,f\r\n\"unterminated", None, 3),
        ("a,b\r\nc,d\r\ne,f\r\nx,\"unterminated", Some("\\"), 3),
    ];
    for (input, escape, idx) in cases {
        let errors = parse_errors(text(input), chars(None, None, escape)).await;
        assert_eq!(errors["UnterminatedQuote"]["idx"], json!([idx]), "{input}");
        assert_eq!(
            errors["UnterminatedQuote"]["message"],
            json!("Unterminated quoted field")
        );
    }
    // An unterminated quote at the end of a non-final chunk is reported too.
    let errors = parse_errors(chunks(&["\"a,b\r\n", "1,2\r\n"]), Default::default()).await;
    assert!(errors.get("UnterminatedQuote").is_some());
}

#[tokio::test]
async fn parse_stream_field_max_size() {
    let options = |escape: Option<&str>| CsvParseOptions {
        field_max_size: Some(10),
        ..chars(None, None, escape)
    };
    let over = format!("\"{}\",y\r\n", "x".repeat(11));
    for escape in [None, Some("\\")] {
        assert_eq!(
            parse_error(text(&over), options(escape)).await,
            "CSV field size (11) exceeds fieldMaxSize (10 bytes)"
        );
    }
    let exact = "x".repeat(10);
    let input = format!("\"{exact}\",y\r\n");
    for escape in [None, Some("\\")] {
        assert_eq!(
            parse(text(&input), options(escape)).await,
            json!([[exact, "y"]])
        );
    }
}

#[tokio::test]
async fn parse_stream_buffer_safety_limit() {
    let options = |max| CsvParseOptions {
        field_max_size: Some(max),
        ..Default::default()
    };
    let big = format!("\"{}", "x".repeat(60));
    assert_eq!(
        parse_error(text(&big), options(10)).await,
        "CSV buffer size (61) exceeds safety limit, likely unterminated quoted field"
    );
    // Exactly twice the limit is accepted.
    let exact = "x".repeat(16);
    assert_eq!(parse(text(&exact), options(8)).await, json!([[exact]]));
}

#[tokio::test]
async fn parse_stream_custom_parser() {
    let unquoted = || CsvParseOptions {
        parser: CsvParser::new(csv_unquoted_parser),
        ..Default::default()
    };
    let cases = [
        (
            text("a,b,c\r\n1,2,3\r\n4,5,6\r\n"),
            json!([["a", "b", "c"], ["1", "2", "3"], ["4", "5", "6"]]),
        ),
        (text("a,b\r\n1,2"), json!([["a", "b"], ["1", "2"]])),
        (
            chunks(&["a,b\r\n1,", "2\r\n"]),
            json!([["a", "b"], ["1", "2"]]),
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(parse(input, unquoted()).await, expected);
    }
}

#[tokio::test]
async fn parse_stream_accumulates_custom_parser_errors() {
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    let parser = CsvParser::new(move |_, _, _| {
        let call = c.fetch_add(1, Ordering::SeqCst) + 1;
        let error = CsvError {
            id: "Synthetic".into(),
            message: "syn".into(),
            idx: vec![call],
        };
        Ok(ParseResult {
            num_cols: 1,
            idx: call,
            errors: [("Synthetic".to_string(), error)].into(),
            ..Default::default()
        })
    });
    let options = CsvParseOptions {
        parser,
        ..Default::default()
    };
    let errors = parse_errors(chunks(&["aaaaaaaaa", "bbbbbbbbb", "ccccccccc"]), options).await;
    assert_eq!(errors["Synthetic"]["idx"], json!([1, 2, 3]));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn parse_stream_collects_parser_errors_while_streaming_and_flushing() {
    let with_error = |on_flush: bool, id: &'static str| CsvParseOptions {
        parser: CsvParser::new(move |text, options, flushing| {
            let mut result = csv_quoted_parser(text, options, flushing)?;
            if flushing == on_flush {
                let error = CsvError {
                    id: id.into(),
                    message: "test".into(),
                    idx: vec![0],
                };
                result.errors.insert(id.into(), error);
            }
            Ok(result)
        }),
        ..Default::default()
    };
    let errors = parse_errors(text("a,b\r\n1,2\r\n"), with_error(false, "TestError")).await;
    assert!(errors.get("TestError").is_some());
    let errors = parse_errors(text("a,b\r\n1,2"), with_error(true, "FlushError")).await;
    assert!(errors.get("FlushError").is_some());
}

#[tokio::test]
async fn parse_stream_round_trips_custom_escape() {
    let value = "a\"b\\c";
    let format = CsvFormatOptions {
        escape_char: Some("\\".into()),
        ..Default::default()
    };
    let formatted = stream_to_string(
        csv_format_stream(values(json!([[value, "next"]])), format),
        None,
    )
    .await
    .unwrap();
    let parsed = parse(text(&formatted), chars(None, None, Some("\\"))).await;
    assert_eq!(parsed, json!([[value, "next"]]));
}

// *** csvQuotedParser *** //

#[test]
fn quoted_parser_defaults() {
    let result = quoted("a,b\r\n1,2\r\n", Default::default(), false);
    assert_eq!(rows(&result), json!([["a", "b"], ["1", "2"]]));
    assert_eq!(result.tail, "");
    assert!(result.errors.is_empty());
    assert_eq!(result.num_cols, 2);
    assert_eq!(result.idx, 2);
}

#[test]
fn quoted_parser_rows() {
    let d = ParserOptions::default;
    let cases: Vec<(&str, ParserOptions, bool, Value)> = vec![
        ("\"plain\",x\r\n", d(), false, json!([["plain", "x"]])),
        ("\"a\"\"b\",\"c\"\r\n", d(), false, json!([["a\"b", "c"]])),
        (
            "a,b,c\r\n\"d\"\r\n",
            d(),
            false,
            json!([["a", "b", "c"], ["d"]]),
        ),
        (
            "a,b||~c,d||~",
            nl("||~"),
            true,
            json!([["a", "b"], ["c", "d"]]),
        ),
        ("\"a\rb\",c\r\n", nl("\r\n"), true, json!([["a\rb", "c"]])),
        ("\"a\\\\\",b\r\n", esc(), true, json!([["a\\", "b"]])),
        ("\"a\\\"b\",c\r\n", esc(), true, json!([["a\"b", "c"]])),
        ("\"a\"\n\"b\"\n", nl("\n"), true, json!([["a"], ["b"]])),
        (
            "\"a\"\r\n\"b\"\r\n",
            nl("\r\n"),
            true,
            json!([["a"], ["b"]]),
        ),
        ("\"a\"~|~\"b\"~|~", nl("~|~"), true, json!([["a"], ["b"]])),
        ("\"a\"\r", nl("\r\n"), true, json!([["a", "\r"]])),
        (
            "\"a\"\r",
            ParserOptions {
                escape_char: Some("\\".into()),
                ..nl("\r\n")
            },
            true,
            json!([["a", "\r"]]),
        ),
        ("\"a\"x,b\r\n", d(), false, json!([["a", "x", "b"]])),
        ("\"a\"x,b\r\n", esc(), false, json!([["a", "x", "b"]])),
        ("a,\"b\"", d(), true, json!([["a", "b"]])),
        ("a,\"b\"", esc(), true, json!([["a", "b"]])),
        ("\"a\",", d(), true, json!([["a", ""]])),
        ("\"a\",", esc(), true, json!([["a", ""]])),
        ("\"\\\"X\",b\r\n", esc(), false, json!([["\"X", "b"]])),
        (
            "a\"b,c\r\nd,e\r\n",
            d(),
            false,
            json!([["a\"b", "c"], ["d", "e"]]),
        ),
        (
            "a\"b,c\r\nd,e\r\n",
            esc(),
            false,
            json!([["a\"b", "c"], ["d", "e"]]),
        ),
        (
            "\"a\",b\r\n\"c\",d\r\n\"e\",f\r\n",
            d(),
            false,
            json!([["a", "b"], ["c", "d"], ["e", "f"]]),
        ),
        ("a,b\r\n1,2", d(), true, json!([["a", "b"], ["1", "2"]])),
        // A delimiter that is a prefix of the newline wins the tie.
        (
            "a\r\n",
            ParserOptions {
                delimiter_char: "\r".into(),
                ..nl("\r\n")
            },
            true,
            json!([["a", "\n"]]),
        ),
    ];
    for (i, (input, options, flushing, expected)) in cases.into_iter().enumerate() {
        assert_eq!(
            rows(&quoted(input, options, flushing)),
            expected,
            "case {i}"
        );
    }
}

#[test]
fn quoted_parser_lone_cr_is_garbage_not_newline() {
    for escape in [None, Some("\\".to_string())] {
        let options = ParserOptions {
            escape_char: escape,
            ..nl("\r\n")
        };
        let result = quoted("\"a\"\rb,c\r\n", options, true);
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], "a");
    }
    let result = quoted("\"a\"~|x~|~", nl("~|~"), true);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], "a");
}

#[test]
fn quoted_parser_idx_and_num_cols() {
    let d = ParserOptions::default;
    // (input, options, flushing, idx, num_cols)
    let cases: Vec<(&str, ParserOptions, bool, usize, usize)> = vec![
        ("a,b\r\nc,d\r\ne,f\r\n", d(), false, 3, 2),
        ("\"a\",b\r\n\"c\",d\r\n", esc(), true, 2, 2),
        ("a,b,c\r\n\"d\"", d(), true, 2, 3),
        ("a,b,c\r\n\"d\"", esc(), true, 2, 3),
        ("\"unterminated", d(), true, 1, 1),
        ("\"unterminated", esc(), true, 1, 1),
        ("\"a\",\"b\"\r\nc,d\r\n", d(), false, 2, 2),
        ("\"a\",\"b\"\r\nc,d\r\n", esc(), false, 2, 2),
        ("\"a\",\"b\"\r\n\"c\"\r\n", d(), false, 2, 2),
        ("\"a\",\"b\"\r\n\"c\"\r\n", esc(), false, 2, 2),
        ("a,b\r\n1,2\r\n3,4\r\n5,6\r\n7,8\r\n", d(), false, 5, 2),
        ("a,b\r\n1,2,3\r\n", d(), false, 2, 2),
        ("\"a\",b\r\n1,2,3\r\n", d(), false, 2, 2),
        ("a,\"b\"\r\n1,2,3\r\n", d(), false, 2, 2),
        ("a,\"b\"\r\n1,2,3\r\n", esc(), false, 2, 2),
        ("\"a\"", d(), true, 1, 1),
        ("a,\"b\"", d(), true, 1, 2),
    ];
    for (input, options, flushing, idx, num_cols) in cases {
        let result = quoted(input, options, flushing);
        assert_eq!((result.idx, result.num_cols), (idx, num_cols), "{input:?}");
    }
}

#[test]
fn quoted_parser_unterminated_quote() {
    for options in [ParserOptions::default(), esc()] {
        let result = quoted("a,b,c\r\n\"unterminated", options.clone(), true);
        assert_eq!(rows(&result), json!([["a", "b", "c"], ["unterminated"]]));
        assert_eq!(result.tail, "");
        assert_eq!(result.errors["UnterminatedQuote"].idx, [1]);
        assert_eq!(
            result.errors["UnterminatedQuote"].message,
            "Unterminated quoted field"
        );

        let result = quoted("a,b\r\n\"abc", options.clone(), false);
        assert_eq!(rows(&result), json!([["a", "b"]]));
        assert_eq!(result.tail, "\"abc");
        assert_eq!(result.idx, 1);
        assert!(result.errors.is_empty());

        let result = quoted("a,b\r\nx,\"abc", options, false);
        assert_eq!(result.tail, "x,\"abc");
    }
    let result = quoted("\"abc", Default::default(), true);
    assert_eq!(rows(&result), json!([["abc"]]));
    assert_eq!(result.errors["UnterminatedQuote"].idx, [0]);

    let result = quoted("\"a\\\"b", esc(), true);
    assert_eq!(rows(&result), json!([["a\"b"]]));
    assert!(result.errors.contains_key("UnterminatedQuote"));

    let result = quoted("x,\"abc", esc(), true);
    assert_eq!(rows(&result), json!([["x", "abc"]]));
    assert_eq!(result.tail, "");

    // A trailing lone escape char is kept literally.
    let result = quoted("\"a\\", esc(), true);
    assert_eq!(rows(&result), json!([["a\\"]]));
    assert!(result.errors.contains_key("UnterminatedQuote"));
}

#[test]
fn quoted_parser_tail() {
    let result = quoted("a,b\r\n1,2\r\n3,4", Default::default(), false);
    assert_eq!(rows(&result), json!([["a", "b"], ["1", "2"]]));
    assert_eq!(result.tail, "3,4");
    assert_eq!(result.idx, 2);

    let result = quoted("a,b\r\n1,2", Default::default(), false);
    assert_eq!(result.tail, "1,2");
    let result = quoted("a,b\r\n1,2", Default::default(), true);
    assert_eq!(result.tail, "");
}

#[test]
fn quoted_parser_surplus_and_short_rows() {
    let result = quoted("a,b,c\r\n1,2\r\n", Default::default(), false);
    assert_eq!(result.rows[1], ["1", "2"]);
    let result = quoted("a,b\r\n1,2,3,4\r\n", Default::default(), false);
    assert_eq!(result.rows[1], ["1", "2", "3", "4"]);
    let result = quoted("a,b,c\r\n1,2,3\r\n4,5,6\r\n", Default::default(), false);
    assert_eq!(result.rows[2], ["4", "5", "6"]);
    assert_eq!(result.idx, 3);
}

#[test]
fn quoted_parser_field_max_size() {
    let options = ParserOptions {
        field_max_size: 3,
        ..Default::default()
    };
    let e = csv_quoted_parser("\"abcd\"", &options, true).unwrap_err();
    assert_eq!(
        e.to_string(),
        "CSV field size (4) exceeds fieldMaxSize (3 bytes)"
    );
    assert!(csv_quoted_parser("\"abc\"", &options, true).is_ok());
}

// *** csvUnquotedParser *** //

#[test]
fn unquoted_parser() {
    let d = ParserOptions::default;
    let result = unquoted("a,b,c\r\n1,2,3\r\n", d(), false);
    assert_eq!(rows(&result), json!([["a", "b", "c"], ["1", "2", "3"]]));
    assert_eq!(result.tail, "");

    let result = unquoted("a,b,c\r\n1,2,3", d(), false);
    assert_eq!(rows(&result), json!([["a", "b", "c"]]));
    assert_eq!(result.tail, "1,2,3");

    let result = unquoted("a,b,c\r\n1,2,3", d(), true);
    assert_eq!(rows(&result), json!([["a", "b", "c"], ["1", "2", "3"]]));
    assert_eq!(result.tail, "");

    let result = unquoted("a,b,c", d(), true);
    assert_eq!(rows(&result), json!([["a", "b", "c"]]));
    assert_eq!(result.num_cols, 3);

    let result = unquoted("a,b\r\n1,2\r\n3,4\r\n", d(), false);
    assert_eq!((result.idx, result.num_cols), (3, 2));

    let result = unquoted("a,b\r\n1,2", d(), true);
    assert_eq!((result.idx, result.num_cols), (2, 2));

    let options = ParserOptions {
        idx: 5,
        num_cols: 2,
        ..d()
    };
    let result = unquoted("1,2\r\n", options, false);
    assert_eq!((result.idx, result.num_cols), (6, 2));

    let result = unquoted("a,b\r\n", d(), true);
    assert_eq!(result.idx, 1);
    assert_eq!(rows(&result), json!([["a", "b"]]));

    let options = ParserOptions {
        delimiter_char: ";".into(),
        ..nl("\n")
    };
    let result = unquoted("a;b\nc;d\n", options, false);
    assert_eq!(rows(&result), json!([["a", "b"], ["c", "d"]]));

    // numCols comes from the first row.
    assert_eq!(unquoted("a,b\r\n1,2,3\r\n", d(), false).num_cols, 2);
    let result = unquoted("a,b\r\n1,2,3", d(), true);
    assert_eq!(result.num_cols, 2);
    assert_eq!(result.rows[1], ["1", "2", "3"]);
}

// *** csvDetectDelimitersStream *** //

async fn detect(input: DataStream<String>) -> Value {
    let (stream, result) = csv_detect_delimiters_stream(input, Default::default());
    collect_strings(stream).await;
    result.get()
}

#[tokio::test]
async fn detect_delimiters_cases() {
    // (input, delimiter, newline, quote)
    let cases = [
        ("a,b,c\n1,2,3\n", ",", "\n", "\""),
        ("a\tb\tc\n1\t2\t3\n", "\t", "\n", "\""),
        ("a|b|c\n1|2|3\n", "|", "\n", "\""),
        ("a;b;c\n1;2;3\n", ";", "\n", "\""),
        ("a,b\r\n1,2\r\n", ",", "\r\n", "\""),
        ("a,b\r1,2\r", ",", "\r", "\""),
        (
            "'first name','last name'\n'Alice','Smith'\n",
            ",",
            "\n",
            "'",
        ),
        ("\"a\tb\"\tc\n\"d\te\"\tf\n", "\t", "\n", "\""),
        ("\u{feff}a,b\n1,2\n", ",", "\n", "\""),
        (
            "quote,author\n'twas the night,Moore\nhello,World\n",
            ",",
            "\n",
            "\"",
        ),
        ("'a',b\n1,2\n", ",", "\n", "'"),
        ("a,'b'\n1,2\n", ",", "\n", "'"),
        ("'a',b\r\n1,2\r\n", ",", "\r\n", "'"),
        ("a,b\n'c',d\n", ",", "\n", "'"),
        ("name,note\n'x,it's fine\nhello,world\n", ",", "\n", "\""),
        ("a\tb,c\n1\t2,3\n", "\t", "\n", "\""),
        ("a|b;c,d\n1|2;3,4\n", "|", "\n", "\""),
        ("a;b,c\n1;2,3\n", ";", "\n", "\""),
        ("x,y\nz,'w'", ",", "\n", "'"),
        ("a,b\r'c',d\r", ",", "\r", "'"),
        ("'a',b,c\n1,2,3\n", ",", "\n", "'"),
        ("'a'\r'b'\r", ",", "\r", "'"),
        ("ab'cd,ef\r\n", ",", "\r\n", "\""),
        ("ab'cd',ef\r\n", ",", "\r\n", "\""),
        ("x,'a'\r\n", ",", "\r\n", "'"),
        ("'twas,ok\r\n", ",", "\r\n", "\""),
        ("a,'b'\r\n", ",", "\r\n", "'"),
        ("',b\r\n", ",", "\r\n", "\""),
        ("'',b\r\n", ",", "\r\n", "'"),
    ];
    for (input, delimiter, newline, quote) in cases {
        let value = detect(text(input)).await;
        assert_eq!(
            value,
            json!({"delimiterChar": delimiter, "newlineChar": newline, "quoteChar": quote, "escapeChar": quote}),
            "{input:?}"
        );
    }
}

#[tokio::test]
async fn detect_delimiters_large_input() {
    let value = detect(text(&"a,b,c,d,e\n".repeat(120))).await;
    assert_eq!(value["delimiterChar"], json!(","));
}

#[tokio::test]
async fn detect_delimiters_passes_text_through() {
    let cases: Vec<(Vec<&str>, Vec<&str>)> = vec![
        (vec!["a,b\n1,2\n"], vec!["a,b\n1,2\n"]),
        (vec!["a;b;c", "\n1;2;3\n"], vec!["a;b;c\n1;2;3\n"]),
        (vec!["abc", "d\n1\n"], vec!["abcd\n1\n"]),
        (vec!["abcd", "e;f\r\n"], vec!["abcde;f\r\n"]),
        (vec!["a;b\r\n", "c;d\r\n"], vec!["a;b\r\n", "c;d\r\n"]),
        (
            vec!["a|b\r\n", "rest1\r\n", "rest2\r\n"],
            vec!["a|b\r\n", "rest1\r\n", "rest2\r\n"],
        ),
        (vec!["a,b,c"], vec!["a,b,c"]),
        (vec![""], vec![]),
    ];
    for (input, expected) in cases {
        let (stream, _) = csv_detect_delimiters_stream(chunks(&input), Default::default());
        assert_eq!(collect_strings(stream).await, expected, "{input:?}");
    }
    let (stream, d) =
        csv_detect_delimiters_stream(chunks(&["a;b;c", "\n1;2;3\n"]), Default::default());
    collect_strings(stream).await;
    assert_eq!(d.get()["delimiterChar"], json!(";"));
}

#[tokio::test]
async fn detect_delimiters_result_key_and_empty_input() {
    let options = CsvDetectDelimitersOptions {
        result_key: Some("delim".into()),
    };
    let (stream, result) = csv_detect_delimiters_stream(text("a,b\n1,2\n"), options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(output["delim"]["delimiterChar"], json!(","));

    let (stream, result) = csv_detect_delimiters_stream(text(""), Default::default());
    assert_eq!(result.key(), "csvDetectDelimiters");
    assert!(collect_strings(stream).await.is_empty());
    assert_eq!(
        result.get(),
        json!({"delimiterChar": null, "newlineChar": null, "quoteChar": null, "escapeChar": null})
    );
}

// *** csvDetectHeaderStream *** //

fn header_options(
    newline: Option<&str>,
    delimiter: Option<&str>,
    escape: Option<&str>,
) -> CsvDetectHeaderOptions {
    CsvDetectHeaderOptions {
        newline_char: newline.map(LazyString::from),
        delimiter_char: delimiter.map(LazyString::from),
        escape_char: escape.map(LazyString::from),
        ..Default::default()
    }
}

#[tokio::test]
async fn detect_header_cases() {
    // (input chunks, options, header, output chunks)
    let n = |newline| header_options(Some(newline), None, None);
    let none = || header_options(None, None, None);
    let cases: Vec<(Vec<&str>, CsvDetectHeaderOptions, Value, Vec<&str>)> = vec![
        (
            vec!["name,age,city\nAlice,30,NYC\nBob,25,LA\n"],
            n("\n"),
            json!(["name", "age", "city"]),
            vec!["Alice,30,NYC\nBob,25,LA\n"],
        ),
        (
            vec!["\"first name\",\"last name\"\nAlice,Smith\n"],
            n("\n"),
            json!(["first name", "last name"]),
            vec!["Alice,Smith\n"],
        ),
        (vec!["a,b,c"], none(), json!(["a", "b", "c"]), vec![]),
        (
            vec!["x,y\r\n1,2\r\n3,4\r\n"],
            n("\r\n"),
            json!(["x", "y"]),
            vec!["1,2\r\n3,4\r\n"],
        ),
        (
            vec!["\"col\"\"1\",col2\nval1,val2\n"],
            n("\n"),
            json!(["col\"1", "col2"]),
            vec!["val1,val2\n"],
        ),
        (
            vec!["col1,col2\nval1,val2\n"],
            n("\n"),
            json!(["col1", "col2"]),
            vec!["val1,val2\n"],
        ),
        (vec!["\u{feff}"], none(), json!([]), vec![]),
        (vec!["\na,b\n"], n("\n"), json!([]), vec!["a,b\n"]),
        (
            vec!["\"col\nwith newline\",col2\nval1,val2\n"],
            n("\n"),
            json!(["col\nwith newline", "col2"]),
            vec!["val1,val2\n"],
        ),
        (
            vec!["\"col\\\"x\nnext\",col2\nval1,val2\n"],
            header_options(Some("\n"), None, Some("\\")),
            json!(["col\"x\nnext", "col2"]),
            vec!["val1,val2\n"],
        ),
        (
            vec!["a::b::c\n1::2::3\n"],
            header_options(Some("\n"), Some("::"), None),
            json!(["a", "b", "c"]),
            vec!["1::2::3\n"],
        ),
        (
            vec!["\"a,b\",c\n1,2\n"],
            n("\n"),
            json!(["a,b", "c"]),
            vec!["1,2\n"],
        ),
        (
            vec!["\"a\\\\\",b\nv1,v2\n"],
            header_options(Some("\n"), None, Some("\\")),
            json!(["a\\", "b"]),
            vec!["v1,v2\n"],
        ),
        (
            vec!["\"h1\nstill h1\",h2\nr1a,r1b\nr2a,r2b\n"],
            n("\n"),
            json!(["h1\nstill h1", "h2"]),
            vec!["r1a,r1b\nr2a,r2b\n"],
        ),
        (
            vec!["only,header\n"],
            n("\n"),
            json!(["only", "header"]),
            vec![],
        ),
        (vec![""], none(), json!([]), vec![]),
        (vec!["a,b,c\r\n"], none(), json!(["a", "b", "c"]), vec![]),
        (
            vec!["a\rb\r\n"],
            header_options(Some("\r\n"), Some("\r"), None),
            json!(["a", "b", "\n"]),
            vec![],
        ),
        (
            vec!["\"unterminated header\r\nmore\r\n"],
            none(),
            json!(["unterminated header\r\nmore\r\n"]),
            vec![],
        ),
        (
            vec!["a,b\r\n1,2\r\n", "3,4\r\n"],
            none(),
            json!(["a", "b"]),
            vec!["1,2\r\n", "3,4\r\n"],
        ),
        (
            vec!["a,", "b\r\n1,2\r\n"],
            none(),
            json!(["a", "b"]),
            vec!["1,2\r\n"],
        ),
    ];
    for (input, options, header, expected) in cases {
        let (stream, result) = csv_detect_header_stream(chunks(&input), options);
        assert_eq!(collect_strings(stream).await, expected, "{input:?}");
        assert_eq!(result.get()["header"], header, "{input:?}");
        assert_eq!(result.key(), "csvDetectHeader");
    }
}

#[tokio::test]
async fn detect_header_then_parse() {
    // (input, escape, delimiter, header, rows)
    let cases = [
        (
            "\"a\"\"b\r\nc\",second\r\n1,2\r\n",
            None,
            None,
            json!(["a\"b\r\nc", "second"]),
            json!([["1", "2"]]),
        ),
        (
            "\"a\\\"b,c\",second\r\n1,2\r\n",
            Some("\\"),
            None,
            json!(["a\"b,c", "second"]),
            json!([["1", "2"]]),
        ),
        (
            "\"a\\\\\",b\r\n1,2\r\n",
            Some("\\"),
            None,
            json!(["a\\", "b"]),
            json!([["1", "2"]]),
        ),
        (
            "x,\"q,uoted\"\r\n1,2\r\n",
            None,
            None,
            json!(["x", "q,uoted"]),
            json!([["1", "2"]]),
        ),
        (
            "\"a::b\"::c\r\n1::2\r\n",
            None,
            Some("::"),
            json!(["a::b", "c"]),
            json!([["1", "2"]]),
        ),
        (
            "a\"b,c\r\nd,e\r\n",
            None,
            None,
            json!(["a\"b", "c"]),
            json!([["d", "e"]]),
        ),
        (
            "a,\"x,y\"\r\n1,2\r\n",
            None,
            None,
            json!(["a", "x,y"]),
            json!([["1", "2"]]),
        ),
        (
            "\"line1\r\nline2\",b\r\n1,2\r\n",
            None,
            None,
            json!(["line1\r\nline2", "b"]),
            json!([["1", "2"]]),
        ),
        (
            "\"x\\\"y\r\nz\",b\r\n1,2\r\n",
            Some("\\"),
            None,
            json!(["x\"y\r\nz", "b"]),
            json!([["1", "2"]]),
        ),
        (
            "\"a\\\"\r\nb\",c\r\nd,e\r\n",
            Some("\\"),
            None,
            json!(["a\"\r\nb", "c"]),
            json!([["d", "e"]]),
        ),
        ("", None, None, json!([]), json!([])),
        ("a,b,c\r\n", None, None, json!(["a", "b", "c"]), json!([])),
        (
            "a,b\r\n1,2\r\n3,4\r\n",
            None,
            None,
            json!(["a", "b"]),
            json!([["1", "2"], ["3", "4"]]),
        ),
        (
            "a\r\nx\r\ny\r\n",
            None,
            None,
            json!(["a"]),
            json!([["x"], ["y"]]),
        ),
        ("\r\na,b\r\n", None, None, json!([]), json!([["a", "b"]])),
        (
            "a\r\nb,c\r\n",
            None,
            None,
            json!(["a"]),
            json!([["b", "c"]]),
        ),
        (
            "\"unterminated header\r\nmore\r\n",
            None,
            None,
            json!(["unterminated header\r\nmore\r\n"]),
            json!([]),
        ),
        (
            "\"\"\"a\r\nb\",c\r\n1,2\r\n",
            None,
            None,
            json!(["\"a\r\nb", "c"]),
            json!([["1", "2"]]),
        ),
        (
            "\"\\\"a\r\nb\",c\r\n1,2\r\n",
            Some("\\"),
            None,
            json!(["\"a\r\nb", "c"]),
            json!([["1", "2"]]),
        ),
        (
            "a,\"x\r\ny\"\r\n1,2\r\n",
            None,
            None,
            json!(["a", "x\r\ny"]),
            json!([["1", "2"]]),
        ),
        (
            "a::\"x\r\ny\"\r\n1::2\r\n",
            None,
            Some("::"),
            json!(["a", "x\r\ny"]),
            json!([["1", "2"]]),
        ),
    ];
    for (input, escape, delimiter, header, expected) in cases {
        let (stream, hdr) =
            csv_detect_header_stream(text(input), header_options(None, delimiter, escape));
        let output = parse(stream, chars(delimiter, None, escape)).await;
        assert_eq!(output, expected, "{input:?}");
        assert_eq!(hdr.get()["header"], header, "{input:?}");
    }
    // Later chunks pass through once the header is detected.
    let (stream, hdr) = csv_detect_header_stream(
        chunks(&["a,b\r\n", "1,2\r\n", "3,4\r\n"]),
        Default::default(),
    );
    assert_eq!(
        parse(stream, Default::default()).await,
        json!([["1", "2"], ["3", "4"]])
    );
    assert_eq!(hdr.get()["header"], json!(["a", "b"]));
}

#[tokio::test]
async fn detect_header_lazy_and_large_input() {
    let (detect, d) = csv_detect_delimiters_stream(text("a\tb\tc\n1\t2\t3\n"), Default::default());
    let (stream, hdr) = csv_detect_header_stream(detect, lazy_header(&d));
    assert_eq!(stream_to_string(stream, None).await.unwrap(), "1\t2\t3\n");
    assert_eq!(hdr.get()["header"], json!(["a", "b", "c"]));

    let first = format!("name,age,city\n{}", "Alice,30,NYC\n".repeat(100));
    let (stream, hdr) = csv_detect_header_stream(
        chunks(&[first.as_str(), "Bob,25,LA\n"]),
        header_options(Some("\n"), None, None),
    );
    let output = stream_to_string(stream, None).await.unwrap();
    assert!(output.contains("Alice,30,NYC"));
    assert!(output.contains("Bob,25,LA"));
    assert_eq!(hdr.get()["header"], json!(["name", "age", "city"]));
}

#[tokio::test]
async fn detect_header_result_key() {
    let options = CsvDetectHeaderOptions {
        result_key: Some("hdr".into()),
        ..header_options(Some("\n"), None, None)
    };
    let (stream, result) = csv_detect_header_stream(text("a,b\n1,2\n"), options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(output["hdr"]["header"], json!(["a", "b"]));
}

#[tokio::test]
async fn format_detect_parse_round_trip_preserves_payloads() {
    let payloads = [
        "=cmd|'/C calc'!A0",
        "+cmd|'/C calc'!A0",
        "-cmd|'/C calc'!A0",
        "@SUM(1+1)",
        "=HYPERLINK(\"http://evil.com\",\"Click\")",
    ];
    let rows = values(Value::Array(payloads.iter().map(|p| json!([p])).collect()));
    let header = CsvInjectHeaderOptions {
        header: vec!["val".into()],
    };
    let formatted = csv_format_stream(csv_inject_header_stream(rows, header), Default::default());
    let formatted = stream_to_string(formatted, None).await.unwrap();

    let (detect, d) = csv_detect_delimiters_stream(text(&formatted), Default::default());
    let (hdr, _) = csv_detect_header_stream(detect, lazy_header(&d));
    let parsed = parse(hdr, lazy_parse(&d)).await;
    for (i, p) in payloads.iter().enumerate() {
        assert_eq!(parsed[i][0], json!(p));
    }
}

// *** csvRemoveMalformedRowsStream *** //

#[tokio::test]
async fn remove_malformed_rows() {
    let input = || {
        values(json!([
            ["1", "2", "3"],
            ["4", "5"],
            ["6", "7", "8"],
            ["9", "10", "11", "12"]
        ]))
    };
    let (stream, result) = csv_remove_malformed_rows_stream(input(), Default::default());
    assert_eq!(
        collect(stream).await,
        json!([["1", "2", "3"], ["6", "7", "8"]])
    );
    assert_eq!(result.key(), "csvRemoveMalformedRows");
    assert_eq!(
        result.get(),
        json!({"MalformedRow": {"id": "MalformedRow", "message": "Row has incorrect number of fields", "idx": [1, 3]}})
    );

    let options = CsvRemoveMalformedRowsOptions {
        on_error_enqueue: true,
        ..Default::default()
    };
    let (stream, result) = csv_remove_malformed_rows_stream(input(), options);
    assert_eq!(collect(stream).await.as_array().unwrap().len(), 4);
    assert_eq!(result.get()["MalformedRow"]["idx"], json!([1, 3]));

    let input = values(json!([["a", "b", "c"], ["1", "2", "3"], ["x"]]));
    let (stream, result) = csv_remove_malformed_rows_stream(input, Default::default());
    assert_eq!(
        collect(stream).await,
        json!([["a", "b", "c"], ["1", "2", "3"]])
    );
    assert_eq!(result.get()["MalformedRow"]["idx"], json!([2]));

    let input = values(json!([["1", "2"], ["3", "4"]]));
    let (stream, result) = csv_remove_malformed_rows_stream(input, Default::default());
    collect(stream).await;
    assert_eq!(result.get(), json!({}));

    let options = CsvRemoveMalformedRowsOptions {
        result_key: Some("bad".into()),
        ..Default::default()
    };
    let (stream, result) =
        csv_remove_malformed_rows_stream(values(json!([["1", "2"], ["3"]])), options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(output["bad"]["MalformedRow"]["idx"], json!([1]));

    let options = CsvRemoveMalformedRowsOptions {
        headers: Some(vec!["a", "b"].into()),
        ..Default::default()
    };
    let (stream, _) = csv_remove_malformed_rows_stream(values(json!([["1"], ["1", "2"]])), options);
    assert_eq!(collect(stream).await, json!([["1", "2"]]));
}

#[tokio::test]
async fn remove_malformed_rows_with_lazy_headers() {
    let (detect, d) =
        csv_detect_delimiters_stream(text("a,b,c\n1,2,3\n4,5\n6,7,8\n"), Default::default());
    let (hdr_stream, hdr) = csv_detect_header_stream(detect, lazy_header(&d));
    let (parsed, _) = csv_parse_stream(hdr_stream, lazy_parse(&d));
    let options = CsvRemoveMalformedRowsOptions {
        headers: Some(header_keys(&hdr)),
        ..Default::default()
    };
    let (stream, result) = csv_remove_malformed_rows_stream(parsed, options);
    assert_eq!(
        collect(stream).await,
        json!([["1", "2", "3"], ["6", "7", "8"]])
    );
    assert_eq!(result.get()["MalformedRow"]["idx"], json!([1]));
}

#[tokio::test]
async fn parse_then_remove_over_long_rows() {
    let (parsed, _) = csv_parse_stream(text("a,b,c\r\n1,2,3,4,5\r\n6,7,8\r\n"), Default::default());
    let (stream, result) = csv_remove_malformed_rows_stream(parsed, Default::default());
    assert_eq!(
        collect(stream).await,
        json!([["a", "b", "c"], ["6", "7", "8"]])
    );
    assert_eq!(result.get()["MalformedRow"]["idx"], json!([1]));
}

// *** csvRemoveEmptyRowsStream *** //

#[tokio::test]
async fn remove_empty_rows() {
    let cases = [
        (
            json!([["1", "2"], ["", ""], ["3", "4"]]),
            json!([["1", "2"], ["3", "4"]]),
            json!([1]),
        ),
        (
            json!([["1", "2"], [], ["3", "4"]]),
            json!([["1", "2"], ["3", "4"]]),
            json!([1]),
        ),
        (
            json!([["", ""], ["1", "2"], ["", ""]]),
            json!([["1", "2"]]),
            json!([0, 2]),
        ),
        (json!([["", "x"], ["", ""]]), json!([["", "x"]]), json!([1])),
        (json!([[], ["a"]]), json!([["a"]]), json!([0])),
    ];
    for (input, expected, idx) in cases {
        let (stream, result) = csv_remove_empty_rows_stream(values(input), Default::default());
        assert_eq!(collect(stream).await, expected);
        assert_eq!(
            result.get()["EmptyRow"],
            json!({"id": "EmptyRow", "message": "Row is empty", "idx": idx})
        );
    }

    let (stream, result) =
        csv_remove_empty_rows_stream(values(json!([["", "", "z"]])), Default::default());
    assert_eq!(collect(stream).await, json!([["", "", "z"]]));
    assert_eq!(result.get(), json!({}));
    assert_eq!(result.key(), "csvRemoveEmptyRows");

    let options = CsvRemoveEmptyRowsOptions {
        on_error_enqueue: true,
        ..Default::default()
    };
    let input = json!([["1", "2"], ["", ""], ["3", "4"]]);
    let (stream, result) = csv_remove_empty_rows_stream(values(input.clone()), options);
    assert_eq!(collect(stream).await, input);
    assert_eq!(result.get()["EmptyRow"]["idx"], json!([1]));

    let options = CsvRemoveEmptyRowsOptions {
        result_key: Some("empty".into()),
        ..Default::default()
    };
    let (stream, result) =
        csv_remove_empty_rows_stream(values(json!([["", ""], ["1", "2"]])), options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(output["empty"]["EmptyRow"]["idx"], json!([0]));
}

// *** csvCoerceValuesStream *** //

async fn coerce(row: Value, columns: &[(&str, CsvCoerceType)]) -> Value {
    let options = CsvCoerceValuesOptions {
        columns: columns.iter().map(|(k, t)| (k.to_string(), *t)).collect(),
        ..Default::default()
    };
    let (stream, _) = csv_coerce_values_stream(values(json!([row])), options);
    collect(stream).await[0].clone()
}

#[tokio::test]
async fn coerce_auto() {
    let cases = [
        (
            json!({"str": "hello", "num": "42", "float": "3.14", "boolT": "true", "boolF": "false", "empty": ""}),
            json!({"str": "hello", "num": 42, "float": 3.14, "boolT": true, "boolF": false, "empty": null}),
        ),
        (
            json!({"arr": "[1,2,3]", "obj": "{\"a\":1}"}),
            json!({"arr": [1, 2, 3], "obj": {"a": 1}}),
        ),
        (json!({"val": "1.5e3"}), json!({"val": 1500})),
        (
            json!({"val": "-42", "neg_float": "-3.14"}),
            json!({"val": -42, "neg_float": -3.14}),
        ),
        (
            json!({"val": "{not json", "arr": "[broken"}),
            json!({"val": "{not json", "arr": "[broken"}),
        ),
        (
            json!({"num": 42, "bool": true, "nil": null}),
            json!({"num": 42, "bool": true, "nil": null}),
        ),
        (
            json!({"zip": "07001", "phone": "0123456789"}),
            json!({"zip": 7001, "phone": 123_456_789}),
        ),
        (
            json!({"val": "TRUE", "val2": "FALSE"}),
            json!({"val": true, "val2": false}),
        ),
        (
            json!({"a": "1e+10", "b": "2e-05", "c": "1.5E3"}),
            json!({"a": 10_000_000_000_i64, "b": 2e-5, "c": 1500}),
        ),
        (
            json!({"a": "1e", "b": "1e+", "c": "1.2.3"}),
            json!({"a": "1e", "b": "1e+", "c": "1.2.3"}),
        ),
        (
            json!({"a": "12abc", "d": "2024-01-15extra"}),
            json!({"a": "12abc", "d": "2024-01-15extra"}),
        ),
        (
            json!({"a": "trUE", "b": "tree", "c": "True", "d": "truer"}),
            json!({"a": true, "b": "tree", "c": true, "d": "truer"}),
        ),
        (
            json!({"a": "false", "b": "False", "c": "fALSE", "d": "falsey"}),
            json!({"a": false, "b": false, "c": false, "d": "falsey"}),
        ),
        (
            json!({"a": "yes", "b": "nope", "c": "rue"}),
            json!({"a": "yes", "b": "nope", "c": "rue"}),
        ),
        (
            json!({"a": "0", "b": "9", "c": "-7"}),
            json!({"a": 0, "b": 9, "c": -7}),
        ),
        (
            json!({"a": "null", "b": "hello"}),
            json!({"a": "null", "b": "hello"}),
        ),
        (
            json!({"a": "{\"x\":1}", "b": "[1,2]", "c": "plain"}),
            json!({"a": {"x": 1}, "b": [1, 2], "c": "plain"}),
        ),
        (
            json!({"eq": "=cmd|'/C calc'!A0", "plus": "+cmd|'/C calc'!A0", "minus": "-cmd", "at": "@SUM(1+1)"}),
            json!({"eq": "=cmd|'/C calc'!A0", "plus": "+cmd|'/C calc'!A0", "minus": "-cmd", "at": "@SUM(1+1)"}),
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(coerce(input.clone(), &[]).await, expected, "{input}");
    }
}

#[tokio::test]
async fn coerce_auto_dates() {
    let cases = [
        ("2024-01-15", json!("2024-01-15T00:00:00.000Z")),
        ("2024-01-15T10:30:00Z", json!("2024-01-15T10:30:00.000Z")),
        (
            "2024-01-15T10:30:00+05:30",
            json!("2024-01-15T05:00:00.000Z"),
        ),
        (
            "2024-01-15T10:30:00+0530",
            json!("2024-01-15T05:00:00.000Z"),
        ),
        (
            "2024-01-15T10:30:00.123Z",
            json!("2024-01-15T10:30:00.123Z"),
        ),
        (
            "2024-01-15T10:30:00.1234567Z",
            json!("2024-01-15T10:30:00.123Z"),
        ),
        ("2024-01-15T10:30:00.1Z", json!("2024-01-15T10:30:00.100Z")),
        ("2024-01-15 10:30:45", json!("2024-01-15T10:30:45.000Z")),
        ("2024-12-31", json!("2024-12-31T00:00:00.000Z")),
        ("2020-01-02T03:04", json!("2020-01-02T03:04:00.000Z")),
        (
            "2020-01-02T03:04:05.678Z",
            json!("2020-01-02T03:04:05.678Z"),
        ),
        // V8 rolls days past the end of the month over and accepts 24:00.
        ("2024-02-30", json!("2024-03-01T00:00:00.000Z")),
        ("2024-01-15T24:00", json!("2024-01-16T00:00:00.000Z")),
        (
            "2024-01-15T23:59:59+23:59",
            json!("2024-01-15T00:00:59.000Z"),
        ),
        (
            "0000-01-01T00:00+01:00",
            json!("-000001-12-31T23:00:00.000Z"),
        ),
        (
            "9999-12-31T23:59-01:00",
            json!("+010000-01-01T00:59:00.000Z"),
        ),
        // Invalid or not ISO shaped: kept as the string.
        ("2024-13-99", json!("2024-13-99")),
        ("2024-01-00", json!("2024-01-00")),
        ("2024-01-32", json!("2024-01-32")),
        ("2024-01-15T24:00:01", json!("2024-01-15T24:00:01")),
        ("2024-01-15T10:60", json!("2024-01-15T10:60")),
        (
            "2024-01-15T10:30:00+25:00",
            json!("2024-01-15T10:30:00+25:00"),
        ),
        ("2024-01-15T10:30:00.abc", json!("2024-01-15T10:30:00.abc")),
        ("x2024-01-15", json!("x2024-01-15")),
        (" 2020-01-02", json!(" 2020-01-02")),
        ("2020-01-02junk", json!("2020-01-02junk")),
    ];
    for (input, expected) in cases {
        assert_eq!(
            coerce(json!({"d": input}), &[]).await["d"],
            expected,
            "{input}"
        );
    }
}

#[tokio::test]
async fn coerce_explicit_types() {
    use CsvCoerceType as T;
    let cases: Vec<(Value, Vec<(&str, CsvCoerceType)>, Value)> = vec![
        (
            json!({"age": "30", "active": "true", "name": "Alice"}),
            vec![("age", T::Number), ("active", T::Boolean)],
            json!({"age": 30, "active": true, "name": "Alice"}),
        ),
        (
            json!({"val": "anything"}),
            vec![("val", T::Null)],
            json!({"val": null}),
        ),
        (
            json!({"val": "2024-06-15"}),
            vec![("val", T::Date)],
            json!({"val": "2024-06-15T00:00:00.000Z"}),
        ),
        (
            json!({"val": "not-a-date"}),
            vec![("val", T::Date)],
            json!({"val": "not-a-date"}),
        ),
        (
            json!({"val": "{\"a\":1}"}),
            vec![("val", T::Json)],
            json!({"val": {"a": 1}}),
        ),
        (
            json!({"val": "[1,2,3]"}),
            vec![("val", T::Json)],
            json!({"val": [1, 2, 3]}),
        ),
        (
            json!({"val": "not-valid-json"}),
            vec![("val", T::Json)],
            json!({"val": "not-valid-json"}),
        ),
        (
            json!({"val": "{not json"}),
            vec![("val", T::Json)],
            json!({"val": "{not json"}),
        ),
        (
            json!({"val": "not-a-number"}),
            vec![("val", T::Number)],
            json!({"val": "not-a-number"}),
        ),
        (
            json!({"val": ""}),
            vec![("val", T::Number)],
            json!({"val": null}),
        ),
        (
            json!({"a": "3.14", "b": "0x1F", "c": " 12 ", "d": "0b11", "e": "1_0"}),
            vec![
                ("a", T::Number),
                ("b", T::Number),
                ("c", T::Number),
                ("d", T::Number),
                ("e", T::Number),
            ],
            json!({"a": 3.14, "b": 31, "c": 12, "d": 3, "e": "1_0"}),
        ),
        // JSON has no Infinity, so it stays a string.
        (
            json!({"a": "Infinity", "b": 7, "c": true, "d": null}),
            vec![
                ("a", T::Number),
                ("b", T::Number),
                ("c", T::Number),
                ("d", T::Number),
            ],
            json!({"a": "Infinity", "b": 7, "c": 1, "d": 0}),
        ),
        (
            json!({"zip": "07001"}),
            vec![("zip", T::String)],
            json!({"zip": "07001"}),
        ),
        (
            json!({"a": "TRUE", "b": "no", "c": "true"}),
            vec![("a", T::Boolean), ("b", T::Boolean), ("c", T::Boolean)],
            json!({"a": true, "b": false, "c": true}),
        ),
        (
            json!({"a": 1, "b": 0, "c": null, "d": [1]}),
            vec![
                ("a", T::Boolean),
                ("b", T::Boolean),
                ("c", T::Boolean),
                ("d", T::Boolean),
            ],
            json!({"a": true, "b": false, "c": false, "d": true}),
        ),
    ];
    for (input, columns, expected) in cases {
        assert_eq!(coerce(input.clone(), &columns).await, expected, "{input}");
    }
}

#[tokio::test]
async fn coerce_result_key() {
    let (stream, result) =
        csv_coerce_values_stream(values(json!([{"a": "1"}])), Default::default());
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(Value::Object(output), json!({"csvCoerceValues": {}}));

    let options = CsvCoerceValuesOptions {
        result_key: Some("coerce".into()),
        ..Default::default()
    };
    let (stream, result) = csv_coerce_values_stream(values(json!([{"a": "1"}])), options);
    let output = pipeline(stream, &[&result]).await.unwrap();
    assert_eq!(Value::Object(output), json!({"coerce": {}}));
}

// *** csvInjectHeaderStream *** //

#[tokio::test]
async fn inject_header() {
    let header = || CsvInjectHeaderOptions {
        header: vec!["a".into(), "b".into()],
    };
    let input = values(json!([["1", "2"], ["3", "4"], ["5", "6"]]));
    assert_eq!(
        collect(csv_inject_header_stream(input, header())).await,
        json!([["a", "b"], ["1", "2"], ["3", "4"], ["5", "6"]])
    );
    assert_eq!(
        collect(csv_inject_header_stream(values(json!([])), header())).await,
        json!([])
    );
}

// *** csvFormatStream *** //

async fn format(rows: Value, options: CsvFormatOptions) -> String {
    stream_to_string(csv_format_stream(values(rows), options), None)
        .await
        .unwrap()
}

fn delimiter(d: &str) -> CsvFormatOptions {
    CsvFormatOptions {
        delimiter_char: Some(d.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn format_cases() {
    let d = CsvFormatOptions::default;
    let escape = || CsvFormatOptions {
        escape_char: Some("\\".into()),
        ..Default::default()
    };
    let cases: Vec<(Value, CsvFormatOptions, &str)> = vec![
        (json!([["hello, world", "plain"]]), d(), "\"hello, world\",plain\r\n"),
        (json!([["say \"hi\"", "plain"]]), d(), "\"say \"\"hi\"\"\",plain\r\n"),
        (json!([["line1\nline2", "plain"]]), d(), "\"line1\nline2\",plain\r\n"),
        (json!([["1", "2"]]), d(), "1,2\r\n"),
        (json!([["=1+2", "safe"], ["+1", "safe"], ["-1", "safe"], ["@SUM(1)", "safe"]]), d(), "\"=1+2\",safe\r\n\"+1\",safe\r\n\"-1\",safe\r\n\"@SUM(1)\",safe\r\n"),
        (json!([[" lead", "ok"]]), d(), "\" lead\",ok\r\n"),
        (json!([["trail ", "ok"]]), d(), "\"trail \",ok\r\n"),
        (json!([["\u{feff}bom", "ok"]]), d(), "\"\u{feff}bom\",ok\r\n"),
        (json!([["a b c", "ok"]]), d(), "a b c,ok\r\n"),
        (json!([["a\rb", "ok"]]), d(), "\"a\rb\",ok\r\n"),
        (json!([["a\"b", "cd"]]), d(), "\"a\"\"b\",cd\r\n"),
        (json!([["a;b", "c,d"]]), d(), "a;b,\"c,d\"\r\n"),
        (json!([["xyz", "ok"]]), d(), "xyz,ok\r\n"),
        (json!([["a", "b", "c"]]), d(), "a,b,c\r\n"),
        (json!([["plain", "needs\nquote"]]), d(), "plain,\"needs\nquote\"\r\n"),
        (json!([["a::b", "plain"]]), delimiter("::"), "\"a::b\"::plain\r\n"),
        (json!([["has\"quote", "plain"]]), delimiter("::"), "\"has\"\"quote\"::plain\r\n"),
        (json!([["plain", "value"]]), delimiter("::"), "plain::value\r\n"),
        (json!([["trail ", "ok"]]), delimiter("::"), "\"trail \"::ok\r\n"),
        (json!([["a\nb", "ok"]]), delimiter("::"), "\"a\nb\"::ok\r\n"),
        (json!([["a\rb", "ok"]]), delimiter("::"), "\"a\rb\"::ok\r\n"),
        (json!([["=x", "ok"], ["+x", "ok"], ["-x", "ok"], ["@x", "ok"], [" x", "ok"], ["\u{feff}x", "ok"]]), delimiter("::"), "\"=x\"::ok\r\n\"+x\"::ok\r\n\"-x\"::ok\r\n\"@x\"::ok\r\n\" x\"::ok\r\n\"\u{feff}x\"::ok\r\n"),
        (json!([["a", "b", "c"]]), delimiter("::"), "a::b::c\r\n"),
        (json!([["a\"b\\c", "ok"]]), escape(), "\"a\\\"b\\\\c\",ok\r\n"),
        (json!([["a\\b,c", "ok"]]), escape(), "\"a\\\\b,c\",ok\r\n"),
        (json!([["a,b", "ok"]]), escape(), "\"a,b\",ok\r\n"),
        (json!([["2024-01-15T10:30:00.000Z", "ok"]]), d(), "2024-01-15T10:30:00.000Z,ok\r\n"),
        (json!([[42, 3.14]]), d(), "42,3.14\r\n"),
        (json!([[1.0, 1e21, 1.5e-7, -0.0]]), d(), "1,1e+21,1.5e-7,0\r\n"),
        (json!([[null, null, "x"]]), d(), ",,x\r\n"),
        (json!([[true, false]]), d(), "true,false\r\n"),
        (json!([[-42, "ok"]]), d(), "\"-42\",ok\r\n"),
        (json!([[null, 42]]), d(), ",42\r\n"),
        (json!([["hello, world", ""]]), d(), "\"hello, world\",\r\n"),
        (json!([[null, "a,b"]]), d(), ",\"a,b\"\r\n"),
        (json!([["", "a,b"]]), d(), ",\"a,b\"\r\n"),
        (json!([["1", "2"], ["3", "4"]]), CsvFormatOptions { newline_char: Some("\n".into()), ..d() }, "1,2\n3,4\n"),
        (json!([["a,b", "ok"]]), CsvFormatOptions { quote_char: Some("'".into()), ..d() }, "'a,b',ok\r\n"),
    ];
    for (input, options, expected) in cases {
        assert_eq!(format(input.clone(), options).await, expected, "{input}");
    }
}

#[tokio::test]
async fn format_batches_64_rows() {
    for (count, chunk_lines) in [
        (10, vec![10]),
        (63, vec![63]),
        (64, vec![64]),
        (65, vec![64, 1]),
        (70, vec![64, 6]),
    ] {
        let rows = Value::Array((0..count).map(|i| json!([i.to_string(), "x"])).collect());
        let output = collect_strings(csv_format_stream(values(rows), Default::default())).await;
        let lines: Vec<usize> = output
            .iter()
            .map(|c| c.split("\r\n").filter(|l| !l.is_empty()).count())
            .collect();
        assert_eq!(lines, chunk_lines, "{count}");
        let all = output.concat();
        let all: Vec<&str> = all.split("\r\n").filter(|l| !l.is_empty()).collect();
        assert_eq!(all[0], "0,x");
        assert_eq!(all[count - 1], format!("{},x", count - 1));
    }
}

#[tokio::test]
async fn format_from_objects() {
    for (headers, expected) in [
        (
            vec!["a", "b", "c", "d"],
            "a,b,c,d\r\n1,2,3,4\r\n1,2,3,4\r\n",
        ),
        (
            vec!["d", "c", "b", "a"],
            "d,c,b,a\r\n4,3,2,1\r\n4,3,2,1\r\n",
        ),
    ] {
        let input = values(
            json!([{"a": "1", "b": "2", "c": "3", "d": "4"}, {"a": "1", "b": "2", "c": "3", "d": "4"}]),
        );
        let rows = csv_object_to_array(
            input,
            CsvHeadersOptions {
                headers: headers.clone().into(),
            },
        );
        let header = CsvInjectHeaderOptions {
            header: headers.iter().map(|h| h.to_string()).collect(),
        };
        let stream = csv_format_stream(csv_inject_header_stream(rows, header), Default::default());
        assert_eq!(stream_to_string(stream, None).await.unwrap(), expected);
    }
}

// *** csvArrayToObject / csvObjectToArray *** //

#[tokio::test]
async fn array_to_object() {
    let options = |headers: Vec<&str>| CsvHeadersOptions {
        headers: headers.into(),
    };
    let output = collect(csv_array_to_object(
        values(json!([["1", "2", "3"]])),
        options(vec!["a", "b", "c"]),
    ))
    .await;
    assert_eq!(output, json!([{"a": "1", "b": "2", "c": "3"}]));
    // Reserved JS names are ordinary keys; missing columns are null.
    let output = collect(csv_array_to_object(
        values(json!([["v1"]])),
        options(vec!["__proto__", "constructor"]),
    ))
    .await;
    assert_eq!(output, json!([{"__proto__": "v1", "constructor": null}]));
    let keys: Vec<&String> = output[0].as_object().unwrap().keys().collect();
    assert_eq!(keys, ["__proto__", "constructor"]);

    let headers = Keys::lazy(|| vec!["x".into()]);
    let output = collect(csv_array_to_object(
        values(json!([["1"], ["2"]])),
        CsvHeadersOptions { headers },
    ))
    .await;
    assert_eq!(output, json!([{"x": "1"}, {"x": "2"}]));
}

#[tokio::test]
async fn object_to_array() {
    let options = CsvHeadersOptions {
        headers: vec!["a", "b", "c"].into(),
    };
    let output = collect(csv_object_to_array(
        values(json!([{"a": "1", "b": "2", "c": "3"}])),
        options,
    ))
    .await;
    assert_eq!(output, json!([["1", "2", "3"]]));
}

// *** helpers *** //

#[test]
fn number_helpers() {
    assert!(is_number_like("-1.5e+3"));
    assert!(!is_number_like("-"));
    assert!(!is_number_like("1."));
    assert!(!is_number_like(".5"));
    let n = |s: &str| number_value(js_string_to_number(s));
    assert_eq!(n(".5"), Some(json!(0.5)));
    assert_eq!(n("5."), Some(json!(5)));
    assert_eq!(n("+5"), Some(json!(5)));
    assert_eq!(n("0o7"), Some(json!(7)));
    assert_eq!(n(""), Some(json!(0)));
    assert!(js_string_to_number("-0x1F").is_nan());
    assert!(js_string_to_number("1e").is_nan());
    assert!(js_string_to_number("inf").is_nan());
    assert!(js_string_to_number("-Infinity").is_infinite());
    assert_eq!(number_value(-0.0), Some(json!(-0.0)));
    assert_eq!(number_value(f64::NAN), None);
}

#[test]
fn unescape_custom_handles_escapes() {
    assert_eq!(unescape_custom("a\\\\b\\\"c", "\\"), "a\\b\"c");
    assert_eq!(unescape_custom("a\\", "\\"), "a\\");
    assert_eq!(unescape_custom("plain", "\\"), "plain");
    assert_eq!(unescape_custom("\\é", "\\"), "é");
}
