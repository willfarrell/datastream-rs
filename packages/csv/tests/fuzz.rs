// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{
    create_readable_stream, stream_to_array, stream_to_string, DataStream, Map, Value,
};
use datastream_csv::{
    csv_coerce_values_stream, csv_detect_delimiters_stream, csv_detect_header_stream,
    csv_format_stream, csv_inject_header_stream, csv_object_to_array, csv_parse_stream,
    csv_quoted_parser, csv_remove_empty_rows_stream, csv_remove_malformed_rows_stream,
    csv_unquoted_parser, CsvCoerceValuesOptions, CsvDetectHeaderOptions, CsvHeadersOptions,
    CsvInjectHeaderOptions, CsvParseOptions, CsvRemoveEmptyRowsOptions,
    CsvRemoveMalformedRowsOptions, LazyString, ParserOptions,
};
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

/// Split `input` at the given char offsets (deduplicated, sorted, modulo length).
fn split(input: &str, cuts: &[usize]) -> Vec<String> {
    let chars: Vec<char> = input.chars().collect();
    let mut at: Vec<usize> = cuts.iter().map(|c| c % (chars.len() + 1)).collect();
    at.sort_unstable();
    at.dedup();
    let mut chunks = Vec::new();
    let mut prev = 0;
    for cut in at {
        chunks.push(chars[prev..cut].iter().collect());
        prev = cut;
    }
    chunks.push(chars[prev..].iter().collect());
    chunks
}

fn strings(chunks: Vec<String>) -> DataStream<String> {
    create_readable_stream(chunks)
}

// Text built from the characters CSV cares about.
fn csv_text(max: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop::sample::select(vec!["a", "é", ",", ";", "\"", "'", "\r", "\n", "\u{feff}"]),
        0..max,
    )
    .prop_map(|units| units.concat())
}

fn field() -> impl Strategy<Value = String> {
    prop_oneof![csv_text(8), ".{0,8}"]
}

fn rows() -> impl Strategy<Value = Vec<Vec<String>>> {
    prop::collection::vec(prop::collection::vec(field(), 1..6), 1..12)
}

fn parse(text: String, cuts: &[usize]) -> (Vec<Value>, Value) {
    let (stream, errors) =
        csv_parse_stream(strings(split(&text, cuts)), CsvParseOptions::default());
    let rows = block_on(stream_to_array(stream, None)).unwrap();
    (rows, errors.get())
}

fn format(rows: Vec<Value>) -> String {
    block_on(stream_to_string(
        csv_format_stream(create_readable_stream(rows), Default::default()),
        None,
    ))
    .unwrap()
}

#[derive(Debug, PartialEq)]
struct Pipeline {
    detected: Value,
    header: Value,
    rows: Vec<Value>,
    errors: Value,
}

// detect delimiters -> detect header -> parse, with the detected chars wired lazily.
fn run_pipeline(chunks: Vec<String>) -> Pipeline {
    let (text, detect) = csv_detect_delimiters_stream(strings(chunks), Default::default());
    let lazy = |pointer: &str| Some(LazyString::from_result(&detect, pointer));
    let (text, header) = csv_detect_header_stream(
        text,
        CsvDetectHeaderOptions {
            delimiter_char: lazy("/delimiterChar"),
            newline_char: lazy("/newlineChar"),
            quote_char: lazy("/quoteChar"),
            ..Default::default()
        },
    );
    let (stream, errors) = csv_parse_stream(
        text,
        CsvParseOptions {
            delimiter_char: lazy("/delimiterChar"),
            newline_char: lazy("/newlineChar"),
            quote_char: lazy("/quoteChar"),
            ..Default::default()
        },
    );
    let rows = block_on(stream_to_array(stream, None)).unwrap();
    Pipeline {
        detected: detect.get(),
        header: header.get()["header"].clone(),
        rows,
        errors: errors.get(),
    }
}

proptest! {
    #[test]
    fn fuzz_csv_quoted_parser(input in any::<String>(), flushing in any::<bool>()) {
        let _ = csv_quoted_parser(&input, &ParserOptions::default(), flushing);
    }

    #[test]
    fn fuzz_csv_quoted_parser_delimiter(input in csv_text(32), delimiter_char in ".{1,3}") {
        let options = ParserOptions { delimiter_char, ..Default::default() };
        let _ = csv_quoted_parser(&input, &options, true);
    }

    #[test]
    fn fuzz_csv_unquoted_parser(input in any::<String>(), delimiter_char in ".{1,3}") {
        let options = ParserOptions { delimiter_char: delimiter_char.clone(), ..Default::default() };
        let parsed = csv_unquoted_parser(&input, &options, true).unwrap();
        // Without quoting, rejoining rows and fields restores the input.
        let lines: Vec<String> = parsed.rows.iter().map(|row| row.join(&delimiter_char)).collect();
        let mut rejoined = lines.join("\r\n");
        if input.ends_with("\r\n") {
            rejoined.push_str("\r\n");
        }
        prop_assert_eq!(rejoined, input);
    }

    #[test]
    fn fuzz_csv_parse_stream_string(input in any::<String>(), cuts in prop::collection::vec(any::<usize>(), 0..6)) {
        let (stream, _) = csv_parse_stream(strings(split(&input, &cuts)), CsvParseOptions::default());
        let _ = block_on(stream_to_array(stream, None));
    }

    #[test]
    fn fuzz_csv_format_parse_roundtrip(input in rows(), cuts in prop::collection::vec(any::<usize>(), 0..6)) {
        let values: Vec<Value> = input.iter().map(|row| json!(row)).collect();
        let (output, errors) = parse(format(values.clone()), &cuts);
        prop_assert_eq!(output, values);
        prop_assert_eq!(errors, json!({}));
    }

    #[test]
    fn fuzz_csv_pipeline_independent_of_chunking(input in csv_text(60), cuts in prop::collection::vec(any::<usize>(), 0..10)) {
        let expected = run_pipeline(vec![input.clone()]);
        let actual = run_pipeline(split(&input, &cuts));
        // Detection only sees data buffered up to the first line, so it may
        // legitimately differ with chunking; compare the rest only when it matches.
        if actual.detected == expected.detected {
            prop_assert_eq!(actual, expected);
        }
    }

    #[test]
    fn fuzz_csv_detect_delimiters_stream(input in any::<String>(), cuts in prop::collection::vec(any::<usize>(), 0..6)) {
        let (stream, result) = csv_detect_delimiters_stream(strings(split(&input, &cuts)), Default::default());
        prop_assert_eq!(block_on(stream_to_string(stream, None)).unwrap(), input.clone());
        let detected = result.get();
        // Detection needs a complete first line.
        if input.contains(['\r', '\n']) {
            for key in ["delimiterChar", "newlineChar", "quoteChar"] {
                prop_assert!(detected[key].is_string(), "{} missing: {}", key, detected);
            }
        }
    }

    #[test]
    fn fuzz_csv_detect_header_stream(input in any::<String>(), cuts in prop::collection::vec(any::<usize>(), 0..6)) {
        let (stream, _) = csv_detect_header_stream(strings(split(&input, &cuts)), CsvDetectHeaderOptions::default());
        let _ = block_on(stream_to_array(stream, None));
    }

    #[test]
    fn fuzz_csv_remove_empty_rows_stream(input in prop::collection::vec(prop::collection::vec("a?", 0..5), 0..12)) {
        let values: Vec<Value> = input.iter().map(|row| json!(row)).collect();
        let (stream, result) = csv_remove_empty_rows_stream(create_readable_stream(values.clone()), CsvRemoveEmptyRowsOptions::default());
        let output = block_on(stream_to_array(stream, None)).unwrap();
        let expected: Vec<Value> = values
            .into_iter()
            .zip(&input)
            .filter(|(_, row)| row.iter().any(|f| !f.is_empty()))
            .map(|(v, _)| v)
            .collect();
        let removed = input.len() - expected.len();
        prop_assert_eq!(output, expected);
        let errors = result.get();
        prop_assert_eq!(errors["EmptyRow"]["idx"].as_array().map_or(0, Vec::len), removed);
    }

    #[test]
    fn fuzz_csv_remove_malformed_rows_stream(input in prop::collection::vec(prop::collection::vec(".{0,3}", 1..5), 1..12)) {
        let values: Vec<Value> = input.iter().map(|row| json!(row)).collect();
        let (stream, _) = csv_remove_malformed_rows_stream(create_readable_stream(values), CsvRemoveMalformedRowsOptions::default());
        let output = block_on(stream_to_array(stream, None)).unwrap();
        let expected = input[0].len();
        prop_assert_eq!(output.len(), input.iter().filter(|row| row.len() == expected).count());
        prop_assert!(output.iter().all(|row| row.as_array().unwrap().len() == expected));
    }

    #[test]
    fn fuzz_csv_coerce_values_stream(input in prop::collection::vec(prop::collection::btree_map("[a-c]", prop_oneof![".{0,8}", "-?[0-9]{1,3}(\\.[0-9]{1,3})?", Just("true".to_string()), Just("null".to_string())], 0..4), 1..8)) {
        let values: Vec<Value> = input
            .iter()
            .map(|row| Value::Object(row.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<Map<_, _>>()))
            .collect();
        let (stream, _) = csv_coerce_values_stream(create_readable_stream(values.clone()), CsvCoerceValuesOptions::default());
        let output = block_on(stream_to_array(stream, None)).unwrap();
        prop_assert_eq!(output.len(), values.len());
        for (before, after) in values.iter().zip(&output) {
            let keys = |v: &Value| v.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
            prop_assert_eq!(keys(before), keys(after));
        }
    }

    #[test]
    fn fuzz_csv_format_objects_via_compose(input in prop::collection::vec((field(), field(), field()), 1..12)) {
        let headers = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let objects: Vec<Value> = input.iter().map(|(a, b, c)| json!({"a": a, "b": b, "c": c})).collect();
        let rows = csv_object_to_array(create_readable_stream(objects), CsvHeadersOptions { headers: headers.clone().into() });
        let rows = csv_inject_header_stream(rows, CsvInjectHeaderOptions { header: headers.clone() });
        let text = block_on(stream_to_string(csv_format_stream(rows, Default::default()), None)).unwrap();
        let (output, _) = parse(text, &[]);
        let mut expected = vec![json!(headers)];
        expected.extend(input.iter().map(|(a, b, c)| json!([a, b, c])));
        prop_assert_eq!(output, expected);
    }
}
