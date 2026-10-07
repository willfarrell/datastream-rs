// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::HashMap;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{
    create_readable_stream, create_readable_stream_from_string, pipeline, stream_to_array,
    DataStream, Map, Value,
};
use datastream_csv::{
    csv_coerce_values_stream, csv_detect_delimiters_stream, csv_detect_header_stream,
    csv_format_stream, csv_inject_header_stream, csv_object_to_array, csv_parse_stream,
    csv_remove_empty_rows_stream, csv_remove_malformed_rows_stream, CsvCoerceType,
    CsvCoerceValuesOptions, CsvDetectHeaderOptions, CsvHeadersOptions, CsvInjectHeaderOptions,
    CsvParseOptions, CsvRemoveMalformedRowsOptions, Keys, LazyString,
};
use serde_json::json;
use tokio::runtime::Runtime;

// The JS bench uses 1M rows; 10K keeps `cargo bench` to a couple of minutes.
const ROWS: usize = 10_000;
const COLS: usize = 10;

fn header() -> Vec<String> {
    (0..COLS).map(|c| format!("col{c}")).collect()
}

fn csv_string(field: impl Fn(usize, usize) -> String, head: impl Fn(usize) -> String) -> String {
    let mut out = (0..COLS).map(head).collect::<Vec<_>>().join(",") + "\r\n";
    for r in 0..ROWS {
        out += &((0..COLS).map(|c| field(r, c)).collect::<Vec<_>>().join(",") + "\r\n");
    }
    out
}

fn text(input: &str) -> DataStream<String> {
    create_readable_stream_from_string(input.to_string(), None).unwrap()
}

fn benches(c: &mut Criterion) {
    let runtime = Runtime::new().unwrap();
    let simple = csv_string(|r, c| format!("val_{r}_{c}"), |c| format!("col{c}"));
    let quoted = csv_string(
        |r, c| format!("\"val \"\"{r}\"\" {c}\""),
        |c| format!("\"col{c}\""),
    );
    let objects: Vec<Value> = (0..ROWS)
        .map(|r| {
            Value::Object(
                (0..COLS)
                    .map(|c| (format!("col{c}"), json!(format!("val_{r}_{c}"))))
                    .collect::<Map<_, _>>(),
            )
        })
        .collect();
    let arrays: Vec<Value> = (0..ROWS)
        .map(|r| {
            json!((0..COLS)
                .map(|c| format!("val_{r}_{c}"))
                .collect::<Vec<_>>())
        })
        .collect();

    let pipe = |name: &str, c: &mut Criterion, f: &dyn Fn() -> DataStream<Value>| {
        c.bench_function(name, |b| {
            b.to_async(&runtime)
                .iter(|| async { pipeline(f(), &[]).await.unwrap() })
        });
    };

    for (name, input) in [("simple", &simple), ("quoted", &quoted)] {
        c.bench_function(&format!("csvParseStream, {name}"), |b| {
            b.to_async(&runtime).iter(|| async {
                let (stream, _) = csv_parse_stream(text(input), CsvParseOptions::default());
                stream_to_array(stream, None).await.unwrap()
            })
        });
    }

    c.bench_function("csvFormatStream, from objects", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows = csv_object_to_array(
                create_readable_stream(objects.clone()),
                CsvHeadersOptions {
                    headers: header().into(),
                },
            );
            let rows = csv_inject_header_stream(rows, CsvInjectHeaderOptions { header: header() });
            pipeline(csv_format_stream(rows, Default::default()), &[])
                .await
                .unwrap()
        })
    });
    c.bench_function("csvFormatStream, from arrays", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows = csv_inject_header_stream(
                create_readable_stream(arrays.clone()),
                CsvInjectHeaderOptions { header: header() },
            );
            pipeline(csv_format_stream(rows, Default::default()), &[])
                .await
                .unwrap()
        })
    });

    c.bench_function("csvDetectDelimitersStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let (stream, result) = csv_detect_delimiters_stream(text(&simple), Default::default());
            pipeline(stream, &[&result]).await.unwrap()
        })
    });
    c.bench_function("csvDetectHeaderStream", |b| {
        b.to_async(&runtime).iter(|| async {
            let (stream, result) = csv_detect_header_stream(text(&simple), Default::default());
            pipeline(stream, &[&result]).await.unwrap()
        })
    });

    let malformed: Vec<Value> = arrays
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let mut row = row.clone();
            if i % 100 == 0 {
                row.as_array_mut().unwrap().pop();
            }
            row
        })
        .collect();
    for (name, input) in [("all valid", &arrays), ("1% malformed", &malformed)] {
        pipe(&format!("csvRemoveMalformedRowsStream, {name}"), c, &|| {
            csv_remove_malformed_rows_stream(
                create_readable_stream(input.clone()),
                Default::default(),
            )
            .0
        });
    }

    let empty: Vec<Value> = arrays
        .iter()
        .enumerate()
        .map(|(i, row)| {
            if i % 100 == 0 {
                json!(vec![""; COLS])
            } else {
                row.clone()
            }
        })
        .collect();
    for (name, input) in [("all valid", &arrays), ("1% empty", &empty)] {
        pipe(&format!("csvRemoveEmptyRowsStream, {name}"), c, &|| {
            csv_remove_empty_rows_stream(create_readable_stream(input.clone()), Default::default())
                .0
        });
    }

    let auto: Vec<Value> = objects
        .iter()
        .map(|obj| {
            let mut obj = obj.clone();
            obj["num"] = json!("42");
            obj["bool"] = json!("true");
            obj
        })
        .collect();
    pipe("csvCoerceValuesStream, auto-coerce", c, &|| {
        csv_coerce_values_stream(create_readable_stream(auto.clone()), Default::default()).0
    });
    let explicit: Vec<Value> = objects
        .iter()
        .map(|obj| {
            let mut obj = obj.clone();
            obj["col1"] = json!("42");
            obj
        })
        .collect();
    let columns = HashMap::from([
        ("col0".to_string(), CsvCoerceType::String),
        ("col1".to_string(), CsvCoerceType::Number),
    ]);
    pipe("csvCoerceValuesStream, explicit types", c, &|| {
        csv_coerce_values_stream(
            create_readable_stream(explicit.clone()),
            CsvCoerceValuesOptions {
                columns: columns.clone(),
                ..Default::default()
            },
        )
        .0
    });

    pipe("full pipeline", c, &|| {
        let (stream, detect) = csv_detect_delimiters_stream(text(&simple), Default::default());
        let lazy = |pointer: &str| Some(LazyString::from_result(&detect, pointer));
        let (stream, header) = csv_detect_header_stream(
            stream,
            CsvDetectHeaderOptions {
                delimiter_char: lazy("/delimiterChar"),
                newline_char: lazy("/newlineChar"),
                quote_char: lazy("/quoteChar"),
                ..Default::default()
            },
        );
        let (stream, _) = csv_parse_stream(
            stream,
            CsvParseOptions {
                delimiter_char: lazy("/delimiterChar"),
                newline_char: lazy("/newlineChar"),
                quote_char: lazy("/quoteChar"),
                ..Default::default()
            },
        );
        let headers = Keys::lazy(move || {
            header.get()["header"]
                .as_array()
                .map(|h| {
                    h.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default()
        });
        let (stream, _) = csv_remove_malformed_rows_stream(
            stream,
            CsvRemoveMalformedRowsOptions {
                headers: Some(headers),
                ..Default::default()
            },
        );
        csv_remove_empty_rows_stream(stream, Default::default()).0
    });
}

criterion_group! {
    name = index;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2));
    targets = benches
}
criterion_main!(index);
