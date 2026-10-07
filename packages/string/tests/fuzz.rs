// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{create_readable_stream, pipeline, stream_to_array, DataStream};
use datastream_string::{
    string_count_stream, string_length_stream, string_minimum_chunk_size,
    string_minimum_first_chunk_size, string_replace_stream, string_skip_consecutive_duplicates,
    string_split_stream, StringCountOptions, StringLengthOptions, StringMinimumChunkSizeOptions,
    StringReplaceOptions, StringSplitOptions,
};
use proptest::prelude::*;
use serde_json::json;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn collect(stream: DataStream<String>) -> Vec<String> {
    block_on(stream_to_array(stream, None)).unwrap()
}

// A small alphabet (with a multi-byte char) so patterns actually match,
// mixed with arbitrary strings.
fn text() -> impl Strategy<Value = String> {
    prop_oneof!["[abé]{0,8}", any::<String>()]
}

fn chunks() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec(text(), 1..8)
}

fn needle() -> impl Strategy<Value = String> {
    prop_oneof!["[abé]{1,3}", ".{1,4}"]
}

// Overlapping occurrences, advancing one char at a time.
fn count_overlapping(haystack: &str, needle: &str) -> u64 {
    haystack
        .char_indices()
        .filter(|(i, _)| haystack[*i..].starts_with(needle))
        .count() as u64
}

proptest! {
    #[test]
    fn fuzz_string_length_stream(input in prop::collection::vec(text(), 0..8)) {
        let (stream, result) =
            string_length_stream(create_readable_stream(input.clone()), StringLengthOptions::default());
        let output = block_on(pipeline(stream, &[&result])).unwrap();
        prop_assert_eq!(&output["length"], &json!(input.concat().chars().count()));
    }

    #[test]
    fn fuzz_string_count_stream(input in chunks(), substr in needle()) {
        let (stream, result) = string_count_stream(
            create_readable_stream(input.clone()),
            StringCountOptions { substr: substr.clone(), ..Default::default() },
        )
        .unwrap();
        let output = block_on(pipeline(stream, &[&result])).unwrap();
        prop_assert_eq!(&output["count"], &json!(count_overlapping(&input.concat(), &substr)));
    }

    #[test]
    fn fuzz_string_skip_consecutive_duplicates(input in prop::collection::vec("[ab]{0,2}", 0..16)) {
        let output = collect(string_skip_consecutive_duplicates(create_readable_stream(input.clone())));
        let mut expected = input;
        expected.dedup();
        prop_assert_eq!(output, expected);
    }

    #[test]
    fn fuzz_string_replace_stream(input in chunks(), pattern in needle(), replacement in text()) {
        let output = collect(string_replace_stream(
            create_readable_stream(input.clone()),
            StringReplaceOptions {
                pattern: pattern.clone().into(),
                replacement: replacement.clone(),
                ..Default::default()
            },
        ));
        prop_assert_eq!(output.concat(), input.concat().replace(&pattern, &replacement));
    }

    #[test]
    fn fuzz_string_split_stream(input in chunks(), separator in needle()) {
        let output = collect(
            string_split_stream(
                create_readable_stream(input.clone()),
                StringSplitOptions { separator: separator.clone(), ..Default::default() },
            )
            .unwrap(),
        );
        let joined = input.concat();
        let expected: Vec<&str> = joined.split(separator.as_str()).collect();
        prop_assert_eq!(output, expected);
    }

    #[test]
    fn fuzz_string_minimum_first_chunk_size(input in chunks(), chunk_size in 0usize..64) {
        let output = collect(string_minimum_first_chunk_size(
            create_readable_stream(input.clone()),
            StringMinimumChunkSizeOptions { chunk_size: Some(chunk_size) },
        ));
        prop_assert_eq!(output.concat(), input.concat());
        if output.len() > 1 {
            prop_assert!(output[0].len() >= chunk_size);
        }
    }

    #[test]
    fn fuzz_string_minimum_chunk_size(input in chunks(), chunk_size in 0usize..64) {
        let output = collect(string_minimum_chunk_size(
            create_readable_stream(input.clone()),
            StringMinimumChunkSizeOptions { chunk_size: Some(chunk_size) },
        ));
        prop_assert_eq!(output.concat(), input.concat());
        if let Some((_, init)) = output.split_last() {
            prop_assert!(init.iter().all(|chunk| chunk.len() >= chunk_size));
        }
    }
}
