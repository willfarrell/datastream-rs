// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_charset::{
    charset_decode_stream, charset_detect_stream, charset_encode_stream, CharsetDetectOptions,
    CharsetOptions,
};
use datastream_core::{create_readable_stream, pipeline, stream_to_buffer, stream_to_string};
use proptest::prelude::*;

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

fn split(data: &[u8], sizes: &[usize]) -> Vec<Vec<u8>> {
    let mut sizes = sizes.iter().cycle();
    let mut rest = data;
    let mut chunks = Vec::new();
    while !rest.is_empty() {
        let (head, tail) = rest.split_at((*sizes.next().unwrap()).min(rest.len()));
        chunks.push(head.to_vec());
        rest = tail;
    }
    chunks
}

const CHARSETS: [&str; 11] = [
    "UTF-8",
    "UTF-16LE",
    "UTF-16BE",
    "ISO-8859-1",
    "windows-1252",
    "Shift_JIS",
    "EUC-JP",
    "GB18030",
    "Big5",
    "EUC-KR",
    "KOI8-R",
];

fn options(charset: &str) -> CharsetOptions {
    CharsetOptions {
        charset: Some(charset.to_string()),
    }
}

async fn decode(chunks: Vec<Vec<u8>>, charset: &str) -> String {
    stream_to_string(
        charset_decode_stream(create_readable_stream(chunks), options(charset)),
        None,
    )
    .await
    .unwrap()
}

proptest! {
    #[test]
    fn fuzz_detect_random_bytes(chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..256), 0..8)) {
        let empty = chunks.iter().all(Vec::is_empty);
        let value = block_on(async {
            let (stream, result) = charset_detect_stream(create_readable_stream(chunks), CharsetDetectOptions::default());
            pipeline(stream, &[&result]).await?;
            Ok::<_, datastream_core::Error>(result.get())
        })
        .unwrap();
        prop_assert_eq!(value["charset"].is_null(), empty);
        let confidence = value["confidence"].as_u64().unwrap();
        prop_assert!(confidence <= 100);
    }

    #[test]
    fn fuzz_unicode_roundtrip(
        text in ".{0,256}",
        charset in prop::sample::select(&CHARSETS[..3]),
        sizes in prop::collection::vec(1usize..16, 1..8),
    ) {
        // The decoder strips a leading BOM.
        prop_assume!(!text.starts_with('\u{feff}'));
        let decoded = block_on(async {
            let bytes = stream_to_buffer(
                charset_encode_stream(create_readable_stream(vec![text.clone()]), options(charset)),
                None,
            )
            .await
            .unwrap();
            decode(split(&bytes, &sizes), charset).await
        });
        prop_assert_eq!(decoded, text);
    }

    #[test]
    fn fuzz_decode_is_chunking_independent(
        data in prop::collection::vec(any::<u8>(), 0..512),
        charset in prop::sample::select(&CHARSETS[..]),
        sizes in prop::collection::vec(1usize..16, 1..8),
    ) {
        let chunked = block_on(decode(split(&data, &sizes), charset));
        let whole = block_on(decode(vec![data], charset));
        prop_assert_eq!(chunked, whole);
    }

    #[test]
    fn fuzz_encode_any_label(text in ".{0,64}", charset in ".{0,16}") {
        // Unknown labels fall back to UTF-8; nothing panics.
        block_on(stream_to_buffer(
            charset_encode_stream(create_readable_stream(vec![text]), options(&charset)),
            None,
        ))
        .unwrap();
    }
}
