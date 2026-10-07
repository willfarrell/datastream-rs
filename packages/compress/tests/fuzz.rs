// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_compress::*;
use datastream_core::{create_readable_stream, stream_to_buffer, DataStream};
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

type Compress = fn(DataStream<Vec<u8>>, CompressOptions) -> DataStream<Vec<u8>>;
type Decompress = fn(DataStream<Vec<u8>>, DecompressOptions) -> DataStream<Vec<u8>>;

// (name, compress, decompress, quality range)
const CODECS: [(&str, Compress, Decompress, (i32, i32)); 4] = [
    (
        "gzip",
        gzip_compress_stream,
        gzip_decompress_stream,
        (-1, 9),
    ),
    (
        "deflate",
        deflate_compress_stream,
        deflate_decompress_stream,
        (-1, 9),
    ),
    (
        "brotli",
        brotli_compress_stream,
        brotli_decompress_stream,
        (0, 11),
    ),
    (
        "zstd",
        zstd_compress_stream,
        zstd_decompress_stream,
        (1, 19),
    ),
];

/// Compressible-ish bytes: random runs drawn from a small alphabet, or raw bytes.
fn payload() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        prop::collection::vec(any::<u8>(), 0..2048),
        prop::collection::vec(prop::sample::select(b"abc{}\":, 0123".to_vec()), 0..4096),
    ]
}

proptest! {
    #[test]
    fn fuzz_roundtrip(
        codec in 0..CODECS.len(),
        quality_seed in any::<u8>(),
        data in payload(),
        sizes in prop::collection::vec(1usize..256, 1..8),
        resplit in prop::collection::vec(1usize..64, 1..8),
    ) {
        let (_, compress, decompress, (lo, hi)) = CODECS[codec];
        let quality = lo + i32::from(quality_seed) % (hi - lo + 1);
        let out = block_on(async {
            let compressed = stream_to_buffer(
                compress(
                    create_readable_stream(split(&data, &sizes)),
                    CompressOptions { quality: Some(quality), ..Default::default() },
                ),
                None,
            )
            .await?;
            stream_to_buffer(
                decompress(create_readable_stream(split(&compressed, &resplit)), DecompressOptions::default()),
                None,
            )
            .await
        })
        .unwrap();
        prop_assert_eq!(out, data);
    }

    #[test]
    fn fuzz_decompress_garbage(
        codec in 0..CODECS.len(),
        data in prop::collection::vec(any::<u8>(), 0..1024),
        sizes in prop::collection::vec(1usize..64, 1..8),
    ) {
        let (_, _, decompress, _) = CODECS[codec];
        let max = 64 * 1024;
        // Errors are fine; panics and unbounded output are not.
        if let Ok(out) = block_on(stream_to_buffer(
            decompress(
                create_readable_stream(split(&data, &sizes)),
                DecompressOptions { max_output_size: Some(max) },
            ),
            None,
        )) {
            prop_assert!(out.len() <= max);
        }
    }
}
