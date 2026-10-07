// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use datastream_base64::{base64_decode_stream, base64_encode_stream};
use datastream_core::{create_readable_stream, stream_to_buffer, stream_to_string};
use proptest::prelude::*;

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
}

/// Split `data` into chunks whose sizes cycle through `sizes`.
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

proptest! {
    #[test]
    fn fuzz_encode_matches_one_shot(
        data in prop::collection::vec(any::<u8>(), 0..1024),
        sizes in prop::collection::vec(1usize..64, 1..8),
    ) {
        let encoded = block_on(stream_to_string(
            base64_encode_stream(create_readable_stream(split(&data, &sizes))),
            None,
        ))
        .unwrap();
        prop_assert_eq!(encoded, STANDARD.encode(&data));
    }

    #[test]
    fn fuzz_roundtrip_encode_decode(
        data in prop::collection::vec(any::<u8>(), 0..1024),
        sizes in prop::collection::vec(1usize..64, 1..8),
        resplit in prop::collection::vec(1usize..64, 1..8),
    ) {
        let decoded = block_on(async {
            let encoded = stream_to_string(
                base64_encode_stream(create_readable_stream(split(&data, &sizes))),
                None,
            )
            .await?;
            let chunks = split(encoded.as_bytes(), &resplit);
            stream_to_buffer(base64_decode_stream(create_readable_stream(chunks)), None).await
        })
        .unwrap();
        prop_assert_eq!(decoded, data);
    }

    #[test]
    fn fuzz_decode_arbitrary_input(
        text in prop_oneof![".{0,256}", "[A-Za-z0-9+/=]{0,64}"],
        sizes in prop::collection::vec(1usize..64, 1..8),
    ) {
        let chunked = block_on(stream_to_buffer(
            base64_decode_stream(create_readable_stream(split(text.as_bytes(), &sizes))),
            None,
        ));
        let whole = block_on(stream_to_buffer(
            base64_decode_stream(create_readable_stream(vec![text.clone()])),
            None,
        ));
        // Valid input decodes the same however it is chunked.
        if let (Ok(chunked), Ok(whole)) = (&chunked, &whole) {
            prop_assert_eq!(chunked, whole);
        }
        prop_assert_eq!(whole.is_ok(), STANDARD.decode(&text).is_ok() || lenient_ok(&text));
    }
}

// The stream decoder allows non-zero trailing bits, which STANDARD rejects.
fn lenient_ok(text: &str) -> bool {
    base64::engine::GeneralPurpose::new(
        &base64::alphabet::STANDARD,
        base64::engine::GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
    )
    .decode(text)
    .is_ok()
}
