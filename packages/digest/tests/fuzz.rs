// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{create_readable_stream, pipeline, Value};
use datastream_digest::{digest_stream, DigestOptions};
use digest::Digest;
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

fn one_shot(algorithm: &str, data: &[u8]) -> String {
    let (name, hex) = match algorithm {
        "SHA2-256" | "SHA256" => ("SHA2-256", hex::encode(sha2::Sha256::digest(data))),
        "SHA2-384" | "SHA384" => ("SHA2-384", hex::encode(sha2::Sha384::digest(data))),
        "SHA2-512" | "SHA512" => ("SHA2-512", hex::encode(sha2::Sha512::digest(data))),
        "SHA3-256" => ("SHA3-256", hex::encode(sha3::Sha3_256::digest(data))),
        "SHA3-384" => ("SHA3-384", hex::encode(sha3::Sha3_384::digest(data))),
        _ => ("SHA3-512", hex::encode(sha3::Sha3_512::digest(data))),
    };
    format!("{name}:{hex}")
}

const ALGORITHMS: [&str; 9] = [
    "SHA2-256", "SHA2-384", "SHA2-512", "SHA3-256", "SHA3-384", "SHA3-512", "SHA256", "SHA384",
    "SHA512",
];

proptest! {
    #[test]
    fn fuzz_digest_matches_one_shot(
        algorithm in prop::sample::select(&ALGORITHMS[..]),
        data in prop::collection::vec(any::<u8>(), 0..2048),
        sizes in prop::collection::vec(1usize..128, 1..8),
    ) {
        let digest = block_on(async {
            let options = DigestOptions { algorithm: algorithm.to_string(), result_key: None };
            let (stream, result) = digest_stream(create_readable_stream(split(&data, &sizes)), options)?;
            pipeline(stream, &[&result]).await?;
            Ok::<_, datastream_core::Error>(result.get())
        })
        .unwrap();
        prop_assert_eq!(digest, Value::String(one_shot(algorithm, &data)));
    }

    #[test]
    fn fuzz_digest_passes_chunks_through(
        chunks in prop::collection::vec(".{0,32}", 0..16),
    ) {
        let out = block_on(async {
            let options = DigestOptions { algorithm: "SHA256".into(), result_key: None };
            let (stream, _) = digest_stream(create_readable_stream(chunks.clone()), options)?;
            datastream_core::stream_to_array(stream, None).await
        })
        .unwrap();
        prop_assert_eq!(out, chunks);
    }

    #[test]
    fn fuzz_digest_rejects_unknown_algorithm(algorithm in "[A-Za-z0-9-]{0,12}") {
        prop_assume!(!ALGORITHMS.contains(&algorithm.as_str()));
        let options = DigestOptions { algorithm, result_key: None };
        prop_assert!(digest_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options).is_err());
    }
}
