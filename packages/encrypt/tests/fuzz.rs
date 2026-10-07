// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{create_readable_stream, stream_to_buffer, Result};
use datastream_encrypt::{
    decrypt_stream, encrypt_stream, Algorithm, DecryptOptions, EncryptOptions,
};
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

const ALGORITHMS: [Algorithm; 5] = [
    Algorithm::Aes128Gcm,
    Algorithm::Aes256Gcm,
    Algorithm::Aes128Ctr,
    Algorithm::Aes256Ctr,
    Algorithm::Chacha20Poly1305,
];

const AEAD: [Algorithm; 3] = [
    Algorithm::Aes128Gcm,
    Algorithm::Aes256Gcm,
    Algorithm::Chacha20Poly1305,
];
const CTR: [Algorithm; 2] = [Algorithm::Aes128Ctr, Algorithm::Aes256Ctr];

fn key_len(algorithm: Algorithm) -> usize {
    match algorithm {
        Algorithm::Aes128Gcm | Algorithm::Aes128Ctr => 16,
        _ => 32,
    }
}

fn is_aead(algorithm: Algorithm) -> bool {
    !matches!(algorithm, Algorithm::Aes128Ctr | Algorithm::Aes256Ctr)
}

fn bytes(value: &serde_json::Value) -> Option<Vec<u8>> {
    value
        .as_array()
        .map(|a| a.iter().map(|b| b.as_u64().unwrap() as u8).collect())
}

#[derive(Debug)]
struct Case {
    algorithm: Algorithm,
    key: Vec<u8>,
    aad: Option<Vec<u8>>,
    data: Vec<u8>,
    sizes: Vec<usize>,
}

fn case(algorithms: &'static [Algorithm]) -> impl Strategy<Value = Case> {
    (
        prop::sample::select(algorithms),
        prop::collection::vec(any::<u8>(), 32),
        prop::option::of(prop::collection::vec(any::<u8>(), 0..32)),
        prop::collection::vec(any::<u8>(), 0..1024),
        prop::collection::vec(1usize..128, 1..8),
    )
        .prop_map(|(algorithm, mut key, aad, data, sizes)| {
            key.truncate(key_len(algorithm));
            let aad = aad.filter(|_| is_aead(algorithm));
            Case {
                algorithm,
                key,
                aad,
                data,
                sizes,
            }
        })
}

/// Encrypt `case.data` chunked by `case.sizes`; returns (ciphertext, iv, tag).
async fn encrypt(case: &Case) -> Result<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)> {
    let options = EncryptOptions {
        algorithm: case.algorithm,
        key: case.key.clone(),
        aad: case.aad.clone(),
        ..Default::default()
    };
    let (stream, result) = encrypt_stream(
        create_readable_stream(split(&case.data, &case.sizes)),
        options,
    )?;
    let ciphertext = stream_to_buffer(stream, None).await?;
    let value = result.get();
    Ok((
        ciphertext,
        bytes(&value["iv"]).unwrap(),
        bytes(&value["authTag"]),
    ))
}

async fn decrypt(
    case: &Case,
    key: Vec<u8>,
    iv: Vec<u8>,
    tag: Option<Vec<u8>>,
    chunks: Vec<Vec<u8>>,
) -> Result<Vec<u8>> {
    let options = DecryptOptions {
        algorithm: case.algorithm,
        key,
        iv,
        auth_tag: tag,
        aad: case.aad.clone(),
        ..Default::default()
    };
    stream_to_buffer(
        decrypt_stream(create_readable_stream(chunks), options)?,
        None,
    )
    .await
}

proptest! {
    #[test]
    fn fuzz_roundtrip(case in case(&ALGORITHMS), resplit in prop::collection::vec(1usize..128, 1..8)) {
        let plaintext = block_on(async {
            let (ciphertext, iv, tag) = encrypt(&case).await.unwrap();
            prop_assert_eq!(ciphertext.len(), case.data.len());
            prop_assert_eq!(tag.is_some(), is_aead(case.algorithm));
            Ok(decrypt(&case, case.key.clone(), iv, tag, split(&ciphertext, &resplit)).await.unwrap())
        })?;
        prop_assert_eq!(plaintext, case.data);
    }

    #[test]
    fn fuzz_wrong_key_or_tampered_ciphertext_fails(case in case(&AEAD), flip in any::<prop::sample::Index>()) {
        block_on(async {
            let (ciphertext, iv, tag) = encrypt(&case).await.unwrap();
            let mut wrong_key = case.key.clone();
            let i = flip.index(wrong_key.len());
            wrong_key[i] ^= 1;
            let result = decrypt(&case, wrong_key, iv.clone(), tag.clone(), vec![ciphertext.clone()]).await;
            prop_assert!(result.is_err());
            if !ciphertext.is_empty() {
                let mut tampered = ciphertext;
                let i = flip.index(tampered.len());
                tampered[i] ^= 1;
                let result = decrypt(&case, case.key.clone(), iv, tag, vec![tampered]).await;
                prop_assert!(result.is_err());
            }
            Ok(())
        })?;
    }

    #[test]
    fn fuzz_ctr_is_chunking_independent(case in case(&CTR), resplit in prop::collection::vec(1usize..128, 1..8)) {
        let (chunked, iv, _) = block_on(encrypt(&case)).unwrap();
        let whole = block_on(async {
            let options = EncryptOptions {
                algorithm: case.algorithm,
                key: case.key.clone(),
                iv: Some(iv),
                ..Default::default()
            };
            let chunks = split(&case.data, &resplit);
            stream_to_buffer(encrypt_stream(create_readable_stream(chunks), options)?.0, None).await
        })
        .unwrap();
        prop_assert_eq!(chunked, whole);
    }
}
