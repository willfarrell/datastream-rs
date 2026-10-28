// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Base64 encoding and decoding streams, ported from `@datastream/base64`.

use async_stream::try_stream;
use base64::alphabet::STANDARD as ALPHABET;
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig, STANDARD};
use base64::Engine as _;
use datastream_core::{DataStream, Error, Result, StreamExt};

// Lenient like node's `Buffer.from(s, "base64")` about unused trailing bits.
const DECODER: GeneralPurpose = GeneralPurpose::new(
    &ALPHABET,
    GeneralPurposeConfig::new().with_decode_allow_trailing_bits(true),
);

/// Quote at most 32 chars of the offending input: a multi-megabyte bad chunk
/// must not be copied into the error message (and on into logs).
fn quote(s: &str) -> String {
    match s.char_indices().nth(32) {
        Some((end, _)) => format!("{:?}", format!("{}…", &s[..end])),
        None => format!("{s:?}"),
    }
}

/// Length must be a multiple of 4, with only alphabet chars followed by at
/// most 2 `=`. Rejects fragments like `YQ=` and standalone padding like `==`.
fn assert_valid_base64(s: &str) -> Result<()> {
    let body = s.trim_end_matches('=');
    let valid = s.len().is_multiple_of(4)
        && s.len() - body.len() <= 2
        && body
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/');
    if valid {
        Ok(())
    } else {
        Err(format!("Invalid base64 string: {}", quote(s)).into())
    }
}

/// A padded quartet split from the data after it (`["YQ==", "YWJj"]`) must
/// fail like the same text in one chunk (`"YQ==YWJj"`) does.
fn assert_not_after_padding(padded: bool, s: &str) -> Result<()> {
    if padded {
        Err(format!("Invalid base64 string: {} follows \"=\" padding", quote(s)).into())
    } else {
        Ok(())
    }
}

/// Encode bytes (or strings) to base64, carrying partial 3-byte groups
/// across chunks so the concatenated output is a single valid encoding.
pub fn base64_encode_stream<T>(mut input: DataStream<T>) -> DataStream<String>
where
    T: AsRef<[u8]> + Send + 'static,
{
    Box::pin(try_stream! {
        let mut extra: Vec<u8> = Vec::new();
        while let Some(chunk) = input.next().await {
            extra.extend_from_slice(chunk?.as_ref());
            let whole = extra.len() - extra.len() % 3;
            if whole > 0 {
                yield STANDARD.encode(&extra[..whole]);
                extra.drain(..whole);
            }
        }
        if !extra.is_empty() {
            yield STANDARD.encode(&extra);
        }
    })
}

/// Decode base64 text (as strings or bytes), carrying partial 4-char groups
/// across chunks. Invalid input or a trailing incomplete group errors.
pub fn base64_decode_stream<T>(mut input: DataStream<T>) -> DataStream<Vec<u8>>
where
    T: AsRef<[u8]> + Send + 'static,
{
    Box::pin(try_stream! {
        let mut extra = String::new();
        // "=" ends a base64 stream; set once a quartet with padding is decoded.
        let mut padded = false;
        while let Some(chunk) = input.next().await {
            extra.push_str(&String::from_utf8_lossy(chunk?.as_ref()));
            // Validation rejects non-ASCII, so a non-boundary split only
            // ever happens on input that is about to error.
            let mut whole = extra.len() - extra.len() % 4;
            while !extra.is_char_boundary(whole) {
                whole -= 1;
            }
            if whole > 0 {
                let s: String = extra.drain(..whole).collect();
                assert_not_after_padding(padded, &s)?;
                assert_valid_base64(&s)?;
                padded = s.ends_with('=');
                yield DECODER.decode(&s).map_err(Error::from)?;
            }
        }
        assert_valid_base64(&extra)?;
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline, stream_to_array, stream_to_buffer};

    fn strings(chunks: &[&str]) -> DataStream<String> {
        create_readable_stream(chunks.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    async fn encode(chunks: Vec<Vec<u8>>) -> Vec<String> {
        stream_to_array(base64_encode_stream(create_readable_stream(chunks)), None)
            .await
            .unwrap()
    }

    async fn decode(chunks: &[&str]) -> Result<Vec<u8>> {
        stream_to_buffer(base64_decode_stream(strings(chunks)), None).await
    }

    #[tokio::test]
    async fn encode_string() {
        let out = stream_to_array(base64_encode_stream(strings(&["encode"])), None)
            .await
            .unwrap();
        assert_eq!(out.concat(), STANDARD.encode("encode"));
    }

    #[tokio::test]
    async fn decode_string() {
        assert_eq!(
            decode(&[STANDARD.encode("decode").as_str()]).await.unwrap(),
            b"decode"
        );
    }

    #[tokio::test]
    async fn round_trip_lengths() {
        for i in 1..=16 {
            let input = "x".repeat(i);
            let stream = base64_decode_stream(base64_encode_stream(strings(&[input.as_str()])));
            assert_eq!(
                stream_to_buffer(stream, None).await.unwrap(),
                input.as_bytes()
            );
        }
    }

    #[tokio::test]
    async fn encode_carries_remainders_across_chunks() {
        let out = encode(vec![b"aaaa".to_vec(), b"bbbb".to_vec(), b"cccc".to_vec()]).await;
        assert_eq!(out.concat(), STANDARD.encode("aaaabbbbcccc"));
        let out = encode(vec![
            vec![1, 2, 3, 4],
            vec![5, 6, 7, 8],
            vec![9, 10, 11, 12],
        ])
        .await;
        assert_eq!(
            out.concat(),
            STANDARD.encode([1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12])
        );
        assert_eq!(encode(vec![b"a".to_vec()]).await, ["YQ=="]);
        assert_eq!(encode(vec![b"ab".to_vec()]).await, ["YWI="]);
    }

    #[tokio::test]
    async fn encode_chunk_shape() {
        assert_eq!(encode(vec![b"aaaa".to_vec()]).await, ["YWFh", "YQ=="]);
        assert_eq!(encode(vec![b"aaa".to_vec()]).await, ["YWFh"]);
        assert_eq!(encode(vec![b"a".to_vec()]).await, ["YQ=="]);
        assert!(encode(vec![]).await.is_empty());
    }

    #[tokio::test]
    async fn decode_carries_remainders_across_chunks() {
        let input = STANDARD.encode("aaaabbbbcccc");
        let out = decode(&[&input[..4], &input[4..8], &input[8..]])
            .await
            .unwrap();
        assert_eq!(out, b"aaaabbbbcccc");
        let input = STANDARD.encode("hello");
        assert_eq!(decode(&[&input[..2], &input[2..]]).await.unwrap(), b"hello");
        assert_eq!(decode(&["YW", "I="]).await.unwrap(), b"ab");
        let input = STANDARD.encode("Hello World!");
        assert_eq!(
            decode(&[&input[..6], &input[6..]]).await.unwrap(),
            b"Hello World!"
        );
    }

    #[tokio::test]
    async fn binary_round_trip() {
        let bytes: Vec<u8> = (0..=255).collect();
        assert_eq!(
            encode(vec![bytes.clone()]).await.concat(),
            STANDARD.encode(&bytes)
        );
        assert_eq!(
            decode(&[STANDARD.encode(&bytes).as_str()]).await.unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn decode_byte_chunks() {
        let b64 = STANDARD.encode("Hello, World!").into_bytes();
        let stream = base64_decode_stream(create_readable_stream([b64]));
        assert_eq!(
            stream_to_buffer(stream, None).await.unwrap(),
            b"Hello, World!"
        );
    }

    #[tokio::test]
    async fn decode_rejects_malformed() {
        for input in [
            "YQ=", "Pw=", "QQ=", "==", "AAAA==", "!AAA", "AAA!", "A===", "é",
        ] {
            assert!(
                decode(&[input]).await.is_err(),
                "{input} should be rejected"
            );
        }
        let e = pipeline(base64_decode_stream(strings(&["!AAA"])), &[])
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Invalid base64 string: \"!AAA\"");
    }

    #[tokio::test]
    async fn decode_rejects_data_after_padding_in_later_chunk() {
        let e = decode(&["YWJjYQ==", "ZGVm"]).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "Invalid base64 string: \"ZGVm\" follows \"=\" padding"
        );
        // Same text in one chunk is rejected by the alphabet check.
        assert!(decode(&["YQ==YWJj"]).await.is_err());
        // Padding in the final quartet of a later chunk is still fine.
        assert_eq!(decode(&["YWJj", "YQ=="]).await.unwrap(), b"abca");
    }

    #[tokio::test]
    async fn decode_emits_one_chunk_per_aligned_input_chunk() {
        let out = stream_to_array(base64_decode_stream(strings(&["YWJj", "ZGVm"])), None)
            .await
            .unwrap();
        assert_eq!(out, [b"abc".to_vec(), b"def".to_vec()]);
    }

    #[tokio::test]
    async fn decode_error_quotes_at_most_32_chars() {
        let bad = format!("!{}", "A".repeat(31));
        let e = decode(&[bad.as_str()]).await.unwrap_err();
        assert_eq!(e.to_string(), format!("Invalid base64 string: \"{bad}\""));

        let bad = format!("!{}", "A".repeat(35));
        let e = decode(&[bad.as_str()]).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            format!("Invalid base64 string: \"!{}…\"", "A".repeat(31))
        );

        let long = "B".repeat(36);
        let e = decode(&["YQ==", long.as_str()]).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            format!(
                "Invalid base64 string: \"{}…\" follows \"=\" padding",
                "B".repeat(32)
            )
        );
    }

    #[tokio::test]
    async fn decode_rejects_high_bit_bytes() {
        let stream = base64_decode_stream(create_readable_stream([vec![0xD9, 0x57, 0x4A, 0x6A]]));
        let e = stream_to_buffer(stream, None).await.unwrap_err();
        assert!(e.to_string().starts_with("Invalid base64 string"));
    }

    #[tokio::test]
    async fn encode_non_ascii_string_as_utf8() {
        let out = stream_to_array(base64_encode_stream(strings(&["é"])), None)
            .await
            .unwrap();
        assert_eq!(out.concat(), STANDARD.encode("é"));
    }

    #[tokio::test]
    async fn decode_accepts_padding() {
        assert_eq!(decode(&["YQ=="]).await.unwrap(), [0x61]);
        assert_eq!(decode(&["YWI="]).await.unwrap(), [0x61, 0x62]);
        assert_eq!(decode(&["AAAA"]).await.unwrap(), [0, 0, 0]);
        // Unused trailing bits are tolerated, like node.
        assert_eq!(decode(&["YR=="]).await.unwrap(), [0x61]);
    }

    #[tokio::test]
    async fn decode_chunk_shape() {
        // An incomplete quartet emits nothing before erroring at the end.
        let mut stream = base64_decode_stream(strings(&["YQ"]));
        let first = stream.next().await.unwrap();
        assert_eq!(
            first.unwrap_err().to_string(),
            "Invalid base64 string: \"YQ\""
        );
        let out = stream_to_array(base64_decode_stream(strings(&["YWJj"])), None)
            .await
            .unwrap();
        assert_eq!(out, [b"abc".to_vec()]);
    }
}
