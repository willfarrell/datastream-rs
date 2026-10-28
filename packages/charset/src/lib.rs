// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Character encoding detection, decoding, and encoding streams, ported from
//! `@datastream/charset`. Charsets are WHATWG labels (via `encoding_rs`);
//! a missing charset means UTF-8, an unknown one is an error.

use async_stream::try_stream;
use datastream_core::{create_pass_through_stream, DataStream, Result, StreamExt, StreamResult};
use encoding_rs::{CoderResult, EncoderResult, Encoding, REPLACEMENT, UTF_16BE, UTF_16LE, UTF_8};
use serde_json::json;
use std::sync::{Arc, Mutex};

// Cap the detection sample so we never buffer an unbounded amount of the stream.
const MAX_DETECTION_SAMPLE: usize = 64 * 1024;

// chardetng has no score: certain answers (BOM, valid UTF-8) report 100,
// statistical guesses report this.
const GUESS_CONFIDENCE: u32 = 50;

#[derive(Default, Clone, Debug)]
pub struct CharsetDetectOptions {
    pub result_key: Option<String>,
}

#[derive(Default, Clone, Debug)]
pub struct CharsetOptions {
    pub charset: Option<String>,
}

/// A missing charset means UTF-8; a given but unknown one is an error, not a
/// silent UTF-8 fallback that would mis-decode/encode the data. `encoding_rs`
/// resolves labels itself (including ISO-8859-8-I). Labels that WHATWG maps
/// to the "replacement" encoding (ISO-2022-KR, HZ-GB-2312, ...) can't be
/// decoded or encoded, so they are unsupported too.
fn encoding(charset: Option<&str>, name: &str) -> Result<&'static Encoding> {
    let Some(label) = charset else {
        return Ok(UTF_8);
    };
    match Encoding::for_label(label.as_bytes()) {
        Some(encoding) if encoding != REPLACEMENT => Ok(encoding),
        _ => Err(format!("{name}: Unsupported encoding \"{label}\"").into()),
    }
}

/// Best guess for `sample`, or `None` when it is empty.
fn detect(sample: &[u8]) -> (Option<&'static str>, u32) {
    if sample.is_empty() {
        return (None, 0);
    }
    if sample.starts_with(&[0xFF, 0xFE, 0, 0]) {
        return (Some("UTF-32LE"), 100);
    }
    if sample.starts_with(&[0, 0, 0xFE, 0xFF]) {
        return (Some("UTF-32BE"), 100);
    }
    if let Some((encoding, _)) = Encoding::for_bom(sample) {
        return (Some(encoding.name()), 100);
    }
    // ASCII is UTF-8. A sequence cut off by the sample cap is still UTF-8.
    match std::str::from_utf8(sample) {
        Ok(_) => return (Some("UTF-8"), 100),
        Err(e) if e.error_len().is_none() => return (Some("UTF-8"), 100),
        Err(_) => {}
    }
    let mut detector = chardetng::EncodingDetector::new();
    detector.feed(sample, true);
    let name = match detector.guess(None, false).name() {
        // Report the names the JS (chardet) uses.
        "GBK" => "GB18030",
        "KOI8-U" => "KOI8-R",
        "ISO-8859-8-I" => "ISO-8859-8",
        name => name,
    };
    (Some(name), GUESS_CONFIDENCE)
}

/// Pass chunks through while sampling up to 64KB, then detect the charset.
/// The result is `{"charset": name | null, "confidence": 0..=100}`.
pub fn charset_detect_stream<T>(
    input: DataStream<T>,
    options: CharsetDetectOptions,
) -> (DataStream<T>, StreamResult)
where
    T: AsRef<[u8]> + Send + 'static,
{
    let result = StreamResult::new(
        options.result_key.unwrap_or_else(|| "charset".to_string()),
        json!({"charset": null, "confidence": 0}),
    );
    let sample = Arc::new(Mutex::new(Vec::new()));
    let (s, r) = (sample.clone(), result.clone());
    let stream = create_pass_through_stream(
        input,
        move |chunk: &T| {
            let mut sample = s.lock().unwrap_or_else(|e| e.into_inner());
            let bytes = chunk.as_ref();
            let take = bytes.len().min(MAX_DETECTION_SAMPLE - sample.len());
            sample.extend_from_slice(&bytes[..take]);
            Ok(())
        },
        move || {
            let sample = sample.lock().unwrap_or_else(|e| e.into_inner());
            let (charset, confidence) = detect(&sample);
            r.set(json!({"charset": charset, "confidence": confidence}));
            Ok(())
        },
    );
    (stream, result)
}

/// Decode byte chunks into strings. Multi-byte sequences split across
/// chunks are carried over; a truncated tail becomes U+FFFD. Errors on an
/// unknown charset.
pub fn charset_decode_stream(
    mut input: DataStream<Vec<u8>>,
    options: CharsetOptions,
) -> Result<DataStream<String>> {
    let mut decoder =
        encoding(options.charset.as_deref(), "charsetDecodeStream")?.new_decoder_with_bom_removal();
    Ok(Box::pin(try_stream! {
        let mut decode = move |bytes: &[u8], last: bool| {
            let mut out = String::with_capacity(decoder.max_utf8_buffer_length(bytes.len()).unwrap_or(0));
            let (result, _, _) = decoder.decode_to_string(bytes, &mut out, last);
            debug_assert!(matches!(result, CoderResult::InputEmpty));
            out
        };
        while let Some(chunk) = input.next().await {
            let out = decode(&chunk?, false);
            if !out.is_empty() {
                yield out;
            }
        }
        let out = decode(&[], true);
        if !out.is_empty() {
            yield out;
        }
    }))
}

/// Encode string chunks into bytes. Unmappable characters become `?`
/// (iconv-lite's default). Stateful encoders (ISO-2022-JP) carry their state
/// across chunks and emit the trailing shift back to ASCII at the end.
/// Errors on an unknown charset.
pub fn charset_encode_stream(
    mut input: DataStream<String>,
    options: CharsetOptions,
) -> Result<DataStream<Vec<u8>>> {
    let encoding = encoding(options.charset.as_deref(), "charsetEncodeStream")?;
    let mut encoder = encoding.new_encoder();
    Ok(Box::pin(try_stream! {
        let mut encode = move |chunk: &str, last: bool| -> Vec<u8> {
            // encoding_rs only encodes UTF-16 as UTF-8 (WHATWG), so do it here.
            if encoding == UTF_16LE {
                return chunk.encode_utf16().flat_map(u16::to_le_bytes).collect();
            }
            if encoding == UTF_16BE {
                return chunk.encode_utf16().flat_map(u16::to_be_bytes).collect();
            }
            let mut out = Vec::new();
            let mut rest = chunk;
            loop {
                // Worst case for what is left; `?` replacements can use up
                // the slack, so re-reserve whenever the output fills.
                let need = encoder
                    .max_buffer_length_from_utf8_without_replacement(rest.len())
                    .unwrap_or(usize::MAX);
                out.reserve(need);
                let (result, read) =
                    encoder.encode_from_utf8_to_vec_without_replacement(rest, &mut out, last);
                rest = &rest[read..];
                match result {
                    EncoderResult::InputEmpty => return out,
                    EncoderResult::OutputFull => {}
                    // The ISO-2022-JP encoder is back in its ASCII state here.
                    EncoderResult::Unmappable(_) => out.push(b'?'),
                }
            }
        };
        while let Some(chunk) = input.next().await {
            let out = encode(&chunk?, false);
            if !out.is_empty() {
                yield out;
            }
        }
        let out = encode("", true);
        if !out.is_empty() {
            yield out;
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{
        create_readable_stream, pipeline, stream_to_array, stream_to_string, Value,
    };

    fn bytes(chunks: &[&[u8]]) -> DataStream<Vec<u8>> {
        create_readable_stream(chunks.iter().map(|c| c.to_vec()).collect::<Vec<_>>())
    }

    fn strings(chunks: &[&str]) -> DataStream<String> {
        create_readable_stream(chunks.iter().map(|c| c.to_string()).collect::<Vec<_>>())
    }

    fn charset(name: &str) -> CharsetOptions {
        CharsetOptions {
            charset: Some(name.to_string()),
        }
    }

    async fn detected(chunks: &[&[u8]]) -> Value {
        let (stream, result) = charset_detect_stream(bytes(chunks), Default::default());
        pipeline(stream, &[]).await.unwrap();
        result.get()
    }

    #[tokio::test]
    async fn detect_default_and_custom_key() {
        let (stream, result) = charset_detect_stream(bytes(&[b"Hello World"]), Default::default());
        pipeline(stream, &[]).await.unwrap();
        assert_eq!(result.key(), "charset");
        let options = CharsetDetectOptions {
            result_key: Some("encoding".into()),
        };
        let (stream, result) = charset_detect_stream(bytes(&[b"Test content"]), options);
        let output = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(output["encoding"], result.get());
    }

    #[tokio::test]
    async fn detect_passes_chunks_through() {
        let (stream, _) = charset_detect_stream(bytes(&[b"a", b"b"]), Default::default());
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            [b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[tokio::test]
    async fn detect_ascii_and_utf8() {
        for text in [
            "Hello World",
            "Test content",
            "abc",
            "1234567890",
            "测试 Hello 世界 Bonjour",
        ] {
            assert_eq!(
                detected(&[text.as_bytes()]).await,
                json!({"charset": "UTF-8", "confidence": 100})
            );
        }
    }

    #[tokio::test]
    async fn detect_utf8_split_across_chunks() {
        let full = "テスト".as_bytes();
        assert_eq!(
            detected(&[&full[..1], &full[1..]]).await["charset"],
            "UTF-8"
        );
    }

    #[tokio::test]
    async fn detect_string_chunks() {
        let (stream, result) =
            charset_detect_stream(strings(&["Hello ", "世界"]), Default::default());
        pipeline(stream, &[]).await.unwrap();
        assert_eq!(result.get()["charset"], "UTF-8");
    }

    #[tokio::test]
    async fn detect_empty_input() {
        assert_eq!(
            detected(&[]).await,
            json!({"charset": null, "confidence": 0})
        );
    }

    #[tokio::test]
    async fn detect_boms() {
        assert_eq!(
            detected(&[&[0xFF, 0xFE, b'a', 0]]).await["charset"],
            "UTF-16LE"
        );
        assert_eq!(
            detected(&[&[0xFE, 0xFF, 0, b'a']]).await["charset"],
            "UTF-16BE"
        );
        assert_eq!(
            detected(&[&[0xFF, 0xFE, 0, 0, b'a', 0, 0, 0]]).await["charset"],
            "UTF-32LE"
        );
        assert_eq!(
            detected(&[&[0, 0, 0xFE, 0xFF, 0, 0, 0, b'a']]).await["charset"],
            "UTF-32BE"
        );
    }

    #[tokio::test]
    async fn detect_legacy_encodings() {
        let cases = [
            (
                "Shift_JIS",
                "これは日本語のテキストです。今日はとても良い天気ですね。",
            ),
            (
                "EUC-KR",
                "이것은 한국어 텍스트입니다. 오늘 날씨가 정말 좋네요.",
            ),
            (
                "windows-1251",
                "Привет, как дела? Это тестовый текст на русском языке.",
            ),
        ];
        for (name, text) in cases {
            let encoded = Encoding::for_label(name.as_bytes())
                .unwrap()
                .encode(text)
                .0
                .into_owned();
            let value = detected(&[&encoded]).await;
            assert_eq!(value["charset"], name, "{text}");
            assert_eq!(value["confidence"], GUESS_CONFIDENCE);
        }
    }

    #[tokio::test]
    async fn detect_koi8r() {
        let koi8r: &[u8] = &[
            0xf0, 0xd2, 0xc9, 0xd7, 0xc5, 0xd4, 0x2c, 0x20, 0xcb, 0xc1, 0xcb, 0x20, 0xc4, 0xc5,
            0xcc, 0xc1, 0x3f, 0x20, 0xfa, 0xd4, 0xcf, 0x20, 0xd4, 0xc5, 0xd3, 0xd4, 0xcf, 0xd7,
            0xd9, 0xca, 0x20, 0xd4, 0xc5, 0xcb, 0xd3, 0xd4, 0x20, 0xce, 0xc1, 0x20, 0xd2, 0xd5,
            0xd3, 0xd3, 0xcb, 0xcf, 0xcd, 0x20, 0xd1, 0xda, 0xd9, 0xcb, 0xc5, 0x2e,
        ];
        assert_eq!(detected(&[koi8r]).await["charset"], "KOI8-R");
    }

    #[tokio::test]
    async fn detect_caps_sample() {
        // UTF-8 inside the cap, invalid bytes past it: still UTF-8.
        let mut big = vec![b'a'; MAX_DETECTION_SAMPLE - 1];
        big.extend_from_slice("é".as_bytes()); // split by the cap
        big.extend_from_slice(&[0xFF; 100]);
        assert_eq!(detected(&[&big]).await["charset"], "UTF-8");
        let (head, tail) = big.split_at(MAX_DETECTION_SAMPLE / 2);
        assert_eq!(detected(&[head, tail]).await["charset"], "UTF-8");
    }

    #[tokio::test]
    async fn detect_instances_are_independent() {
        let (s1, r1) = charset_detect_stream(bytes(&[b"Hello World"]), Default::default());
        let (s2, r2) = charset_detect_stream(bytes(&[]), Default::default());
        pipeline(s1, &[]).await.unwrap();
        pipeline(s2, &[]).await.unwrap();
        assert_eq!(r1.get()["charset"], "UTF-8");
        assert_eq!(r2.get()["charset"], Value::Null);
    }

    async fn encoded(chunks: &[&str], options: CharsetOptions) -> Vec<Vec<u8>> {
        stream_to_array(
            charset_encode_stream(strings(chunks), options).unwrap(),
            None,
        )
        .await
        .unwrap()
    }

    async fn decoded(chunks: &[&[u8]], options: CharsetOptions) -> String {
        stream_to_string(charset_decode_stream(bytes(chunks), options).unwrap(), None)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn encode_utf8_by_default() {
        assert_eq!(
            encoded(&["Hello", " ", "World"], Default::default()).await,
            [b"Hello".to_vec(), b" ".to_vec(), b"World".to_vec()]
        );
        assert_eq!(
            encoded(&["Hello"], charset("UTF-8")).await,
            [b"Hello".to_vec()]
        );
    }

    #[tokio::test]
    async fn encode_utf8_labels() {
        for name in ["utf8", "utf-8"] {
            assert_eq!(encoded(&["Hé"], charset(name)).await, ["Hé".as_bytes()]);
        }
    }

    #[tokio::test]
    async fn encode_empty() {
        assert!(encoded(&[], charset("UTF-8")).await.is_empty());
        assert_eq!(
            encoded(&["", "a", ""], charset("UTF-8")).await,
            [b"a".to_vec()]
        );
    }

    #[tokio::test]
    async fn encode_utf16() {
        assert_eq!(
            encoded(&["té"], charset("UTF-16LE")).await,
            [vec![b't', 0, 0xE9, 0]]
        );
        assert_eq!(
            encoded(&["té"], charset("UTF-16BE")).await,
            [vec![0, b't', 0, 0xE9]]
        );
    }

    #[test]
    fn unsupported_charset_errors() {
        // Never a silent UTF-8 fallback that would mis-decode/encode the data.
        for name in ["unsupported-charset", "ISO-2022-KR", "replacement", ""] {
            let e = charset_encode_stream(strings(&[]), charset(name))
                .err()
                .unwrap();
            assert_eq!(
                e.to_string(),
                format!("charsetEncodeStream: Unsupported encoding \"{name}\"")
            );
            let e = charset_decode_stream(bytes(&[]), charset(name))
                .err()
                .unwrap();
            assert_eq!(
                e.to_string(),
                format!("charsetDecodeStream: Unsupported encoding \"{name}\"")
            );
        }
    }

    #[tokio::test]
    async fn encode_iso_8859_8_i() {
        assert_eq!(
            encoded(&["t\u{5d0}"], charset("ISO-8859-8-I")).await,
            [vec![0x74, 0xE0]]
        );
    }

    #[tokio::test]
    async fn encode_unmappable_becomes_question_mark() {
        assert_eq!(
            encoded(&["a€", "世b"], charset("ISO-8859-2")).await,
            [b"a?".to_vec(), b"?b".to_vec()]
        );
        assert_eq!(
            encoded(&["x😀y"], charset("Shift_JIS")).await,
            [b"x?y".to_vec()]
        );
    }

    #[tokio::test]
    async fn encode_stateful_iso_2022_jp_round_trips() {
        let original = "Hello 世界";
        let stream = charset_decode_stream(
            charset_encode_stream(strings(&[original]), charset("ISO-2022-JP")).unwrap(),
            charset("ISO-2022-JP"),
        )
        .unwrap();
        assert_eq!(stream_to_string(stream, None).await.unwrap(), original);
    }

    #[tokio::test]
    async fn encode_stateful_flushes_tail() {
        // The shift back to ASCII is only written once the input ends.
        let chunks = encoded(&["世", "界"], charset("ISO-2022-JP")).await;
        assert_eq!(chunks.last().unwrap(), &[0x1B, 0x28, 0x42]);
        let stream =
            charset_decode_stream(create_readable_stream(chunks), charset("ISO-2022-JP")).unwrap();
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "世界");
        // Unmappable chars mid-sequence leave the encoder in ASCII for the `?`.
        let chunks = encoded(&["世😀界"], charset("ISO-2022-JP")).await;
        let stream =
            charset_decode_stream(create_readable_stream(chunks), charset("ISO-2022-JP")).unwrap();
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "世?界");
    }

    #[tokio::test]
    async fn decode_utf8() {
        assert_eq!(
            decoded(&[b"Hello", b" ", b"World"], charset("UTF-8")).await,
            "Hello World"
        );
        assert_eq!(decoded(&[b"Test"], Default::default()).await, "Test");
    }

    #[tokio::test]
    async fn decode_utf16le() {
        assert_eq!(
            decoded(
                &[&[b't', 0, b'e', 0, b's', 0, b't', 0]],
                charset("UTF-16LE")
            )
            .await,
            "test"
        );
    }

    #[tokio::test]
    async fn decode_split_multibyte() {
        let full = "测试".as_bytes();
        assert_eq!(
            decoded(&[&full[..3], &full[3..]], charset("UTF-8")).await,
            "测试"
        );
        // Partial sequences don't emit empty chunks.
        let stream = charset_decode_stream(
            bytes(&[&full[..1], &full[1..2], &full[2..]]),
            charset("UTF-8"),
        )
        .unwrap();
        assert_eq!(stream_to_array(stream, None).await.unwrap(), ["测试"]);
    }

    #[tokio::test]
    async fn decode_truncated_tail_is_replacement_char() {
        assert_eq!(decoded(&[&[0xC2]], charset("UTF-8")).await, "\u{FFFD}");
    }

    #[tokio::test]
    async fn decode_iso_8859_8_i() {
        // 0xE0 is HEBREW LETTER ALEF in ISO-8859-8 (the -I "logical order"
        // label shares the same byte mapping).
        assert_eq!(
            decoded(&[&[0x74, 0xE0]], charset("ISO-8859-8-I")).await,
            "t\u{5d0}"
        );
    }

    #[tokio::test]
    async fn decode_empty_chunks() {
        let stream = charset_decode_stream(bytes(&[b"", b"test"]), charset("UTF-8")).unwrap();
        assert_eq!(stream_to_array(stream, None).await.unwrap(), ["test"]);
    }

    #[tokio::test]
    async fn decode_preserves_legacy_charset() {
        assert_eq!(decoded(&[&[0xE9]], charset("ISO-8859-1")).await, "é");
        assert_eq!(decoded(&[&[0xC1]], charset("KOI8-R")).await, "\u{430}");
    }

    #[tokio::test]
    async fn encode_decode_round_trip() {
        let stream = charset_decode_stream(
            charset_encode_stream(strings(&["Hello", " ", "Wörld"]), charset("windows-1252"))
                .unwrap(),
            charset("windows-1252"),
        )
        .unwrap();
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "Hello Wörld");
    }

    #[tokio::test]
    async fn none_charset_means_utf8() {
        let stream = charset_decode_stream(
            charset_encode_stream(strings(&["héllo"]), Default::default()).unwrap(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "héllo");
    }
}
