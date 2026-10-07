// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Character encoding detection, decoding, and encoding streams, ported from
//! `@datastream/charset`. Charsets are WHATWG labels (via `encoding_rs`);
//! unknown labels fall back to UTF-8.

use async_stream::try_stream;
use datastream_core::{create_pass_through_stream, DataStream, StreamExt, StreamResult};
use encoding_rs::{CoderResult, Encoding, UTF_16BE, UTF_16LE, UTF_8};
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

pub fn get_supported_encoding(charset: &str) -> &str {
    if charset == "ISO-8859-8-I" {
        "ISO-8859-8"
    } else {
        charset
    }
}

fn encoding(charset: Option<&str>) -> &'static Encoding {
    charset
        .and_then(|c| Encoding::for_label(get_supported_encoding(c).as_bytes()))
        .unwrap_or(UTF_8)
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
/// chunks are carried over; a truncated tail becomes U+FFFD.
pub fn charset_decode_stream(
    mut input: DataStream<Vec<u8>>,
    options: CharsetOptions,
) -> DataStream<String> {
    let mut decoder = encoding(options.charset.as_deref()).new_decoder_with_bom_removal();
    Box::pin(try_stream! {
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
    })
}

/// Encode string chunks into bytes. Unmappable characters become HTML
/// numeric character references (the WHATWG encoder behaviour).
pub fn charset_encode_stream(
    input: DataStream<String>,
    options: CharsetOptions,
) -> DataStream<Vec<u8>> {
    let encoding = encoding(options.charset.as_deref());
    Box::pin(input.filter_map(move |chunk| async move {
        let bytes: Vec<u8> = match chunk {
            Err(e) => return Some(Err(e)),
            // encoding_rs only encodes UTF-16 as UTF-8 (WHATWG), so do it here.
            Ok(chunk) if encoding == UTF_16LE => {
                chunk.encode_utf16().flat_map(u16::to_le_bytes).collect()
            }
            Ok(chunk) if encoding == UTF_16BE => {
                chunk.encode_utf16().flat_map(u16::to_be_bytes).collect()
            }
            Ok(chunk) => encoding.encode(&chunk).0.into_owned(),
        };
        (!bytes.is_empty()).then_some(Ok(bytes))
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

    #[tokio::test]
    async fn encode_utf8_by_default() {
        let output = stream_to_array(
            charset_encode_stream(strings(&["Hello", " ", "World"]), Default::default()),
            None,
        );
        assert_eq!(
            output.await.unwrap(),
            [b"Hello".to_vec(), b" ".to_vec(), b"World".to_vec()]
        );
        let output = stream_to_array(
            charset_encode_stream(strings(&["Hello"]), charset("UTF-8")),
            None,
        );
        assert_eq!(output.await.unwrap(), [b"Hello".to_vec()]);
    }

    #[tokio::test]
    async fn encode_empty() {
        let output =
            stream_to_array(charset_encode_stream(strings(&[]), charset("UTF-8")), None).await;
        assert!(output.unwrap().is_empty());
        let output = stream_to_array(
            charset_encode_stream(strings(&["", "a", ""]), charset("UTF-8")),
            None,
        )
        .await;
        assert_eq!(output.unwrap(), [b"a".to_vec()]);
    }

    #[tokio::test]
    async fn encode_utf16() {
        let output = stream_to_array(
            charset_encode_stream(strings(&["té"]), charset("UTF-16LE")),
            None,
        )
        .await;
        assert_eq!(output.unwrap(), [vec![b't', 0, 0xE9, 0]]);
        let output = stream_to_array(
            charset_encode_stream(strings(&["té"]), charset("UTF-16BE")),
            None,
        )
        .await;
        assert_eq!(output.unwrap(), [vec![0, b't', 0, 0xE9]]);
    }

    #[tokio::test]
    async fn encode_unsupported_falls_back_to_utf8() {
        let output = stream_to_array(
            charset_encode_stream(strings(&["tést"]), charset("unsupported-charset")),
            None,
        );
        assert_eq!(output.await.unwrap(), ["tést".as_bytes().to_vec()]);
    }

    #[tokio::test]
    async fn encode_iso_8859_8_i() {
        let output = stream_to_array(
            charset_encode_stream(strings(&["test"]), charset("ISO-8859-8-I")),
            None,
        );
        assert_eq!(output.await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn encode_stateful_iso_2022_jp_round_trips() {
        let original = "Hello 世界";
        let stream = charset_decode_stream(
            charset_encode_stream(strings(&[original]), charset("ISO-2022-JP")),
            charset("ISO-2022-JP"),
        );
        assert_eq!(stream_to_string(stream, None).await.unwrap(), original);
    }

    #[tokio::test]
    async fn decode_utf8() {
        let output = stream_to_string(
            charset_decode_stream(bytes(&[b"Hello", b" ", b"World"]), charset("UTF-8")),
            None,
        );
        assert_eq!(output.await.unwrap(), "Hello World");
        let output = stream_to_string(
            charset_decode_stream(bytes(&[b"Test"]), Default::default()),
            None,
        );
        assert_eq!(output.await.unwrap(), "Test");
    }

    #[tokio::test]
    async fn decode_split_multibyte() {
        let full = "测试".as_bytes();
        let stream = charset_decode_stream(bytes(&[&full[..3], &full[3..]]), charset("UTF-8"));
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "测试");
        // Partial sequences don't emit empty chunks.
        let stream = charset_decode_stream(
            bytes(&[&full[..1], &full[1..2], &full[2..]]),
            charset("UTF-8"),
        );
        assert_eq!(stream_to_array(stream, None).await.unwrap(), ["测试"]);
    }

    #[tokio::test]
    async fn decode_truncated_tail_is_replacement_char() {
        let stream = charset_decode_stream(bytes(&[&[0xC2]]), charset("UTF-8"));
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "\u{FFFD}");
    }

    #[tokio::test]
    async fn decode_unsupported_and_aliases() {
        for name in ["unsupported-charset", "ISO-8859-8-I"] {
            let stream = charset_decode_stream(bytes(&[b"test"]), charset(name));
            assert_eq!(stream_to_string(stream, None).await.unwrap(), "test");
        }
    }

    #[tokio::test]
    async fn decode_empty_chunks() {
        let stream = charset_decode_stream(bytes(&[b"", b"test"]), charset("UTF-8"));
        assert_eq!(stream_to_array(stream, None).await.unwrap(), ["test"]);
    }

    #[tokio::test]
    async fn decode_preserves_legacy_charset() {
        let stream = charset_decode_stream(bytes(&[&[0xE9]]), charset("ISO-8859-1"));
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "é");
        let stream = charset_decode_stream(bytes(&[&[0xC1]]), charset("KOI8-R"));
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "\u{430}");
    }

    #[tokio::test]
    async fn encode_decode_round_trip() {
        let stream = charset_decode_stream(
            charset_encode_stream(strings(&["Hello", " ", "Wörld"]), charset("windows-1252")),
            charset("windows-1252"),
        );
        assert_eq!(stream_to_string(stream, None).await.unwrap(), "Hello Wörld");
    }

    #[test]
    fn supported_encoding_aliases() {
        assert_eq!(get_supported_encoding("ISO-8859-8-I"), "ISO-8859-8");
        assert_eq!(get_supported_encoding("UTF-8"), "UTF-8");
    }
}
