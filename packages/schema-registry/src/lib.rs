// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Schema Registry wire-format framing streams, ported from
//! `@datastream/schema-registry`.
//!
//! Each input chunk is one framed record (one Kafka message). Unframe streams
//! emit per-chunk envelopes so a downstream decoder can pick the schema per
//! record; the shared result only reflects the most recently seen id and
//! errors once more than one distinct id was seen.

use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_stream::try_stream;
use datastream_core::{
    create_transform_stream, noop_flush, DataStream, Result, StreamExt, StreamResult, Value,
};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde_json::json;

const GLUE_COMPRESSION_NONE: u8 = 0x00;
const GLUE_COMPRESSION_ZLIB: u8 = 0x05;
const DEFAULT_MAX_DECOMPRESSED_BYTES: usize = 10 * 1024 * 1024;

/// Result of an unframe stream: the last seen id, unless several were seen.
#[derive(Clone, Debug)]
pub struct UnframeResult {
    result: StreamResult,
    id_key: &'static str,
    distinct: Arc<AtomicUsize>,
    error: &'static str,
}

impl UnframeResult {
    fn new(key: String, value: Value, id_key: &'static str, error: &'static str) -> Self {
        let result = StreamResult::new(key, value);
        Self {
            result,
            id_key,
            distinct: Arc::new(AtomicUsize::new(0)),
            error,
        }
    }
    /// Errors when the stream carried more than one distinct id.
    pub fn result(&self) -> Result<StreamResult> {
        if self.distinct.load(Ordering::Relaxed) > 1 {
            return Err(self.error.into());
        }
        Ok(self.result.clone())
    }
    fn observe(&self, id: Value) {
        self.result.update(|v| {
            if v[self.id_key] != id {
                self.distinct.fetch_add(1, Ordering::Relaxed);
            }
            v[self.id_key] = id;
        });
    }
}

// Envelopes carry no length, so a chunk that starts with `magic` and holds a
// full header starts a new frame; any other chunk continues the current one.
struct FrameBuffer {
    magic: u8,
    header_size: usize,
    frame: Option<Vec<u8>>,
}

impl FrameBuffer {
    fn new(magic: u8, header_size: usize) -> Self {
        Self {
            magic,
            header_size,
            frame: None,
        }
    }
    /// Buffer `bytes`, returning the previous frame if this one starts a new frame.
    fn push(&mut self, bytes: Vec<u8>) -> Option<Vec<u8>> {
        if bytes.len() >= self.header_size && bytes[0] == self.magic {
            return self.frame.replace(bytes);
        }
        self.frame
            .get_or_insert_with(Vec::new)
            .extend_from_slice(&bytes);
        None
    }
    fn flush(&mut self) -> Option<Vec<u8>> {
        self.frame.take()
    }
}

// *** Confluent (5-byte: 0x00 magic + uint32 BE schema id) *** //

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfluentEnvelope {
    pub schema_id: u32,
    pub payload: Vec<u8>,
}

impl AsRef<[u8]> for ConfluentEnvelope {
    fn as_ref(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Default, Clone, Debug)]
pub struct ConfluentFrameOptions {
    pub schema_id: Option<u32>,
    pub result_key: Option<String>,
}

/// Prepend `0x00` + the big-endian schema id. The result echoes `{ schemaId }`.
pub fn confluent_frame_stream(
    input: DataStream<Vec<u8>>,
    options: ConfluentFrameOptions,
) -> Result<(DataStream<Vec<u8>>, StreamResult)> {
    let schema_id = options
        .schema_id
        .ok_or("confluentFrameStream: schemaId must be an unsigned 32-bit integer")?;
    let mut header = vec![0x00];
    header.extend_from_slice(&schema_id.to_be_bytes());
    let key = options
        .result_key
        .unwrap_or_else(|| "confluentSchemaId".into());
    let result = StreamResult::new(key, json!({ "schemaId": schema_id }));
    let stream = create_transform_stream(
        input,
        move |chunk: Vec<u8>, enqueue: &mut Vec<Vec<u8>>| {
            enqueue.push([header.as_slice(), chunk.as_slice()].concat());
            Ok(())
        },
        noop_flush,
    );
    Ok((stream, result))
}

#[derive(Default, Clone, Debug)]
pub struct ConfluentUnframeOptions {
    pub result_key: Option<String>,
}

fn parse_confluent(seen: &UnframeResult, mut frame: Vec<u8>) -> Result<ConfluentEnvelope> {
    if frame.len() < 5 || frame[0] != 0x00 {
        return Err("confluentUnframeStream: missing 0x00 magic byte / frame is too short".into());
    }
    let schema_id = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
    seen.observe(json!(schema_id));
    Ok(ConfluentEnvelope {
        schema_id,
        payload: frame.split_off(5),
    })
}

/// Strip the Confluent header, emitting `{ schema_id, payload }` envelopes.
pub fn confluent_unframe_stream(
    mut input: DataStream<Vec<u8>>,
    options: ConfluentUnframeOptions,
) -> (DataStream<ConfluentEnvelope>, UnframeResult) {
    let seen = UnframeResult::new(
        options.result_key.unwrap_or_else(|| "confluentSchemaId".into()),
        json!({ "schemaId": null }),
        "schemaId",
        "confluentUnframeStream.result(): stream carried multiple distinct schemaIds; use the per-chunk envelope { schemaId, payload } instead",
    );
    let s = seen.clone();
    let stream = Box::pin(try_stream! {
        let mut frames = FrameBuffer::new(0x00, 5);
        while let Some(chunk) = input.next().await {
            if let Some(frame) = frames.push(chunk?) {
                yield parse_confluent(&s, frame)?;
            }
        }
        if let Some(frame) = frames.flush() {
            yield parse_confluent(&s, frame)?;
        }
    });
    (stream, seen)
}

// *** Glue (18-byte: 0x03 magic + 1 byte compression + 16 bytes UUID) *** //

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlueEnvelope {
    pub schema_version_id: String,
    /// `"none"` or `"zlib"`.
    pub compression: &'static str,
    pub payload: Vec<u8>,
}

impl AsRef<[u8]> for GlueEnvelope {
    fn as_ref(&self) -> &[u8] {
        &self.payload
    }
}

fn uuid_to_bytes(uuid: &str) -> Result<[u8; 16]> {
    let hex = uuid.replace('-', "");
    if hex.len() != 32 || !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(
            format!("glueFrameStream: schemaVersionId must be a valid UUID (got {uuid})").into(),
        );
    }
    let mut out = [0u8; 16];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)?;
    }
    Ok(out)
}

fn bytes_to_uuid(bytes: &[u8]) -> String {
    let h: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..]
    )
}

fn deflate(bytes: &[u8], max: Option<usize>) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    let out = encoder.finish()?;
    if let Some(max) = max.filter(|&max| out.len() > max) {
        return Err(format!("schema-registry: maxFrameBytes exceeded ({max})").into());
    }
    Ok(out)
}

fn inflate(bytes: &[u8], max: usize) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    // Read one byte past the limit so an oversized payload is detected
    // without inflating all of it.
    let limit = u64::try_from(max).unwrap_or(u64::MAX).saturating_add(1);
    ZlibDecoder::new(bytes).take(limit).read_to_end(&mut out)?;
    if out.len() > max {
        return Err(format!("schema-registry: maxDecompressedBytes exceeded ({max})").into());
    }
    Ok(out)
}

#[derive(Default, Clone, Debug)]
pub struct GlueFrameOptions {
    pub schema_version_id: Option<String>,
    /// `"none"` (default) or `"zlib"`.
    pub compression: Option<String>,
    /// Limit on the compressed payload size (default unlimited).
    pub max_frame_bytes: Option<usize>,
    pub result_key: Option<String>,
}

/// Prepend `0x03` + compression byte + 16-byte UUID, compressing the payload
/// with zlib when asked. The result echoes `{ schemaVersionId }`.
pub fn glue_frame_stream(
    input: DataStream<Vec<u8>>,
    options: GlueFrameOptions,
) -> Result<(DataStream<Vec<u8>>, StreamResult)> {
    let Some(schema_version_id) = options.schema_version_id else {
        return Err("glueFrameStream: schemaVersionId required".into());
    };
    let zlib = match options.compression.as_deref().unwrap_or("none") {
        "none" => false,
        "zlib" => true,
        other => {
            let message = format!("glueFrameStream: unsupported compression \"{other}\" (expected \"none\" or \"zlib\")");
            return Err(message.into());
        }
    };
    let mut header = vec![
        0x03,
        if zlib {
            GLUE_COMPRESSION_ZLIB
        } else {
            GLUE_COMPRESSION_NONE
        },
    ];
    header.extend_from_slice(&uuid_to_bytes(&schema_version_id)?);
    let key = options
        .result_key
        .unwrap_or_else(|| "glueSchemaVersionId".into());
    let result = StreamResult::new(key, json!({ "schemaVersionId": schema_version_id }));
    let max = options.max_frame_bytes;
    let stream = create_transform_stream(
        input,
        move |chunk: Vec<u8>, enqueue: &mut Vec<Vec<u8>>| {
            let payload = if zlib { deflate(&chunk, max)? } else { chunk };
            enqueue.push([header.as_slice(), payload.as_slice()].concat());
            Ok(())
        },
        noop_flush,
    );
    Ok((stream, result))
}

#[derive(Default, Clone, Debug)]
pub struct GlueUnframeOptions {
    /// Limit on each inflated payload (default 10MB; `usize::MAX` for none).
    pub max_decompressed_bytes: Option<usize>,
    pub result_key: Option<String>,
}

fn parse_glue(seen: &UnframeResult, mut frame: Vec<u8>, max: usize) -> Result<GlueEnvelope> {
    if frame.len() < 18 || frame[0] != 0x03 {
        return Err("glueUnframeStream: missing 0x03 magic byte / frame is too short".into());
    }
    let compression_byte = frame[1];
    let schema_version_id = bytes_to_uuid(&frame[2..18]);
    seen.observe(json!(schema_version_id));
    let payload = frame.split_off(18);
    let (compression, payload) = match compression_byte {
        GLUE_COMPRESSION_NONE => ("none", payload),
        GLUE_COMPRESSION_ZLIB => ("zlib", inflate(&payload, max)?),
        other => {
            return Err(
                format!("glueUnframeStream: unsupported compression byte 0x{other:02x}").into(),
            )
        }
    };
    seen.result
        .update(|v| v["compression"] = json!(compression));
    Ok(GlueEnvelope {
        schema_version_id,
        compression,
        payload,
    })
}

/// Strip the Glue header (inflating zlib payloads), emitting
/// `{ schema_version_id, compression, payload }` envelopes.
pub fn glue_unframe_stream(
    mut input: DataStream<Vec<u8>>,
    options: GlueUnframeOptions,
) -> (DataStream<GlueEnvelope>, UnframeResult) {
    let max = options
        .max_decompressed_bytes
        .unwrap_or(DEFAULT_MAX_DECOMPRESSED_BYTES);
    let seen = UnframeResult::new(
        options.result_key.unwrap_or_else(|| "glueSchemaVersionId".into()),
        json!({ "schemaVersionId": null, "compression": null }),
        "schemaVersionId",
        "glueUnframeStream.result(): stream carried multiple distinct schemaVersionIds; use the per-chunk envelope { schemaVersionId, compression, payload } instead",
    );
    let s = seen.clone();
    let stream = Box::pin(try_stream! {
        let mut frames = FrameBuffer::new(0x03, 18);
        while let Some(chunk) = input.next().await {
            if let Some(frame) = frames.push(chunk?) {
                yield parse_glue(&s, frame, max)?;
            }
        }
        if let Some(frame) = frames.flush() {
            yield parse_glue(&s, frame, max)?;
        }
    });
    (stream, seen)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{
        create_readable_stream, create_readable_stream_from_bytes, stream_to_array,
    };

    const HELLO: &[u8] = b"hello world";
    const UUID: &str = "12345678-1234-1234-1234-1234567890ab";

    async fn confluent_frame(payload: &[u8], schema_id: u32) -> Vec<u8> {
        let options = ConfluentFrameOptions {
            schema_id: Some(schema_id),
            ..Default::default()
        };
        let (stream, _) =
            confluent_frame_stream(create_readable_stream([payload.to_vec()]), options).unwrap();
        stream_to_array(stream, None).await.unwrap().remove(0)
    }

    async fn confluent_unframe_with(
        input: DataStream<Vec<u8>>,
    ) -> (Result<Vec<ConfluentEnvelope>>, UnframeResult) {
        let (stream, seen) = confluent_unframe_stream(input, ConfluentUnframeOptions::default());
        (stream_to_array(stream, None).await, seen)
    }

    async fn confluent_unframe(
        chunks: Vec<Vec<u8>>,
    ) -> (Result<Vec<ConfluentEnvelope>>, UnframeResult) {
        confluent_unframe_with(create_readable_stream(chunks)).await
    }

    fn glue_options(id: &str, compression: &str) -> GlueFrameOptions {
        GlueFrameOptions {
            schema_version_id: Some(id.into()),
            compression: Some(compression.into()),
            ..Default::default()
        }
    }

    async fn glue_frame(payload: &[u8], options: GlueFrameOptions) -> Result<Vec<u8>> {
        let (stream, _) = glue_frame_stream(create_readable_stream([payload.to_vec()]), options)?;
        Ok(stream_to_array(stream, None).await?.remove(0))
    }

    async fn glue_unframe_with(
        input: DataStream<Vec<u8>>,
        max_decompressed_bytes: Option<usize>,
    ) -> (Result<Vec<GlueEnvelope>>, UnframeResult) {
        let options = GlueUnframeOptions {
            max_decompressed_bytes,
            ..Default::default()
        };
        let (stream, seen) = glue_unframe_stream(input, options);
        (stream_to_array(stream, None).await, seen)
    }

    async fn glue_unframe(chunks: Vec<Vec<u8>>) -> (Result<Vec<GlueEnvelope>>, UnframeResult) {
        glue_unframe_with(create_readable_stream(chunks), None).await
    }

    // *** Confluent *** //

    #[tokio::test]
    async fn confluent_frame_prepends_magic_and_big_endian_id() {
        let framed = confluent_frame(HELLO, 257).await;
        assert_eq!(framed[..5], [0x00, 0x00, 0x00, 0x01, 0x01]);
        assert_eq!(framed[5..], *HELLO);
        assert_eq!(
            confluent_frame(&[0xaa], 0x12345678).await[..5],
            [0x00, 0x12, 0x34, 0x56, 0x78]
        );
        assert_eq!(confluent_frame(HELLO, 0).await[1..5], [0, 0, 0, 0]);
        assert_eq!(confluent_frame(HELLO, u32::MAX).await[1..5], [0xff; 4]);
        assert_eq!(confluent_frame(&[], 500).await.len(), 5);
    }

    #[test]
    fn confluent_frame_requires_schema_id() {
        let e = confluent_frame_stream(
            create_readable_stream(Vec::<Vec<u8>>::new()),
            ConfluentFrameOptions::default(),
        )
        .err()
        .unwrap();
        assert_eq!(
            e.to_string(),
            "confluentFrameStream: schemaId must be an unsigned 32-bit integer"
        );
    }

    #[tokio::test]
    async fn confluent_frame_result_key() {
        let options = ConfluentFrameOptions {
            schema_id: Some(7),
            ..Default::default()
        };
        let (_, result) =
            confluent_frame_stream(create_readable_stream([HELLO.to_vec()]), options).unwrap();
        assert_eq!(result.key(), "confluentSchemaId");
        assert_eq!(result.get(), json!({"schemaId": 7}));
        let options = ConfluentFrameOptions {
            schema_id: Some(7),
            result_key: Some("myConfluentKey".into()),
        };
        let (_, result) =
            confluent_frame_stream(create_readable_stream([HELLO.to_vec()]), options).unwrap();
        assert_eq!(result.key(), "myConfluentKey");
    }

    #[tokio::test]
    async fn confluent_round_trip_emits_envelope() {
        let (out, seen) = confluent_unframe(vec![confluent_frame(HELLO, 42).await]).await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 42,
                payload: HELLO.to_vec()
            }]
        );
        let result = seen.result().unwrap();
        assert_eq!(result.key(), "confluentSchemaId");
        assert_eq!(result.get(), json!({"schemaId": 42}));
    }

    #[tokio::test]
    async fn confluent_unframe_reads_big_endian() {
        let (out, _) = confluent_unframe(vec![vec![0x00, 0x01, 0x02, 0x03, 0x04, 0xaa]]).await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 0x01020304,
                payload: vec![0xaa]
            }]
        );
    }

    #[tokio::test]
    async fn confluent_unframe_rejects_bad_frames() {
        let message = "confluentUnframeStream: missing 0x00 magic byte / frame is too short";
        let (out, _) = confluent_unframe(vec![vec![0x01, 0, 0, 0, 0, 0x68, 0x69]]).await;
        assert_eq!(out.unwrap_err().to_string(), message);
        let (out, _) = confluent_unframe(vec![vec![0x00, 0x00, 0x00, 0x01]]).await;
        assert_eq!(out.unwrap_err().to_string(), message);
    }

    #[tokio::test]
    async fn confluent_result_tracks_distinct_ids() {
        let a = confluent_frame(HELLO, 5).await;
        let b = confluent_frame(&[0x01], 5).await;
        let (_, seen) = confluent_unframe(vec![a, b]).await;
        assert_eq!(seen.result().unwrap().get(), json!({"schemaId": 5}));

        let a = confluent_frame(HELLO, 10).await;
        let b = confluent_frame(HELLO, 11).await;
        let (out, seen) = confluent_unframe(vec![a, b]).await;
        assert_eq!(
            out.unwrap().iter().map(|e| e.schema_id).collect::<Vec<_>>(),
            [10, 11]
        );
        let e = seen.result().unwrap_err().to_string();
        assert!(e.contains("distinct") && e.contains("schemaId"), "{e}");
    }

    #[test]
    fn confluent_result_before_frames_and_custom_key() {
        let (_, seen) = confluent_unframe_stream(
            create_readable_stream(Vec::<Vec<u8>>::new()),
            ConfluentUnframeOptions::default(),
        );
        assert_eq!(seen.result().unwrap().get(), json!({"schemaId": null}));
        let options = ConfluentUnframeOptions {
            result_key: Some("myKey".into()),
        };
        let (_, seen) =
            confluent_unframe_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options);
        assert_eq!(seen.result().unwrap().key(), "myKey");
    }

    #[tokio::test]
    async fn confluent_reassembles_auto_chunked_frame() {
        let big: Vec<u8> = (0..40 * 1024).map(|i| (i % 251) as u8).collect();
        let framed = confluent_frame(&big, 123).await;
        assert!(framed.len() > 16384);
        let (out, _) =
            confluent_unframe_with(create_readable_stream_from_bytes(framed, None).unwrap()).await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 123,
                payload: big
            }]
        );
    }

    #[tokio::test]
    async fn confluent_reassembles_split_frames() {
        let full = confluent_frame(HELLO, 200).await;
        let (out, _) = confluent_unframe(vec![full[..5].to_vec(), full[5..].to_vec()]).await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 200,
                payload: HELLO.to_vec()
            }]
        );

        let payload: Vec<u8> = (1..=12).collect();
        let full = confluent_frame(&payload, 201).await;
        let (out, _) = confluent_unframe(vec![
            full[..5].to_vec(),
            full[5..9].to_vec(),
            full[9..].to_vec(),
        ])
        .await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 201,
                payload
            }]
        );

        // A short chunk starting with the magic byte is a continuation.
        let full = confluent_frame(HELLO, 600).await;
        let (out, _) = confluent_unframe(vec![full[..3].to_vec(), full[3..].to_vec()]).await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 600,
                payload: HELLO.to_vec()
            }]
        );
    }

    #[tokio::test]
    async fn confluent_emits_multi_part_frame_when_next_frame_starts() {
        let payload = vec![0xde, 0xad, 0xbe, 0xef, 0xca, 0xfe];
        let a = confluent_frame(&payload, 700).await;
        let b = confluent_frame(&[0x01], 701).await;
        let (out, _) = confluent_unframe(vec![a[..5].to_vec(), a[5..].to_vec(), b]).await;
        assert_eq!(
            out.unwrap(),
            [
                ConfluentEnvelope {
                    schema_id: 700,
                    payload
                },
                ConfluentEnvelope {
                    schema_id: 701,
                    payload: vec![0x01]
                },
            ]
        );
    }

    #[tokio::test]
    async fn confluent_frame_boundary_uses_header_length() {
        // A <5-byte magic-leading chunk stays attached to the open frame.
        let full = confluent_frame(HELLO, 808).await;
        let (out, _) = confluent_unframe(vec![full, vec![0x00, 0x99]]).await;
        assert_eq!(
            out.unwrap(),
            [ConfluentEnvelope {
                schema_id: 808,
                payload: [HELLO, &[0x00u8, 0x99][..]].concat()
            }]
        );
        // An exactly-5-byte chunk starts a new frame.
        let a = confluent_frame(HELLO, 811).await;
        let b = confluent_frame(&[], 812).await;
        let (out, _) = confluent_unframe(vec![a, b]).await;
        assert_eq!(
            out.unwrap(),
            [
                ConfluentEnvelope {
                    schema_id: 811,
                    payload: HELLO.to_vec()
                },
                ConfluentEnvelope {
                    schema_id: 812,
                    payload: vec![]
                },
            ]
        );
    }

    #[tokio::test]
    async fn unframe_empty_streams() {
        assert!(confluent_unframe(vec![]).await.0.unwrap().is_empty());
        assert!(glue_unframe(vec![]).await.0.unwrap().is_empty());
    }

    // *** Glue *** //

    #[tokio::test]
    async fn glue_frame_prepends_header() {
        let framed = glue_frame(HELLO, glue_options(UUID, "none")).await.unwrap();
        assert_eq!(framed[0], 0x03);
        assert_eq!(framed[1], 0x00);
        assert_eq!(framed[2..18], uuid_to_bytes(UUID).unwrap());
        assert_eq!(framed.len(), 18 + HELLO.len());
        let framed = glue_frame(
            HELLO,
            GlueFrameOptions {
                schema_version_id: Some(UUID.into()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(framed[1], 0x00);
        assert_eq!(
            glue_frame(&[], glue_options(UUID, "none"))
                .await
                .unwrap()
                .len(),
            18
        );
    }

    #[tokio::test]
    async fn glue_round_trip_uncompressed() {
        let (out, seen) = glue_unframe(vec![glue_frame(HELLO, glue_options(UUID, "none"))
            .await
            .unwrap()])
        .await;
        let expected = GlueEnvelope {
            schema_version_id: UUID.into(),
            compression: "none",
            payload: HELLO.to_vec(),
        };
        assert_eq!(out.unwrap(), [expected]);
        let result = seen.result().unwrap();
        assert_eq!(result.key(), "glueSchemaVersionId");
        assert_eq!(
            result.get(),
            json!({"schemaVersionId": UUID, "compression": "none"})
        );
    }

    #[tokio::test]
    async fn glue_round_trip_zlib() {
        let payload = vec![b'a'; 2048];
        let framed = glue_frame(&payload, glue_options(UUID, "zlib"))
            .await
            .unwrap();
        assert_eq!(framed[1], 0x05);
        assert!(framed.len() < 18 + payload.len());
        let (out, seen) = glue_unframe(vec![framed]).await;
        let out = out.unwrap();
        assert_eq!(out[0].compression, "zlib");
        assert_eq!(out[0].payload, payload);
        assert_eq!(seen.result().unwrap().get()["compression"], "zlib");
        // Empty payload.
        let framed = glue_frame(&[], glue_options(UUID, "zlib")).await.unwrap();
        let (out, _) = glue_unframe(vec![framed]).await;
        assert!(out.unwrap()[0].payload.is_empty());
    }

    #[tokio::test]
    async fn glue_max_decompressed_bytes() {
        let framed = glue_frame(&[b'a'; 100], glue_options(UUID, "zlib"))
            .await
            .unwrap();
        let (out, _) = glue_unframe_with(create_readable_stream([framed.clone()]), Some(100)).await;
        assert_eq!(out.unwrap()[0].payload.len(), 100);
        let (out, _) = glue_unframe_with(create_readable_stream([framed]), Some(10)).await;
        assert_eq!(
            out.unwrap_err().to_string(),
            "schema-registry: maxDecompressedBytes exceeded (10)"
        );
        // usize::MAX disables the limit.
        let framed = glue_frame(&[b'a'; 2048], glue_options(UUID, "zlib"))
            .await
            .unwrap();
        let (out, _) = glue_unframe_with(create_readable_stream([framed]), Some(usize::MAX)).await;
        assert_eq!(out.unwrap()[0].payload.len(), 2048);
    }

    #[tokio::test]
    async fn glue_max_frame_bytes() {
        let options = GlueFrameOptions {
            max_frame_bytes: Some(10),
            ..glue_options(UUID, "zlib")
        };
        let e = glue_frame(&[b'a'; 4096], options).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "schema-registry: maxFrameBytes exceeded (10)"
        );
    }

    #[tokio::test]
    async fn glue_unframe_rejects_bad_frames() {
        let mut frame = vec![0u8; 19];
        frame[0] = 0x03;
        frame[1] = 0x09;
        let (out, _) = glue_unframe(vec![frame]).await;
        assert_eq!(
            out.unwrap_err().to_string(),
            "glueUnframeStream: unsupported compression byte 0x09"
        );

        let message = "glueUnframeStream: missing 0x03 magic byte / frame is too short";
        let mut frame = vec![0u8; 20];
        frame[0] = 0x07;
        assert_eq!(
            glue_unframe(vec![frame]).await.0.unwrap_err().to_string(),
            message
        );
        let mut frame = vec![0u8; 17];
        frame[0] = 0x03;
        assert_eq!(
            glue_unframe(vec![frame]).await.0.unwrap_err().to_string(),
            message
        );

        // Malformed zlib payload.
        let mut frame = vec![0u8; 18];
        frame[0] = 0x03;
        frame[1] = 0x05;
        frame.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
        assert!(glue_unframe(vec![frame]).await.0.is_err());
    }

    #[test]
    fn glue_frame_validates_options() {
        let build = |options| {
            glue_frame_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options)
                .err()
                .unwrap()
                .to_string()
        };
        assert_eq!(
            build(GlueFrameOptions::default()),
            "glueFrameStream: schemaVersionId required"
        );
        assert_eq!(
            build(glue_options(UUID, "gzip")),
            "glueFrameStream: unsupported compression \"gzip\" (expected \"none\" or \"zlib\")"
        );
        for bad in [
            "not-a-uuid",
            "zzzzzzzz-1234-1234-1234-1234567890ab",
            "1234567812341234123412345678ab",
            "0123456789abcdef0123456789abcdef0",
        ] {
            let e = build(glue_options(bad, "none"));
            assert_eq!(
                e,
                format!("glueFrameStream: schemaVersionId must be a valid UUID (got {bad})")
            );
        }
    }

    #[tokio::test]
    async fn glue_frame_result_key() {
        let (_, result) = glue_frame_stream(
            create_readable_stream([HELLO.to_vec()]),
            glue_options(UUID, "none"),
        )
        .unwrap();
        assert_eq!(result.key(), "glueSchemaVersionId");
        assert_eq!(result.get(), json!({"schemaVersionId": UUID}));
        let options = GlueFrameOptions {
            result_key: Some("myGlueFrameKey".into()),
            ..glue_options(UUID, "none")
        };
        let (_, result) =
            glue_frame_stream(create_readable_stream([HELLO.to_vec()]), options).unwrap();
        assert_eq!(result.key(), "myGlueFrameKey");
    }

    #[tokio::test]
    async fn glue_reassembles_split_frames() {
        let big: Vec<u8> = (0..40 * 1024).map(|i| ((i * 7) % 251) as u8).collect();
        let framed = glue_frame(&big, glue_options(UUID, "none")).await.unwrap();
        let (out, _) = glue_unframe_with(
            create_readable_stream_from_bytes(framed, None).unwrap(),
            None,
        )
        .await;
        assert_eq!(out.unwrap()[0].payload, big);

        let payload: Vec<u8> = (1..=20).collect();
        let full = glue_frame(&payload, glue_options(UUID, "none"))
            .await
            .unwrap();
        let (out, _) = glue_unframe(vec![full[..18].to_vec(), full[18..].to_vec()]).await;
        assert_eq!(out.unwrap()[0].payload, payload);

        // A short chunk starting with the magic byte is a continuation.
        let full = glue_frame(HELLO, glue_options(UUID, "none")).await.unwrap();
        let (out, _) = glue_unframe(vec![full[..10].to_vec(), full[10..].to_vec()]).await;
        let out = out.unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].schema_version_id, UUID);
        assert_eq!(out[0].payload, HELLO);
    }

    #[tokio::test]
    async fn glue_envelopes_pair_payload_with_their_id() {
        let id_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let id_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        let a = glue_frame(b"payload-a", glue_options(id_a, "none"))
            .await
            .unwrap();
        let b = glue_frame(b"payload-b", glue_options(id_b, "none"))
            .await
            .unwrap();
        let (out, seen) = glue_unframe(vec![a, b]).await;
        let out = out.unwrap();
        assert_eq!(
            (out[0].schema_version_id.as_str(), out[0].payload.as_slice()),
            (id_a, &b"payload-a"[..])
        );
        assert_eq!(
            (out[1].schema_version_id.as_str(), out[1].payload.as_slice()),
            (id_b, &b"payload-b"[..])
        );
        let e = seen.result().unwrap_err().to_string();
        assert!(
            e.contains("distinct") && e.contains("schemaVersionId"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn glue_result_with_one_id_and_defaults() {
        let a = glue_frame(HELLO, glue_options(UUID, "none")).await.unwrap();
        let b = glue_frame(&[0x42], glue_options(UUID, "none"))
            .await
            .unwrap();
        let (_, seen) = glue_unframe(vec![a, b]).await;
        assert_eq!(seen.result().unwrap().get()["schemaVersionId"], UUID);

        let (_, seen) = glue_unframe_stream(
            create_readable_stream(Vec::<Vec<u8>>::new()),
            GlueUnframeOptions::default(),
        );
        assert_eq!(
            seen.result().unwrap().get(),
            json!({"schemaVersionId": null, "compression": null})
        );
        let options = GlueUnframeOptions {
            result_key: Some("myGlueKey".into()),
            ..Default::default()
        };
        let (_, seen) = glue_unframe_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options);
        assert_eq!(seen.result().unwrap().key(), "myGlueKey");
    }

    #[tokio::test]
    async fn glue_uuid_round_trips_every_byte() {
        for id in [
            "00010203-0405-0607-0809-0a0b0c0d0e0f",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
        ] {
            let framed = glue_frame(&[0x42], glue_options(id, "none")).await.unwrap();
            let (out, _) = glue_unframe(vec![framed]).await;
            assert_eq!(out.unwrap()[0].schema_version_id, id);
        }
        // Uppercase input is accepted; ids are emitted lowercase.
        let framed = glue_frame(HELLO, glue_options(&UUID.to_uppercase(), "none"))
            .await
            .unwrap();
        assert_eq!(
            glue_unframe(vec![framed]).await.0.unwrap()[0].schema_version_id,
            UUID
        );
    }
}
