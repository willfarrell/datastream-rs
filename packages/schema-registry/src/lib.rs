// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Schema Registry wire-format framing streams, ported from
//! `@datastream/schema-registry`.
//!
//! Each input chunk is one WHOLE framed record (one Kafka message). Envelopes
//! carry no embedded length, so a frame split across chunks cannot be
//! reassembled reliably (a continuation slice may itself start with the magic
//! byte); re-frame sliced input upstream. Each chunk is decoded and emitted
//! immediately, so a live consumer's latest message is never held back.
//!
//! An unframe chunk may also be a message carrying the frame in its value
//! (see [`FramedChunk`]); the message rides along on the emitted envelope, so
//! a Kafka message can still be committed after it has been unframed.
//!
//! Unframe streams emit per-chunk envelopes so a downstream decoder can pick
//! the schema per record. The shared result lists every distinct id seen plus
//! the most recently seen one, which is racy under backpressure.

use std::collections::HashSet;
use std::hash::Hash;
use std::io::{Read, Write};

use datastream_core::{
    create_transform_stream, noop_flush, DataStream, Result, StreamResult, Value,
};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde_json::json;

#[cfg(feature = "resolver")]
mod resolver;
#[cfg(feature = "resolver")]
pub use resolver::{
    confluent_schema_registry_resolver, ConfluentSchema, ConfluentSchemaRegistryError,
    ConfluentSchemaRegistryOptions, ConfluentSchemaRegistryResolver,
};

const GLUE_COMPRESSION_NONE: u8 = 0x00;
const GLUE_COMPRESSION_ZLIB: u8 = 0x05;
// Cap on distinct ids an unframe stream records in its result: a long-lived
// consumer must not grow its id list forever.
const DEFAULT_MAX_SCHEMA_IDS: usize = 1000;
// Cap on bytes a zlib (de)compression buffers per frame, matching
// datastream-compress.
const DEFAULT_MAX_OUTPUT_SIZE: usize = 268_435_456; // 256MiB

/// One unframe-stream input chunk: a whole frame, or a message carrying one.
pub trait FramedChunk: Send + 'static {
    /// What rides along on the emitted envelope (`()` for bare bytes).
    type Message: Send + 'static;
    /// Split the frame off. `None` (e.g. a Kafka tombstone) errors the stream.
    fn into_frame(self) -> (Option<Vec<u8>>, Self::Message);
}

impl FramedChunk for Vec<u8> {
    type Message = ();
    fn into_frame(self) -> (Option<Vec<u8>>, ()) {
        (Some(self), ())
    }
}

/// The frame is the message's `value`; the message (with `value` taken, as
/// it is now the envelope's `payload`) keeps its topic/partition/offset.
#[cfg(feature = "kafka")]
impl FramedChunk for datastream_kafka::ConsumerMessage {
    type Message = Self;
    fn into_frame(mut self) -> (Option<Vec<u8>>, Self) {
        (self.value.take(), self)
    }
}

fn split_frame<T: FramedChunk>(chunk: T) -> Result<(Vec<u8>, T::Message)> {
    match chunk.into_frame() {
        (Some(frame), message) => Ok((frame, message)),
        (None, _) => {
            Err("schema-registry: chunk must be bytes (got a message with no value)".into())
        }
    }
}

// Distinct ids for the unframe result, first-seen order, up to `limit`;
// `untracked` counts frames whose new id was past the cap.
struct SeenIds<K> {
    result: StreamResult,
    seen: HashSet<K>,
    limit: usize,
    untracked: u64,
    // Result keys: most recent id, distinct ids, untracked count.
    keys: [&'static str; 3],
}

impl<K: Eq + Hash + Clone + Into<Value>> SeenIds<K> {
    fn new(result: StreamResult, limit: usize, keys: [&'static str; 3]) -> Self {
        Self {
            result,
            seen: HashSet::new(),
            limit,
            untracked: 0,
            keys,
        }
    }

    fn observe(&mut self, id: &K) {
        let [last, ids, untracked] = self.keys;
        let mut added = false;
        if !self.seen.contains(id) {
            if self.seen.len() < self.limit {
                self.seen.insert(id.clone());
                added = true;
            } else {
                self.untracked += 1;
            }
        }
        let count = self.untracked;
        self.result.update(|v| {
            if added {
                if let Some(list) = v[ids].as_array_mut() {
                    list.push(id.clone().into());
                }
            }
            v[untracked] = count.into();
            v[last] = id.clone().into();
        });
    }
}

// *** Confluent (5-byte: 0x00 magic + uint32 BE schema id) *** //

/// An unframed Confluent record; `message` is what the input chunk carried
/// besides the frame (see [`FramedChunk`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfluentEnvelope<M = ()> {
    pub schema_id: u32,
    pub payload: Vec<u8>,
    pub message: M,
}

impl<M> AsRef<[u8]> for ConfluentEnvelope<M> {
    fn as_ref(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Default, Clone, Debug)]
pub struct ConfluentFrameOptions {
    pub schema_id: Option<u32>,
    pub result_key: Option<String>,
}

/// Prepend `0x00` + the big-endian schema id. The result echoes `{ schemaId }`
/// under `confluentFrameSchemaId`.
pub fn confluent_frame_stream(
    input: DataStream<Vec<u8>>,
    options: ConfluentFrameOptions,
) -> Result<(DataStream<Vec<u8>>, StreamResult)> {
    let schema_id = options
        .schema_id
        .ok_or("confluentFrameStream: schemaId must be an unsigned 32-bit integer")?;
    let mut header = vec![0x00];
    header.extend_from_slice(&schema_id.to_be_bytes());
    // Its default key differs from confluent_unframe_stream's so both can
    // share one pipeline result.
    let key = options
        .result_key
        .unwrap_or_else(|| "confluentFrameSchemaId".into());
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
    /// Distinct ids recorded in the result (default 1000; `usize::MAX` for
    /// no cap).
    pub max_schema_ids: Option<usize>,
    pub result_key: Option<String>,
}

/// Strip the Confluent header, emitting `{ schema_id, payload, message }`
/// envelopes. The result is `{ schemaId, schemaIds, untrackedSchemaIds }`.
pub fn confluent_unframe_stream<T: FramedChunk>(
    input: DataStream<T>,
    options: ConfluentUnframeOptions,
) -> (DataStream<ConfluentEnvelope<T::Message>>, StreamResult) {
    let key = options
        .result_key
        .unwrap_or_else(|| "confluentSchemaId".into());
    let mut seen = SeenIds::<u32>::new(
        StreamResult::new(
            key,
            json!({ "schemaId": null, "schemaIds": [], "untrackedSchemaIds": 0 }),
        ),
        options.max_schema_ids.unwrap_or(DEFAULT_MAX_SCHEMA_IDS),
        ["schemaId", "schemaIds", "untrackedSchemaIds"],
    );
    let result = seen.result.clone();
    let stream = create_transform_stream(
        input,
        move |chunk: T, enqueue: &mut Vec<ConfluentEnvelope<T::Message>>| {
            let (mut frame, message) = split_frame(chunk)?;
            if frame.len() < 5 || frame[0] != 0x00 {
                return Err("confluentUnframeStream: missing 0x00 magic byte / frame is too short (each chunk must be one whole frame)".into());
            }
            let schema_id = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]);
            seen.observe(&schema_id);
            enqueue.push(ConfluentEnvelope {
                schema_id,
                payload: frame.split_off(5),
                message,
            });
            Ok(())
        },
        noop_flush,
    );
    (stream, result)
}

// *** Glue (18-byte: 0x03 magic + 1 byte compression + 16 bytes UUID) *** //

/// An unframed Glue record; `message` is what the input chunk carried besides
/// the frame (see [`FramedChunk`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GlueEnvelope<M = ()> {
    pub schema_version_id: String,
    /// `"none"` or `"zlib"`.
    pub compression: &'static str,
    pub payload: Vec<u8>,
    pub message: M,
}

impl<M> AsRef<[u8]> for GlueEnvelope<M> {
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

fn output_size_exceeded(max: usize) -> datastream_core::Error {
    format!("schema-registry: maxOutputSize exceeded ({max})").into()
}

fn deflate(bytes: &[u8], max: usize) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(bytes)?;
    let out = encoder.finish()?;
    if out.len() > max {
        return Err(output_size_exceeded(max));
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
        return Err(output_size_exceeded(max));
    }
    Ok(out)
}

#[derive(Default, Clone, Debug)]
pub struct GlueFrameOptions {
    pub schema_version_id: Option<String>,
    /// `"none"` (default) or `"zlib"`.
    pub compression: Option<String>,
    /// Limit on each compressed payload (default 256MiB; `usize::MAX` for
    /// none).
    pub max_output_size: Option<usize>,
    pub result_key: Option<String>,
}

/// Prepend `0x03` + compression byte + 16-byte UUID, compressing the payload
/// with zlib when asked. The result echoes `{ schemaVersionId }` under
/// `glueFrameSchemaVersionId`.
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
    // Default key differs from glue_unframe_stream's so both can share one
    // pipeline result.
    let key = options
        .result_key
        .unwrap_or_else(|| "glueFrameSchemaVersionId".into());
    let result = StreamResult::new(key, json!({ "schemaVersionId": schema_version_id }));
    let max = options.max_output_size.unwrap_or(DEFAULT_MAX_OUTPUT_SIZE);
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
    /// Limit on each inflated payload (default 256MiB; `usize::MAX` for none).
    pub max_output_size: Option<usize>,
    /// Distinct ids recorded in the result (default 1000; `usize::MAX` for
    /// no cap).
    pub max_schema_ids: Option<usize>,
    pub result_key: Option<String>,
}

/// Strip the Glue header (inflating zlib payloads), emitting
/// `{ schema_version_id, compression, payload, message }` envelopes. The
/// result is `{ schemaVersionId, compression, schemaVersionIds,
/// untrackedSchemaVersionIds }`.
pub fn glue_unframe_stream<T: FramedChunk>(
    input: DataStream<T>,
    options: GlueUnframeOptions,
) -> (DataStream<GlueEnvelope<T::Message>>, StreamResult) {
    let max = options.max_output_size.unwrap_or(DEFAULT_MAX_OUTPUT_SIZE);
    let key = options
        .result_key
        .unwrap_or_else(|| "glueSchemaVersionId".into());
    let value = json!({
        "schemaVersionId": null,
        "compression": null,
        "schemaVersionIds": [],
        "untrackedSchemaVersionIds": 0,
    });
    let mut seen = SeenIds::<String>::new(
        StreamResult::new(key, value),
        options.max_schema_ids.unwrap_or(DEFAULT_MAX_SCHEMA_IDS),
        [
            "schemaVersionId",
            "schemaVersionIds",
            "untrackedSchemaVersionIds",
        ],
    );
    let result = seen.result.clone();
    let stream = create_transform_stream(
        input,
        move |chunk: T, enqueue: &mut Vec<GlueEnvelope<T::Message>>| {
            let (mut frame, message) = split_frame(chunk)?;
            if frame.len() < 18 || frame[0] != 0x03 {
                return Err("glueUnframeStream: missing 0x03 magic byte / frame is too short (each chunk must be one whole frame)".into());
            }
            let compression_byte = frame[1];
            let schema_version_id = bytes_to_uuid(&frame[2..18]);
            seen.observe(&schema_version_id);
            let payload = frame.split_off(18);
            let (compression, payload) = match compression_byte {
                GLUE_COMPRESSION_NONE => ("none", payload),
                GLUE_COMPRESSION_ZLIB => ("zlib", inflate(&payload, max)?),
                other => {
                    return Err(format!(
                        "glueUnframeStream: unsupported compression byte 0x{other:02x}"
                    )
                    .into())
                }
            };
            seen.result
                .update(|v| v["compression"] = json!(compression));
            enqueue.push(GlueEnvelope {
                schema_version_id,
                compression,
                payload,
                message,
            });
            Ok(())
        },
        noop_flush,
    );
    (stream, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{
        create_readable_stream, create_readable_stream_from_bytes, stream_to_array,
    };

    const HELLO: &[u8] = b"hello world";
    const UUID: &str = "12345678-1234-1234-1234-1234567890ab";

    fn confluent(schema_id: u32, payload: &[u8]) -> ConfluentEnvelope {
        ConfluentEnvelope {
            schema_id,
            payload: payload.to_vec(),
            message: (),
        }
    }

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
        options: ConfluentUnframeOptions,
    ) -> (Result<Vec<ConfluentEnvelope>>, StreamResult) {
        let (stream, seen) = confluent_unframe_stream(input, options);
        (stream_to_array(stream, None).await, seen)
    }

    async fn confluent_unframe(
        chunks: Vec<Vec<u8>>,
    ) -> (Result<Vec<ConfluentEnvelope>>, StreamResult) {
        confluent_unframe_with(create_readable_stream(chunks), Default::default()).await
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
        options: GlueUnframeOptions,
    ) -> (Result<Vec<GlueEnvelope>>, StreamResult) {
        let (stream, seen) = glue_unframe_stream(input, options);
        (stream_to_array(stream, None).await, seen)
    }

    async fn glue_unframe(chunks: Vec<Vec<u8>>) -> (Result<Vec<GlueEnvelope>>, StreamResult) {
        glue_unframe_with(create_readable_stream(chunks), Default::default()).await
    }

    fn max_output_size(max: usize) -> GlueUnframeOptions {
        GlueUnframeOptions {
            max_output_size: Some(max),
            ..Default::default()
        }
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
        // Differs from the unframe default so both can share one result.
        assert_eq!(result.key(), "confluentFrameSchemaId");
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
        assert_eq!(out.unwrap(), [confluent(42, HELLO)]);
        assert_eq!(seen.key(), "confluentSchemaId");
        assert_eq!(
            seen.get(),
            json!({"schemaId": 42, "schemaIds": [42], "untrackedSchemaIds": 0})
        );
    }

    #[tokio::test]
    async fn confluent_unframe_reads_big_endian() {
        let (out, _) = confluent_unframe(vec![vec![0x00, 0x01, 0x02, 0x03, 0x04, 0xaa]]).await;
        assert_eq!(out.unwrap(), [confluent(0x01020304, &[0xaa])]);
    }

    #[tokio::test]
    async fn confluent_unframe_rejects_bad_frames() {
        let message = "confluentUnframeStream: missing 0x00 magic byte / frame is too short (each chunk must be one whole frame)";
        let (out, _) = confluent_unframe(vec![vec![0x01, 0, 0, 0, 0, 0x68, 0x69]]).await;
        assert_eq!(out.unwrap_err().to_string(), message);
        let (out, _) = confluent_unframe(vec![vec![0x00, 0x00, 0x00, 0x01]]).await;
        assert_eq!(out.unwrap_err().to_string(), message);
    }

    #[tokio::test]
    async fn confluent_result_lists_distinct_ids() {
        let a = confluent_frame(HELLO, 5).await;
        let b = confluent_frame(&[0x01], 5).await;
        let (_, seen) = confluent_unframe(vec![a, b]).await;
        assert_eq!(
            seen.get(),
            json!({"schemaId": 5, "schemaIds": [5], "untrackedSchemaIds": 0})
        );

        // Mixed ids are reported, not an error: the latest plus every id seen.
        let chunks = vec![
            confluent_frame(HELLO, 10).await,
            confluent_frame(HELLO, 11).await,
            confluent_frame(HELLO, 10).await,
        ];
        let (out, seen) = confluent_unframe(chunks).await;
        assert_eq!(
            out.unwrap().iter().map(|e| e.schema_id).collect::<Vec<_>>(),
            [10, 11, 10]
        );
        assert_eq!(
            seen.get(),
            json!({"schemaId": 10, "schemaIds": [10, 11], "untrackedSchemaIds": 0})
        );
    }

    #[tokio::test]
    async fn confluent_result_caps_distinct_ids() {
        let mut chunks = Vec::new();
        for id in [1, 2, 3, 1, 3, 4] {
            chunks.push(confluent_frame(HELLO, id).await);
        }
        let options = ConfluentUnframeOptions {
            max_schema_ids: Some(2),
            ..Default::default()
        };
        let (out, seen) =
            confluent_unframe_with(create_readable_stream(chunks.clone()), options).await;
        assert_eq!(out.unwrap().len(), 6);
        // 3 (twice) and 4 were past the cap; the latest id is still reported.
        assert_eq!(
            seen.get(),
            json!({"schemaId": 4, "schemaIds": [1, 2], "untrackedSchemaIds": 3})
        );

        // Default cap is 1000; usize::MAX opts out.
        let mut many = Vec::new();
        for id in 0..1001 {
            many.push(confluent_frame(&[], id).await);
        }
        let (_, seen) = confluent_unframe(many.clone()).await;
        assert_eq!(seen.get()["schemaIds"].as_array().unwrap().len(), 1000);
        assert_eq!(seen.get()["untrackedSchemaIds"], 1);
        let options = ConfluentUnframeOptions {
            max_schema_ids: Some(usize::MAX),
            ..Default::default()
        };
        let (_, seen) = confluent_unframe_with(create_readable_stream(many), options).await;
        assert_eq!(seen.get()["schemaIds"].as_array().unwrap().len(), 1001);
    }

    #[test]
    fn confluent_result_before_frames_and_custom_key() {
        let (_, seen) = confluent_unframe_stream(
            create_readable_stream(Vec::<Vec<u8>>::new()),
            ConfluentUnframeOptions::default(),
        );
        assert_eq!(
            seen.get(),
            json!({"schemaId": null, "schemaIds": [], "untrackedSchemaIds": 0})
        );
        let options = ConfluentUnframeOptions {
            result_key: Some("myKey".into()),
            ..Default::default()
        };
        let (_, seen) =
            confluent_unframe_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options);
        assert_eq!(seen.key(), "myKey");
    }

    #[tokio::test]
    async fn confluent_each_chunk_is_one_whole_frame() {
        // A chunk is never folded into the previous frame, even one that does
        // not start with the magic byte.
        let full = confluent_frame(HELLO, 200).await;
        let (out, _) = confluent_unframe(vec![full[..5].to_vec(), full[5..].to_vec()]).await;
        assert!(out.unwrap_err().to_string().contains("one whole frame"));

        // A frame sliced by create_readable_stream_from_bytes is not reassembled.
        let big = vec![0x01u8; 40 * 1024];
        let framed = confluent_frame(&big, 123).await;
        let (out, _) = confluent_unframe_with(
            create_readable_stream_from_bytes(framed, None).unwrap(),
            Default::default(),
        )
        .await;
        assert!(out.is_err());

        // A payload starting with the magic byte stays in its own frame.
        let a = confluent_frame(&[0x00, 0x00, 0x00, 0x00, 0x09], 811).await;
        let b = confluent_frame(&[], 812).await;
        let (out, _) = confluent_unframe(vec![a, b]).await;
        assert_eq!(
            out.unwrap(),
            [
                confluent(811, &[0x00, 0x00, 0x00, 0x00, 0x09]),
                confluent(812, &[])
            ]
        );
    }

    #[tokio::test]
    async fn confluent_emits_a_frame_before_the_next_chunk_arrives() {
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Vec<u8>>>(1);
        let mut rx = rx;
        let input: DataStream<Vec<u8>> = Box::pin(async_stream::stream! {
            while let Some(item) = rx.recv().await {
                yield item;
            }
        });
        let (mut stream, _) = confluent_unframe_stream(input, ConfluentUnframeOptions::default());
        tx.send(Ok(confluent_frame(HELLO, 1).await)).await.unwrap();
        use datastream_core::StreamExt;
        assert_eq!(stream.next().await.unwrap().unwrap(), confluent(1, HELLO));
        drop(tx);
        assert!(stream.next().await.is_none());
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
            message: (),
        };
        assert_eq!(out.unwrap(), [expected]);
        assert_eq!(seen.key(), "glueSchemaVersionId");
        assert_eq!(
            seen.get(),
            json!({
                "schemaVersionId": UUID,
                "compression": "none",
                "schemaVersionIds": [UUID],
                "untrackedSchemaVersionIds": 0,
            })
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
        assert_eq!(seen.get()["compression"], "zlib");
        // Empty payload.
        let framed = glue_frame(&[], glue_options(UUID, "zlib")).await.unwrap();
        let (out, _) = glue_unframe(vec![framed]).await;
        assert!(out.unwrap()[0].payload.is_empty());
    }

    #[tokio::test]
    async fn glue_unframe_max_output_size() {
        let framed = glue_frame(&[b'a'; 100], glue_options(UUID, "zlib"))
            .await
            .unwrap();
        let input = create_readable_stream([framed.clone()]);
        let (out, _) = glue_unframe_with(input, max_output_size(100)).await;
        assert_eq!(out.unwrap()[0].payload.len(), 100);
        let input = create_readable_stream([framed]);
        let (out, _) = glue_unframe_with(input, max_output_size(10)).await;
        assert_eq!(
            out.unwrap_err().to_string(),
            "schema-registry: maxOutputSize exceeded (10)"
        );
        // usize::MAX disables the limit.
        let framed = glue_frame(&[b'a'; 2048], glue_options(UUID, "zlib"))
            .await
            .unwrap();
        let input = create_readable_stream([framed]);
        let (out, _) = glue_unframe_with(input, max_output_size(usize::MAX)).await;
        assert_eq!(out.unwrap()[0].payload.len(), 2048);
    }

    #[tokio::test]
    async fn glue_frame_max_output_size() {
        let options = GlueFrameOptions {
            max_output_size: Some(10),
            ..glue_options(UUID, "zlib")
        };
        let e = glue_frame(&[b'a'; 4096], options).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "schema-registry: maxOutputSize exceeded (10)"
        );
        // Uncompressed frames are not bounded (nothing is buffered).
        let options = GlueFrameOptions {
            max_output_size: Some(10),
            ..glue_options(UUID, "none")
        };
        assert_eq!(
            glue_frame(&[b'a'; 4096], options).await.unwrap().len(),
            18 + 4096
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

        let message = "glueUnframeStream: missing 0x03 magic byte / frame is too short (each chunk must be one whole frame)";
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
        // Differs from the unframe default so both can share one result.
        assert_eq!(result.key(), "glueFrameSchemaVersionId");
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
    async fn glue_each_chunk_is_one_whole_frame() {
        let payload: Vec<u8> = (1..=20).collect();
        let full = glue_frame(&payload, glue_options(UUID, "none"))
            .await
            .unwrap();
        let (out, _) = glue_unframe(vec![full[..18].to_vec(), full[18..].to_vec()]).await;
        assert!(out.unwrap_err().to_string().contains("one whole frame"));
    }

    #[tokio::test]
    async fn glue_envelopes_pair_payload_with_their_id() {
        let id_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let id_b = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";
        let a = glue_frame(b"payload-a", glue_options(id_a, "none"))
            .await
            .unwrap();
        let b = glue_frame(b"payload-b", glue_options(id_b, "zlib"))
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
        assert_eq!(
            seen.get(),
            json!({
                "schemaVersionId": id_b,
                "compression": "zlib",
                "schemaVersionIds": [id_a, id_b],
                "untrackedSchemaVersionIds": 0,
            })
        );
    }

    #[tokio::test]
    async fn glue_result_caps_distinct_ids_and_defaults() {
        let id_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
        let a = glue_frame(HELLO, glue_options(UUID, "none")).await.unwrap();
        let b = glue_frame(&[0x42], glue_options(id_a, "none"))
            .await
            .unwrap();
        let options = GlueUnframeOptions {
            max_schema_ids: Some(1),
            ..Default::default()
        };
        let input = create_readable_stream(vec![a.clone(), b.clone(), b]);
        let (_, seen) = glue_unframe_with(input, options).await;
        assert_eq!(seen.get()["schemaVersionId"], id_a);
        assert_eq!(seen.get()["schemaVersionIds"], json!([UUID]));
        assert_eq!(seen.get()["untrackedSchemaVersionIds"], 2);

        let (_, seen) = glue_unframe_stream(
            create_readable_stream(Vec::<Vec<u8>>::new()),
            GlueUnframeOptions::default(),
        );
        assert_eq!(
            seen.get(),
            json!({
                "schemaVersionId": null,
                "compression": null,
                "schemaVersionIds": [],
                "untrackedSchemaVersionIds": 0,
            })
        );
        let options = GlueUnframeOptions {
            result_key: Some("myGlueKey".into()),
            ..Default::default()
        };
        let (_, seen) = glue_unframe_stream(create_readable_stream(Vec::<Vec<u8>>::new()), options);
        assert_eq!(seen.key(), "myGlueKey");
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

    // *** Message envelopes *** //

    /// Shaped like a Kafka message: the frame is in `value`.
    #[derive(Clone, Debug, PartialEq)]
    struct Message {
        offset: i64,
        value: Option<Vec<u8>>,
    }

    impl FramedChunk for Message {
        type Message = Self;
        fn into_frame(mut self) -> (Option<Vec<u8>>, Self) {
            (self.value.take(), self)
        }
    }

    fn message(offset: i64, value: Option<Vec<u8>>) -> Message {
        Message { offset, value }
    }

    #[tokio::test]
    async fn confluent_unframes_a_message_value_and_keeps_the_message() {
        let frame = vec![0, 0, 0, 0, 7, 0xaa, 0xbb];
        let input =
            create_readable_stream([message(5, Some(frame.clone())), message(6, Some(frame))]);
        let (stream, seen) = confluent_unframe_stream(input, ConfluentUnframeOptions::default());
        let out = stream_to_array(stream, None).await.unwrap();
        assert_eq!(
            out,
            [5, 6].map(|offset| ConfluentEnvelope {
                schema_id: 7,
                payload: vec![0xaa, 0xbb],
                message: message(offset, None),
            })
        );
        assert_eq!(seen.get()["schemaIds"], json!([7]));
    }

    #[tokio::test]
    async fn glue_unframes_a_message_value_and_keeps_the_message() {
        let frame = glue_frame(HELLO, glue_options(UUID, "zlib")).await.unwrap();
        let input = create_readable_stream([message(5, Some(frame))]);
        let (stream, _) = glue_unframe_stream(input, GlueUnframeOptions::default());
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            [GlueEnvelope {
                schema_version_id: UUID.into(),
                compression: "zlib",
                payload: HELLO.to_vec(),
                message: message(5, None),
            }]
        );
    }

    #[tokio::test]
    async fn unframe_rejects_a_message_without_a_value() {
        let message_error = "schema-registry: chunk must be bytes (got a message with no value)";
        let input = create_readable_stream([message(5, None)]);
        let (stream, _) = confluent_unframe_stream(input, ConfluentUnframeOptions::default());
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), message_error);
        let input = create_readable_stream([message(5, None)]);
        let (stream, _) = glue_unframe_stream(input, GlueUnframeOptions::default());
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), message_error);
    }

    #[cfg(feature = "kafka")]
    #[tokio::test]
    async fn unframes_kafka_consumer_messages() {
        use datastream_kafka::ConsumerMessage;
        let kafka = |value| ConsumerMessage {
            topic: "orders".into(),
            partition: 2,
            offset: 5,
            value,
            headers: vec![("trace".into(), b"abc".to_vec())],
            ..Default::default()
        };
        let frame = confluent_frame(HELLO, 9).await;
        let input = create_readable_stream([kafka(Some(frame))]);
        let (stream, _) = confluent_unframe_stream(input, ConfluentUnframeOptions::default());
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            [ConfluentEnvelope {
                schema_id: 9,
                payload: HELLO.to_vec(),
                message: kafka(None),
            }]
        );
        // A tombstone (no value) errors the stream.
        let input = create_readable_stream([kafka(None)]);
        let (stream, _) = glue_unframe_stream(input, GlueUnframeOptions::default());
        assert!(stream_to_array(stream, None).await.is_err());
    }
}
