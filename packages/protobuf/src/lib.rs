// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Protocol Buffers streams, ported from `@datastream/protobuf`.
//!
//! Messages are `serde_json::Value` objects (protobuf JSON mapping) encoded
//! and decoded with a [`MessageDescriptor`] from a caller-supplied
//! [`prost_reflect::DescriptorPool`].

use std::future::Future;
use std::sync::Arc;

use async_stream::try_stream;
use datastream_core::{
    create_transform_stream, noop_flush, BoxFuture, DataStream, Error, Result, StreamExt, Value,
};
pub use prost_reflect;
use prost_reflect::prost::Message;
use prost_reflect::{DeserializeOptions, DynamicMessage, MessageDescriptor, SerializeOptions};

pub type ResolveType<I> = Arc<dyn Fn(&I) -> BoxFuture<Result<MessageDescriptor>> + Send + Sync>;

/// The message type: fixed, or resolved per chunk (e.g. from a schema id).
pub enum ProtobufType<I> {
    Static(MessageDescriptor),
    Dynamic(ResolveType<I>),
}

impl<I> From<MessageDescriptor> for ProtobufType<I> {
    fn from(desc: MessageDescriptor) -> Self {
        Self::Static(desc)
    }
}

impl<I: 'static> ProtobufType<I> {
    /// Resolve the type per chunk with an async function.
    pub fn dynamic<F, Fut>(f: F) -> Self
    where
        F: Fn(&I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<MessageDescriptor>> + Send + 'static,
    {
        Self::Dynamic(Arc::new(
            move |chunk: &I| -> BoxFuture<Result<MessageDescriptor>> { Box::pin(f(chunk)) },
        ))
    }
}

impl<I> ProtobufType<I> {
    fn resolve(&self, chunk: &I) -> BoxFuture<Result<MessageDescriptor>> {
        match self {
            Self::Static(desc) => {
                let desc = desc.clone();
                Box::pin(async move { Ok(desc) })
            }
            Self::Dynamic(f) => f(chunk),
        }
    }
}

pub struct ProtobufEncodeOptions {
    pub type_: ProtobufType<Value>,
}

fn encode(desc: MessageDescriptor, chunk: Value) -> Result<Vec<u8>> {
    // Like protobufjs `Type.create`, unknown fields are ignored.
    let options = DeserializeOptions::new().deny_unknown_fields(false);
    let message = DynamicMessage::deserialize_with_options(desc, chunk, &options)?;
    Ok(message.encode_to_vec())
}

/// Encode each object to protobuf bytes.
pub fn protobuf_encode_stream(
    mut input: DataStream<Value>,
    options: ProtobufEncodeOptions,
) -> DataStream<Vec<u8>> {
    Box::pin(try_stream! {
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            let desc = options.type_.resolve(&chunk);
            yield encode(desc.await?, chunk)?;
        }
    })
}

pub struct ProtobufDecodeOptions<I> {
    pub type_: ProtobufType<I>,
    /// Limit on the total input bytes (default unlimited).
    pub max_output_size: Option<usize>,
}

fn decode(desc: MessageDescriptor, bytes: &[u8]) -> Result<Value> {
    let message = DynamicMessage::decode(desc, bytes)?;
    let options = SerializeOptions::new().skip_default_fields(false);
    Ok(message.serialize_with_options(serde_json::value::Serializer, &options)?)
}

/// Decode each chunk's bytes to an object. Chunks are anything that exposes
/// the payload bytes via `AsRef<[u8]>` (bytes, or an envelope carrying a schema id).
pub fn protobuf_decode_stream<I>(
    mut input: DataStream<I>,
    options: ProtobufDecodeOptions<I>,
) -> DataStream<Value>
where
    I: AsRef<[u8]> + Send + 'static,
{
    let max = options.max_output_size.unwrap_or(usize::MAX);
    Box::pin(try_stream! {
        let mut input_size = 0usize;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            input_size = input_size.saturating_add(chunk.as_ref().len());
            if input_size > max {
                Err::<(), Error>(
                    format!("Protobuf decode input exceeds maxOutputSize ({max} bytes)").into(),
                )?;
            }
            let desc = options.type_.resolve(&chunk);
            yield decode(desc.await?, chunk.as_ref())?;
        }
    })
}

/// Prefix each message with its base-128 varint length.
pub fn protobuf_length_prefix_frame_stream(input: DataStream<Vec<u8>>) -> DataStream<Vec<u8>> {
    create_transform_stream(
        input,
        |chunk: Vec<u8>, enqueue: &mut Vec<Vec<u8>>| {
            let mut framed = Vec::with_capacity(chunk.len() + 10);
            let mut len = chunk.len();
            while len > 0x7f {
                framed.push((len as u8 & 0x7f) | 0x80);
                len >>= 7;
            }
            framed.push(len as u8);
            framed.extend_from_slice(&chunk);
            enqueue.push(framed);
            Ok(())
        },
        noop_flush,
    )
}

#[derive(Default, Clone, Debug)]
pub struct ProtobufLengthPrefixUnframeOptions {
    /// Largest accepted message in bytes (default unlimited).
    pub max_message_size: Option<usize>,
}

// Parse one varint length prefix off the front of `buf`. Returns the prefix
// size and message length once the whole message is buffered.
fn read_prefix(buf: &[u8], max: usize) -> Result<Option<(usize, usize)>> {
    let mut length = 0u64;
    let mut scale = 1u64;
    for (i, &byte) in buf.iter().enumerate() {
        length = length.saturating_add(u64::from(byte & 0x7f).saturating_mul(scale));
        if byte & 0x80 == 0 {
            let length = usize::try_from(length).unwrap_or(usize::MAX);
            if length > max {
                return Err(
                    format!("Protobuf message exceeds maxMessageSize ({max} bytes)").into(),
                );
            }
            let prefix = i + 1;
            return Ok((buf.len() - prefix >= length).then_some((prefix, length)));
        }
        scale = scale.saturating_mul(0x80);
    }
    Ok(None)
}

/// Split a byte stream of varint length-prefixed messages back into messages.
pub fn protobuf_length_prefix_unframe_stream(
    mut input: DataStream<Vec<u8>>,
    options: ProtobufLengthPrefixUnframeOptions,
) -> DataStream<Vec<u8>> {
    let max = options.max_message_size.unwrap_or(usize::MAX);
    Box::pin(try_stream! {
        let mut pending = Vec::new();
        while let Some(chunk) = input.next().await {
            pending.extend_from_slice(&chunk?);
            let mut start = 0;
            while let Some((prefix, length)) = read_prefix(&pending[start..], max)? {
                start += prefix;
                yield pending[start..start + length].to_vec();
                start += length;
            }
            pending.drain(..start);
        }
        if !pending.is_empty() {
            Err::<(), Error>("Protobuf unframe stream ended with an incomplete message".into())?;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, stream_to_array};
    use prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
    use prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto};
    use prost_reflect::DescriptorPool;
    use serde_json::json;

    fn msg_type() -> MessageDescriptor {
        let field = |name: &str, number, kind: Type| FieldDescriptorProto {
            name: Some(name.into()),
            number: Some(number),
            label: Some(Label::Optional as i32),
            r#type: Some(kind as i32),
            ..Default::default()
        };
        let file = FileDescriptorProto {
            name: Some("msg.proto".into()),
            package: Some("test".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Msg".into()),
                field: vec![field("id", 1, Type::Int32), field("name", 2, Type::String)],
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file).unwrap();
        pool.get_message_by_name("test.Msg").unwrap()
    }

    fn messages() -> Vec<Value> {
        vec![
            json!({"id": 1, "name": "alpha"}),
            json!({"id": 2, "name": "beta"}),
            json!({"id": 3, "name": "gamma"}),
        ]
    }

    // Encoded form is >127 bytes, forcing a multi-byte varint prefix.
    fn big_message() -> Value {
        json!({"id": 42, "name": "x".repeat(200)})
    }

    async fn encode_all(input: Vec<Value>) -> Vec<Vec<u8>> {
        let options = ProtobufEncodeOptions {
            type_: msg_type().into(),
        };
        let stream = protobuf_encode_stream(create_readable_stream(input), options);
        stream_to_array(stream, None).await.unwrap()
    }

    fn decode_all(input: &[Vec<u8>]) -> Vec<Value> {
        input
            .iter()
            .map(|b| decode(msg_type(), b).unwrap())
            .collect()
    }

    async fn frame_all(input: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
        let stream = protobuf_length_prefix_frame_stream(create_readable_stream(input));
        stream_to_array(stream, None).await.unwrap()
    }

    async fn unframe_all(
        input: Vec<Vec<u8>>,
        max_message_size: Option<usize>,
    ) -> Result<Vec<Vec<u8>>> {
        let options = ProtobufLengthPrefixUnframeOptions { max_message_size };
        let stream = protobuf_length_prefix_unframe_stream(create_readable_stream(input), options);
        stream_to_array(stream, None).await
    }

    fn bytes_one_at_a_time(frames: &[Vec<u8>]) -> Vec<Vec<u8>> {
        frames.concat().into_iter().map(|b| vec![b]).collect()
    }

    #[tokio::test]
    async fn encode_static_type_round_trips() {
        let encoded = encode_all(messages()).await;
        assert_eq!(encoded.len(), 3);
        assert_eq!(decode_all(&encoded), messages());
    }

    #[tokio::test]
    async fn encode_dynamic_type() {
        let options = ProtobufEncodeOptions {
            type_: ProtobufType::dynamic(|_: &Value| {
                let desc = msg_type();
                async move { Ok::<_, Error>(desc) }
            }),
        };
        let stream = protobuf_encode_stream(create_readable_stream(messages()), options);
        let encoded = stream_to_array(stream, None).await.unwrap();
        assert_eq!(decode_all(&encoded), messages());
    }

    #[tokio::test]
    async fn encode_ignores_unknown_fields_and_rejects_bad_values() {
        let encoded = encode_all(vec![json!({"id": 1, "name": "a", "extra": true})]).await;
        assert_eq!(decode_all(&encoded), [json!({"id": 1, "name": "a"})]);
        let options = ProtobufEncodeOptions {
            type_: msg_type().into(),
        };
        let stream =
            protobuf_encode_stream(create_readable_stream([json!({"id": "abc"})]), options);
        assert!(stream_to_array(stream, None).await.is_err());
    }

    #[tokio::test]
    async fn decode_bytes_to_objects() {
        let encoded = encode_all(messages()).await;
        let options = ProtobufDecodeOptions {
            type_: msg_type().into(),
            max_output_size: None,
        };
        let stream = protobuf_decode_stream(create_readable_stream(encoded), options);
        assert_eq!(stream_to_array(stream, None).await.unwrap(), messages());
    }

    #[tokio::test]
    async fn decode_includes_default_values() {
        let encoded = encode_all(vec![json!({"id": 0, "name": ""})]).await;
        assert_eq!(decode_all(&encoded), [json!({"id": 0, "name": ""})]);
    }

    struct Wrapped {
        data: Vec<u8>,
    }

    impl AsRef<[u8]> for Wrapped {
        fn as_ref(&self) -> &[u8] {
            &self.data
        }
    }

    #[tokio::test]
    async fn decode_payload_from_envelope_with_dynamic_type() {
        let wrapped: Vec<Wrapped> = encode_all(messages())
            .await
            .into_iter()
            .map(|data| Wrapped { data })
            .collect();
        let options = ProtobufDecodeOptions {
            type_: ProtobufType::dynamic(|_: &Wrapped| {
                let desc = msg_type();
                async move { Ok::<_, Error>(desc) }
            }),
            max_output_size: None,
        };
        let stream = protobuf_decode_stream(create_readable_stream(wrapped), options);
        assert_eq!(stream_to_array(stream, None).await.unwrap(), messages());
    }

    #[tokio::test]
    async fn decode_max_output_size() {
        let encoded = encode_all(messages()).await;
        let exact: usize = encoded.iter().map(Vec::len).sum();
        for (max, ok) in [(1024, true), (exact, true), (exact - 1, false), (1, false)] {
            let options = ProtobufDecodeOptions {
                type_: msg_type().into(),
                max_output_size: Some(max),
            };
            let stream = protobuf_decode_stream(create_readable_stream(encoded.clone()), options);
            match stream_to_array(stream, None).await {
                Ok(decoded) => {
                    assert!(ok);
                    assert_eq!(decoded, messages());
                }
                Err(e) => {
                    assert!(!ok);
                    assert_eq!(
                        e.to_string(),
                        format!("Protobuf decode input exceeds maxOutputSize ({max} bytes)")
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn decode_rejects_malformed_bytes() {
        let options = ProtobufDecodeOptions {
            type_: msg_type().into(),
            max_output_size: None,
        };
        let stream = protobuf_decode_stream(create_readable_stream([vec![0xff, 0xff]]), options);
        assert!(stream_to_array(stream, None).await.is_err());
    }

    #[tokio::test]
    async fn frame_then_unframe_round_trips() {
        let frames = frame_all(encode_all(messages()).await).await;
        assert_eq!(frames.len(), 3);
        let unframed = unframe_all(frames, None).await.unwrap();
        assert_eq!(decode_all(&unframed), messages());
    }

    #[tokio::test]
    async fn frame_varint_boundaries() {
        let body = vec![7u8; 127];
        let frames = frame_all(vec![body.clone()]).await;
        assert_eq!(frames[0].len(), 128);
        assert_eq!(unframe_all(frames, None).await.unwrap(), [body]);
        let frames = frame_all(vec![vec![1u8; 128]]).await;
        assert_eq!(frames[0][..2], [0x80u8, 0x01]);
        let frames = frame_all(vec![vec![]]).await;
        assert_eq!(frames, [vec![0u8]]);
        assert_eq!(unframe_all(frames, None).await.unwrap(), [Vec::<u8>::new()]);
    }

    #[tokio::test]
    async fn unframe_allows_exactly_max_message_size() {
        let body = vec![3u8; 50];
        let frames = frame_all(vec![body.clone()]).await;
        assert_eq!(unframe_all(frames, Some(50)).await.unwrap(), [body]);
    }

    #[tokio::test]
    async fn unframe_reassembles_byte_sized_chunks() {
        let mut input = vec![big_message()];
        input.extend(messages());
        let frames = frame_all(encode_all(input.clone()).await).await;
        let unframed = unframe_all(bytes_one_at_a_time(&frames), Some(4096))
            .await
            .unwrap();
        assert_eq!(decode_all(&unframed), input);
    }

    #[tokio::test]
    async fn unframe_reassembles_body_split_mid_chunk() {
        let mut input = vec![big_message()];
        input.extend(messages());
        let all = frame_all(encode_all(input.clone()).await).await.concat();
        let chunks = vec![all[..100].to_vec(), all[100..].to_vec()];
        let unframed = unframe_all(chunks, Some(4096)).await.unwrap();
        assert_eq!(decode_all(&unframed), input);
    }

    #[tokio::test]
    async fn unframe_many_small_messages_one_byte_at_a_time() {
        let small: Vec<Value> = (0..20)
            .map(|i| json!({"id": i, "name": format!("m{i}")}))
            .collect();
        let frames = frame_all(encode_all(small.clone()).await).await;
        let unframed = unframe_all(bytes_one_at_a_time(&frames), Some(4096))
            .await
            .unwrap();
        assert_eq!(decode_all(&unframed), small);
    }

    #[tokio::test]
    async fn unframe_rejects_oversized_message() {
        let frames = frame_all(encode_all(vec![big_message()]).await).await;
        let e = unframe_all(frames, Some(8)).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "Protobuf message exceeds maxMessageSize (8 bytes)"
        );
    }

    #[tokio::test]
    async fn unframe_rejects_truncated_stream() {
        let e = unframe_all(vec![vec![5, 1, 2]], None).await.unwrap_err();
        assert_eq!(
            e.to_string(),
            "Protobuf unframe stream ended with an incomplete message"
        );
        // An incomplete prefix too.
        assert!(unframe_all(vec![vec![0x80]], None).await.is_err());
    }
}
