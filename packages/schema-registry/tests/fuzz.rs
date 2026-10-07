// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{create_readable_stream, stream_to_array, Result};
use datastream_schema_registry::{
    confluent_frame_stream, confluent_unframe_stream, glue_frame_stream, glue_unframe_stream,
    ConfluentEnvelope, ConfluentFrameOptions, ConfluentUnframeOptions, GlueEnvelope,
    GlueFrameOptions, GlueUnframeOptions,
};
use proptest::prelude::*;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

fn uuid(bytes: [u8; 16]) -> String {
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

async fn confluent_frame(payloads: Vec<Vec<u8>>, schema_id: u32) -> Vec<Vec<u8>> {
    let options = ConfluentFrameOptions {
        schema_id: Some(schema_id),
        ..Default::default()
    };
    let (stream, _) = confluent_frame_stream(create_readable_stream(payloads), options).unwrap();
    stream_to_array(stream, None).await.unwrap()
}

async fn confluent_unframe(chunks: Vec<Vec<u8>>) -> Result<Vec<ConfluentEnvelope>> {
    let (stream, _) = confluent_unframe_stream(
        create_readable_stream(chunks),
        ConfluentUnframeOptions::default(),
    );
    stream_to_array(stream, None).await
}

async fn glue_frame(payloads: Vec<Vec<u8>>, id: &str, compression: &str) -> Vec<Vec<u8>> {
    let options = GlueFrameOptions {
        schema_version_id: Some(id.into()),
        compression: Some(compression.into()),
        ..Default::default()
    };
    let (stream, _) = glue_frame_stream(create_readable_stream(payloads), options).unwrap();
    stream_to_array(stream, None).await.unwrap()
}

async fn glue_unframe(chunks: Vec<Vec<u8>>) -> Result<Vec<GlueEnvelope>> {
    let (stream, _) = glue_unframe_stream(
        create_readable_stream(chunks),
        GlueUnframeOptions::default(),
    );
    stream_to_array(stream, None).await
}

fn payloads() -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::vec(prop::collection::vec(any::<u8>(), 0..256), 1..8)
}

proptest! {
    #[test]
    fn fuzz_confluent_frame_prepends_header(payload in prop::collection::vec(any::<u8>(), 0..256), schema_id: u32) {
        let framed = block_on(confluent_frame(vec![payload.clone()], schema_id));
        let mut expected = vec![0x00];
        expected.extend_from_slice(&schema_id.to_be_bytes());
        expected.extend_from_slice(&payload);
        prop_assert_eq!(framed, vec![expected]);
    }

    #[test]
    fn fuzz_confluent_unframe_random_bytes(input in prop::collection::vec(any::<u8>(), 0..64)) {
        match block_on(confluent_unframe(vec![input.clone()])) {
            Ok(envelopes) => {
                prop_assert!(input.len() >= 5 && input[0] == 0x00);
                prop_assert_eq!(envelopes.len(), 1);
                prop_assert_eq!(&envelopes[0].payload, &input[5..].to_vec());
            }
            Err(e) => prop_assert!(e.to_string().contains("magic byte")),
        }
    }

    #[test]
    fn fuzz_confluent_roundtrip(payloads in payloads(), schema_id: u32) {
        let framed = block_on(confluent_frame(payloads.clone(), schema_id));
        let envelopes = block_on(confluent_unframe(framed)).unwrap();
        let expected: Vec<_> = payloads
            .into_iter()
            .map(|payload| ConfluentEnvelope { schema_id, payload })
            .collect();
        prop_assert_eq!(envelopes, expected);
    }

    #[test]
    fn fuzz_glue_frame_prepends_header(payload in prop::collection::vec(any::<u8>(), 0..256), id: [u8; 16]) {
        let framed = block_on(glue_frame(vec![payload.clone()], &uuid(id), "none"));
        let mut expected = vec![0x03, 0x00];
        expected.extend_from_slice(&id);
        expected.extend_from_slice(&payload);
        prop_assert_eq!(framed, vec![expected]);
    }

    #[test]
    fn fuzz_glue_unframe_random_bytes(input in prop::collection::vec(any::<u8>(), 0..64)) {
        // Must never panic; anything that unframes started with a valid header.
        if let Ok(envelopes) = block_on(glue_unframe(vec![input.clone()])) {
            prop_assert!(input.len() >= 18 && input[0] == 0x03);
            prop_assert_eq!(envelopes.len(), 1);
            prop_assert_eq!(envelopes[0].schema_version_id.clone(), uuid(input[2..18].try_into().unwrap()));
        }
    }

    #[test]
    fn fuzz_glue_roundtrip(payloads in payloads(), id: [u8; 16], zlib: bool) {
        let compression = if zlib { "zlib" } else { "none" };
        let framed = block_on(glue_frame(payloads.clone(), &uuid(id), compression));
        let envelopes = block_on(glue_unframe(framed)).unwrap();
        let expected: Vec<_> = payloads
            .into_iter()
            .map(|payload| GlueEnvelope {
                schema_version_id: uuid(id),
                compression,
                payload,
            })
            .collect();
        prop_assert_eq!(envelopes, expected);
    }
}
