// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use datastream_core::{
    create_readable_stream, result, stream_to_array, BoxFuture, DataStream, Result, StreamExt,
};
use datastream_ipfs::{ipfs_add_stream, ipfs_get_stream, IpfsAddOptions, IpfsNode};
use proptest::prelude::*;
use serde_json::{json, Value};

/// `get` echoes `data-<cid>`; `add` drains the source and returns its byte
/// count as the cid, so the test can check every byte reached the node.
struct Node;

impl IpfsNode for Node {
    fn get(&self, cid: &str) -> BoxFuture<Result<DataStream<Vec<u8>>>> {
        let data = format!("data-{cid}").into_bytes();
        Box::pin(async move { Ok(create_readable_stream([data])) })
    }
    fn add(&self, mut source: DataStream<Vec<u8>>) -> BoxFuture<Result<String>> {
        Box::pin(async move {
            let mut bytes = 0;
            while let Some(chunk) = source.next().await {
                bytes += chunk?.len();
            }
            Ok(bytes.to_string())
        })
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn fuzz_ipfs_get_stream_cid(cid in ".{1,100}") {
        let chunks = runtime().block_on(async {
            let stream = ipfs_get_stream(&Node, &cid).await.unwrap();
            stream_to_array(stream, None).await.unwrap()
        });
        prop_assert_eq!(chunks.concat(), format!("data-{cid}").into_bytes());
    }

    #[test]
    fn fuzz_ipfs_add_stream_result_key(
        result_key in prop::option::of(".{1,50}"),
        chunks in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..64), 0..20),
    ) {
        let bytes: usize = chunks.iter().map(Vec::len).sum();
        let (output, result) = runtime().block_on(async {
            let (stream, add_result) = ipfs_add_stream(
                create_readable_stream(chunks.clone()),
                &Node,
                IpfsAddOptions { result_key: result_key.clone() },
            );
            let output = stream_to_array(stream, None).await.unwrap();
            (output, result(&[&add_result]))
        });
        prop_assert_eq!(output, chunks);
        let key = result_key.unwrap_or_else(|| "cid".into());
        prop_assert_eq!(result.get(&key), Some(&Value::String(bytes.to_string())));
        prop_assert_eq!(Value::Object(result), json!({ key: bytes.to_string() }));
    }
}
