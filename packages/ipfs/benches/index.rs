// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use criterion::{criterion_group, criterion_main, Criterion};
use datastream_core::{
    create_readable_stream, pipeline, stream_to_array, BoxFuture, DataStream, Result, StreamExt,
};
use datastream_ipfs::{ipfs_add_stream, ipfs_get_stream, IpfsAddOptions, IpfsNode};

/// In-memory node: `get` replays `chunks`, `add` drains its source.
struct Node {
    chunks: Vec<Vec<u8>>,
}

impl IpfsNode for Node {
    fn get(&self, _cid: &str) -> BoxFuture<Result<DataStream<Vec<u8>>>> {
        let chunks = self.chunks.clone();
        Box::pin(async move { Ok(create_readable_stream(chunks)) })
    }
    fn add(&self, mut source: DataStream<Vec<u8>>) -> BoxFuture<Result<String>> {
        Box::pin(async move {
            while let Some(chunk) = source.next().await {
                chunk?;
            }
            Ok("QmResult".into())
        })
    }
}

fn benches(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    for count in [100, 1_000] {
        let node = Node {
            chunks: vec![vec![b'x'; 1_024]; count],
        };
        c.bench_function(&format!("ipfsGetStream/{count} × 1KB chunks"), |b| {
            b.to_async(&rt).iter(|| async {
                let stream = ipfs_get_stream(&node, "QmPerf").await.unwrap();
                stream_to_array(stream, None).await.unwrap()
            })
        });
        c.bench_function(&format!("ipfsAddStream/{count} × 1KB chunks"), |b| {
            b.to_async(&rt).iter(|| async {
                let input = create_readable_stream(node.chunks.clone());
                let (stream, result) = ipfs_add_stream(input, &node, IpfsAddOptions::default());
                pipeline(stream, &[&result]).await.unwrap()
            })
        });
    }
}

criterion_group!(index, benches);
criterion_main!(index);
