// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! IPFS get and add streams, ported from `@datastream/ipfs`. The IPFS client
//! is supplied by the caller through the [`IpfsNode`] trait.

use async_stream::try_stream;
use datastream_core::{BoxFuture, DataStream, Result, StreamExt, StreamResult};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle};

/// The IPFS client operations the streams need.
pub trait IpfsNode: Send + Sync {
    /// Content stream for `cid`.
    fn get(&self, cid: &str) -> BoxFuture<Result<DataStream<Vec<u8>>>>;
    /// Store everything `source` yields and return its cid.
    fn add(&self, source: DataStream<Vec<u8>>) -> BoxFuture<Result<String>>;
}

/// Readable of the content stored under `cid`.
pub async fn ipfs_get_stream(node: &dyn IpfsNode, cid: &str) -> Result<DataStream<Vec<u8>>> {
    node.get(cid).await
}

#[derive(Default, Clone, Debug)]
pub struct IpfsAddOptions {
    pub result_key: Option<String>,
}

struct AbortOnDrop(AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn join(add: &mut JoinHandle<Result<String>>) -> Result<String> {
    add.await?
}

/// Pass-through that feeds every chunk to `node.add` as it streams. The
/// result (default key `cid`) holds the cid once the input ends. If the stream
/// errors upstream or is dropped early, the in-flight `node.add` is cancelled.
pub fn ipfs_add_stream(
    mut input: DataStream<Vec<u8>>,
    node: &dyn IpfsNode,
    options: IpfsAddOptions,
) -> (DataStream<Vec<u8>>, StreamResult) {
    let result = StreamResult::new(
        options.result_key.unwrap_or_else(|| "cid".into()),
        Value::Null,
    );
    let output = result.clone();
    // A one-slot queue: upstream can't run ahead of node.add (backpressure).
    let (tx, mut rx) = mpsc::channel::<Result<Vec<u8>>>(1);
    let add = node.add(Box::pin(async_stream::stream! {
        while let Some(item) = rx.recv().await {
            yield item;
        }
    }));
    let stream = Box::pin(try_stream! {
        let mut add = tokio::spawn(add);
        let _abort = AbortOnDrop(add.abort_handle());
        let mut cid = None;
        while let Some(chunk) = input.next().await {
            let chunk = chunk?;
            // A failed send means node.add finished without draining the
            // source: settle it and pass the rest straight through.
            if cid.is_none() && tx.send(Ok(chunk.clone())).await.is_err() {
                cid = Some(join(&mut add).await?);
            }
            yield chunk;
        }
        drop(tx);
        let cid = match cid {
            Some(cid) => cid,
            None => join(&mut add).await?,
        };
        output.set(Value::String(cid));
    });
    (stream, result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, pipeline, stream_to_array, Error};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    type AddFn = Box<dyn Fn(DataStream<Vec<u8>>) -> BoxFuture<Result<String>> + Send + Sync>;

    struct Node {
        cids: Mutex<Vec<String>>,
        add: AddFn,
    }

    impl IpfsNode for Node {
        fn get(&self, cid: &str) -> BoxFuture<Result<DataStream<Vec<u8>>>> {
            self.cids.lock().unwrap().push(cid.to_string());
            Box::pin(async {
                Ok(create_readable_stream(vec![
                    b"chunk1".to_vec(),
                    b"chunk2".to_vec(),
                ]))
            })
        }
        fn add(&self, source: DataStream<Vec<u8>>) -> BoxFuture<Result<String>> {
            (self.add)(source)
        }
    }

    fn node(
        add: impl Fn(DataStream<Vec<u8>>) -> BoxFuture<Result<String>> + Send + Sync + 'static,
    ) -> Node {
        Node {
            cids: Mutex::default(),
            add: Box::new(add),
        }
    }

    /// node.add that collects every chunk, then returns `cid`.
    fn collecting(received: Arc<Mutex<Vec<Vec<u8>>>>, cid: &'static str) -> Node {
        node(move |mut source| {
            let received = received.clone();
            Box::pin(async move {
                while let Some(chunk) = source.next().await {
                    received.lock().unwrap().push(chunk?);
                }
                Ok(cid.to_string())
            })
        })
    }

    /// Sets the flag when dropped, i.e. when node.add's future settles or is cancelled.
    struct SetOnDrop(Arc<AtomicBool>);
    impl Drop for SetOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    fn forever(settled: Arc<AtomicBool>) -> Node {
        node(move |mut source| {
            let guard = SetOnDrop(settled.clone());
            Box::pin(async move {
                let _guard = guard;
                while source.next().await.is_some() {}
                std::future::pending::<()>().await;
                Ok(String::new())
            })
        })
    }

    async fn wait_for(flag: &AtomicBool) {
        for _ in 0..100 {
            if flag.load(Ordering::SeqCst) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("node.add never settled");
    }

    fn input(chunks: &[&str]) -> DataStream<Vec<u8>> {
        create_readable_stream(
            chunks
                .iter()
                .map(|c| c.as_bytes().to_vec())
                .collect::<Vec<_>>(),
        )
    }

    #[tokio::test]
    async fn get_stream_reads_cid_content() {
        let node = node(|_| unreachable!());
        let stream = ipfs_get_stream(&node, "bafyTestCidV1").await.unwrap();
        assert_eq!(
            stream_to_array(stream, None).await.unwrap(),
            vec![b"chunk1".to_vec(), b"chunk2".to_vec()]
        );
        assert_eq!(*node.cids.lock().unwrap(), vec!["bafyTestCidV1"]);
    }

    #[tokio::test]
    async fn add_stream_passes_through_and_sets_cid() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let node = collecting(received.clone(), "QmResult123");
        let (stream, result) = ipfs_add_stream(
            input(&["chunk1", "chunk2", "chunk3"]),
            &node,
            IpfsAddOptions::default(),
        );
        let output = stream_to_array(stream, None).await.unwrap();
        assert_eq!(
            output,
            vec![b"chunk1".to_vec(), b"chunk2".to_vec(), b"chunk3".to_vec()]
        );
        assert_eq!(*received.lock().unwrap(), output);
        assert_eq!(result.key(), "cid");
        assert_eq!(result.get(), "QmResult123");
    }

    #[tokio::test]
    async fn add_stream_custom_result_key_via_pipeline() {
        let node = collecting(Arc::default(), "QmResult123");
        let options = IpfsAddOptions {
            result_key: Some("ipfsCid".into()),
        };
        let (stream, result) = ipfs_add_stream(input(&["data"]), &node, options);
        let out = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(out["ipfsCid"], "QmResult123");
    }

    #[tokio::test]
    async fn add_stream_propagates_add_errors() {
        let node = node(|mut source| {
            Box::pin(async move {
                source.next().await;
                Err::<String, Error>("add failed".into())
            })
        });
        let (stream, result) =
            ipfs_add_stream(input(&["a", "b", "c"]), &node, IpfsAddOptions::default());
        let err = pipeline(stream, &[&result]).await.unwrap_err();
        assert_eq!(err.to_string(), "add failed");
        assert_eq!(result.get(), Value::Null);
    }

    #[tokio::test]
    async fn add_stream_passes_through_after_add_resolves_early() {
        let node = node(|mut source| {
            Box::pin(async move {
                source.next().await;
                Ok("QmEarlyResolve".to_string())
            })
        });
        let (stream, result) = ipfs_add_stream(
            input(&["first", "second", "third"]),
            &node,
            IpfsAddOptions::default(),
        );
        assert_eq!(stream_to_array(stream, None).await.unwrap().len(), 3);
        assert_eq!(result.get(), "QmEarlyResolve");
    }

    #[tokio::test]
    async fn add_stream_applies_backpressure() {
        let accepted = Arc::new(AtomicUsize::new(0));
        let consumed = Arc::new(AtomicUsize::new(0));
        let max_in_flight = Arc::new(AtomicUsize::new(0));
        let (a, c, m) = (accepted.clone(), consumed.clone(), max_in_flight.clone());
        let upstream = Box::pin(
            datastream_core::create_readable_stream(0..50).map(move |i| {
                let accepted = a.fetch_add(1, Ordering::SeqCst) + 1;
                m.fetch_max(accepted - c.load(Ordering::SeqCst), Ordering::SeqCst);
                i.map(|i: i32| i.to_string().into_bytes())
            }),
        );
        let consumed_in_add = consumed.clone();
        let node = node(move |mut source| {
            let consumed = consumed_in_add.clone();
            Box::pin(async move {
                while source.next().await.is_some() {
                    consumed.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Ok("QmBounded".to_string())
            })
        });
        let (stream, result) = ipfs_add_stream(upstream, &node, IpfsAddOptions::default());
        let out = pipeline(stream, &[&result]).await.unwrap();
        assert_eq!(out["cid"], "QmBounded");
        assert_eq!(consumed.load(Ordering::SeqCst), 50);
        assert!(
            max_in_flight.load(Ordering::SeqCst) <= 3,
            "queue depth {max_in_flight:?}"
        );
    }

    #[tokio::test]
    async fn add_stream_cancels_add_on_upstream_error() {
        let settled = Arc::new(AtomicBool::new(false));
        let node = forever(settled.clone());
        let upstream: DataStream<Vec<u8>> =
            create_readable_stream(vec![Ok(b"a".to_vec()), Err(Error::from("upstream boom"))])
                .map(|r| r.and_then(|r| r))
                .boxed();
        let (stream, result) = ipfs_add_stream(upstream, &node, IpfsAddOptions::default());
        let err = pipeline(stream, &[&result]).await.unwrap_err();
        assert_eq!(err.to_string(), "upstream boom");
        wait_for(&settled).await;
    }

    #[tokio::test]
    async fn add_stream_cancels_add_when_dropped() {
        let settled = Arc::new(AtomicBool::new(false));
        let node = forever(settled.clone());
        let (mut stream, _) = ipfs_add_stream(input(&["a", "b"]), &node, IpfsAddOptions::default());
        stream.next().await.unwrap().unwrap();
        drop(stream);
        wait_for(&settled).await;
    }
}
