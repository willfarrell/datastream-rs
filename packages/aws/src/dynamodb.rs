// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! DynamoDB query, scan, PartiQL, batch get, put and delete streams.

use std::collections::HashMap;

use async_stream::try_stream;
use aws_sdk_dynamodb::operation::execute_statement::builders::ExecuteStatementFluentBuilder;
use aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder;
use aws_sdk_dynamodb::operation::scan::builders::ScanFluentBuilder;
use aws_sdk_dynamodb::types::{
    AttributeValue, DeleteRequest, KeysAndAttributes, PutRequest, WriteRequest,
};
use aws_sdk_dynamodb::Client;
use datastream_core::{CancellationToken, DataStream, Error, Result, StreamExt};

use crate::client::{backoff, batch_write, cause_error, send, Batcher, IntoVec, Retry};

/// A DynamoDB item or key.
pub type Item = HashMap<String, AttributeValue>;

/// Options for the batch get / put / delete streams.
#[derive(Default, Clone, Debug)]
pub struct AwsDynamoDBOptions {
    pub table_name: String,
    /// Retries for unprocessed keys / items (default 10).
    pub retry_max_count: Option<u32>,
    pub signal: Option<CancellationToken>,
}

/// Read every page of a Query.
pub fn aws_dynamodb_query_stream(
    mut request: QueryFluentBuilder,
    signal: Option<CancellationToken>,
) -> DataStream<Item> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), signal.as_ref()).await?;
            for item in output.items.into_vec() {
                yield item;
            }
            match output.last_evaluated_key {
                Some(key) => request = request.set_exclusive_start_key(Some(key)),
                None => break,
            }
        }
    })
}

/// Read every page of a Scan.
pub fn aws_dynamodb_scan_stream(
    mut request: ScanFluentBuilder,
    signal: Option<CancellationToken>,
) -> DataStream<Item> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), signal.as_ref()).await?;
            for item in output.items.into_vec() {
                yield item;
            }
            match output.last_evaluated_key {
                Some(key) => request = request.set_exclusive_start_key(Some(key)),
                None => break,
            }
        }
    })
}

/// Read every page of a PartiQL ExecuteStatement.
pub fn aws_dynamodb_execute_statement_stream(
    mut request: ExecuteStatementFluentBuilder,
    signal: Option<CancellationToken>,
) -> DataStream<Item> {
    Box::pin(try_stream! {
        loop {
            let output = send(request.clone().send(), signal.as_ref()).await?;
            for item in output.items.into_vec() {
                yield item;
            }
            match output.next_token {
                Some(token) => request = request.next_token(token),
                None => break,
            }
        }
    })
}

/// Fetch `keys` (at most 100) with BatchGetItem, retrying unprocessed keys.
pub fn aws_dynamodb_get_item_stream(
    client: Client,
    keys: Vec<Item>,
    options: AwsDynamoDBOptions,
) -> Result<DataStream<Item>> {
    if keys.len() > 100 {
        return Err(format!(
            "awsDynamoDBGetItemStream Keys.length ({}) exceeds BatchGetItem limit of 100",
            keys.len()
        )
        .into());
    }
    let retry_max_count = options.retry_max_count.unwrap_or(10);
    Ok(Box::pin(try_stream! {
        let table = options.table_name;
        let mut keys = keys;
        let mut retry_count = 0;
        loop {
            let request = KeysAndAttributes::builder()
                .set_keys(Some(keys))
                .build()
                .map_err(Error::from)?;
            let request = client.batch_get_item().request_items(&table, request);
            let mut output = send(request.send(), options.signal.as_ref()).await?;
            let items = output.responses.as_mut().and_then(|r| r.remove(&table));
            for item in items.unwrap_or_default() {
                yield item;
            }
            let unprocessed = output.unprocessed_keys.as_mut().and_then(|u| u.remove(&table));
            keys = unprocessed.map(|k| k.keys.into_vec()).unwrap_or_default();
            if keys.is_empty() {
                break;
            }
            if retry_count >= retry_max_count {
                let cause = format!("TableName: {table}, UnprocessedKeysCount: {}", keys.len());
                Err::<(), Error>(cause_error("awsDynamoDBBatchGetItem has UnprocessedKeys", cause))?;
            }
            backoff(retry_count, options.signal.as_ref()).await?;
            retry_count += 1;
        }
    }))
}

/// Write items with BatchWriteItem (25 per batch), retrying unprocessed items.
pub async fn aws_dynamodb_put_item_stream(
    client: Client,
    input: DataStream<Item>,
    options: AwsDynamoDBOptions,
) -> Result<()> {
    let requests = input.map(|item| -> Result<WriteRequest> {
        let put = PutRequest::builder().set_item(Some(item?)).build()?;
        Ok(WriteRequest::builder().put_request(put).build())
    });
    batch_write_item(client, Box::pin(requests), options).await
}

/// Delete keys with BatchWriteItem (25 per batch), retrying unprocessed items.
pub async fn aws_dynamodb_delete_item_stream(
    client: Client,
    input: DataStream<Item>,
    options: AwsDynamoDBOptions,
) -> Result<()> {
    let requests = input.map(|key| -> Result<WriteRequest> {
        let delete = DeleteRequest::builder().set_key(Some(key?)).build()?;
        Ok(WriteRequest::builder().delete_request(delete).build())
    });
    batch_write_item(client, Box::pin(requests), options).await
}

async fn batch_write_item(
    client: Client,
    input: DataStream<WriteRequest>,
    options: AwsDynamoDBOptions,
) -> Result<()> {
    let table = options.table_name;
    let signal = options.signal.clone();
    let batcher = Batcher::<WriteRequest> {
        max_entries: 25,
        max_entry_bytes: usize::MAX,
        max_batch_bytes: usize::MAX,
        size: |_| 0,
        oversize: |_, _| unreachable!("no byte limit"),
    };
    let retry = Retry {
        max_count: options.retry_max_count.unwrap_or(10),
        signal: options.signal,
        message: "awsDynamoDBBatchWriteItem has UnprocessedItems",
    };
    batch_write(input, batcher, retry, move |batch| {
        let request = client.batch_write_item().request_items(&table, batch);
        let table = table.clone();
        let signal = signal.clone();
        async move {
            let mut output = send(request.send(), signal.as_ref()).await?;
            let unprocessed = output
                .unprocessed_items
                .as_mut()
                .and_then(|u| u.remove(&table));
            let unprocessed = unprocessed.unwrap_or_default();
            let cause = format!(
                "TableName: {table}, UnprocessedItemsCount: {}",
                unprocessed.len()
            );
            Ok::<_, Error>((unprocessed, cause))
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_dynamodb::operation::batch_get_item::BatchGetItemOutput;
    use aws_sdk_dynamodb::operation::batch_write_item::BatchWriteItemOutput;
    use aws_sdk_dynamodb::operation::execute_statement::ExecuteStatementOutput;
    use aws_sdk_dynamodb::operation::query::QueryOutput;
    use aws_sdk_dynamodb::operation::scan::ScanOutput;
    use aws_smithy_mocks::{mock, mock_client, RuleMode};
    use datastream_core::{create_readable_stream, stream_to_array};
    use std::sync::{Arc, Mutex};

    fn item(id: &str) -> Item {
        HashMap::from([("id".to_string(), AttributeValue::S(id.to_string()))])
    }

    fn ids(items: &[Item]) -> Vec<&str> {
        items
            .iter()
            .map(|i| i["id"].as_s().unwrap().as_str())
            .collect()
    }

    fn options(retry_max_count: Option<u32>) -> AwsDynamoDBOptions {
        AwsDynamoDBOptions {
            table_name: "t".to_string(),
            retry_max_count,
            signal: None,
        }
    }

    #[tokio::test]
    async fn query_stream_paginates_without_mutating_request() {
        let starts = Arc::new(Mutex::new(Vec::new()));
        let seen = starts.clone();
        let rule = mock!(Client::query).then_compute_output(move |req| {
            let mut starts = seen.lock().unwrap();
            starts.push(req.exclusive_start_key().cloned());
            match starts.len() {
                1 => QueryOutput::builder()
                    .items(item("1"))
                    .set_last_evaluated_key(Some(item("1")))
                    .build(),
                2 => QueryOutput::builder().build(),
                _ => QueryOutput::builder().items(item("2")).build(),
            }
        });
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let request = client.query().table_name("t");
        let stream = aws_dynamodb_query_stream(request.clone(), None);
        assert_eq!(ids(&stream_to_array(stream, None).await.unwrap()), ["1"]);
        assert_eq!(*starts.lock().unwrap(), [None, Some(item("1"))]);
        // The caller's builder is untouched.
        assert!(request.get_exclusive_start_key().is_none());
    }

    #[tokio::test]
    async fn scan_stream_paginates() {
        let rule = mock!(Client::scan)
            .sequence()
            .output(|| {
                ScanOutput::builder()
                    .items(item("1"))
                    .set_last_evaluated_key(Some(item("1")))
                    .build()
            })
            .output(|| ScanOutput::builder().items(item("2")).build())
            .build();
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let stream = aws_dynamodb_scan_stream(client.scan().table_name("t"), None);
        assert_eq!(
            ids(&stream_to_array(stream, None).await.unwrap()),
            ["1", "2"]
        );
    }

    #[tokio::test]
    async fn execute_statement_stream_paginates_and_handles_empty() {
        let rule = mock!(Client::execute_statement)
            .sequence()
            .output(|| {
                ExecuteStatementOutput::builder()
                    .items(item("1"))
                    .next_token("n")
                    .build()
            })
            .output(|| ExecuteStatementOutput::builder().build())
            .build();
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let stream = aws_dynamodb_execute_statement_stream(
            client.execute_statement().statement("SELECT"),
            None,
        );
        assert_eq!(ids(&stream_to_array(stream, None).await.unwrap()), ["1"]);
        assert_eq!(rule.num_calls(), 2);
    }

    #[tokio::test]
    async fn streams_abort_via_signal() {
        let rule = mock!(Client::query).then_output(|| QueryOutput::builder().build());
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        signal.cancel();
        let stream = aws_dynamodb_query_stream(client.query(), Some(signal));
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    fn get_output(items: &[&str], unprocessed: &[&str]) -> BatchGetItemOutput {
        let items = items.iter().map(|id| item(id)).collect();
        let output = BatchGetItemOutput::builder().responses("t", items);
        if unprocessed.is_empty() {
            return output.build();
        }
        let keys = KeysAndAttributes::builder()
            .set_keys(Some(unprocessed.iter().map(|id| item(id)).collect()))
            .build()
            .unwrap();
        output.unprocessed_keys("t", keys).build()
    }

    #[tokio::test(start_paused = true)]
    async fn get_item_stream_retries_unprocessed_keys() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::batch_get_item).then_compute_output(move |req| {
            let mut calls = seen.lock().unwrap();
            let keys = req.request_items().unwrap()["t"].keys().len();
            calls.push(keys);
            match calls.len() {
                1 => get_output(&["1"], &["2"]),
                _ => get_output(&["2"], &[]),
            }
        });
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let stream =
            aws_dynamodb_get_item_stream(client, vec![item("1"), item("2")], options(None))
                .unwrap();
        assert_eq!(
            ids(&stream_to_array(stream, None).await.unwrap()),
            ["1", "2"]
        );
        assert_eq!(*calls.lock().unwrap(), [2, 1]);
    }

    #[tokio::test]
    async fn get_item_stream_handles_empty_response() {
        let rule =
            mock!(Client::batch_get_item).then_output(|| BatchGetItemOutput::builder().build());
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let stream = aws_dynamodb_get_item_stream(client, vec![item("1")], options(None)).unwrap();
        assert!(stream_to_array(stream, None).await.unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn get_item_stream_throws_after_retry_max_count() {
        let rule = mock!(Client::batch_get_item).then_output(|| get_output(&[], &["1"]));
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let stream =
            aws_dynamodb_get_item_stream(client, vec![item("1")], options(Some(2))).unwrap();
        let e = stream_to_array(stream, None).await.unwrap_err();
        assert_eq!(e.to_string(), "awsDynamoDBBatchGetItem has UnprocessedKeys");
        assert_eq!(
            e.downcast_ref::<crate::CauseError>().unwrap().cause,
            "TableName: t, UnprocessedKeysCount: 1"
        );
        assert_eq!(rule.num_calls(), 3);
    }

    #[tokio::test]
    async fn get_item_stream_limits_keys_to_100() {
        let rule =
            mock!(Client::batch_get_item).then_output(|| BatchGetItemOutput::builder().build());
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let keys: Vec<_> = (0..101).map(|i| item(&i.to_string())).collect();
        let e = aws_dynamodb_get_item_stream(client.clone(), keys.clone(), options(None))
            .err()
            .unwrap();
        assert_eq!(
            e.to_string(),
            "awsDynamoDBGetItemStream Keys.length (101) exceeds BatchGetItem limit of 100"
        );
        assert!(aws_dynamodb_get_item_stream(client, keys[..100].to_vec(), options(None)).is_ok());
    }

    #[tokio::test]
    async fn get_item_stream_aborts_retry_backoff() {
        let rule = mock!(Client::batch_get_item).then_output(|| get_output(&[], &["1"]));
        let client = mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]);
        let signal = CancellationToken::new();
        let opts = AwsDynamoDBOptions {
            signal: Some(signal.clone()),
            ..options(None)
        };
        let stream = aws_dynamodb_get_item_stream(client, vec![item("1")], opts).unwrap();
        let task = tokio::spawn(stream_to_array(stream, None));
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        signal.cancel();
        assert_eq!(task.await.unwrap().unwrap_err().to_string(), "Aborted");
    }

    fn write_client(unprocessed_calls: usize) -> (Client, Arc<Mutex<Vec<Vec<WriteRequest>>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let seen = calls.clone();
        let rule = mock!(Client::batch_write_item).then_compute_output(move |req| {
            let mut calls = seen.lock().unwrap();
            let batch = req.request_items().unwrap()["t"].clone();
            calls.push(batch.clone());
            let output = BatchWriteItemOutput::builder();
            if calls.len() <= unprocessed_calls {
                output.unprocessed_items("t", batch[..1].to_vec()).build()
            } else {
                output.build()
            }
        });
        (
            mock_client!(aws_sdk_dynamodb, RuleMode::MatchAny, [&rule]),
            calls,
        )
    }

    #[tokio::test]
    async fn put_item_stream_batches_by_25() {
        let (client, calls) = write_client(0);
        let input = (0..26).map(|i| item(&i.to_string())).collect::<Vec<_>>();
        aws_dynamodb_put_item_stream(client, create_readable_stream(input), options(None))
            .await
            .unwrap();
        let calls = calls.lock().unwrap();
        let sizes: Vec<_> = calls.iter().map(Vec::len).collect();
        assert_eq!(sizes, [25, 1]);
        let put = calls[1][0].put_request().unwrap();
        assert_eq!(put.item(), &item("25"));
    }

    #[tokio::test]
    async fn delete_item_stream_sends_delete_requests() {
        let (client, calls) = write_client(0);
        aws_dynamodb_delete_item_stream(
            client,
            create_readable_stream(vec![item("1")]),
            options(None),
        )
        .await
        .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0][0].delete_request().unwrap().key(), &item("1"));
    }

    #[tokio::test]
    async fn write_streams_handle_empty_input() {
        let (client, calls) = write_client(0);
        aws_dynamodb_put_item_stream(
            client.clone(),
            create_readable_stream(Vec::new()),
            options(None),
        )
        .await
        .unwrap();
        aws_dynamodb_delete_item_stream(client, create_readable_stream(Vec::new()), options(None))
            .await
            .unwrap();
        assert!(calls.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn put_item_stream_retries_unprocessed_items() {
        let (client, calls) = write_client(1);
        let input = vec![item("1"), item("2")];
        aws_dynamodb_put_item_stream(client, create_readable_stream(input), options(None))
            .await
            .unwrap();
        let sizes: Vec<_> = calls.lock().unwrap().iter().map(Vec::len).collect();
        assert_eq!(sizes, [2, 1]);
    }

    #[tokio::test(start_paused = true)]
    async fn put_item_stream_throws_after_retry_max_count() {
        let (client, calls) = write_client(usize::MAX);
        let e = aws_dynamodb_put_item_stream(
            client,
            create_readable_stream(vec![item("1")]),
            options(Some(2)),
        )
        .await
        .unwrap_err();
        assert_eq!(
            e.to_string(),
            "awsDynamoDBBatchWriteItem has UnprocessedItems"
        );
        assert_eq!(
            e.downcast_ref::<crate::CauseError>().unwrap().cause,
            "TableName: t, UnprocessedItemsCount: 1"
        );
        assert_eq!(calls.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn put_item_stream_aborts() {
        let (client, _) = write_client(0);
        let signal = CancellationToken::new();
        signal.cancel();
        let opts = AwsDynamoDBOptions {
            signal: Some(signal),
            ..options(None)
        };
        let e = aws_dynamodb_put_item_stream(client, create_readable_stream(vec![item("1")]), opts)
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }
}
