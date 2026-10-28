// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Cosmos DB query, upsert and delete streams. Throttled (429) requests are
//! retried by the SDK itself.

use async_stream::try_stream;
use azure_data_cosmos::models::PartitionKeyValue;
use azure_data_cosmos::options::{
    ItemWriteOptions, OperationOptionsBuilder, QueryOptions, ThrottlingRetryOptionsBuilder,
};
use azure_data_cosmos::{ContainerClient, FeedScope, PartitionKey, Query};
use datastream_core::{
    CancellationToken, DataStream, Error, Result, StreamExt, TryStreamExt, Value,
};

use crate::client::send;

/// Stream every item a query returns, across pages. `signal` aborts the
/// in-flight page request.
pub async fn azure_cosmos_query_stream(
    client: &ContainerClient,
    query: impl Into<Query>,
    scope: FeedScope,
    options: Option<QueryOptions>,
    signal: Option<CancellationToken>,
) -> Result<DataStream<Value>> {
    let items = send(
        client.query_items::<Value>(query, scope, options),
        signal.as_ref(),
    )
    .await?;
    Ok(abortable(items, signal))
}

fn abortable<T, E>(
    items: impl futures::Stream<Item = std::result::Result<T, E>> + Send + 'static,
    signal: Option<CancellationToken>,
) -> DataStream<T>
where
    T: Send + 'static,
    E: Into<Error>,
{
    Box::pin(try_stream! {
        let mut items = Box::pin(items);
        while let Some(item) = send(async { items.next().await.transpose() }, signal.as_ref()).await? {
            yield item;
        }
    })
}

#[derive(Clone, Debug)]
pub struct AzureCosmosWriteOptions {
    /// JSON pointers to the partition key in each item, as in the container
    /// definition, e.g. `["/tenantId"]` (more than one for hierarchical keys).
    pub partition_key_paths: Vec<String>,
    /// Requests in flight at once (default 1).
    pub concurrency: Option<usize>,
    /// Retries of a throttled (429) request (default: the SDK's, 9 retries
    /// within 30s of waiting).
    pub retry_max_count: Option<u32>,
    pub signal: Option<CancellationToken>,
}

impl AzureCosmosWriteOptions {
    fn item_options(&self) -> Option<ItemWriteOptions> {
        let throttling = ThrottlingRetryOptionsBuilder::new()
            .with_max_retry_count(self.retry_max_count?)
            .build();
        let operation = OperationOptionsBuilder::new()
            .with_throttling_retry_options(throttling)
            .build();
        Some(ItemWriteOptions::default().with_operation_options(operation))
    }
}

/// Upsert each item; its id is the item's `id` field.
pub async fn azure_cosmos_upsert_item_stream(
    input: DataStream<Value>,
    client: &ContainerClient,
    options: AzureCosmosWriteOptions,
) -> Result<()> {
    let concurrency = options.concurrency.unwrap_or(1).max(1);
    let options = &options;
    input
        .try_for_each_concurrent(concurrency, |item| async move {
            let (key, id) = item_key(&item, &options.partition_key_paths)?;
            send(
                client.upsert_item(key, &id, &item, options.item_options()),
                options.signal.as_ref(),
            )
            .await?;
            Ok(())
        })
        .await
}

/// Delete each item, identified by its `id` and partition key fields.
pub async fn azure_cosmos_delete_item_stream(
    input: DataStream<Value>,
    client: &ContainerClient,
    options: AzureCosmosWriteOptions,
) -> Result<()> {
    let concurrency = options.concurrency.unwrap_or(1).max(1);
    let options = &options;
    input
        .try_for_each_concurrent(concurrency, |item| async move {
            let (key, id) = item_key(&item, &options.partition_key_paths)?;
            send(
                client.delete_item(key, &id, options.item_options()),
                options.signal.as_ref(),
            )
            .await?;
            Ok(())
        })
        .await
}

fn item_key(item: &Value, paths: &[String]) -> Result<(PartitionKey, String)> {
    let Some(id) = item.get("id").and_then(Value::as_str) else {
        return Err("azureCosmos item requires a string id".into());
    };
    let values = paths
        .iter()
        .map(|path| match item.pointer(path) {
            Some(Value::String(s)) => Ok(PartitionKeyValue::from(s.clone())),
            Some(Value::Number(n)) => n.as_f64().map(PartitionKeyValue::from).ok_or_else(|| {
                format!("azureCosmos partition key {path} is not a finite number").into()
            }),
            Some(Value::Bool(b)) => Ok(PartitionKeyValue::from(*b)),
            Some(Value::Null) => Ok(PartitionKeyValue::NULL),
            _ => Err(format!("azureCosmos item is missing partition key {path}").into()),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((PartitionKey::from(values), id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn paths(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| p.to_string()).collect()
    }

    fn write_options(retry_max_count: Option<u32>) -> AzureCosmosWriteOptions {
        AzureCosmosWriteOptions {
            partition_key_paths: paths(&["/pk"]),
            concurrency: None,
            retry_max_count,
            signal: None,
        }
    }

    #[test]
    fn item_options_sets_throttle_retries() {
        assert!(write_options(None).item_options().is_none());
        let options = write_options(Some(3)).item_options().unwrap();
        let throttling = options.operation.throttling_retry_options.unwrap();
        assert_eq!(throttling.max_retry_count, Some(3));
        assert_eq!(throttling.max_retry_wait_time, None);
    }

    #[tokio::test]
    async fn query_pages_stream_items() {
        let items = futures::stream::iter([Ok::<_, Error>(json!(1)), Ok(json!(2))]);
        let out: Vec<_> = abortable(items, None).try_collect().await.unwrap();
        assert_eq!(out, [json!(1), json!(2)]);
    }

    #[tokio::test]
    async fn query_aborts_on_signal() {
        let signal = CancellationToken::new();
        signal.cancel();
        let items = futures::stream::pending::<Result<Value>>();
        let e = abortable(items, Some(signal))
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(e.to_string(), "Aborted");
    }

    #[test]
    fn item_key_reads_id_and_partition_key() {
        let item = json!({"id": "1", "pk": "a", "nested": {"n": 2}});
        let (key, id) = item_key(&item, &paths(&["/pk"])).unwrap();
        assert_eq!(id, "1");
        assert_eq!(key, PartitionKey::from("a"));
        let (key, _) = item_key(&item, &paths(&["/pk", "/nested/n"])).unwrap();
        assert_eq!(key, PartitionKey::from(("a", 2.0)));
    }

    #[test]
    fn item_key_accepts_bool_and_null() {
        let item = json!({"id": "1", "b": true, "z": null});
        let (key, _) = item_key(&item, &paths(&["/b", "/z"])).unwrap();
        assert_eq!(
            key,
            PartitionKey::from(vec![PartitionKeyValue::from(true), PartitionKeyValue::NULL])
        );
    }

    #[test]
    fn item_key_rejects_missing_fields() {
        let e = item_key(&json!({"pk": "a"}), &paths(&["/pk"])).unwrap_err();
        assert_eq!(e.to_string(), "azureCosmos item requires a string id");
        let e = item_key(&json!({"id": "1"}), &paths(&["/pk"])).unwrap_err();
        assert_eq!(
            e.to_string(),
            "azureCosmos item is missing partition key /pk"
        );
        let e = item_key(&json!({"id": "1", "pk": []}), &paths(&["/pk"])).unwrap_err();
        assert_eq!(
            e.to_string(),
            "azureCosmos item is missing partition key /pk"
        );
    }
}
