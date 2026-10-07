// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Cosmos DB query, upsert and delete streams. Throttled (429) requests are
//! retried by the SDK itself.

use azure_data_cosmos::models::PartitionKeyValue;
use azure_data_cosmos::options::QueryOptions;
use azure_data_cosmos::{ContainerClient, FeedScope, PartitionKey, Query};
use datastream_core::{CancellationToken, DataStream, Result, TryStreamExt, Value};

use crate::client::send;

/// Stream every item a query returns, across pages.
pub async fn azure_cosmos_query_stream(
    client: &ContainerClient,
    query: impl Into<Query>,
    scope: FeedScope,
    options: Option<QueryOptions>,
) -> Result<DataStream<Value>> {
    let items = client.query_items::<Value>(query, scope, options).await?;
    Ok(Box::pin(items.map_err(Into::into)))
}

#[derive(Clone, Debug)]
pub struct AzureCosmosWriteOptions {
    /// JSON pointers to the partition key in each item, as in the container
    /// definition, e.g. `["/tenantId"]` (more than one for hierarchical keys).
    pub partition_key_paths: Vec<String>,
    /// Requests in flight at once (default 1).
    pub concurrency: Option<usize>,
    pub signal: Option<CancellationToken>,
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
                client.upsert_item(key, &id, &item, None),
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
            send(client.delete_item(key, &id, None), options.signal.as_ref()).await?;
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
