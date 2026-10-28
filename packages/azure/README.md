# datastream-azure

Azure service streams for Blob Storage, Queue Storage, Event Hubs, and Cosmos DB, plus Entra ID tokens for the Event Hubs Kafka endpoint. Each service is a cargo feature (all on by default). Callers pass the `azure_*` SDK client.

- `client`: `AzurePollingOptions`, `AzureFailedMessagesError` (the unsent entries of a partly filled batch when a send stream fails on input error or abort)
- `blob`: `azure_blob_download_stream`, `azure_blob_list_stream` (flat listing, paged), `azure_blob_upload_stream` (staged blocks + commit)
- `queue`: `azure_queue_receive_messages_stream`, `azure_queue_send_message_stream`, `azure_queue_delete_message_stream`
- `event_hubs`: `azure_event_hubs_receive_events_stream` (one partition; ends when the receiver's `receive_timeout` elapses with no event, or runs until aborted without one), `azure_event_hubs_send_events_stream`
- `cosmos`: `azure_cosmos_query_stream`, `azure_cosmos_upsert_item_stream`, `azure_cosmos_delete_item_stream` (one request per item; the SDK retries throttled requests, capped by `retry_max_count`)
- `event_hubs_kafka`: `azure_event_hubs_kafka_mechanism` (SASL/OAUTHBEARER; the namespace must follow Azure naming rules, since it becomes the token scope's host)

| AWS (`datastream-aws`) | Azure |
|---|---|
| S3 | Blob Storage |
| SQS | Queue Storage |
| Kinesis | Event Hubs |
| DynamoDB | Cosmos DB |
| MSK IAM | Event Hubs Kafka (Entra ID) |

Not ported: Service Bus (the only published Rust crate, `azure_messaging_servicebus` 0.21, is the deprecated pre-1.0 SDK on `azure_core` 0.21; the new SDK's crate is not on crates.io yet), Functions (no invoke API in the SDK), Monitor Logs and Schema Registry (no Rust SDK yet).

`azure_messaging_eventhubs` is pinned to 0.15: 0.16 requires `azure_core 1.2.0-beta`, which conflicts with the Storage and Cosmos crates.
