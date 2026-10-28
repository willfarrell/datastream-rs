# datastream-aws

AWS service streams for CloudWatch Logs, DynamoDB, DynamoDB Streams, Kinesis, Lambda, S3, SNS, and SQS, plus Glue Schema Registry lookups and MSK IAM auth tokens. Each service is a cargo feature (all on by default). Callers pass the `aws-sdk-*` client or fluent request builder.

- `client`: `aws_region_supports_fips`, `aws_client_defaults_use_fips_endpoint`, `AwsUnsentError`
- `cloudwatch_logs`: `aws_cloudwatch_logs_get_log_events_stream`, `aws_cloudwatch_logs_filter_log_events_stream`, `aws_cloudwatch_logs_put_log_events_stream` (`AwsCloudWatchLogsRejectedEvents`)
- `dynamodb`: `aws_dynamodb_query_stream`, `aws_dynamodb_scan_stream`, `aws_dynamodb_execute_statement_stream`, `aws_dynamodb_get_item_stream`, `aws_dynamodb_put_item_stream`, `aws_dynamodb_delete_item_stream`
- `dynamodb_streams`: `aws_dynamodb_streams_get_records_stream`
- `glue_schema_registry`: `aws_glue_schema_registry_resolver`
- `kinesis`: `aws_kinesis_get_records_stream`, `aws_kinesis_put_records_stream`
- `lambda`: `aws_lambda_readable_stream` (alias `aws_lambda_response_stream`)
- `msk_iam`: `aws_msk_iam_mechanism`
- `s3`: `aws_s3_get_object_stream`, `aws_s3_list_objects_stream`, `aws_s3_put_object_stream`, `aws_s3_checksum_stream` (both default to a 17,179,870-byte part size, so a default checksum matches a default upload). The upload rejects a part size below 5 MiB, more than 10,000 parts, or a part without an ETag, and reports progress through `on_progress` (`AwsS3UploadProgress`)
- `sns`: `aws_sns_publish_message_stream`
- `sqs`: `aws_sqs_receive_message_stream`, `aws_sqs_send_message_stream`, `aws_sqs_delete_message_stream`

The batching writers (CloudWatch Logs put, DynamoDB put/delete, Kinesis put, SNS publish, SQS send/delete) stop on an input error, an abort, or an oversize entry with entries still buffered below a batch limit. Those entries were never sent, so the error is an `AwsUnsentError<E>` carrying them as `failed_messages` for the caller to recover or retry. SNS/SQS `SenderFault` failures are not retried; they are reported with any still-failing entries once the batch settles.

The retrying streams (DynamoDB batch get/put/delete, Kinesis put, SNS publish, SQS send/delete) retry a partially failed batch up to `retry_max_count` times (default 10); `u32::MAX` retries without limit.
