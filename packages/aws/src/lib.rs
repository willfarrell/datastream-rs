// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! AWS service streams, ported from `@datastream/aws`.
//!
//! Callers build the SDK request (client, table, queue, ...) with the official
//! `aws-sdk-*` fluent builders and hand it to a stream function, which sends it
//! (paginating, polling or batching as needed). Each service sits behind a
//! cargo feature of the same name; all are on by default.

pub mod client;
pub use client::*;

#[cfg(feature = "cloudwatch-logs")]
pub mod cloudwatch_logs;
#[cfg(feature = "dynamodb")]
pub mod dynamodb;
#[cfg(feature = "dynamodb-streams")]
pub mod dynamodb_streams;
#[cfg(feature = "glue-schema-registry")]
pub mod glue_schema_registry;
#[cfg(feature = "kinesis")]
pub mod kinesis;
#[cfg(feature = "lambda")]
pub mod lambda;
#[cfg(feature = "msk-iam")]
pub mod msk_iam;
#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "sns")]
pub mod sns;
#[cfg(feature = "sqs")]
pub mod sqs;
