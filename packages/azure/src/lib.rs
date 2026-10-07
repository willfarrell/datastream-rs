// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Azure service streams, the Azure counterpart to `datastream-aws`.
//!
//! Callers build the SDK client (blob, queue, Event Hubs producer/receiver,
//! Cosmos container) with the official `azure_*` crates and hand it to a stream
//! function, which sends requests (paging, polling or batching as needed). Each
//! service sits behind a cargo feature of the same name; all are on by default.

pub mod client;
pub use client::*;

#[cfg(feature = "blob")]
pub mod blob;
#[cfg(feature = "cosmos")]
pub mod cosmos;
#[cfg(feature = "event-hubs")]
pub mod event_hubs;
#[cfg(feature = "event-hubs-kafka")]
pub mod event_hubs_kafka;
#[cfg(feature = "queue")]
pub mod queue;
