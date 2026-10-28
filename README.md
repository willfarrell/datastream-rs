# datastream-rs

Rust port of [datastream](https://github.com/willfarrell/datastream): commonly used stream patterns, built on `futures::Stream` + tokio.

One crate per JS package under `packages/`:

| Crate | Ports |
|---|---|
| datastream-core | `@datastream/core` (pipeline, result, streamTo*, create*Stream, timeout) |
| datastream-string, datastream-object | `@datastream/string`, `@datastream/object` |
| datastream-csv, datastream-json | `@datastream/csv`, `@datastream/json` |
| datastream-base64, datastream-charset, datastream-compress, datastream-digest, datastream-encrypt | same-named JS packages |
| datastream-file, datastream-fetch, datastream-ipfs | same-named JS packages |
| datastream-validate | `@datastream/validate` (jsonschema instead of ajv) |
| datastream-arrow, datastream-avro, datastream-protobuf, datastream-schema-registry | same-named JS packages |
| datastream-duckdb | `@datastream/duckdb` (bundled DuckDB) |
| datastream-sqlite | `@datastream/sqlite` (bundled SQLite via rusqlite) |
| datastream-kafka | `@datastream/kafka` (trait + optional `rdkafka` feature) |
| datastream-aws | `@datastream/aws` (aws-sdk-*, one feature per service) |
| datastream-azure | `@datastream/azure` (azure_* SDK crates, one feature per service) |

`@datastream/indexeddb` is browser-only and is not ported. Azure Service Bus is not ported (no azure_core 1.x Service Bus crate on crates.io yet).

```rust
use datastream_core::{create_readable_stream, pipeline, Pipe};
use datastream_string::{string_split_stream, StringSplitOptions};

let stream = create_readable_stream(["a,b,c".to_string()])
    .pipe(|s| string_split_stream(s, StringSplitOptions { separator: ",".into(), ..Default::default() }))?;
pipeline(stream, &[]).await?;
```

```sh
cargo test --workspace
```
