# datastream-avro

Apache Avro encode/decode streams. One whole record per chunk (no container
file, no framing); records are `serde_json::Value`s.

- `avro_encode_stream`
- `avro_decode_stream`: one schema for every chunk; each message is capped by
  `max_message_size` (default 64MiB).
- `avro_decode_envelope_stream`: per-message writer schemas, looked up by the
  envelope's schema id with `resolve_schema` (compiled types are cached per
  id). Enable the `schema-registry` feature to decode
  `datastream-schema-registry` unframe envelopes directly.

A schema is Avro JSON, its JSON text, or a compiled `AvroType`, given up front
or produced lazily on the first chunk (`AvroSchemaInput::lazy`). Logical types
are read and written as their underlying type, and `bytes`/`fixed` values are
arrays of byte numbers.

Decoding rejects an array or map block that claims more items than there are
bytes left, so a few bytes cannot make it allocate millions of items. Arrays of
zero-width items (null, a record with no fields, fixed of size 0) longer than
the bytes that follow them are rejected too.
