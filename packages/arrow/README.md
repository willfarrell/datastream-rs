# datastream-arrow

Apache Arrow record batch transform streams.

- `arrow_detect_schema_stream`
- `arrow_batch_from_array_stream`
- `arrow_batch_from_object_stream`
- `arrow_to_array_stream`
- `arrow_to_object_stream`

Batching supports Boolean, every integer width, Float32/64, Utf8/LargeUtf8, Date32/64 and Timestamp (any unit) columns. Dates and timestamps are epoch milliseconds. Each cell is checked against its column type, and a value that does not fit fails the stream instead of being coerced.
