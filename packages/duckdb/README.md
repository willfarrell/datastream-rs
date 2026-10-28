# datastream-duckdb

DuckDB query readable and writable streams, ported from `@datastream/duckdb`.

- `duckdb_connect(path, options)`
- `duckdb_query_stream(options)`: rows as JSON objects; `params` bind to positional `?`; dropping the stream or cancelling `signal` interrupts a running query. DATE/TIMESTAMP are epoch milliseconds.
- `duckdb_appender_stream(input, options)`: object rows match columns by name, array rows by position.
- `duckdb_arrow_insert_stream(input, options)`: batch fields match table columns by name, case-insensitively; a missing column or a column-count mismatch fails. Date/Timestamp fields bound for a non-DATE/TIMESTAMP column are written as epoch milliseconds.

`options.schema` (a `Schema`, or `DuckdbSchemaInput::Lazy` resolved when the first chunk arrives) creates `table` if it does not exist. Rows are always laid out by the table's physical column order.

Both write streams run in their own transaction on `db` (BEGIN on the first chunk, COMMIT at the end, ROLLBACK on any error or when the future is dropped), so a failed pipeline commits nothing. `db` must not already have a transaction open.

A stream holds the `Db` lock while it runs, so give concurrent work its own connection.
