# datastream-compress

Compression streams for gzip, brotli, zstd, and deflate.

- `brotli_compress_stream`, `brotli_decompress_stream`
- `deflate_compress_stream`, `deflate_decompress_stream`
- `gzip_compress_stream`, `gzip_decompress_stream`
- `zstd_compress_stream`, `zstd_decompress_stream`

Decompression output is capped at 256MiB by default (`max_output_size`), and input that ends mid-stream fails with "unexpected end of file" instead of yielding partial output.
