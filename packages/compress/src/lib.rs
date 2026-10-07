// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! Compression streams for gzip, deflate (zlib), brotli, and zstd, ported
//! from `@datastream/compress`.
//!
//! Every stream takes and yields byte chunks. Output is size-guarded:
//! compression only when `max_output_size` is set, decompression always
//! (default 256MiB, zip-bomb protection). Pass `Some(usize::MAX)` to opt out.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

use async_stream::try_stream;
use datastream_core::{DataStream, Error, StreamExt};
use flate2::Compression;

/// Default decompression output ceiling (256MiB).
pub const DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE: usize = 256 * 1024 * 1024;

#[derive(Default, Clone, Debug)]
pub struct CompressOptions {
    /// gzip/deflate: -1 to 9, brotli: 0 to 11 (default 11), zstd: level (default 3).
    pub quality: Option<i32>,
    /// Unbounded when `None`.
    pub max_output_size: Option<usize>,
}

#[derive(Default, Clone, Debug)]
pub struct DecompressOptions {
    /// Defaults to [`DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE`].
    pub max_output_size: Option<usize>,
}

struct Output {
    buf: Vec<u8>,
    total: usize,
    exceeded: bool,
}

/// Writer the codecs write into. Counts every byte so a single small input
/// chunk can't expand past the limit before we notice.
#[derive(Clone)]
struct Sink {
    out: Arc<Mutex<Output>>,
    max: usize,
    label: &'static str,
}

impl Sink {
    fn new(max: usize, label: &'static str) -> Self {
        let out = Output {
            buf: Vec::new(),
            total: 0,
            exceeded: false,
        };
        Self {
            out: Arc::new(Mutex::new(out)),
            max,
            label,
        }
    }
    fn message(&self) -> String {
        format!(
            "{} output exceeds maxOutputSize ({} bytes)",
            self.label, self.max
        )
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, Output> {
        self.out.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Take what has been written so far, or the limit error.
    fn take(&self) -> Result<Vec<u8>, Error> {
        let mut out = self.lock();
        if out.exceeded {
            return Err(self.message().into());
        }
        Ok(std::mem::take(&mut out.buf))
    }
}

impl Write for Sink {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        let mut out = self.lock();
        out.total += data.len();
        if out.total > self.max {
            out.exceeded = true;
            return Err(io::Error::other(self.message()));
        }
        out.buf.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Feed every chunk through a write-side codec and yield what it produced.
fn code<C, M, F>(
    mut input: DataStream<Vec<u8>>,
    max: usize,
    label: &'static str,
    make: M,
    finish: F,
) -> DataStream<Vec<u8>>
where
    C: Write + Send + 'static,
    M: FnOnce(Sink) -> io::Result<C> + Send + 'static,
    F: FnOnce(C) -> io::Result<()> + Send + 'static,
{
    let sink = Sink::new(max, label);
    Box::pin(try_stream! {
        let mut coder = make(sink.clone()).map_err(Error::from)?;
        while let Some(chunk) = input.next().await {
            let written = coder.write_all(&chunk?);
            // The limit error wins over whatever the codec made of it.
            let out = sink.take()?;
            written.map_err(Error::from)?;
            if !out.is_empty() {
                yield out;
            }
        }
        let finished = finish(coder);
        let out = sink.take()?;
        finished.map_err(Error::from)?;
        if !out.is_empty() {
            yield out;
        }
    })
}

fn compress_limit(options: &CompressOptions) -> usize {
    options.max_output_size.unwrap_or(usize::MAX)
}

fn decompress_limit(options: &DecompressOptions) -> usize {
    options
        .max_output_size
        .unwrap_or(DEFAULT_DECOMPRESS_MAX_OUTPUT_SIZE)
}

fn zlib_level(quality: Option<i32>) -> Compression {
    match quality {
        None | Some(-1) => Compression::default(),
        Some(q) => Compression::new(q.clamp(0, 9) as u32),
    }
}

// *** gzip *** //

pub fn gzip_compress_stream(
    input: DataStream<Vec<u8>>,
    options: CompressOptions,
) -> DataStream<Vec<u8>> {
    let level = zlib_level(options.quality);
    code(
        input,
        compress_limit(&options),
        "Compression",
        move |sink| Ok(flate2::write::GzEncoder::new(sink, level)),
        |c: flate2::write::GzEncoder<Sink>| c.finish().map(drop),
    )
}

pub fn gzip_decompress_stream(
    input: DataStream<Vec<u8>>,
    options: DecompressOptions,
) -> DataStream<Vec<u8>> {
    code(
        input,
        decompress_limit(&options),
        "Decompression",
        |sink| Ok(flate2::write::GzDecoder::new(sink)),
        |c: flate2::write::GzDecoder<Sink>| c.finish().map(drop),
    )
}

// *** deflate (zlib format, like node's createDeflate) *** //

pub fn deflate_compress_stream(
    input: DataStream<Vec<u8>>,
    options: CompressOptions,
) -> DataStream<Vec<u8>> {
    let level = zlib_level(options.quality);
    code(
        input,
        compress_limit(&options),
        "Compression",
        move |sink| Ok(flate2::write::ZlibEncoder::new(sink, level)),
        |c: flate2::write::ZlibEncoder<Sink>| c.finish().map(drop),
    )
}

pub fn deflate_decompress_stream(
    input: DataStream<Vec<u8>>,
    options: DecompressOptions,
) -> DataStream<Vec<u8>> {
    code(
        input,
        decompress_limit(&options),
        "Decompression",
        |sink| Ok(flate2::write::ZlibDecoder::new(sink)),
        |c: flate2::write::ZlibDecoder<Sink>| c.finish().map(drop),
    )
}

// *** brotli *** //

const BROTLI_BUFFER_SIZE: usize = 4096;
const BROTLI_DEFAULT_QUALITY: i32 = 11;
const BROTLI_DEFAULT_WINDOW: u32 = 22;

pub fn brotli_compress_stream(
    input: DataStream<Vec<u8>>,
    options: CompressOptions,
) -> DataStream<Vec<u8>> {
    let quality = options
        .quality
        .unwrap_or(BROTLI_DEFAULT_QUALITY)
        .clamp(0, 11) as u32;
    code(
        input,
        compress_limit(&options),
        "Compression",
        move |sink| {
            Ok(brotli::CompressorWriter::new(
                sink,
                BROTLI_BUFFER_SIZE,
                quality,
                BROTLI_DEFAULT_WINDOW,
            ))
        },
        // into_inner writes the final block.
        |c: brotli::CompressorWriter<Sink>| {
            c.into_inner();
            Ok(())
        },
    )
}

pub fn brotli_decompress_stream(
    input: DataStream<Vec<u8>>,
    options: DecompressOptions,
) -> DataStream<Vec<u8>> {
    code(
        input,
        decompress_limit(&options),
        "Decompression",
        |sink| Ok(brotli::DecompressorWriter::new(sink, BROTLI_BUFFER_SIZE)),
        |mut c: brotli::DecompressorWriter<Sink>| c.close(),
    )
}

// *** zstd *** //

const ZSTD_CLEVEL_DEFAULT: i32 = 3;

pub fn zstd_compress_stream(
    input: DataStream<Vec<u8>>,
    options: CompressOptions,
) -> DataStream<Vec<u8>> {
    let level = options.quality.unwrap_or(ZSTD_CLEVEL_DEFAULT);
    code(
        input,
        compress_limit(&options),
        "Compression",
        move |sink| zstd::stream::write::Encoder::new(sink, level),
        |c: zstd::stream::write::Encoder<'static, Sink>| c.finish().map(drop),
    )
}

pub fn zstd_decompress_stream(
    input: DataStream<Vec<u8>>,
    options: DecompressOptions,
) -> DataStream<Vec<u8>> {
    code(
        input,
        decompress_limit(&options),
        "Decompression",
        zstd::stream::write::Decoder::new,
        |mut c: zstd::stream::write::Decoder<'static, Sink>| c.flush(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{
        create_readable_stream, create_readable_stream_from_bytes, stream_to_buffer,
    };
    use std::io::Read;

    type Compressor = fn(DataStream<Vec<u8>>, CompressOptions) -> DataStream<Vec<u8>>;
    type Decompressor = fn(DataStream<Vec<u8>>, DecompressOptions) -> DataStream<Vec<u8>>;

    const ALL: [(&str, Compressor, Decompressor); 4] = [
        ("gzip", gzip_compress_stream, gzip_decompress_stream),
        (
            "deflate",
            deflate_compress_stream,
            deflate_decompress_stream,
        ),
        ("brotli", brotli_compress_stream, brotli_decompress_stream),
        ("zstd", zstd_compress_stream, zstd_decompress_stream),
    ];

    // JSON.stringify(new Array(1024).fill(0))
    fn body() -> Vec<u8> {
        format!("[{}]", vec!["0"; 1024].join(",")).into_bytes()
    }

    fn chunks(bytes: Vec<u8>) -> DataStream<Vec<u8>> {
        create_readable_stream_from_bytes(bytes, Some(100)).unwrap()
    }

    async fn run(stream: DataStream<Vec<u8>>) -> Result<Vec<u8>, Error> {
        stream_to_buffer(stream, None).await
    }

    fn compress_sync(name: &str, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        match name {
            "gzip" => {
                let mut e = flate2::write::GzEncoder::new(Vec::new(), Compression::default());
                e.write_all(data).unwrap();
                out = e.finish().unwrap();
            }
            "deflate" => {
                let mut e = flate2::write::ZlibEncoder::new(Vec::new(), Compression::default());
                e.write_all(data).unwrap();
                out = e.finish().unwrap();
            }
            "brotli" => {
                let mut e = brotli::CompressorWriter::new(&mut out, 4096, 11, 22);
                e.write_all(data).unwrap();
            }
            _ => out = zstd::encode_all(data, 3).unwrap(),
        }
        out
    }

    fn decompress_sync(name: &str, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        match name {
            "gzip" => flate2::read::GzDecoder::new(data)
                .read_to_end(&mut out)
                .map(drop)
                .unwrap(),
            "deflate" => flate2::read::ZlibDecoder::new(data)
                .read_to_end(&mut out)
                .map(drop)
                .unwrap(),
            "brotli" => brotli::Decompressor::new(data, 4096)
                .read_to_end(&mut out)
                .map(drop)
                .unwrap(),
            _ => out = zstd::decode_all(data).unwrap(),
        }
        out
    }

    #[tokio::test]
    async fn compress_then_sync_decompress() {
        for (name, compress, _) in ALL {
            let output = run(compress(chunks(body()), CompressOptions::default()))
                .await
                .unwrap();
            assert_eq!(decompress_sync(name, &output), body(), "{name}");
            assert!(output.len() < body().len(), "{name}");
        }
    }

    #[tokio::test]
    async fn sync_compress_then_decompress() {
        for (name, _, decompress) in ALL {
            let input = compress_sync(name, &body());
            let output = run(decompress(chunks(input), DecompressOptions::default()))
                .await
                .unwrap();
            assert_eq!(output, body(), "{name}");
        }
    }

    #[tokio::test]
    async fn round_trips_qualities() {
        for (name, compress, decompress) in ALL {
            for quality in [0, 1, 9] {
                let options = CompressOptions {
                    quality: Some(quality),
                    ..Default::default()
                };
                let stream = decompress(
                    compress(chunks(body()), options),
                    DecompressOptions::default(),
                );
                assert_eq!(run(stream).await.unwrap(), body(), "{name} {quality}");
            }
        }
    }

    #[tokio::test]
    async fn quality_changes_output() {
        let large: Vec<u8> = (0..20_000u32)
            .flat_map(|i| format!("{i},{},", i % 7).into_bytes())
            .collect();
        let cases: [(&str, Compressor, i32, i32); 3] = [
            ("gzip", gzip_compress_stream, 1, 9),
            ("brotli", brotli_compress_stream, 0, 11),
            ("zstd", zstd_compress_stream, 1, 19),
        ];
        for (name, compress, low, high) in cases {
            let at = |q| CompressOptions {
                quality: Some(q),
                ..Default::default()
            };
            let a = run(compress(chunks(large.clone()), at(low))).await.unwrap();
            let b = run(compress(chunks(large.clone()), at(high)))
                .await
                .unwrap();
            assert_ne!(a, b, "{name}");
        }
    }

    #[tokio::test]
    async fn default_quality_matches_explicit_default() {
        let at = |q| CompressOptions {
            quality: q,
            ..Default::default()
        };
        let cases: [(Compressor, i32); 3] = [
            (gzip_compress_stream, -1),
            (brotli_compress_stream, 11),
            (zstd_compress_stream, 3),
        ];
        for (compress, default) in cases {
            let a = run(compress(chunks(body()), at(None))).await.unwrap();
            let b = run(compress(chunks(body()), at(Some(default))))
                .await
                .unwrap();
            assert_eq!(a, b);
        }
    }

    #[tokio::test]
    async fn deflate_matches_sync_level() {
        for level in [1, 9] {
            let options = CompressOptions {
                quality: Some(level),
                ..Default::default()
            };
            let output = run(deflate_compress_stream(chunks(body()), options))
                .await
                .unwrap();
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), Compression::new(level as u32));
            e.write_all(&body()).unwrap();
            assert_eq!(output, e.finish().unwrap());
        }
    }

    #[tokio::test]
    async fn decompress_enforces_max_output_size() {
        for (name, _, decompress) in ALL {
            let input = compress_sync(name, &body());
            let options = DecompressOptions {
                max_output_size: Some(100),
            };
            let e = run(decompress(chunks(input), options))
                .await
                .unwrap_err()
                .to_string();
            assert_eq!(
                e, "Decompression output exceeds maxOutputSize (100 bytes)",
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn decompress_exact_boundary() {
        let size = body().len();
        for (name, _, decompress) in ALL {
            let at = |max| DecompressOptions {
                max_output_size: Some(max),
            };
            let input = compress_sync(name, &body());
            assert_eq!(
                run(decompress(chunks(input.clone()), at(size)))
                    .await
                    .unwrap(),
                body(),
                "{name}"
            );
            assert!(
                run(decompress(chunks(input), at(size - 1))).await.is_err(),
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn decompress_within_and_without_limit() {
        for (name, _, decompress) in ALL {
            for max in [Some(1024 * 1024), Some(usize::MAX)] {
                let input = compress_sync(name, &body());
                let options = DecompressOptions {
                    max_output_size: max,
                };
                assert_eq!(
                    run(decompress(chunks(input), options)).await.unwrap(),
                    body(),
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn only_decompression_is_bounded_by_default() {
        assert_eq!(
            decompress_limit(&DecompressOptions::default()),
            256 * 1024 * 1024
        );
        assert_eq!(compress_limit(&CompressOptions::default()), usize::MAX);
    }

    #[tokio::test]
    async fn compress_enforces_max_output_size() {
        for (name, compress, _) in ALL {
            let options = CompressOptions {
                max_output_size: Some(5),
                ..Default::default()
            };
            let e = run(compress(chunks(body()), options))
                .await
                .unwrap_err()
                .to_string();
            assert_eq!(
                e, "Compression output exceeds maxOutputSize (5 bytes)",
                "{name}"
            );
        }
    }

    #[tokio::test]
    async fn compress_within_limit() {
        for (name, compress, _) in ALL {
            let options = CompressOptions {
                max_output_size: Some(1024 * 1024),
                ..Default::default()
            };
            let output = run(compress(chunks(body()), options)).await.unwrap();
            assert_eq!(decompress_sync(name, &output), body(), "{name}");
        }
    }

    #[tokio::test]
    async fn invocations_are_independent() {
        let input = compress_sync("gzip", &body());
        let small = DecompressOptions {
            max_output_size: Some(10),
        };
        assert!(run(gzip_decompress_stream(chunks(input.clone()), small))
            .await
            .is_err());
        let output = run(gzip_decompress_stream(chunks(input), Default::default()))
            .await
            .unwrap();
        assert_eq!(output, body());
    }

    #[tokio::test]
    async fn empty_input_round_trips() {
        for (name, compress, decompress) in ALL {
            let stream = decompress(
                compress(
                    create_readable_stream(Vec::<Vec<u8>>::new()),
                    Default::default(),
                ),
                Default::default(),
            );
            assert!(run(stream).await.unwrap().is_empty(), "{name}");
        }
    }

    #[tokio::test]
    async fn corrupt_input_errors() {
        for (name, _, decompress) in ALL {
            let garbage = create_readable_stream([b"definitely not compressed data".to_vec()]);
            assert!(
                run(decompress(garbage, Default::default())).await.is_err(),
                "{name}"
            );
        }
    }
}
