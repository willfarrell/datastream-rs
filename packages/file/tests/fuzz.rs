// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use datastream_core::{create_readable_stream, Error};
use datastream_file::{file_read_stream, file_write_stream, FileOptions, FileType};
use proptest::prelude::*;

const EXPECTED_ERRORS: [&str; 4] = [
    "Invalid extension",
    "Path traversal detected",
    "Symbolic links are not allowed",
    "Path not found",
];

fn types() -> Vec<FileType> {
    vec![
        FileType {
            accept: BTreeMap::from([("text/csv".into(), vec![".csv".into()])]),
        },
        FileType {
            accept: BTreeMap::from([("application/json".into(), vec![".json".into()])]),
        },
    ]
}

/// Expected rejections and OS errors (missing file, bad name, ...) are fine;
/// anything else is a bug.
fn check_error(input: &str, e: &Error) {
    if EXPECTED_ERRORS.contains(&e.to_string().as_str())
        || e.downcast_ref::<std::io::Error>().is_some()
    {
        return;
    }
    panic!("{input:?}: {e}");
}

/// Per-process scratch directory, so writes never land outside it.
fn base_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("datastream-file-fuzz-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

proptest! {
    #![proptest_config(ProptestConfig { failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn fuzz_file_read_stream_path(path in any::<String>()) {
        if let Err(e) = file_read_stream(FileOptions {
            path: path.clone().into(),
            types: types(),
            ..Default::default()
        }) {
            check_error(&path, &e);
        }
    }

    // Unlike the JS fuzz (which writes to arbitrary paths), writes are confined
    // to a scratch base_path, and any success must land inside it.
    #[test]
    fn fuzz_file_write_stream_path(path in any::<String>()) {
        let base = base_dir();
        let target = base.join(&path);
        let result = runtime().block_on(file_write_stream(
            create_readable_stream([b"x".to_vec()]),
            FileOptions {
                path: target.clone(),
                base_path: Some(base.clone()),
                types: types(),
            },
        ));
        match result {
            Ok(()) => {
                let written = std::fs::canonicalize(&target).unwrap();
                prop_assert!(written.starts_with(std::fs::canonicalize(&base).unwrap()));
                std::fs::remove_file(&target).unwrap();
            }
            Err(e) => check_error(&path, &e),
        }
    }

    #[test]
    fn fuzz_file_read_stream_types(
        fuzz_types in prop::collection::vec(
            prop::collection::btree_map(any::<String>(), prop::collection::vec(".{1,5}", 0..4), 0..4),
            0..4,
        ),
    ) {
        let unrestricted = fuzz_types.is_empty();
        let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let types: Vec<FileType> = fuzz_types.into_iter().map(|accept| FileType { accept }).collect();
        let accepts_toml = types
            .iter()
            .any(|t| t.accept.values().flatten().any(|e| e == ".toml"));
        let result = file_read_stream(FileOptions { path, types, ..Default::default() });
        match result {
            Ok(_) => prop_assert!(unrestricted || accepts_toml),
            Err(e) => {
                prop_assert_eq!(e.to_string(), "Invalid extension");
                prop_assert!(!unrestricted && !accepts_toml);
            }
        }
    }
}

#[test]
fn rejects_path_traversal_with_base_path() {
    for path in ["/etc/passwd", "/tmp/safe/../../etc/passwd"] {
        let e = file_read_stream(FileOptions {
            path: path.into(),
            base_path: Some("/tmp/safe".into()),
            types: Vec::new(),
        })
        .err()
        .unwrap();
        assert_eq!(e.to_string(), "Path traversal detected");
    }
    let e = runtime()
        .block_on(file_write_stream(
            create_readable_stream([b"x".to_vec()]),
            FileOptions {
                path: "/etc/shadow".into(),
                base_path: Some("/tmp/safe".into()),
                types: Vec::new(),
            },
        ))
        .unwrap_err();
    assert_eq!(e.to_string(), "Path traversal detected");
}
