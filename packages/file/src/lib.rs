// Copyright 2026 will Farrell, and datastream contributors.
// SPDX-License-Identifier: MIT
//! File read and write streams, ported from `@datastream/file`.
//!
//! With `base_path` set, the path must resolve inside it, must not be a
//! symbolic link, its real (symlink-resolved) parent directory must be inside
//! the real `base_path`, and it is opened with `O_NOFOLLOW` (unix) so a symlink
//! swapped in after the check is still refused. `types` restricts the
//! extension (case-insensitive; `""` accepts an extensionless name).

use std::collections::BTreeMap;
use std::fmt;
use std::fs::OpenOptions;
use std::io;
use std::path::{Component, Path, PathBuf};

use async_stream::try_stream;
use datastream_core::{DataStream, Error, Result, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Node's fs stream default highWaterMark.
const CHUNK_SIZE: usize = 64 * 1024;

/// Accepted extensions by mime type, e.g. `{"text/csv": [".csv"]}`
/// (the shape of the File System Access API `types` option).
#[derive(Default, Clone, Debug)]
pub struct FileType {
    pub accept: BTreeMap<String, Vec<String>>,
}

#[derive(Default, Clone, Debug)]
pub struct FileOptions {
    pub path: PathBuf,
    pub base_path: Option<PathBuf>,
    pub types: Vec<FileType>,
}

/// "Path not found", with the underlying `lstat` error as its source.
#[derive(Debug)]
struct PathNotFound(io::Error);

impl fmt::Display for PathNotFound {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Path not found")
    }
}

impl std::error::Error for PathNotFound {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Lexical absolute path, like node's `path.resolve` (symlinks untouched).
fn resolve(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

/// The resolved path, once it is known to be inside `base_path` and not a symlink.
fn enforce_path(path: &Path, base_path: &Path) -> Result<PathBuf> {
    let resolved = resolve(path)?;
    let resolved_base = resolve(base_path)?;
    match resolved.strip_prefix(&resolved_base) {
        Ok(rel) if !rel.as_os_str().is_empty() => {}
        _ => return Err("Path traversal detected".into()),
    }
    match std::fs::symlink_metadata(&resolved) {
        Ok(stat) if stat.file_type().is_symlink() => {
            return Err("Symbolic links are not allowed".into())
        }
        Ok(_) => {}
        // File may not exist yet (for writes), that's ok
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(PathNotFound(e).into()),
    }
    // The checks above are lexical and only look at the final component, so a
    // symlinked directory (base_path/link -> /elsewhere) would still escape.
    // Compare the real parent directory against the real base_path.
    // Limit: a local attacker swapping a parent directory between this check
    // and the open (TOCTOU) is out of scope; O_NOFOLLOW only guards the leaf.
    let real = |p: &Path| std::fs::canonicalize(p).map_err(PathNotFound);
    // `resolved` is strictly inside `resolved_base`, so it has a parent.
    let parent = resolved.parent().unwrap_or(&resolved_base);
    if !real(parent)?.starts_with(real(&resolved_base)?) {
        return Err("Path traversal detected".into());
    }
    Ok(resolved)
}

// Case-insensitive: "REPORT.CSV" is still a .csv file. Like node's
// `path.extname`, an extensionless name or a dotfile (".csv") has the
// extension "", which `types` can list to accept it.
fn enforce_type(path: &Path, types: &[FileType]) -> Result<()> {
    if types.is_empty() {
        return Ok(());
    }
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy().to_lowercase()))
        .unwrap_or_default();
    let accepted = types
        .iter()
        .flat_map(|t| t.accept.values().flatten())
        .any(|e| e.to_lowercase() == ext);
    if accepted {
        Ok(())
    } else {
        Err("Invalid extension".into())
    }
}

fn open(options: &FileOptions, write: bool) -> Result<tokio::fs::File> {
    // Open what was checked, not a path the OS might resolve differently.
    let path = match &options.base_path {
        Some(base_path) => enforce_path(&options.path, base_path)?,
        None => options.path.clone(),
    };
    enforce_type(&options.path, &options.types)?;
    let mut open = OpenOptions::new();
    if write {
        open.write(true).create(true).truncate(true);
    } else {
        open.read(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        if options.base_path.is_some() {
            // Prevent a TOCTOU symlink race after the lstat check.
            open.custom_flags(libc::O_NOFOLLOW);
        }
    }
    Ok(tokio::fs::File::from_std(open.open(path)?))
}

/// Readable of the file's bytes. Checks and opening happen up front.
pub fn file_read_stream(options: FileOptions) -> Result<DataStream<Vec<u8>>> {
    let mut file = open(&options, false)?;
    Ok(Box::pin(try_stream! {
        loop {
            let mut buf = vec![0; CHUNK_SIZE];
            let n = file.read(&mut buf).await.map_err(Error::from)?;
            if n == 0 {
                break;
            }
            buf.truncate(n);
            yield buf;
        }
    }))
}

/// Write every chunk to the file (created or truncated).
pub async fn file_write_stream<T: AsRef<[u8]>>(
    mut input: DataStream<T>,
    options: FileOptions,
) -> Result<()> {
    let mut file = open(&options, true)?;
    while let Some(chunk) = input.next().await {
        file.write_all(chunk?.as_ref()).await?;
    }
    file.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datastream_core::{create_readable_stream, stream_to_string};
    use std::fs;

    const CONTENT: &str = "a,b,c\n1,2,3\n";

    /// Fresh directory holding `test.csv`.
    fn setup(name: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "datastream-file-test-{}-{name}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let file = dir.join("test.csv");
        fs::write(&file, CONTENT).unwrap();
        (dir, file)
    }

    fn options(path: &Path, base_path: Option<&Path>) -> FileOptions {
        FileOptions {
            path: path.to_path_buf(),
            base_path: base_path.map(Path::to_path_buf),
            types: Vec::new(),
        }
    }

    fn types(mime: &str, ext: &str) -> Vec<FileType> {
        vec![FileType {
            accept: BTreeMap::from([(mime.to_string(), vec![ext.to_string()])]),
        }]
    }

    async fn read(options: FileOptions) -> String {
        let bytes = file_read_stream(options).unwrap();
        let text: DataStream<String> =
            Box::pin(bytes.map(|c| c.map(|c| String::from_utf8(c).unwrap())));
        stream_to_string(text, None).await.unwrap()
    }

    async fn write(options: FileOptions, content: &str) -> Result<()> {
        file_write_stream(
            create_readable_stream([content.as_bytes().to_vec()]),
            options,
        )
        .await
    }

    fn read_err(options: FileOptions) -> String {
        file_read_stream(options).err().unwrap().to_string()
    }

    #[tokio::test]
    async fn reads_with_and_without_base_path() {
        let (dir, file) = setup("read");
        assert_eq!(read(options(&file, None)).await, CONTENT);
        assert_eq!(read(options(&file, Some(&dir))).await, CONTENT);
    }

    #[tokio::test]
    async fn reads_large_file_in_chunks() {
        let (dir, file) = setup("large");
        let big = "x".repeat(CHUNK_SIZE * 2 + 1);
        fs::write(&file, &big).unwrap();
        let chunks: Vec<_> = file_read_stream(options(&file, Some(&dir)))
            .unwrap()
            .collect()
            .await;
        assert!(chunks.len() >= 3);
        assert_eq!(read(options(&file, None)).await, big);
    }

    #[tokio::test]
    async fn writes_with_base_path() {
        let (dir, _) = setup("write");
        let out = dir.join("written.csv");
        write(options(&out, Some(&dir)), "written content")
            .await
            .unwrap();
        assert_eq!(fs::read_to_string(&out).unwrap(), "written content");
        // Overwriting truncates.
        write(options(&out, Some(&dir)), "new").await.unwrap();
        assert_eq!(fs::read_to_string(&out).unwrap(), "new");
    }

    #[test]
    fn rejects_path_traversal() {
        let (dir, _) = setup("traversal");
        let msg = "Path traversal detected";
        assert_eq!(read_err(options(Path::new("/etc/passwd"), Some(&dir))), msg);
        assert_eq!(
            read_err(options(&dir.join("../../etc/passwd"), Some(&dir))),
            msg
        );
        let sibling = PathBuf::from(format!("{}-sibling/x.csv", dir.display()));
        assert_eq!(read_err(options(&sibling, Some(&dir))), msg);
        assert_eq!(read_err(options(&dir, Some(&dir))), msg);
        assert_eq!(read_err(options(&dir.join("sub/.."), Some(&dir))), msg);
    }

    #[tokio::test]
    async fn write_rejects_path_traversal() {
        let (dir, _) = setup("write-traversal");
        let e = write(options(Path::new("/etc/shadow"), Some(&dir)), "x")
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Path traversal detected");
        let e = write(options(&dir, Some(&dir)), "x").await.unwrap_err();
        assert_eq!(e.to_string(), "Path traversal detected");
    }

    #[tokio::test]
    async fn resolves_relative_inner_paths() {
        let (dir, file) = setup("relative");
        assert_eq!(
            read(options(&dir.join("sub/../test.csv"), Some(&dir))).await,
            CONTENT
        );
        assert_eq!(read(options(&file, Some(&dir.join(".")))).await, CONTENT);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_followed_only_without_base_path() {
        let (dir, file) = setup("symlink");
        let link = dir.join("link.csv");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert_eq!(read(options(&link, None)).await, CONTENT);
        assert_eq!(
            read_err(options(&link, Some(&dir))),
            "Symbolic links are not allowed"
        );
        let e = write(options(&link, Some(&dir)), "x").await.unwrap_err();
        assert_eq!(e.to_string(), "Symbolic links are not allowed");
        assert_eq!(fs::read_to_string(&file).unwrap(), CONTENT);
        write(options(&link, None), "via-symlink").await.unwrap();
        assert_eq!(fs::read_to_string(&file).unwrap(), "via-symlink");
    }

    #[test]
    fn enforces_extension() {
        let (dir, file) = setup("types");
        let with = |t| FileOptions {
            types: t,
            ..options(&file, None)
        };
        assert!(file_read_stream(with(types("text/csv", ".csv"))).is_ok());
        assert!(file_read_stream(with(Vec::new())).is_ok());
        assert_eq!(
            read_err(with(types("application/json", ".json"))),
            "Invalid extension"
        );
        let no_ext = FileOptions {
            types: types("text/csv", ".csv"),
            ..options(&dir.join("README"), None)
        };
        assert_eq!(read_err(no_ext), "Invalid extension");
    }

    #[tokio::test]
    async fn extensionless_path_accepted_when_types_allow_empty() {
        let (dir, _) = setup("types-empty");
        for name in ["noext", ".csv"] {
            let out = dir.join(name);
            let o = FileOptions {
                types: types("text/plain", ""),
                ..options(&out, None)
            };
            write(o, "x").await.unwrap();
            assert_eq!(fs::read_to_string(&out).unwrap(), "x");
        }
        // "" does not accept a name that has an extension.
        let o = FileOptions {
            types: types("text/plain", ""),
            ..options(&dir.join("test.csv"), None)
        };
        assert_eq!(read_err(o), "Invalid extension");
    }

    #[tokio::test]
    async fn extension_check_is_case_insensitive() {
        let (dir, _) = setup("types-case");
        let upper = dir.join("UPPER.CSV");
        fs::write(&upper, CONTENT).unwrap();
        let o = FileOptions {
            types: types("text/csv", ".csv"),
            ..options(&upper, None)
        };
        assert_eq!(read(o).await, CONTENT);
        let out = dir.join("out.json");
        let o = FileOptions {
            types: types("application/json", ".JSON"),
            ..options(&out, None)
        };
        write(o, "{}").await.unwrap();
        assert_eq!(fs::read_to_string(&out).unwrap(), "{}");
        let o = FileOptions {
            types: types("application/json", ".json"),
            ..options(&upper, None)
        };
        assert_eq!(read_err(o), "Invalid extension");
    }

    #[tokio::test]
    async fn write_enforces_extension() {
        let (dir, _) = setup("write-types");
        let out = dir.join("out.csv");
        let o = FileOptions {
            types: types("application/json", ".json"),
            ..options(&out, None)
        };
        assert_eq!(
            write(o, "x").await.unwrap_err().to_string(),
            "Invalid extension"
        );
        assert!(!out.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_paths_through_symlinked_directories() {
        let (dir, _) = setup("dirlink");
        let outside = std::env::temp_dir().join(format!(
            "datastream-file-test-{}-dirlink-outside",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&outside);
        fs::create_dir_all(&outside).unwrap();
        fs::write(outside.join("secret.csv"), "secret").unwrap();
        std::os::unix::fs::symlink(&outside, dir.join("dirlink")).unwrap();
        let msg = "Path traversal detected";
        assert_eq!(
            read_err(options(&dir.join("dirlink/secret.csv"), Some(&dir))),
            msg
        );
        let e = write(options(&dir.join("dirlink/new.csv"), Some(&dir)), "x")
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), msg);
        assert!(!outside.join("new.csv").exists());
        // A symlinked directory pointing at the parent of base_path.
        std::os::unix::fs::symlink(dir.parent().unwrap(), dir.join("uplink")).unwrap();
        assert_eq!(
            read_err(options(&dir.join("uplink/x.csv"), Some(&dir))),
            msg
        );
        // A symlinked base_path itself is allowed, but path == base_path is not.
        let link_base = dir.join("linkbase");
        std::os::unix::fs::symlink(&dir, &link_base).unwrap();
        assert_eq!(
            read(options(&link_base.join("test.csv"), Some(&link_base))).await,
            CONTENT
        );
        assert_eq!(read_err(options(&link_base, Some(&link_base))), msg);
        fs::remove_dir_all(&outside).unwrap();
    }

    #[tokio::test]
    async fn names_starting_with_dots_stay_inside_base_path() {
        let (dir, _) = setup("dots");
        fs::create_dir_all(dir.join("..cache")).unwrap();
        for out in [dir.join("..data.csv"), dir.join("..cache/x.csv")] {
            write(options(&out, Some(&dir)), "dots").await.unwrap();
            assert_eq!(read(options(&out, Some(&dir))).await, "dots");
        }
    }

    #[tokio::test]
    async fn write_missing_parent_directory_is_path_not_found() {
        let (dir, _) = setup("missing-parent");
        let e = write(options(&dir.join("missing/x.csv"), Some(&dir)), "x")
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "Path not found");
        let source = e.source().unwrap().downcast_ref::<io::Error>().unwrap();
        assert_eq!(source.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn path_not_found_for_non_enoent_errors() {
        let (dir, file) = setup("notdir");
        // test.csv is not a directory: ENOTDIR, not ENOENT.
        let e = file_read_stream(options(&file.join("inside.csv"), Some(&dir)))
            .err()
            .unwrap();
        assert_eq!(e.to_string(), "Path not found");
        assert!(e.source().is_some());
    }

    #[test]
    fn read_missing_file_errors() {
        let (dir, _) = setup("missing");
        assert!(file_read_stream(options(&dir.join("nope.csv"), Some(&dir))).is_err());
        assert!(file_read_stream(options(&dir.join("nope.csv"), None)).is_err());
    }

    #[tokio::test]
    async fn write_propagates_input_errors() {
        let (dir, _) = setup("write-error");
        let input: DataStream<Vec<u8>> = Box::pin(futures_err());
        let e = file_write_stream(input, options(&dir.join("e.csv"), Some(&dir)))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "boom");
    }

    fn futures_err() -> impl datastream_core::Stream<Item = Result<Vec<u8>>> + Send {
        try_stream! {
            yield b"a".to_vec();
            Err::<(), Error>("boom".into())?;
        }
    }
}
