//! OS-main-thread file ownership, with bounded network pages between writes.
use super::{CliError, Context, Result};
use focal_client::{Client, ClientError, ClientTransport};
use focal_model::{Artifact, ArtifactPayload, ContentRef};
use focal_wire::{ContentChunk, Operation, RequestEnvelope, Response};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    panic::AssertUnwindSafe,
    path::{Path, PathBuf},
};
use tokio::runtime::Runtime;

const PAGE_BYTES: u32 = 64 * 1024;

pub(super) fn artifact(
    runtime: &Runtime,
    context: &Context,
    artifact: &Artifact,
    output: &Path,
) -> Result<()> {
    if artifact.content().ledger != context.build.ledger {
        return Err(CliError::InvalidResponse);
    }
    payload(
        runtime,
        &context.client,
        |operation| context.envelope(operation),
        &artifact.content().payload,
        output,
    )
}

fn payload<T: ClientTransport>(
    runtime: &Runtime,
    client: &Client<T>,
    mut envelope: impl FnMut(Operation) -> Result<RequestEnvelope>,
    payload: &ArtifactPayload,
    output: &Path,
) -> Result<()> {
    // A caller inside a runtime must not perform synchronous writes or nest
    // block_on. The CLI invokes this owner from its OS main thread.
    if tokio::runtime::Handle::try_current().is_ok() {
        return Err(CliError::Input(
            "artifact download requires the CLI main thread outside an async runtime".into(),
        ));
    }
    let mut temporary = Temporary::create(output)?;
    match payload {
        ArtifactPayload::Inline(bytes) => temporary.file.write_all(bytes)?,
        ArtifactPayload::Content(content) => {
            stream(content, &mut temporary.file, |offset, max_bytes| {
                let request = envelope(Operation::Download {
                    content: content.clone(),
                    offset,
                    max_bytes,
                })?;
                // Tokio/transport dependencies can panic on a supplied runtime
                // without drivers. The owner still removes the temporary file.
                let response = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    runtime.block_on(client.request(request))
                }))
                .map_err(|_| CliError::Client(ClientError::Transport))??;
                match response.result {
                    Response::Content(chunk) => Ok(chunk),
                    _ => Err(CliError::InvalidResponse),
                }
            })?;
        }
    }
    temporary.install(output)
}

fn stream(
    content: &ContentRef,
    file: &mut File,
    mut read: impl FnMut(u64, u32) -> Result<ContentChunk>,
) -> Result<()> {
    let mut download = focal_client::artifact_transfer::PayloadDownload::new(
        content.clone(),
        content.length,
        PAGE_BYTES,
    )
    .map_err(|_| CliError::InvalidResponse)?;
    while !download.complete() {
        let chunk = read(download.offset(), PAGE_BYTES)?;
        download.accept(&chunk, file).map_err(|error| match error {
            focal_client::artifact_transfer::TransferError::Io(error) => CliError::Io(error),
            _ => CliError::InvalidResponse,
        })?;
    }
    Ok(())
}

pub(super) struct Temporary {
    pub(super) file: File,
    path: Option<PathBuf>,
    parent: File,
}
impl Temporary {
    pub(super) fn create(output: &Path) -> Result<Self> {
        if output.file_name().is_none() {
            return Err(CliError::Input("output must name a new file".into()));
        }
        // Catch existing files, directories, and dangling symlinks before any
        // transfer. hard_link below repeats the no-clobber check atomically.
        match fs::symlink_metadata(output) {
            Ok(_) => return Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let parent = output
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent_file = File::open(parent)?;
        for _ in 0..8 {
            let random = super::random_id()?;
            let path = parent.join(format!(
                ".focal-download-{:032x}",
                u128::from_be_bytes(random)
            ));
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&path) {
                Ok(file) => {
                    return Ok(Self {
                        file,
                        path: Some(path),
                        parent: parent_file,
                    });
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(std::io::Error::from(std::io::ErrorKind::AlreadyExists).into())
    }
    pub(super) fn install(mut self, output: &Path) -> Result<()> {
        self.file.sync_all()?;
        let path = self.path.as_ref().ok_or(CliError::InvalidResponse)?;
        // Same-directory publication never replaces an existing output. Once
        // linked, an fsync error can leave a complete output, but reports error.
        fs::hard_link(path, output)?;
        self.parent.sync_all()?;
        fs::remove_file(path)?;
        self.path = None;
        self.parent.sync_all()?;
        Ok(())
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            let _ = fs::remove_file(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use focal_client::{RetryPolicy, TransportFuture};
    use focal_evidence::{ContentStore, StoreLimits, UploadId};
    use focal_model::*;
    use focal_wire::*;

    fn reference(length: u64) -> ContentRef {
        ContentRef {
            domain: ContentDomainId::from_u128(1),
            root: ContentHash([2; 32]),
            length,
            class: ContentClass::Evidence,
        }
    }
    fn envelope(operation: Operation) -> Result<RequestEnvelope> {
        Ok(RequestEnvelope {
            protocol: PROTOCOL_VERSION,
            ledger: LedgerId {
                tenant: TenantId::from_u128(1),
                session: SessionId::from_u128(2),
            },
            route_epoch: RouteEpoch(1),
            request_epoch: RequestEpoch(1),
            request_id: RequestId::from_u128(3),
            operation,
        })
    }
    struct StoreTransport(ContentStore);
    impl ClientTransport for StoreTransport {
        fn request<'a>(
            &'a self,
            _: Option<&'a RouteHint>,
            request: &'a RequestEnvelope,
        ) -> TransportFuture<'a> {
            Box::pin(async move {
                let Operation::Download {
                    content,
                    offset,
                    max_bytes,
                } = &request.operation
                else {
                    panic!("download only")
                };
                // Exercise real content verification with a server whose local
                // preferred page is smaller than the client's requested page.
                let bytes = self
                    .0
                    .read_range(content, *offset, (*max_bytes as usize).min(4096))
                    .map_err(|_| WireError::Access(AccessError::InvalidRequest))?;
                Ok(ResponseEnvelope {
                    protocol: request.protocol,
                    ledger: request.ledger,
                    route_epoch: request.route_epoch,
                    request_epoch: request.request_epoch,
                    request_id: request.request_id,
                    result: Response::Content(ContentChunk {
                        offset: *offset,
                        eof: *offset + bytes.len() as u64 == content.length,
                        bytes,
                    }),
                })
            })
        }
    }
    fn store(path: &Path, bytes: &[u8]) -> (Client<StoreTransport>, ContentRef) {
        let mut store = ContentStore::open(
            path,
            StoreLimits {
                max_content_bytes: 1024 * 1024,
                max_staging_bytes: 1024 * 1024,
                max_uploads: 1,
                chunk_bytes: 4096,
                max_manifest_bytes: 4096,
            },
        )
        .unwrap();
        let id = UploadId([4; 16]);
        store
            .begin(
                id,
                reference(0).domain,
                ContentClass::Evidence,
                bytes.len() as u64,
                None,
            )
            .unwrap();
        let mut offset = 0;
        for chunk in bytes.chunks(4096) {
            offset = store.append(id, offset, chunk).unwrap();
        }
        let reference = store.seal(id).unwrap();
        let client = Client::new(
            StoreTransport(store),
            RetryPolicy::default(),
            WireLimits::default(),
            1,
        )
        .unwrap();
        (client, reference)
    }
    #[test]
    fn actual_verified_chunks_and_inline_bytes_publish_complete_new_private_files() {
        use std::os::unix::fs::MetadataExt;
        let directory = tempfile::tempdir().unwrap();
        let bytes = (0..20_111)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let (client, reference) = store(&directory.path().join("store"), &bytes);
        assert_ne!(
            reference.root,
            ContentHash(*blake3::hash(&bytes).as_bytes())
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        for (name, data) in [
            ("content", ArtifactPayload::Content(reference)),
            ("inline", ArtifactPayload::Inline(bytes.clone())),
        ] {
            let output = directory.path().join(name);
            payload(&runtime, &client, envelope, &data, &output).unwrap();
            assert_eq!(fs::read(&output).unwrap(), bytes);
            assert_eq!(fs::metadata(output).unwrap().mode() & 0o777, 0o600);
        }
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 3);
    }
    #[test]
    fn existing_and_racing_outputs_are_never_clobbered_and_temporary_is_removed() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output");
        let mut temporary = Temporary::create(&output).unwrap();
        temporary.file.write_all(b"new bytes").unwrap();
        fs::write(&output, b"previous").unwrap();
        assert!(temporary.install(&output).is_err());
        assert_eq!(fs::read(&output).unwrap(), b"previous");
        assert!(Temporary::create(&output).is_err());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
        fs::remove_file(&output).unwrap();
        std::os::unix::fs::symlink(directory.path().join("absent"), &output).unwrap();
        assert!(Temporary::create(&output).is_err());
    }
    #[test]
    fn malformed_offsets_lengths_eof_and_empty_progress_never_publish_partial_content() {
        let directory = tempfile::tempdir().unwrap();
        for malformed in 0..6 {
            let output = directory.path().join(format!("{malformed}"));
            let mut temporary = Temporary::create(&output).unwrap();
            let result = stream(&reference(4), &mut temporary.file, |offset, _| {
                if offset == 0 {
                    return Ok(ContentChunk {
                        offset: 0,
                        bytes: vec![1, 2],
                        eof: false,
                    });
                }
                Ok(match malformed {
                    0 => ContentChunk {
                        offset: 0,
                        bytes: vec![3, 4],
                        eof: true,
                    },
                    1 => ContentChunk {
                        offset,
                        bytes: vec![3],
                        eof: true,
                    },
                    2 => ContentChunk {
                        offset,
                        bytes: vec![3, 4],
                        eof: false,
                    },
                    3 => ContentChunk {
                        offset,
                        bytes: vec![3, 4, 5],
                        eof: true,
                    },
                    4 => ContentChunk {
                        offset,
                        bytes: Vec::new(),
                        eof: false,
                    },
                    _ => ContentChunk {
                        offset,
                        bytes: vec![0; PAGE_BYTES as usize + 1],
                        eof: true,
                    },
                })
            });
            assert!(matches!(result, Err(CliError::InvalidResponse)));
            drop(temporary);
            assert!(!output.exists());
        }
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    #[test]
    fn missing_runtime_drivers_are_typed_and_leave_no_output_or_temporary() {
        let directory = tempfile::tempdir().unwrap();
        let (client, reference) = store(&directory.path().join("store"), b"bytes");
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let output = directory.path().join("output");
        assert!(
            payload(
                &runtime,
                &client,
                envelope,
                &ArtifactPayload::Content(reference),
                &output
            )
            .is_err()
        );
        assert!(!output.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
