use super::*;
use crate::{
    Client, ClientTransport, ContentChunk, Response, RetryPolicy, RouteHint, TransportFuture,
    WireLimits,
};
use focal_evidence::{ContentStore, StoreLimits, UploadId};
use std::{
    fs,
    io::{Cursor, Write},
    sync::Mutex,
};

pub(super) fn context() -> OperationContext {
    OperationContext {
        cluster: [1; 16],
        principal: ParticipantId::from_u128(2),
        ledger: LedgerId {
            tenant: TenantId::from_u128(3),
            session: SessionId::from_u128(4),
        },
    }
}
pub(super) fn spec(bytes: &[u8]) -> UploadSpec {
    UploadSpec {
        upload: [5; 16],
        class: ContentClass::Evidence,
        length: bytes.len() as u64,
        digest: ContentHash(*blake3::hash(bytes).as_bytes()),
    }
}
fn begin(path: &Path, bytes: &[u8]) -> UploadJournal {
    UploadJournal::begin(
        path,
        context(),
        spec(bytes),
        RouteEpoch(1),
        TransferLimits::default(),
    )
    .unwrap()
}
fn content(path: &Path) -> ContentStore {
    ContentStore::open(
        path,
        StoreLimits {
            max_content_bytes: MAX_TRANSFER_BYTES,
            max_staging_bytes: MAX_TRANSFER_BYTES,
            max_uploads: 4,
            chunk_bytes: TRANSFER_CHUNK_BYTES,
            max_manifest_bytes: 64 * 1024,
        },
    )
    .unwrap()
}
fn send(store: &mut ContentStore, request: &RequestEnvelope) -> UploadReply {
    let Operation::Upload(request) = &request.operation else {
        panic!("upload expected")
    };
    let id = UploadId(request.upload());
    match request {
        UploadRequest::Begin {
            length,
            digest,
            class,
            ..
        } => UploadReply::Offset(
            store
                .begin(
                    id,
                    ContentDomainId(context().ledger.tenant.0),
                    *class,
                    *length,
                    Some(*digest),
                )
                .unwrap(),
        ),
        UploadRequest::Append { offset, bytes, .. } => {
            UploadReply::Offset(store.append(id, *offset, bytes).unwrap())
        }
        UploadRequest::Seal { .. } => UploadReply::Sealed(store.seal(id).unwrap()),
        UploadRequest::Cancel { .. } => {
            store.finish(id).unwrap();
            UploadReply::Cancelled
        }
    }
}
fn step(journal: &mut UploadJournal, store: &mut ContentStore) -> RequestEnvelope {
    let request = journal.next_request().unwrap().unwrap();
    let reply = send(store, &request);
    journal.record_reply(&request, &reply).unwrap();
    request
}

#[test]
fn progressive_real_content_upload_preserves_exact_requests_across_response_loss_and_both_restarts()
{
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let server = root.path().join("server");
    let bytes = (0..140_007).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    let mut journal = begin(&path, &bytes);
    let mut store = content(&server);
    let initial = journal.next_request().unwrap().unwrap();
    assert_eq!(send(&mut store, &initial), UploadReply::Offset(0));
    drop(journal);
    drop(store);
    journal = UploadJournal::open(&path, &context()).unwrap();
    store = content(&server);
    assert_eq!(journal.next_request().unwrap().unwrap(), initial);
    step(&mut journal, &mut store);
    assert!(journal.next_request().unwrap().is_none());
    assert!(journal.reference().is_none());
    journal.stage(0, &bytes[..3]).unwrap();
    let append = journal.next_request().unwrap().unwrap();
    // Newly staged bytes must not enlarge an already exposed append.
    journal.stage(3, &bytes[3..65_003]).unwrap();
    assert_eq!(journal.next_request().unwrap().unwrap(), append);
    assert_eq!(send(&mut store, &append), UploadReply::Offset(3));
    drop(journal);
    drop(store);
    journal = UploadJournal::open(&path, &context()).unwrap();
    store = content(&server);
    assert_eq!(journal.next_request().unwrap().unwrap(), append);
    step(&mut journal, &mut store);
    let mut staged = 65_003usize;
    while staged < bytes.len() {
        let end = (staged + TRANSFER_CHUNK_BYTES).min(bytes.len());
        journal.stage(staged as u64, &bytes[staged..end]).unwrap();
        staged = end;
    }
    while journal.progress().received < bytes.len() as u64 {
        step(&mut journal, &mut store);
    }
    let seal = journal.next_request().unwrap().unwrap();
    let sealed = send(&mut store, &seal);
    let UploadReply::Sealed(reference) = &sealed else {
        panic!("sealed")
    };
    assert_ne!(reference.root, spec(&bytes).digest);
    drop(journal);
    drop(store);
    journal = UploadJournal::open(&path, &context()).unwrap();
    store = content(&server);
    assert_eq!(journal.next_request().unwrap().unwrap(), seal);
    assert_eq!(send(&mut store, &seal), sealed);
    journal.record_reply(&seal, &sealed).unwrap();
    step(&mut journal, &mut store);
    assert!(journal.progress().cancel_acknowledged);
    assert_eq!(journal.reference(), Some(reference));
    assert!(matches!(
        store.offset(UploadId([5; 16])),
        Err(focal_evidence::ContentError::MissingUpload)
    ));
    let mut download = PayloadDownload::new(reference.clone(), MAX_TRANSFER_BYTES, 4096).unwrap();
    let mut result = Vec::new();
    while let Some(Operation::Download {
        offset, max_bytes, ..
    }) = download.operation()
    {
        let bytes = store
            .read_range(reference, offset, max_bytes as usize)
            .unwrap();
        let eof = offset + bytes.len() as u64 == reference.length;
        download
            .accept(&ContentChunk { offset, bytes, eof }, &mut result)
            .unwrap();
    }
    assert_eq!(result, bytes);
    assert!(download.complete());
}

#[test]
fn metadata_context_bytes_and_seal_reference_are_bound_without_local_mutation() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let bytes = b"abcdef";
    let mut journal = begin(&path, bytes);
    let mut store = content(&root.path().join("server"));
    journal.stage(0, b"abc").unwrap();
    assert_eq!(journal.stage(0, b"abc").unwrap(), 3);
    let saved = fs::read(path.join("upload.bin")).unwrap();
    for (offset, bytes) in [(0, b"abd".as_slice()), (4, b"ef"), (3, b"deg")] {
        assert!(journal.stage(offset, bytes).is_err());
    }
    assert_eq!(fs::read(path.join("upload.bin")).unwrap(), saved);
    assert_eq!(fs::read(path.join("payload.bin")).unwrap(), b"abc");
    journal.stage(3, b"def").unwrap();
    step(&mut journal, &mut store);
    step(&mut journal, &mut store);
    let seal = journal.next_request().unwrap().unwrap();
    let UploadReply::Sealed(reference) = send(&mut store, &seal) else {
        panic!()
    };
    for bad in [
        ContentRef {
            length: 7,
            ..reference.clone()
        },
        ContentRef {
            class: ContentClass::Checkpoint,
            ..reference.clone()
        },
        ContentRef {
            domain: ContentDomainId([7; 16]),
            ..reference.clone()
        },
        ContentRef {
            root: ContentHash([0; 32]),
            ..reference.clone()
        },
    ] {
        assert!(matches!(
            journal.record_reply(&seal, &UploadReply::Sealed(bad)),
            Err(TransferError::Invalid)
        ));
        assert!(journal.reference().is_none());
    }
    drop(journal);
    let mut wrong = context();
    wrong.principal = ParticipantId::from_u128(99);
    assert!(matches!(
        UploadJournal::open(&path, &wrong),
        Err(TransferError::Invalid)
    ));
    let mut journal = UploadJournal::open(&path, &context()).unwrap();
    journal
        .record_reply(&seal, &UploadReply::Sealed(reference))
        .unwrap();
}

#[test]
fn staging_crash_boundaries_reopen_only_the_durable_prefix_and_never_regenerate_request() {
    for fault in [files::Fault::FileSynced, files::Fault::Renamed] {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("upload");
        let mut journal = begin(&path, b"abcdef");
        let request = journal.next_request().unwrap().unwrap();
        journal.directory.inject(fault);
        assert!(journal.stage(0, b"abc").is_err());
        assert!(matches!(journal.next_request(), Err(TransferError::Failed)));
        drop(journal);
        let mut journal = UploadJournal::open(&path, &context()).unwrap();
        assert_eq!(journal.next_request().unwrap().unwrap(), request);
        let expected = if fault == files::Fault::FileSynced {
            0
        } else {
            3
        };
        assert_eq!(journal.progress().staged, expected);
        assert_eq!(
            fs::metadata(path.join("payload.bin")).unwrap().len(),
            expected
        );
        journal.stage(0, b"abc").unwrap();
        journal.stage(3, b"def").unwrap();
    }
}

#[test]
fn prefix_bitrot_missing_state_and_external_links_fail_closed() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let mut journal = begin(&path, b"abcdef");
    journal.stage(0, b"abc").unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context()),
        Err(TransferError::Locked)
    ));
    drop(journal);
    fs::write(path.join("payload.bin"), b"abd").unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context()),
        Err(TransferError::Corrupt)
    ));
    fs::write(path.join("payload.bin"), b"abc").unwrap();
    fs::hard_link(path.join("payload.bin"), root.path().join("linked")).unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context()),
        Err(TransferError::Permissions)
    ));
    fs::remove_file(root.path().join("linked")).unwrap();
    fs::remove_file(path.join("upload.bin")).unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context()),
        Err(TransferError::Corrupt)
    ));
    assert!(matches!(
        begin_result(&path, b"abcdef"),
        Err(TransferError::Exists)
    ));
    let missing = root.path().join("missing");
    assert!(matches!(
        UploadJournal::open(&missing, &context()),
        Err(TransferError::Corrupt)
    ));
    assert!(!missing.exists());
    let link = root.path().join("symlink");
    symlink(&path, &link).unwrap();
    assert!(matches!(
        UploadJournal::open(&link, &context()),
        Err(TransferError::Permissions)
    ));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context()),
        Err(TransferError::Permissions)
    ));
}
fn begin_result(path: &Path, bytes: &[u8]) -> Result<UploadJournal, TransferError> {
    UploadJournal::begin(
        path,
        context(),
        spec(bytes),
        RouteEpoch(1),
        TransferLimits::default(),
    )
}

#[test]
fn explicit_cancel_is_durable_and_does_not_convert_staging_to_a_reference() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let mut journal = begin(&path, b"abcdef");
    let mut store = content(&root.path().join("server"));
    step(&mut journal, &mut store);
    journal.stage(0, b"abc").unwrap();
    step(&mut journal, &mut store);
    journal.cancel().unwrap();
    let cancel = journal.next_request().unwrap().unwrap();
    drop(journal);
    let mut journal = UploadJournal::open(&path, &context()).unwrap();
    assert_eq!(journal.next_request().unwrap().unwrap(), cancel);
    step(&mut journal, &mut store);
    assert!(journal.progress().cancelled);
    assert!(journal.progress().cancel_acknowledged);
    assert!(journal.reference().is_none());
    assert!(journal.next_request().unwrap().is_none());
    assert!(matches!(
        journal.stage(3, b"def"),
        Err(TransferError::Cancelled)
    ));
}

#[test]
fn acknowledged_cancel_fences_an_older_begin_across_both_restarts() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let mut journal = begin(&path, b"abc");
    let mut store = content(&root.path().join("server"));
    let delayed = journal.next_request().unwrap().unwrap();
    journal.cancel().unwrap();
    let cancel = step(&mut journal, &mut store);
    assert!(journal.progress().cancel_acknowledged);
    assert!(store.offset(UploadId(spec(b"abc").upload)).is_err());
    drop(store);
    let mut store = content(&root.path().join("server"));
    let Operation::Upload(UploadRequest::Begin {
        upload,
        length,
        digest,
        class,
    }) = delayed.operation
    else {
        panic!("begin");
    };
    assert!(matches!(
        store.begin(
            UploadId(upload),
            ContentDomainId(context().ledger.tenant.0),
            class,
            length,
            Some(digest)
        ),
        Err(focal_evidence::ContentError::FinishedUpload)
    ));
    drop(journal);
    let mut journal = UploadJournal::open(&path, &context()).unwrap();
    assert!(journal.progress().cancelled);
    assert!(matches!(
        journal.stage(0, b"abc"),
        Err(TransferError::Cancelled)
    ));
    journal.cancel().unwrap();
    assert_eq!(journal.next_request().unwrap().unwrap(), cancel);
    step(&mut journal, &mut store);
    assert!(store.offset(UploadId(spec(b"abc").upload)).is_err());
    assert!(journal.progress().cancel_acknowledged);
}

#[test]
fn zero_length_and_bounds_are_explicit_before_creation_or_output() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let mut journal = begin(&path, b"");
    let mut store = content(&root.path().join("server"));
    step(&mut journal, &mut store);
    step(&mut journal, &mut store);
    assert_eq!(journal.reference().unwrap().length, 0);
    step(&mut journal, &mut store);
    let other = root.path().join("too-large");
    let mut spec = spec(b"abc");
    spec.length = MAX_TRANSFER_BYTES + 1;
    assert!(
        UploadJournal::begin(
            &other,
            context(),
            spec,
            RouteEpoch(1),
            TransferLimits::default()
        )
        .is_err()
    );
    assert!(!other.exists());
    let mut output = Vec::new();
    assert!(
        retrieve_payload(
            &ArtifactPayload::Inline(vec![1, 2]),
            1,
            &mut output,
            |_| panic!(),
            |_| panic!()
        )
        .is_err()
    );
    assert!(output.is_empty());
    retrieve_payload(
        &ArtifactPayload::Inline(vec![1, 2]),
        2,
        &mut output,
        |_| panic!(),
        |_| panic!(),
    )
    .unwrap();
    assert_eq!(output, vec![1, 2]);
    assert_eq!(
        digest_reader(&mut Cursor::new(b"abc"), 3).unwrap(),
        spec_digest(b"abc")
    );
    assert!(digest_reader(&mut Cursor::new(b"abcd"), 3).is_err());
}
fn spec_digest(bytes: &[u8]) -> ContentHash {
    ContentHash(*blake3::hash(bytes).as_bytes())
}

#[test]
fn malformed_downloads_never_write_or_advance() {
    let reference = ContentRef {
        domain: ContentDomainId([3; 16]),
        root: ContentHash([4; 32]),
        class: ContentClass::Evidence,
        length: 4,
    };
    for chunk in [
        ContentChunk {
            offset: 1,
            bytes: vec![1],
            eof: false,
        },
        ContentChunk {
            offset: 0,
            bytes: vec![1],
            eof: true,
        },
        ContentChunk {
            offset: 0,
            bytes: vec![],
            eof: false,
        },
        ContentChunk {
            offset: 0,
            bytes: vec![1; 5],
            eof: true,
        },
    ] {
        let mut download = PayloadDownload::new(reference.clone(), 4, 4).unwrap();
        let mut output = Vec::new();
        assert!(download.accept(&chunk, &mut output).is_err());
        assert!(output.is_empty());
        assert_eq!(download.offset(), 0);
    }
    struct Failing;
    impl Write for Failing {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::other("full"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut download = PayloadDownload::new(reference, 4, 4).unwrap();
    assert!(
        download
            .accept(
                &ContentChunk {
                    offset: 0,
                    bytes: vec![1; 4],
                    eof: true
                },
                &mut Failing
            )
            .is_err()
    );
    assert_eq!(download.offset(), 0);
}

struct Downloads {
    store: Mutex<ContentStore>,
}
impl ClientTransport for Downloads {
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
                panic!("download")
            };
            let bytes = self
                .store
                .lock()
                .unwrap()
                .read_range(content, *offset, (*max_bytes as usize).min(4096))
                .unwrap();
            let eof = *offset + bytes.len() as u64 == content.length;
            Ok(request.reply(Response::Content(ContentChunk {
                offset: *offset,
                bytes,
                eof,
            })))
        })
    }
}
#[tokio::test]
async fn client_retrieves_all_verified_chunks_and_caps_aggregate_before_network() {
    let root = tempfile::tempdir().unwrap();
    let mut store = content(root.path());
    let bytes = vec![9; 70_001];
    let id = UploadId([6; 16]);
    store
        .begin(
            id,
            ContentDomainId(context().ledger.tenant.0),
            ContentClass::Evidence,
            bytes.len() as u64,
            Some(spec_digest(&bytes)),
        )
        .unwrap();
    let mut offset = 0;
    for chunk in bytes.chunks(TRANSFER_CHUNK_BYTES) {
        offset = store.append(id, offset, chunk).unwrap();
    }
    let reference = store.seal(id).unwrap();
    let client = Client::new(
        Downloads {
            store: Mutex::new(store),
        },
        RetryPolicy::default(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    let request = RequestEnvelope {
        protocol: focal_wire::PROTOCOL_VERSION,
        request_id: RequestId([1; 16]),
        request_epoch: RequestEpoch(1),
        ledger: context().ledger,
        route_epoch: RouteEpoch(1),
        operation: Operation::Download {
            content: reference.clone(),
            offset: 0,
            max_bytes: 1,
        },
    };
    let payload = ArtifactPayload::Content(reference);
    assert!(matches!(
        client
            .payload_bytes(request.clone(), &payload, 70_000)
            .await,
        Err(TransferError::Capacity)
    ));
    assert_eq!(
        client
            .payload_bytes(request, &payload, 70_001)
            .await
            .unwrap(),
        bytes
    );
}

#[test]
fn immutable_attachment_binding_and_external_bootstrap_survive_restart_and_reject_loss() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let limits = UploadStoreLimits::default();
    let store = UploadStore::bootstrap(root.path(), "uploads", context(), limits).unwrap();
    let mut journal = store.begin(context(), spec(b"abc"), RouteEpoch(1)).unwrap();
    journal
        .bind_intent(b"exact receipt and authored attachment")
        .unwrap();
    journal.stage(0, b"abc").unwrap();
    let request = journal.next_request().unwrap().unwrap();
    drop(journal);
    let mut journal = store.open_upload([5; 16], &context()).unwrap();
    assert_eq!(
        journal.intent(),
        Some(b"exact receipt and authored attachment".as_slice())
    );
    assert!(matches!(
        journal.bind_intent(b"changed"),
        Err(TransferError::Conflict)
    ));
    assert_eq!(journal.next_request().unwrap().unwrap(), request);
    drop(journal);
    let mut other = context();
    other.principal = ParticipantId::from_u128(44);
    assert!(matches!(
        UploadStore::bootstrap(root.path(), "uploads", other, limits),
        Err(TransferError::Conflict)
    ));
    fs::remove_dir_all(root.path().join("uploads")).unwrap();
    assert!(matches!(
        UploadStore::bootstrap(root.path(), "uploads", context(), limits),
        Err(TransferError::Corrupt)
    ));
    assert!(!root.path().join("uploads").exists());
}

struct SlowDownload(std::sync::atomic::AtomicUsize);
impl ClientTransport for SlowDownload {
    fn request<'a>(
        &'a self,
        _: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        Box::pin(async move {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            let Operation::Download {
                content, offset, ..
            } = &request.operation
            else {
                panic!()
            };
            Ok(request.reply(Response::Content(ContentChunk {
                offset: *offset,
                bytes: vec![1],
                eof: *offset + 1 == content.length,
            })))
        })
    }
}
impl ClientTransport for &SlowDownload {
    fn request<'a>(
        &'a self,
        route: Option<&'a RouteHint>,
        request: &'a RequestEnvelope,
    ) -> TransportFuture<'a> {
        <SlowDownload as ClientTransport>::request(self, route, request)
    }
}
fn tiny_download() -> (RequestEnvelope, ArtifactPayload) {
    let reference = ContentRef {
        domain: ContentDomainId(context().ledger.tenant.0),
        root: ContentHash([9; 32]),
        length: 3,
        class: ContentClass::Evidence,
    };
    (
        RequestEnvelope {
            protocol: focal_wire::PROTOCOL_VERSION,
            request_id: RequestId([7; 16]),
            request_epoch: RequestEpoch(1),
            ledger: context().ledger,
            route_epoch: RouteEpoch(1),
            operation: Operation::Download {
                content: reference.clone(),
                offset: 0,
                max_bytes: 1,
            },
        },
        ArtifactPayload::Content(reference),
    )
}
#[tokio::test]
async fn complete_retrieval_deadline_covers_all_chunks() {
    let transport = SlowDownload(std::sync::atomic::AtomicUsize::new(0));
    let client = Client::new(
        &transport,
        RetryPolicy {
            max_elapsed: std::time::Duration::from_millis(40),
            max_backoff: std::time::Duration::from_millis(20),
            ..RetryPolicy::default()
        },
        WireLimits::default(),
        1,
    )
    .unwrap();
    let (request, payload) = tiny_download();
    assert!(matches!(
        client.payload_bytes(request, &payload, 3).await,
        Err(TransferError::Client(ClientError::Transport))
    ));
    assert!(transport.0.load(std::sync::atomic::Ordering::Relaxed) <= 2);
}
#[test]
fn retrieval_without_runtime_or_time_driver_returns_typed_failure() {
    use std::future::Future;
    let client = Client::new(
        SlowDownload(std::sync::atomic::AtomicUsize::new(0)),
        RetryPolicy::default(),
        WireLimits::default(),
        1,
    )
    .unwrap();
    let (request, payload) = tiny_download();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(matches!(
        runtime.block_on(client.payload_bytes(request.clone(), &payload, 3)),
        Err(TransferError::Client(ClientError::Transport))
    ));
    let future = client.payload_bytes(request, &payload, 3);
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(TransferError::Client(ClientError::Transport)))
    ));
}
