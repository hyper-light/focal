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
        crate::at_rest::AtRest::clear(),
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
        &focal_seal::test_keys(path).unwrap(),
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
    journal = UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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
    journal = UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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
    journal = UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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
        UploadJournal::open(&path, &wrong, crate::at_rest::AtRest::clear()),
        Err(TransferError::Invalid)
    ));
    let mut journal =
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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
        let mut journal =
            UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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

#[cfg(unix)]
#[test]
fn prefix_bitrot_missing_state_and_external_links_fail_closed() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("upload");
    let mut journal = begin(&path, b"abcdef");
    journal.stage(0, b"abc").unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Locked)
    ));
    drop(journal);
    fs::write(path.join("payload.bin"), b"abd").unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Corrupt)
    ));
    fs::write(path.join("payload.bin"), b"abc").unwrap();
    fs::hard_link(path.join("payload.bin"), root.path().join("linked")).unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Permissions)
    ));
    fs::remove_file(root.path().join("linked")).unwrap();
    fs::remove_file(path.join("upload.bin")).unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Corrupt)
    ));
    assert!(matches!(
        begin_result(&path, b"abcdef"),
        Err(TransferError::Exists)
    ));
    let missing = root.path().join("missing");
    assert!(matches!(
        UploadJournal::open(&missing, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Corrupt)
    ));
    assert!(!missing.exists());
    let link = root.path().join("symlink");
    symlink(&path, &link).unwrap();
    assert!(matches!(
        UploadJournal::open(&link, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Permissions)
    ));
    fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
    assert!(matches!(
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()),
        Err(TransferError::Permissions)
    ));
}
#[cfg(unix)]
fn begin_result(path: &Path, bytes: &[u8]) -> Result<UploadJournal, TransferError> {
    UploadJournal::begin(
        path,
        context(),
        spec(bytes),
        RouteEpoch(1),
        TransferLimits::default(),
        crate::at_rest::AtRest::clear(),
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
    let mut journal =
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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
    let mut journal =
        UploadJournal::open(&path, &context(), crate::at_rest::AtRest::clear()).unwrap();
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
            TransferLimits::default(),
            crate::at_rest::AtRest::clear()
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
#[test]
fn client_retrieves_all_verified_chunks_and_caps_aggregate_before_network() {
    focal_rt::block_on(
        client_retrieves_all_verified_chunks_and_caps_aggregate_before_network_on_a_shard(),
    )
    .unwrap();
}
async fn client_retrieves_all_verified_chunks_and_caps_aggregate_before_network_on_a_shard() {
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

#[cfg(unix)]
#[test]
fn immutable_attachment_binding_and_external_bootstrap_survive_restart_and_reject_loss() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
    let limits = UploadStoreLimits::default();
    let store = UploadStore::bootstrap(
        root.path(),
        "uploads",
        context(),
        limits,
        crate::at_rest::AtRest::clear(),
    )
    .unwrap();
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
        UploadStore::bootstrap(
            root.path(),
            "uploads",
            other,
            limits,
            crate::at_rest::AtRest::clear()
        ),
        Err(TransferError::Conflict)
    ));
    fs::remove_dir_all(root.path().join("uploads")).unwrap();
    assert!(matches!(
        UploadStore::bootstrap(
            root.path(),
            "uploads",
            context(),
            limits,
            crate::at_rest::AtRest::clear()
        ),
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
            hyper_rt::futures::sleep(crate::time::nanos(std::time::Duration::from_millis(25)))
                .await
                .unwrap();
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
#[test]
fn complete_retrieval_deadline_covers_all_chunks() {
    focal_rt::block_on(complete_retrieval_deadline_covers_all_chunks_on_a_shard()).unwrap();
}
async fn complete_retrieval_deadline_covers_all_chunks_on_a_shard() {
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
    // Off a shard, where no deadline could be kept: refused, typed.
    let future = client.payload_bytes(request, &payload, 3);
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(matches!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(TransferError::Client(ClientError::Transport)))
    ));
}

/// What no file of a sealed upload may hold: a sentence its payload carries.
const SENTENCE: &[u8] = b"the upload's own bytes, in the clear";

/// An upload's bytes, `SENTENCE` among them in each of its chunks.
fn sentenced(length: usize) -> Vec<u8> {
    let mut bytes = (0..length).map(|i| (i % 251) as u8).collect::<Vec<_>>();
    for at in (0..length.saturating_sub(SENTENCE.len())).step_by(TRANSFER_CHUNK_BYTES / 2) {
        bytes[at..at + SENTENCE.len()].copy_from_slice(SENTENCE);
    }
    bytes
}

fn begin_in(path: &Path, bytes: &[u8], rest: crate::at_rest::AtRest) -> UploadJournal {
    UploadJournal::begin(
        path,
        context(),
        spec(bytes),
        RouteEpoch(1),
        TransferLimits::default(),
        rest,
    )
    .unwrap()
}

/// Every staged byte and request sent through to the content store: the reference it seals.
fn finish(journal: &mut UploadJournal, store: &mut ContentStore, bytes: &[u8]) -> ContentRef {
    let mut staged = journal.progress().staged as usize;
    while staged < bytes.len() {
        let end = (staged + TRANSFER_CHUNK_BYTES).min(bytes.len());
        journal.stage(staged as u64, &bytes[staged..end]).unwrap();
        staged = end;
    }
    while journal.reference().is_none() {
        step(journal, store);
    }
    journal.reference().unwrap().clone()
}

/// The content `reference` names, read back from the content store whole.
fn downloaded(store: &ContentStore, reference: &ContentRef) -> Vec<u8> {
    store
        .read_range(reference, 0, reference.length as usize)
        .unwrap()
}

/// An upload half done below the at-rest level (its bytes partly staged, partly sent, an append
/// pending) converted at its open and cut after each of its steps in turn (its part made, its state
/// sealed, its payload in the clear removed): the next open finishes; then its bytes lie in a part
/// and its state sealed, nothing of either in the clear; the pending append goes out as it was, and
/// the upload completes with every byte the content store reads back (29 §12.11).
#[test]
fn a_half_done_upload_converts_through_a_cut_at_any_step_and_completes() {
    use crate::at_rest::support::{assert_sealed_under, below, directory, sealing};
    let bytes = sentenced(140_007);
    for cut in 0.. {
        let (data, keys) = directory();
        let path = data.path().join("upload");
        let server = tempfile::tempdir().unwrap();
        let mut store = content(server.path());
        let mut journal = begin_in(&path, &bytes, below(&keys));
        step(&mut journal, &mut store);
        journal.stage(0, &bytes[..TRANSFER_CHUNK_BYTES]).unwrap();
        journal
            .stage(
                TRANSFER_CHUNK_BYTES as u64,
                &bytes[TRANSFER_CHUNK_BYTES..90_000],
            )
            .unwrap();
        step(&mut journal, &mut store);
        let pending = journal.next_request().unwrap().unwrap();
        drop(journal);
        assert!(path.join("payload.bin").exists());
        assert!(
            fs::read(path.join("upload.bin"))
                .unwrap()
                .starts_with(b"FCLUP001")
        );
        crate::at_rest::cut::after(cut);
        let first = UploadJournal::open(&path, &context(), sealing(&keys));
        let finished = first.is_ok();
        crate::at_rest::cut::none();
        let mut journal = match first {
            Ok(journal) => journal,
            Err(_) => UploadJournal::open(&path, &context(), sealing(&keys)).unwrap(),
        };
        assert!(!path.join("payload.bin").exists());
        assert!(path.join("payload.part").exists());
        assert!(path.join(crate::at_rest::SEALED).exists());
        assert_sealed_under(&path, &[SENTENCE]);
        assert_eq!(journal.progress().staged, 90_000);
        assert_eq!(journal.next_request().unwrap().unwrap(), pending);
        let reference = finish(&mut journal, &mut store, &bytes);
        assert_eq!(downloaded(&store, &reference), bytes);
        assert_sealed_under(&path, &[SENTENCE]);
        if finished {
            assert!(cut > 0, "a conversion of no step");
            break;
        }
    }
}

/// A sealed upload whose stage wrote its bytes and was cut before its state recorded them opens to
/// its recorded bytes alone, its part written again under a new key, and goes on: the same bytes
/// staged again, and the upload completes exactly.
#[test]
fn a_sealed_upload_cut_between_its_bytes_and_its_state_goes_on_under_a_new_key() {
    use crate::at_rest::support::{assert_sealed_under, directory, sealing};
    let bytes = sentenced(100_000);
    let (data, keys) = directory();
    let path = data.path().join("upload");
    let server = tempfile::tempdir().unwrap();
    let mut store = content(server.path());
    let mut journal = begin_in(&path, &bytes, sealing(&keys));
    journal.stage(0, &bytes[..40_000]).unwrap();
    let id = fs::read(path.join("payload.part")).unwrap()[8..24].to_vec();
    journal.directory.inject(files::Fault::FileSynced);
    assert!(journal.stage(40_000, &bytes[40_000..80_000]).is_err());
    drop(journal);
    let mut journal = UploadJournal::open(&path, &context(), sealing(&keys)).unwrap();
    assert_eq!(journal.progress().staged, 40_000);
    assert_ne!(
        fs::read(path.join("payload.part")).unwrap()[8..24].to_vec(),
        id,
        "written again under a new key"
    );
    let reference = finish(&mut journal, &mut store, &bytes);
    assert_eq!(downloaded(&store, &reference), bytes);
    assert_sealed_under(&path, &[SENTENCE]);
}

/// A sealed upload's state changed or swapped with another upload's, its part changed or carried
/// from another upload, or its payload planted in the clear after its mark: each refused; put back,
/// it opens.
#[test]
fn a_sealed_uploads_state_and_part_changed_swapped_carried_or_planted_in_the_clear_are_refused() {
    use crate::at_rest::AtRestError;
    use crate::at_rest::support::{directory, flip, sealing, swap};
    use focal_seal::named::NamedError;
    let bytes = sentenced(50_000);
    let (data, keys) = directory();
    let one = data.path().join("one");
    let two = data.path().join("two");
    let mut journal = begin_in(&one, &bytes, sealing(&keys));
    journal.stage(0, &bytes).unwrap();
    drop(journal);
    let mut other = begin_in(&two, &bytes, sealing(&keys));
    other.stage(0, &bytes[..20_000]).unwrap();
    drop(other);
    let opens = || UploadJournal::open(&one, &context(), sealing(&keys));

    let state = one.join("upload.bin");
    let saved = fs::read(&state).unwrap();
    flip(&state);
    assert!(matches!(
        opens(),
        Err(TransferError::AtRest(AtRestError::Sealed(
            NamedError::Seal(_)
        )))
    ));
    fs::write(&state, &saved).unwrap();
    swap(&state, &two.join("upload.bin"));
    assert!(matches!(
        opens(),
        Err(TransferError::AtRest(AtRestError::Sealed(
            NamedError::Moved(_)
        )))
    ));
    swap(&state, &two.join("upload.bin"));

    let part = one.join("payload.part");
    let kept = fs::read(&part).unwrap();
    let mut changed = kept.clone();
    changed[kept.len() / 2] ^= 1;
    fs::write(&part, &changed).unwrap();
    assert!(matches!(opens(), Err(TransferError::Corrupt)));
    fs::write(&part, fs::read(two.join("payload.part")).unwrap()).unwrap();
    assert!(matches!(opens(), Err(TransferError::Corrupt)));
    fs::write(&part, &kept).unwrap();

    fs::write(one.join("payload.bin"), &bytes).unwrap();
    assert!(matches!(
        opens(),
        Err(TransferError::AtRest(AtRestError::ClearAfterSealed(name))) if name == "payload.bin"
    ));
    fs::remove_file(one.join("payload.bin")).unwrap();
    assert_eq!(opens().unwrap().progress().staged, 50_000);
}

/// An upload store below the at-rest level (its bootstrap marker, its catalogue and an upload half
/// staged) converted at its bootstrap and its upload's open, cut after each step in turn: the next
/// opens finish; then nothing of the store lies in the clear, its context still binds it, and the
/// upload goes on (29 §12.11).
#[cfg(unix)]
#[test]
fn an_upload_store_converts_through_a_cut_at_any_step() {
    use crate::at_rest::support::{assert_sealed_under, below, directory, sealing};
    use std::os::unix::fs::PermissionsExt;
    let bytes = sentenced(70_000);
    let limits = UploadStoreLimits::default();
    for cut in 0.. {
        let (data, keys) = directory();
        let parent = data.path().join("context");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o700)).unwrap();
        let store =
            UploadStore::bootstrap(&parent, "uploads", context(), limits, below(&keys)).unwrap();
        let mut journal = store.begin(context(), spec(&bytes), RouteEpoch(1)).unwrap();
        journal.stage(0, &bytes[..30_000]).unwrap();
        let request = journal.next_request().unwrap().unwrap();
        drop((journal, store));
        crate::at_rest::cut::after(cut);
        let first = UploadStore::bootstrap(&parent, "uploads", context(), limits, sealing(&keys))
            .and_then(|store| store.open_upload([5; 16], &context()).map(|_| store));
        let finished = first.is_ok();
        crate::at_rest::cut::none();
        let store = match first {
            Ok(store) => store,
            Err(_) => UploadStore::bootstrap(&parent, "uploads", context(), limits, sealing(&keys))
                .unwrap(),
        };
        let mut journal = store.open_upload([5; 16], &context()).unwrap();
        assert_eq!(journal.next_request().unwrap().unwrap(), request);
        drop(journal);
        assert!(
            fs::read(parent.join("uploads.initialized"))
                .unwrap()
                .starts_with(crate::at_rest::SEALED_MAGIC)
        );
        assert_sealed_under(&parent, &[SENTENCE, b"FCLUPBS1", b"FCLUPCAT"]);
        let mut other = context();
        other.principal = ParticipantId::from_u128(44);
        assert!(matches!(
            UploadStore::bootstrap(&parent, "uploads", other, limits, sealing(&keys)),
            Err(TransferError::Conflict)
        ));
        if finished {
            assert!(cut > 0, "a conversion of no step");
            break;
        }
    }
}

/// What sealing an upload's staging costs (29 §12.11): 8 MiB staged 64 KiB a stage (each its bytes
/// written and synced, then its state installed), sealed and in the clear interleaved, 5 rounds of
/// each: the median round and stage; then an upload of 8 MiB staged in the clear converted at its
/// open, the median of 5. Run by hand, in release: `cargo test -p focal-client --release --lib --
/// --ignored measure_upload_staging --nocapture`.
#[test]
#[ignore = "a measurement, run by hand"]
fn measure_upload_staging() {
    use crate::at_rest::support::{below, directory, sealing};
    let median = |mut times: Vec<std::time::Duration>| {
        times.sort_unstable();
        times[times.len() / 2]
    };
    let bytes = sentenced(8 << 20);
    let (data, keys) = directory();
    let (mut rounds, mut stages) = ([Vec::new(), Vec::new()], [Vec::new(), Vec::new()]);
    for round in 0..5 {
        for (form, rest) in [below(&keys), sealing(&keys)].into_iter().enumerate() {
            let path = data.path().join(format!("upload-{round}-{form}"));
            let mut journal = begin_in(&path, &bytes, rest);
            let began = std::time::Instant::now();
            for (index, chunk) in bytes.chunks(TRANSFER_CHUNK_BYTES).enumerate() {
                let stage = std::time::Instant::now();
                journal
                    .stage((index * TRANSFER_CHUNK_BYTES) as u64, chunk)
                    .unwrap();
                stages[form].push(stage.elapsed());
            }
            rounds[form].push(began.elapsed());
        }
    }
    let [clear_rounds, sealed_rounds] = rounds;
    let [clear_stages, sealed_stages] = stages;
    eprintln!(
        "8 MiB staged 64 KiB a stage: in the clear {:?} (a stage {:?}), sealed {:?} (a stage {:?})",
        median(clear_rounds),
        median(clear_stages),
        median(sealed_rounds),
        median(sealed_stages)
    );
    let mut conversions = Vec::new();
    for round in 0..5 {
        let path = data.path().join(format!("converted-{round}"));
        let mut journal = begin_in(&path, &bytes, below(&keys));
        for (index, chunk) in bytes.chunks(TRANSFER_CHUNK_BYTES).enumerate() {
            journal
                .stage((index * TRANSFER_CHUNK_BYTES) as u64, chunk)
                .unwrap();
        }
        drop(journal);
        let began = std::time::Instant::now();
        drop(UploadJournal::open(&path, &context(), sealing(&keys)).unwrap());
        conversions.push(began.elapsed());
    }
    eprintln!(
        "an upload of 8 MiB converted at its open: {:?}",
        median(conversions)
    );
}
