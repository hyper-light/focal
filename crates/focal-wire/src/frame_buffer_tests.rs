//! Raw receive ownership and V1 framing behavior. These helpers provide no
//! payload checksum or actor authority; typed decoding remains a separate step.
use super::*;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::ReadBuf;

struct Fragmented<'a> {
    bytes: &'a [u8],
    consumed: usize,
    chunk: usize,
    pause: bool,
}
impl<'a> Fragmented<'a> {
    fn new(bytes: &'a [u8], chunk: usize) -> Self {
        assert_ne!(chunk, 0);
        Self {
            bytes,
            consumed: 0,
            chunk,
            pause: true,
        }
    }
}
impl AsyncRead for Fragmented<'_> {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.pause {
            this.pause = false;
            context.waker().wake_by_ref();
            return Poll::Pending;
        }
        this.pause = true;
        let count = (this.bytes.len() - this.consumed)
            .min(this.chunk)
            .min(output.remaining());
        output.put_slice(&this.bytes[this.consumed..this.consumed + count]);
        this.consumed += count;
        Poll::Ready(Ok(()))
    }
}

fn header(kind: FrameKind, bytes: u32) -> [u8; 16] {
    let mut header = *b"FOCALQ01\0\x01\0\0\0\0\0\0";
    header[10..12].copy_from_slice(&(kind as u16).to_be_bytes());
    header[12..16].copy_from_slice(&bytes.to_be_bytes());
    header
}
fn frame(kind: FrameKind, payload: &[u8]) -> Vec<u8> {
    let mut bytes = header(kind, u32::try_from(payload.len()).unwrap()).to_vec();
    bytes.extend_from_slice(payload);
    bytes
}
fn eof<T: std::fmt::Debug>(result: Result<T, WireError>) {
    assert!(
        matches!(result, Err(WireError::Io(ref error)) if error.kind() == std::io::ErrorKind::UnexpectedEof),
        "{result:?}"
    );
}

#[tokio::test]
async fn caller_buffer_receives_fragmented_v1_payload_without_touching_its_tail() {
    for kind in [
        FrameKind::Hello,
        FrameKind::HelloReply,
        FrameKind::Request,
        FrameKind::Response,
    ] {
        // Postcard encodes this u16 as LEB128. This fixture is independent of the
        // writer/measurement code, including the exact sixteen header bytes.
        let expected = frame(kind, &[0xac, 0x02]);
        let mut written = Vec::new();
        write_frame(&mut written, kind, &300u16, 1024)
            .await
            .unwrap();
        assert_eq!(written, expected);
        for chunk in [1, 3, 16, 64] {
            let mut reader = Fragmented::new(&expected, chunk);
            let mut buffer = [0xa5; 8];
            let pointer = buffer.as_ptr();
            let payload = read_frame_into(&mut reader, kind, 1024, &mut buffer)
                .await
                .unwrap();
            assert_eq!(payload.as_ptr(), pointer);
            assert_eq!(payload, [0xac, 0x02]);
            assert_eq!(decode_payload::<u16>(payload).unwrap(), 300);
            assert_eq!(&buffer[2..], &[0xa5; 6]);
            assert_eq!(reader.consumed, expected.len());
            require_end(&mut reader).await.unwrap();
            let mut reader = Fragmented::new(&expected, chunk);
            assert_eq!(
                read_frame::<_, u16>(&mut reader, kind, 1024).await.unwrap(),
                300
            );
        }
    }
}

#[tokio::test]
async fn checked_header_allows_capacity_refusal_and_retry_without_consuming_body() {
    let bytes = frame(FrameKind::Request, &[3, 4, 5]);
    let mut reader = Fragmented::new(&bytes, 1);
    let checked = read_frame_header(&mut reader, FrameKind::Request, 3)
        .await
        .unwrap();
    assert_eq!(checked.kind(), FrameKind::Request);
    assert_eq!(checked.payload_bytes(), 3);
    assert_eq!(reader.consumed, HEADER_BYTES);
    let mut short = [0xa5; 2];
    assert!(matches!(
        read_frame_payload_into(&mut reader, checked, &mut short).await,
        Err(WireError::Limit),
    ));
    assert_eq!(short, [0xa5; 2]);
    assert_eq!(reader.consumed, HEADER_BYTES);
    let mut enough = [0xa5; 3];
    assert_eq!(
        read_frame_payload_into(&mut reader, checked, &mut enough)
            .await
            .unwrap(),
        &[3, 4, 5]
    );
    require_end(&mut reader).await.unwrap();

    let mut reader = Fragmented::new(&bytes, 1);
    assert!(matches!(
        read_frame_into(&mut reader, FrameKind::Request, 3, &mut short).await,
        Err(WireError::Limit)
    ));
    assert_eq!(short, [0xa5; 2]);
    assert_eq!(reader.consumed, HEADER_BYTES);
}

#[tokio::test]
async fn every_header_and_payload_truncation_returns_io_failure() {
    let bytes = frame(FrameKind::Request, &[3, 4, 5]);
    for end in 0..bytes.len() {
        let mut reader = Fragmented::new(&bytes[..end], 1);
        let mut buffer = [0xa5; 8];
        eof(read_frame_into(&mut reader, FrameKind::Request, 3, &mut buffer).await);
        assert_eq!(reader.consumed, end);
        if end < HEADER_BYTES {
            assert_eq!(buffer, [0xa5; 8]);
        }
        assert_eq!(&buffer[3..], &[0xa5; 5]);
    }
}

#[tokio::test]
async fn corrupt_header_and_both_length_caps_refuse_before_body_reads() {
    for (offset, changed) in [(0, b'X'), (8, 1), (9, 2), (10, 1), (11, 0xff)] {
        let mut bytes = frame(FrameKind::Request, &[1]);
        bytes[offset] = changed;
        let mut reader = Fragmented::new(&bytes, 64);
        let mut buffer = [0xa5; 1];
        assert!(matches!(
            read_frame_into(&mut reader, FrameKind::Request, 1024, &mut buffer).await,
            Err(WireError::InvalidFrame)
        ));
        assert_eq!(reader.consumed, HEADER_BYTES);
        assert_eq!(buffer, [0xa5]);
    }
    for (length, limit) in [
        (4, 3),
        (MAX_FRAME_BYTES as u32 + 1, u32::MAX),
        (u32::MAX, u32::MAX),
    ] {
        let bytes = header(FrameKind::Request, length);
        let mut reader = Fragmented::new(&bytes, 1);
        let mut buffer = [0xa5; 8];
        assert!(matches!(
            read_frame_into(&mut reader, FrameKind::Request, limit, &mut buffer).await,
            Err(WireError::Limit)
        ));
        assert_eq!(reader.consumed, HEADER_BYTES);
        assert_eq!(buffer, [0xa5; 8]);
    }
}

#[tokio::test]
async fn zero_length_raw_frame_is_valid_and_typed_trailing_payload_remains_invalid() {
    let bytes = frame(FrameKind::Request, &[]);
    let mut reader = Fragmented::new(&bytes, 1);
    assert!(
        read_frame_into(&mut reader, FrameKind::Request, 0, &mut [])
            .await
            .unwrap()
            .is_empty()
    );
    let mut reader = Fragmented::new(&bytes, 1);
    read_frame::<_, ()>(&mut reader, FrameKind::Request, 0)
        .await
        .unwrap();
    assert_eq!(encode_payload(&(), 0).unwrap(), Vec::<u8>::new());
    assert!(matches!(payload_buffer(usize::MAX), Err(WireError::Limit)));

    // No checksum exists in the unchanged V1 header. Raw framing faithfully
    // supplies bytes; malformed postcard and an extra encoded value still fail
    // typed decoding instead of being mistaken for a complete request.
    for payload in [&[2][..], &[1, 0][..]] {
        let bytes = frame(FrameKind::Request, payload);
        let mut reader = Fragmented::new(&bytes, 1);
        let mut buffer = [0xa5; 8];
        let raw = read_frame_into(&mut reader, FrameKind::Request, 1024, &mut buffer)
            .await
            .unwrap();
        assert_eq!(raw, payload);
        assert!(matches!(
            decode_payload::<bool>(raw),
            Err(WireError::InvalidFrame)
        ));
        let mut reader = Fragmented::new(&bytes, 1);
        assert!(matches!(
            read_frame::<_, bool>(&mut reader, FrameKind::Request, 1024).await,
            Err(WireError::InvalidFrame)
        ));
    }
}

#[tokio::test]
async fn raw_read_stops_at_declared_frame_and_stream_end_is_checked_separately() {
    let first = frame(FrameKind::Request, &[1]);
    let mut bytes = first.clone();
    bytes.extend_from_slice(&frame(FrameKind::Response, &[0]));
    let mut reader = Fragmented::new(&bytes, 64);
    let mut buffer = [0xa5; 8];
    assert_eq!(
        read_frame_into(&mut reader, FrameKind::Request, 1024, &mut buffer)
            .await
            .unwrap(),
        &[1]
    );
    assert_eq!(reader.consumed, first.len());
    assert_eq!(
        read_frame_into(&mut reader, FrameKind::Response, 1024, &mut buffer)
            .await
            .unwrap(),
        &[0]
    );
    require_end(&mut reader).await.unwrap();

    let mut reader = Fragmented::new(&bytes, 64);
    read_frame::<_, bool>(&mut reader, FrameKind::Request, 1024)
        .await
        .unwrap();
    assert!(matches!(
        require_end(&mut reader).await,
        Err(WireError::InvalidFrame)
    ));
}
