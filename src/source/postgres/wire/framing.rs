//! PostgreSQL v3 frontend/backend message framing.
//!
//! Only what a replication connection needs: the startup exchange, authentication
//! request/response messages, a simple `Query`, and the `CopyBoth` data frames. Ordinary
//! query execution stays with `tokio-postgres`.
//!
//! # Wire conventions that are easy to get wrong
//!
//! * Every message except the startup and SSL-request packets carries a **one-byte type
//!   tag** before its length.
//! * The length field **includes itself** but excludes the type tag. So a message with an
//!   `n`-byte payload declares `n + 4`, and the reader must subtract 4 before reading the
//!   payload. Off-by-four here desynchronises the stream permanently rather than failing
//!   at the point of the mistake, which is why the length arithmetic lives in exactly one
//!   place.
//! * All integers are big-endian.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::core::{Error, Result};

/// Protocol version 3.0, as the startup packet encodes it: major 3, minor 0.
pub(super) const PROTOCOL_VERSION_3_0: i32 = 196_608;

/// Magic "version" that asks the server to begin a TLS handshake.
pub(super) const SSL_REQUEST_CODE: i32 = 80_877_103;

/// Upper bound on a single backend message, as a guard against a desynchronised stream.
///
/// PostgreSQL's own limit on a message is 1 GB. A frame larger than this means the reader
/// has lost sync and is interpreting payload bytes as a length; allocating on that is how
/// a protocol bug becomes an out-of-memory abort. `wal_sender` never emits anything close.
const MAX_MESSAGE_LEN: usize = 512 * 1024 * 1024;

/// A backend message, tag plus payload.
#[derive(Debug, Clone)]
pub(super) struct BackendMessage {
    /// The one-byte message type tag.
    pub(super) tag: u8,
    /// Payload, with the length prefix already stripped.
    pub(super) payload: Vec<u8>,
}

/// An incremental, cancel-safe reader for tagged backend messages.
///
/// # Why this is buffered rather than reading fields straight off the socket
///
/// A replication poll has a time budget, so the read has to be cancellable. Reading a
/// message field by field directly from the socket is **not** cancel-safe: a timeout that
/// fires between the tag and the payload discards bytes that have already left the kernel,
/// and the next read then interprets payload bytes as a tag and length. That desynchronises
/// the connection permanently, and it does so silently — the failure surfaces later as a
/// nonsensical message length or a decode error with no relation to the real cause.
///
/// Buffering separates the two concerns. Filling the buffer is cancel-safe because
/// `AsyncReadExt::read` consumes nothing when cancelled, and decoding only ever consumes a
/// **complete** frame, so a partially-arrived message simply stays in the buffer until the
/// next attempt.
pub(super) struct MessageReader {
    buffer: Vec<u8>,
}

impl MessageReader {
    /// Buffer capacity. Sized so a batch of WAL records is typically assembled from one
    /// syscall rather than one per record.
    const READ_CHUNK: usize = 64 * 1024;

    pub(super) fn new() -> Self {
        Self {
            buffer: Vec::with_capacity(Self::READ_CHUNK),
        }
    }

    /// Decode a complete message if one is buffered, without touching the socket.
    ///
    /// Returns `Ok(None)` when more bytes are needed — never a partial consume.
    pub(super) fn try_decode(&mut self) -> Result<Option<BackendMessage>> {
        // Tag plus a four-byte length is the minimum framing.
        if self.buffer.len() < 5 {
            return Ok(None);
        }

        let tag = self.buffer[0];
        let declared = i32::from_be_bytes(
            self.buffer[1..5]
                .try_into()
                .expect("slice of exactly four bytes"),
        );

        // The length includes its own four bytes. Anything below that is malformed, and
        // trusting it would underflow the payload size into an enormous allocation.
        let payload_len = usize::try_from(declared)
            .ok()
            .and_then(|len| len.checked_sub(4))
            .ok_or_else(|| {
                Error::SourceError(format!(
                    "postgres message with tag {} declared an impossible length {declared}",
                    render_tag(tag)
                ))
            })?;

        if payload_len > MAX_MESSAGE_LEN {
            return Err(Error::SourceError(format!(
                "postgres message with tag {} declared {payload_len} bytes, beyond the \
                 {MAX_MESSAGE_LEN}-byte sanity limit; the connection is desynchronised",
                render_tag(tag)
            )));
        }

        let frame_len = payload_len + 5;
        if self.buffer.len() < frame_len {
            return Ok(None);
        }

        let payload = self.buffer[5..frame_len].to_vec();
        self.buffer.drain(..frame_len);
        Ok(Some(BackendMessage { tag, payload }))
    }

    /// Read more bytes from the socket into the buffer.
    ///
    /// Cancel-safe: `read` consumes nothing if the caller drops this future, so a timeout
    /// leaves the buffer exactly as it was.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SourceError`] on I/O failure, or when the peer closed the
    /// connection with a partial frame buffered.
    pub(super) async fn fill<S>(&mut self, stream: &mut S) -> Result<()>
    where
        S: AsyncRead + Unpin,
    {
        let mut chunk = [0u8; Self::READ_CHUNK];
        let read = stream.read(&mut chunk).await.map_err(io_error)?;
        if read == 0 {
            return Err(Error::SourceError(format!(
                "postgres closed the replication connection{}",
                if self.buffer.is_empty() {
                    String::new()
                } else {
                    format!(
                        " with {} bytes of an incomplete message buffered",
                        self.buffer.len()
                    )
                }
            )));
        }
        self.buffer.extend_from_slice(&chunk[..read]);
        Ok(())
    }

    /// Read one complete message, filling the buffer as needed.
    ///
    /// **Not** cancel-safe as a whole — cancelling loses nothing from the socket, but the
    /// caller must not rely on the message being re-readable. Use it only where a partial
    /// wait cannot happen (connection setup); on the metered poll path, drive
    /// [`Self::try_decode`] and [`Self::fill`] separately so the timeout wraps only the fill.
    pub(super) async fn read_message<S>(&mut self, stream: &mut S) -> Result<BackendMessage>
    where
        S: AsyncRead + Unpin,
    {
        loop {
            if let Some(message) = self.try_decode()? {
                return Ok(message);
            }
            self.fill(stream).await?;
        }
    }
}

/// Write a tagged frontend message, computing the length prefix.
pub(super) async fn write_message<S>(stream: &mut S, tag: u8, payload: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let len = i32::try_from(payload.len() + 4).map_err(|_| {
        Error::SourceError(format!(
            "postgres frontend message with tag {} is too large to frame",
            render_tag(tag)
        ))
    })?;

    let mut framed = Vec::with_capacity(payload.len() + 5);
    framed.push(tag);
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(payload);
    stream.write_all(&framed).await.map_err(io_error)?;
    stream.flush().await.map_err(io_error)
}

/// Write an untagged packet — only the startup and SSL-request messages are shaped this way.
pub(super) async fn write_untagged<S>(stream: &mut S, body: &[u8]) -> Result<()>
where
    S: AsyncWrite + Unpin,
{
    let len = i32::try_from(body.len() + 4)
        .map_err(|_| Error::SourceError("postgres startup packet is too large".into()))?;
    let mut framed = Vec::with_capacity(body.len() + 4);
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(body);
    stream.write_all(&framed).await.map_err(io_error)?;
    stream.flush().await.map_err(io_error)
}

/// Ask the server to upgrade the connection to TLS.
///
/// Returns `true` when the server agreed (`'S'`), `false` when it declined (`'N'`).
pub(super) async fn request_tls<S>(stream: &mut S) -> Result<bool>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    write_untagged(stream, &SSL_REQUEST_CODE.to_be_bytes()).await?;
    // The reply to an SSLRequest is a bare byte, not a framed message.
    match stream.read_u8().await.map_err(io_error)? {
        b'S' => Ok(true),
        b'N' => Ok(false),
        // 'E' means the server rejected the packet outright — usually a pre-8.0 server or
        // something that is not PostgreSQL at all.
        other => Err(Error::SourceError(format!(
            "postgres refused the TLS negotiation with an unexpected reply {}; the endpoint \
             may not be a PostgreSQL server",
            render_tag(other)
        ))),
    }
}

/// Build the startup packet for a **replication** connection.
///
/// `replication=database` is what makes the connection a logical replication one: it puts
/// the backend into a mode where `START_REPLICATION ... LOGICAL` is accepted and ordinary
/// SQL is mostly not. `database` (rather than `true`) is required for *logical*
/// replication, because logical decoding is per-database.
pub(super) fn startup_packet(user: &str, database: &str, application_name: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(128);
    body.extend_from_slice(&PROTOCOL_VERSION_3_0.to_be_bytes());
    for (key, value) in [
        ("user", user),
        ("database", database),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
        ("application_name", application_name),
    ] {
        body.extend_from_slice(key.as_bytes());
        body.push(0);
        body.extend_from_slice(value.as_bytes());
        body.push(0);
    }
    // Terminating empty key.
    body.push(0);
    body
}

/// Render an `ErrorResponse` / `NoticeResponse` payload as a diagnostic string.
///
/// The payload is a sequence of `field-code + NUL-terminated value`, ending with a zero
/// code byte. Severity, SQLSTATE and the primary message are the fields worth surfacing;
/// including the SQLSTATE matters because callers key recoverability off it.
pub(super) fn render_error_response(payload: &[u8]) -> String {
    let mut severity = None;
    let mut code = None;
    let mut message = None;
    let mut detail = None;
    let mut hint = None;

    let mut rest = payload;
    while let Some((&field, tail)) = rest.split_first() {
        if field == 0 {
            break;
        }
        let end = tail
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(tail.len());
        let value = String::from_utf8_lossy(&tail[..end]).into_owned();
        rest = tail.get(end + 1..).unwrap_or(&[]);
        match field {
            b'S' => severity = Some(value),
            b'C' => code = Some(value),
            b'M' => message = Some(value),
            b'D' => detail = Some(value),
            b'H' => hint = Some(value),
            _ => {}
        }
    }

    let mut rendered = String::new();
    if let Some(severity) = severity {
        rendered.push_str(&severity);
        rendered.push_str(": ");
    }
    rendered.push_str(message.as_deref().unwrap_or("unspecified postgres error"));
    if let Some(code) = code {
        rendered.push_str(&format!(" (SQLSTATE {code})"));
    }
    if let Some(detail) = detail {
        rendered.push_str(&format!(" detail: {detail}"));
    }
    if let Some(hint) = hint {
        rendered.push_str(&format!(" hint: {hint}"));
    }
    rendered
}

/// Render a message tag for diagnostics, printable or not.
pub(super) fn render_tag(tag: u8) -> String {
    if tag.is_ascii_graphic() {
        format!("'{}'", tag as char)
    } else {
        format!("{tag:#04x}")
    }
}

fn io_error(error: io::Error) -> Error {
    Error::SourceError(format!(
        "postgres replication connection I/O failed: {error}"
    ))
}

/// Read a big-endian `i64` from the front of `bytes`, advancing it.
pub(super) fn take_i64(bytes: &mut &[u8]) -> Result<i64> {
    if bytes.len() < 8 {
        return Err(Error::SourceError(
            "truncated postgres replication payload: expected an 8-byte integer".into(),
        ));
    }
    let (head, tail) = bytes.split_at(8);
    *bytes = tail;
    Ok(i64::from_be_bytes(head.try_into().expect("8 bytes")))
}

/// Read a single byte from the front of `bytes`, advancing it.
pub(super) fn take_u8(bytes: &mut &[u8]) -> Result<u8> {
    let (&head, tail) = bytes.split_first().ok_or_else(|| {
        Error::SourceError("truncated postgres replication payload: expected a byte".into())
    })?;
    *bytes = tail;
    Ok(head)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_message_round_trips_through_its_own_framing() {
        // The length field includes itself, so a round trip through both halves is the
        // cheapest guard against an off-by-four that would desynchronise the stream.
        let mut buffer = Vec::new();
        write_message(&mut buffer, b'Q', b"START_REPLICATION")
            .await
            .expect("writes");

        let mut cursor = std::io::Cursor::new(buffer);
        let message = MessageReader::new()
            .read_message(&mut cursor)
            .await
            .expect("reads");
        assert_eq!(message.tag, b'Q');
        assert_eq!(message.payload, b"START_REPLICATION");
    }

    #[tokio::test]
    async fn an_empty_payload_round_trips() {
        // CopyDone and Terminate carry no payload; a reader that mishandles length 4
        // stalls forever waiting for bytes that are not coming.
        let mut buffer = Vec::new();
        write_message(&mut buffer, b'c', b"").await.expect("writes");
        let mut cursor = std::io::Cursor::new(buffer);
        let message = MessageReader::new()
            .read_message(&mut cursor)
            .await
            .expect("reads");
        assert_eq!(message.tag, b'c');
        assert!(message.payload.is_empty());
    }

    #[tokio::test]
    async fn a_length_below_the_header_size_is_rejected_rather_than_underflowing() {
        // `len - 4` on a declared length of 0 underflows to a colossal allocation.
        let framed = [b'E', 0, 0, 0, 0];
        let mut cursor = std::io::Cursor::new(framed.to_vec());
        let error = MessageReader::new()
            .read_message(&mut cursor)
            .await
            .expect_err("must reject");
        assert!(error.to_string().contains("impossible length"));
    }

    #[tokio::test]
    async fn an_absurd_length_is_refused_before_allocating() {
        let mut framed = vec![b'd'];
        framed.extend_from_slice(&i32::MAX.to_be_bytes());
        let mut cursor = std::io::Cursor::new(framed);
        let error = MessageReader::new()
            .read_message(&mut cursor)
            .await
            .expect_err("must reject");
        assert!(error.to_string().contains("desynchronised"));
    }

    #[test]
    fn a_partially_arrived_frame_is_never_consumed() {
        // The property the whole buffered design exists for. A poll budget can expire at any
        // byte boundary; if `try_decode` consumed a partial frame the next decode would read
        // payload bytes as a tag and length, and the connection would be silently
        // desynchronised from then on.
        let mut framed = Vec::new();
        framed.push(b'd');
        framed.extend_from_slice(&(4 + 10_i32).to_be_bytes());
        framed.extend_from_slice(b"0123456789");

        // Feed the frame one byte at a time; nothing may decode until the last byte.
        let mut reader = MessageReader::new();
        for (index, byte) in framed.iter().enumerate() {
            reader.buffer.push(*byte);
            let decoded = reader.try_decode().expect("no error on a partial frame");
            if index + 1 < framed.len() {
                assert!(
                    decoded.is_none(),
                    "a frame must not decode until all {} bytes have arrived (had {})",
                    framed.len(),
                    index + 1
                );
            } else {
                let message = decoded.expect("the complete frame decodes");
                assert_eq!(message.tag, b'd');
                assert_eq!(message.payload, b"0123456789");
            }
        }
        assert!(
            reader.buffer.is_empty(),
            "consuming a frame must leave no residue"
        );
    }

    #[test]
    fn back_to_back_frames_in_one_read_all_decode() {
        // A single socket read commonly carries several WAL records. Decoding must drain
        // them without another read, or throughput collapses to one record per syscall.
        let mut reader = MessageReader::new();
        for payload in [b"aaa".as_slice(), b"bb".as_slice(), b"c".as_slice()] {
            reader.buffer.push(b'd');
            reader
                .buffer
                .extend_from_slice(&(4 + payload.len() as i32).to_be_bytes());
            reader.buffer.extend_from_slice(payload);
        }

        let mut decoded = Vec::new();
        while let Some(message) = reader.try_decode().expect("decodes") {
            decoded.push(message.payload);
        }
        assert_eq!(
            decoded,
            vec![b"aaa".to_vec(), b"bb".to_vec(), b"c".to_vec()]
        );
        assert!(reader.buffer.is_empty());
    }

    #[tokio::test]
    async fn a_closed_connection_with_a_partial_frame_buffered_says_so() {
        // Distinguishes an orderly close from a truncated message, which are very different
        // things to see in a log during an incident.
        let mut reader = MessageReader::new();
        reader.buffer.extend_from_slice(&[b'd', 0, 0, 1]);
        let mut empty = std::io::Cursor::new(Vec::new());
        let error = reader.fill(&mut empty).await.expect_err("must error");
        assert!(
            error.to_string().contains("incomplete message"),
            "the error must name the truncation: {error}"
        );
    }

    #[test]
    fn the_startup_packet_declares_a_logical_replication_connection() {
        let packet = startup_packet("cdc", "app", "rustcdc");
        assert_eq!(
            &packet[..4],
            &PROTOCOL_VERSION_3_0.to_be_bytes(),
            "the packet must open with protocol version 3.0"
        );
        let rendered = String::from_utf8_lossy(&packet);
        assert!(
            rendered.contains("replication\0database\0"),
            "logical decoding is per-database, so `replication` must be `database` rather \
             than `true`: {rendered:?}"
        );
        assert!(
            packet.ends_with(&[0]),
            "the parameter list must be terminated"
        );
    }

    /// A duplex stream: reads come from `reply`, writes accumulate for inspection.
    ///
    /// A single `Cursor` cannot stand in for a socket here — `request_tls` writes before it
    /// reads, and a cursor's write would overwrite the very reply the test staged.
    fn server_replying(reply: &[u8]) -> tokio::io::Join<std::io::Cursor<Vec<u8>>, Vec<u8>> {
        tokio::io::join(std::io::Cursor::new(reply.to_vec()), Vec::new())
    }

    #[tokio::test]
    async fn the_tls_request_reads_a_bare_reply_byte_not_a_framed_message() {
        // The reply to an SSLRequest is the one place in the protocol where the server sends
        // a single byte with no length prefix. Reading it as a framed message would consume
        // three bytes of whatever follows, and the failure would surface as something
        // unrelated to the actual mistake.
        let mut agreed = server_replying(b"S");
        assert!(request_tls(&mut agreed).await.expect("reads reply"));

        let mut declined = server_replying(b"N");
        assert!(!request_tls(&mut declined).await.expect("reads reply"));
    }

    #[tokio::test]
    async fn a_non_postgres_reply_to_the_tls_request_is_refused() {
        // 'E' means the server rejected the packet outright; anything else means the endpoint
        // is not speaking the PostgreSQL protocol. Either way, continuing into a TLS
        // handshake produces a misleading error.
        let mut hostile = server_replying(b"E");
        let error = request_tls(&mut hostile).await.expect_err("must refuse");
        assert!(
            error.to_string().contains("may not be a PostgreSQL server"),
            "the error must point at the endpoint rather than at TLS: {error}"
        );
    }

    #[tokio::test]
    async fn the_tls_request_packet_carries_the_documented_magic_code() {
        // The "version" is a magic number, not a protocol version. Sending 3.0 here asks for
        // an ordinary startup instead of a TLS upgrade, and the connection proceeds
        // unencrypted while the caller believes it negotiated TLS.
        let mut stream = server_replying(b"N");
        let _ = request_tls(&mut stream).await.expect("reads reply");

        let (_, written) = stream.into_inner();
        assert_eq!(written.len(), 8, "the SSLRequest packet is exactly 8 bytes");
        assert_eq!(
            &written[..4],
            &8_i32.to_be_bytes(),
            "length includes itself"
        );
        assert_eq!(
            &written[4..],
            &80_877_103_i32.to_be_bytes(),
            "the magic SSLRequest code, not a protocol version"
        );
    }

    #[test]
    fn an_error_response_renders_severity_sqlstate_and_message() {
        let mut payload = Vec::new();
        for (field, value) in [
            (b'S', "FATAL"),
            (b'C', "55006"),
            (b'M', "replication slot \"s\" is active"),
            (b'H', "wait for the other session"),
        ] {
            payload.push(field);
            payload.extend_from_slice(value.as_bytes());
            payload.push(0);
        }
        payload.push(0);

        let rendered = render_error_response(&payload);
        assert!(rendered.contains("FATAL"));
        assert!(rendered.contains("55006"), "SQLSTATE drives recoverability");
        assert!(rendered.contains("is active"));
        assert!(rendered.contains("wait for the other session"));
    }

    #[test]
    fn an_error_response_without_a_message_still_renders() {
        assert!(!render_error_response(&[0]).is_empty());
    }
}
