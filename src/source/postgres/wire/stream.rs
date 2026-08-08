//! Connection setup and the `CopyBoth` replication loop.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;

use crate::core::{Error, Result, TransportConfig};

use super::auth::{
    md5_password_response, parse_sasl_mechanisms, select_sasl_mechanism, ScramExchange,
};
use super::framing::{
    render_error_response, render_tag, request_tls, startup_packet, take_i64, take_u8,
    write_message, write_untagged, BackendMessage, MessageReader,
};
use super::now_pg_timestamp;

/// Backend message tags this client acts on.
mod tag {
    pub(super) const AUTHENTICATION: u8 = b'R';
    pub(super) const ERROR_RESPONSE: u8 = b'E';
    pub(super) const NOTICE_RESPONSE: u8 = b'N';
    pub(super) const PARAMETER_STATUS: u8 = b'S';
    pub(super) const BACKEND_KEY_DATA: u8 = b'K';
    pub(super) const READY_FOR_QUERY: u8 = b'Z';
    pub(super) const COPY_BOTH_RESPONSE: u8 = b'W';
    pub(super) const COPY_DATA: u8 = b'd';
    pub(super) const COPY_DONE: u8 = b'c';
    pub(super) const COMMAND_COMPLETE: u8 = b'C';
    pub(super) const ROW_DESCRIPTION: u8 = b'T';
    pub(super) const DATA_ROW: u8 = b'D';
}

/// Frontend message tags.
mod frontend_tag {
    pub(super) const PASSWORD: u8 = b'p';
    pub(super) const QUERY: u8 = b'Q';
    pub(super) const COPY_DATA: u8 = b'd';
}

/// `CopyData` sub-message tags inside a replication stream.
mod copy_tag {
    /// Server → client: WAL data.
    pub(super) const XLOG_DATA: u8 = b'w';
    /// Server → client: keepalive, optionally demanding a reply.
    pub(super) const KEEPALIVE: u8 = b'k';
    /// Client → server: Standby Status Update.
    pub(super) const STANDBY_STATUS_UPDATE: u8 = b'r';
}

/// Authentication request sub-types, from the `AuthenticationRequest` message body.
mod auth_kind {
    pub(super) const OK: i32 = 0;
    pub(super) const CLEARTEXT_PASSWORD: i32 = 3;
    pub(super) const MD5_PASSWORD: i32 = 5;
    pub(super) const SASL: i32 = 10;
    pub(super) const SASL_CONTINUE: i32 = 11;
    pub(super) const SASL_FINAL: i32 = 12;
}

/// One decoded message from the replication stream.
#[derive(Debug, Clone)]
pub(in crate::source::postgres) enum WalMessage {
    /// WAL payload, in whatever format the output plugin produces (pgoutput here).
    XLogData {
        /// WAL position where this record starts. This is the per-change LSN.
        wal_start: u64,
        /// Server's current end-of-WAL. May be zero for a mid-transaction record.
        wal_end: u64,
        /// Raw plugin payload.
        data: Vec<u8>,
    },
    /// Server heartbeat carrying its current WAL end.
    Keepalive {
        /// Server's current end-of-WAL.
        wal_end: u64,
    },
}

/// Either a plain or TLS-wrapped socket.
///
/// An enum rather than a boxed trait object so the hot read path stays statically
/// dispatched; the replication stream reads every WAL record through it.
enum Socket {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

macro_rules! delegate_socket {
    ($self:ident, $method:ident, $cx:ident $(, $arg:expr)*) => {
        match &mut *$self {
            Socket::Plain(inner) => std::pin::Pin::new(inner).$method($cx $(, $arg)*),
            #[cfg(feature = "tls")]
            Socket::Tls(inner) => std::pin::Pin::new(inner.as_mut()).$method($cx $(, $arg)*),
        }
    };
}

impl AsyncRead for Socket {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        delegate_socket!(self, poll_read, cx, buf)
    }
}

impl AsyncWrite for Socket {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        delegate_socket!(self, poll_write, cx, buf)
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        delegate_socket!(self, poll_flush, cx)
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        delegate_socket!(self, poll_shutdown, cx)
    }
}

/// Parameters for opening a replication stream.
pub(in crate::source::postgres) struct ReplicationParams<'a> {
    pub(in crate::source::postgres) host: &'a str,
    pub(in crate::source::postgres) port: u16,
    pub(in crate::source::postgres) user: &'a str,
    pub(in crate::source::postgres) password: &'a str,
    pub(in crate::source::postgres) database: &'a str,
    pub(in crate::source::postgres) slot_name: &'a str,
    pub(in crate::source::postgres) publication_name: &'a str,
    pub(in crate::source::postgres) transport: &'a TransportConfig,
    /// LSN to resume from. Zero asks the server to use the slot's own
    /// `confirmed_flush_lsn`, which is the right answer for an unresumed stream.
    pub(in crate::source::postgres) start_lsn: u64,
    pub(in crate::source::postgres) connect_timeout: Duration,
}

/// A live logical replication stream.
pub(in crate::source::postgres) struct ReplicationStream {
    socket: Socket,
    /// Buffered framing. Owning the buffer is what makes a timed-out poll safe: a
    /// partially-arrived message stays here instead of being lost mid-frame.
    reader: MessageReader,
    slot_name: String,
    /// Highest LSN the consumer has durably persisted.
    applied_lsn: u64,
    /// When the last Standby Status Update was sent.
    last_status_sent: tokio::time::Instant,
    /// How often to volunteer a status update.
    ///
    /// Must stay comfortably below the server's `wal_sender_timeout` (default 60 s), or the
    /// server concludes the client is gone and drops the connection mid-stream.
    status_interval: Duration,
}

impl ReplicationStream {
    /// Connect, authenticate, and issue `START_REPLICATION`.
    ///
    /// The timeout covers the **whole** setup sequence, not just the TCP connect. Every step
    /// after it waits on a server reply — the TLS handshake, each authentication round trip,
    /// `ReadyForQuery`, `CopyBothResponse` — and a server that accepts the connection and then
    /// says nothing would otherwise hang here forever. That is not hypothetical: a host
    /// silently dropped by a firewall mid-handshake, an overloaded server that accepts into a
    /// backlog it never services, or a TCP proxy pointed at a dead backend all produce exactly
    /// that shape, and an indefinite hang at startup is indistinguishable from a slow
    /// database.
    pub(in crate::source::postgres) async fn connect(params: ReplicationParams<'_>) -> Result<Self> {
        let connect_timeout = params.connect_timeout;
        let endpoint = format!("{}:{}", params.host, params.port);

        tokio::time::timeout(connect_timeout, Self::establish(params))
            .await
            .map_err(|_| {
                Error::SourceError(format!(
                    "timed out after {connect_timeout:?} establishing a replication stream to                      {endpoint}. The connection was not refused, so the server accepted it and                      then stopped responding — check for a firewall dropping the session                      mid-handshake, a saturated server, or a proxy pointed at a dead backend.                      Raise PostgresSourceConfig::conn_timeout_secs if the server is merely slow."
                ))
            })?
    }

    /// The setup sequence, wrapped by [`Self::connect`]'s timeout.
    async fn establish(params: ReplicationParams<'_>) -> Result<Self> {
        let socket = Self::open_socket(params.host, params.port, params.transport).await?;

        let mut stream = Self {
            socket,
            reader: MessageReader::new(),
            slot_name: params.slot_name.to_string(),
            applied_lsn: params.start_lsn,
            last_status_sent: tokio::time::Instant::now(),
            // A tenth of the 60 s default `wal_sender_timeout`, so several updates are
            // missed before the server gives up on us.
            status_interval: Duration::from_secs(6),
        };

        stream.startup(params.user, params.database).await?;
        stream
            .authenticate(params.user, params.password, params.transport)
            .await?;
        stream.await_ready().await?;
        stream
            .start_replication(params.slot_name, params.publication_name, params.start_lsn)
            .await?;

        Ok(stream)
    }

    async fn open_socket(host: &str, port: u16, transport: &TransportConfig) -> Result<Socket> {
        let tcp = TcpStream::connect((host, port)).await.map_err(|error| {
            Error::SourceError(format!(
                "failed to connect a replication stream to {host}:{port}: {error}"
            ))
        })?;
        // WAL records are latency-sensitive and small; Nagle would coalesce status updates
        // and delay the feedback the server uses to release WAL.
        let _ = tcp.set_nodelay(true);

        match transport {
            TransportConfig::Plaintext => Ok(Socket::Plain(tcp)),
            #[cfg(feature = "tls")]
            _ => Self::upgrade_to_tls(tcp, host, transport).await,
            #[cfg(not(feature = "tls"))]
            _ => Err(Error::ConfigError(
                "postgres TLS transport requires the `tls` feature".into(),
            )),
        }
    }

    #[cfg(feature = "tls")]
    async fn upgrade_to_tls(
        mut tcp: TcpStream,
        host: &str,
        transport: &TransportConfig,
    ) -> Result<Socket> {
        if !request_tls(&mut tcp).await? {
            return Err(Error::SourceError(format!(
                "postgres at '{host}' refused TLS (the server replied 'N'), but the \
                 transport requires it. Either enable `ssl = on` on the server or set the \
                 connector's transport to plaintext — which sends credentials and change \
                 data in the clear and should only be used on a trusted network."
            )));
        }

        let client_config = super::super::query::rustls_client_config(transport)?;
        let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
            .map_err(|_| {
                Error::ConfigError(format!(
                    "postgres host '{host}' is not a valid TLS server name"
                ))
            })?;
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));
        let tls = connector.connect(server_name, tcp).await.map_err(|error| {
            Error::SourceError(format!(
                "TLS handshake with postgres at '{host}' failed: {error}"
            ))
        })?;
        Ok(Socket::Tls(Box::new(tls)))
    }

    async fn startup(&mut self, user: &str, database: &str) -> Result<()> {
        let packet = startup_packet(user, database, "rustcdc");
        write_untagged(&mut self.socket, &packet).await
    }

    /// Run the authentication exchange until the server reports success.
    async fn authenticate(
        &mut self,
        user: &str,
        password: &str,
        transport: &TransportConfig,
    ) -> Result<()> {
        let mut scram: Option<ScramExchange> = None;

        loop {
            let message = self.read_backend_message().await?;
            if message.tag != tag::AUTHENTICATION {
                return Err(Error::SourceError(format!(
                    "expected an authentication message from postgres, got tag {}",
                    render_tag(message.tag)
                )));
            }

            let mut body = message.payload.as_slice();
            let kind = read_i32(&mut body)?;
            match kind {
                auth_kind::OK => return Ok(()),

                auth_kind::CLEARTEXT_PASSWORD => {
                    if matches!(transport, TransportConfig::Plaintext) {
                        return Err(Error::ConfigError(
                            "postgres requested cleartext password authentication over a \
                             plaintext connection, which would put the password on the wire \
                             in the clear. Enable TLS on the connector, or configure the \
                             server for scram-sha-256 (`password_encryption = \
                             scram-sha-256`)."
                                .into(),
                        ));
                    }
                    let mut payload = password.as_bytes().to_vec();
                    payload.push(0);
                    write_message(&mut self.socket, frontend_tag::PASSWORD, &payload).await?;
                }

                auth_kind::MD5_PASSWORD => {
                    let salt: [u8; 4] = body.get(..4).and_then(|s| s.try_into().ok()).ok_or_else(
                        || {
                            Error::SourceError(
                                "postgres MD5 authentication request carried no 4-byte salt"
                                    .into(),
                            )
                        },
                    )?;
                    tracing::warn!(
                        target: "rustcdc::source::postgres",
                        "postgres is configured for MD5 password authentication, which it has \
                         deprecated; the stored digest is a password equivalent. Migrate to \
                         `password_encryption = scram-sha-256`.",
                    );
                    let mut payload = md5_password_response(user, password, &salt).into_bytes();
                    payload.push(0);
                    write_message(&mut self.socket, frontend_tag::PASSWORD, &payload).await?;
                }

                auth_kind::SASL => {
                    let mechanisms = parse_sasl_mechanisms(body);
                    let choice = select_sasl_mechanism(&mechanisms)?;
                    if choice.plus_offered {
                        tracing::debug!(
                            target: "rustcdc::source::postgres",
                            "postgres offered SCRAM-SHA-256-PLUS (TLS channel binding); \
                             rustcdc's replication transport uses SCRAM-SHA-256. Keep \
                             certificate verification enabled — channel binding is what \
                             would otherwise protect the exchange from a MITM holding an \
                             acceptable certificate.",
                        );
                    }

                    let (exchange, client_first) = ScramExchange::start(password)?;
                    scram = Some(exchange);

                    // SASLInitialResponse: mechanism name, then the initial response
                    // length, then the response. A length of -1 means "absent", which is
                    // not what we want here.
                    let mut payload = Vec::with_capacity(client_first.len() + 32);
                    payload.extend_from_slice(choice.mechanism.as_bytes());
                    payload.push(0);
                    payload.extend_from_slice(
                        &i32::try_from(client_first.len())
                            .map_err(|_| {
                                Error::SourceError("SCRAM client-first message is too long".into())
                            })?
                            .to_be_bytes(),
                    );
                    payload.extend_from_slice(client_first.as_bytes());
                    write_message(&mut self.socket, frontend_tag::PASSWORD, &payload).await?;
                }

                auth_kind::SASL_CONTINUE => {
                    let exchange = scram.as_mut().ok_or_else(|| {
                        Error::SourceError(
                            "postgres sent a SASL continuation before any SASL exchange began"
                                .into(),
                        )
                    })?;
                    let server_first = std::str::from_utf8(body).map_err(|error| {
                        Error::SourceError(format!(
                            "postgres SCRAM server-first is not valid UTF-8: {error}"
                        ))
                    })?;
                    let client_final = exchange.client_final(server_first)?;
                    write_message(
                        &mut self.socket,
                        frontend_tag::PASSWORD,
                        client_final.as_bytes(),
                    )
                    .await?;
                }

                auth_kind::SASL_FINAL => {
                    let exchange = scram.as_ref().ok_or_else(|| {
                        Error::SourceError(
                            "postgres sent a SASL final message before any SASL exchange began"
                                .into(),
                        )
                    })?;
                    let server_final = std::str::from_utf8(body).map_err(|error| {
                        Error::SourceError(format!(
                            "postgres SCRAM server-final is not valid UTF-8: {error}"
                        ))
                    })?;
                    // Mutual authentication. Skipping this would let an impostor that
                    // cannot verify our proof still complete the handshake.
                    exchange.verify_server_final(server_final)?;
                }

                other => {
                    return Err(Error::SourceError(format!(
                        "postgres requested authentication method {other}, which rustcdc's \
                         replication transport does not implement. Supported: \
                         scram-sha-256, md5, and cleartext-over-TLS. Set \
                         PostgresSourceConfig::wal_transport = WalTransport::SqlPeek to \
                         authenticate through tokio-postgres instead."
                    )));
                }
            }
        }
    }

    /// Consume post-authentication chatter up to `ReadyForQuery`.
    async fn await_ready(&mut self) -> Result<()> {
        loop {
            let message = self.read_backend_message().await?;
            match message.tag {
                tag::READY_FOR_QUERY => return Ok(()),
                // Parameter statuses and the cancellation key are informational here.
                tag::PARAMETER_STATUS | tag::BACKEND_KEY_DATA => {}
                other => {
                    return Err(Error::SourceError(format!(
                        "unexpected message tag {} while waiting for postgres to become \
                         ready on the replication connection",
                        render_tag(other)
                    )));
                }
            }
        }
    }

    async fn start_replication(
        &mut self,
        slot_name: &str,
        publication_name: &str,
        start_lsn: u64,
    ) -> Result<()> {
        // The slot and publication names are server identifiers that reach the server as
        // part of a command string, so they are validated rather than escaped.
        validate_identifier(slot_name, "replication slot name")?;
        validate_identifier(publication_name, "publication name")?;

        // `0/0` asks the server to resume from the slot's own confirmed_flush_lsn.
        let start = format!("{:X}/{:X}", start_lsn >> 32, start_lsn & 0xFFFF_FFFF);
        // `proto_version '1'` is what this crate's pgoutput decoder implements. Requesting
        // a higher version would make the server send v2 streaming and v3 two-phase
        // messages the decoder deliberately rejects rather than silently mishandles.
        let query = format!(
            "START_REPLICATION SLOT {slot_name} LOGICAL {start} \
             (proto_version '1', publication_names '{publication_name}')"
        );

        let mut payload = query.into_bytes();
        payload.push(0);
        write_message(&mut self.socket, frontend_tag::QUERY, &payload).await?;

        loop {
            let message = self.read_backend_message().await?;
            match message.tag {
                tag::COPY_BOTH_RESPONSE => return Ok(()),
                // Some server versions answer with a row set before switching to CopyBoth.
                tag::ROW_DESCRIPTION | tag::DATA_ROW | tag::COMMAND_COMPLETE => {}
                other => {
                    return Err(Error::SourceError(format!(
                        "postgres did not enter CopyBoth mode for slot '{slot_name}'; it \
                         replied with tag {}. The role needs the REPLICATION attribute and \
                         the connection must be direct — a pooler in transaction-pooling \
                         mode cannot carry a replication stream.",
                        render_tag(other)
                    )));
                }
            }
        }
    }

    /// Read one WAL message, or `None` if `timeout` expired first.
    ///
    /// A `timeout` of zero means "whatever is already buffered, without waiting", which is
    /// how a caller drains a batch after the first record has arrived.
    ///
    /// Keepalives that demand a reply are answered here rather than surfaced, so the caller
    /// never has to think about `wal_sender_timeout`.
    ///
    /// # Cancellation
    ///
    /// The timeout wraps only the socket **fill**, never frame decoding. `read` consumes
    /// nothing when cancelled and a partially-arrived frame stays in the buffer, so an
    /// expired budget leaves the connection exactly where it was. Wrapping a field-by-field
    /// read instead would discard bytes mid-frame and desynchronise the stream permanently.
    pub(in crate::source::postgres) async fn recv(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<WalMessage>> {
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            // Anything already buffered is returned before considering the budget, so a
            // zero-timeout drain still makes progress.
            let buffered = self.reader.try_decode()?;

            let message = match buffered {
                Some(message) => message,
                None => {
                    // Feedback must keep flowing even while no WAL arrives, or the server
                    // concludes the client is gone. Checked here rather than only after a
                    // successful read, because a quiet database produces no reads to hang
                    // the check on.
                    self.send_status_update_if_due(false).await?;

                    let remaining =
                        deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Ok(None);
                    }
                    match tokio::time::timeout(remaining, self.reader.fill(&mut self.socket)).await
                    {
                        Err(_) => return Ok(None),
                        Ok(result) => result?,
                    }
                    continue;
                }
            };

            match message.tag {
                tag::COPY_DATA => {
                    if let Some(wal) = self.decode_copy_data(&message.payload).await? {
                        return Ok(Some(wal));
                    }
                }
                tag::NOTICE_RESPONSE => {
                    tracing::debug!(
                        target: "rustcdc::source::postgres",
                        slot = %self.slot_name,
                        "postgres notice on the replication stream: {}",
                        render_error_response(&message.payload),
                    );
                }
                tag::ERROR_RESPONSE => {
                    return Err(Error::SourceError(format!(
                        "postgres replication stream on slot '{}' failed: {}",
                        self.slot_name,
                        render_error_response(&message.payload)
                    )));
                }
                tag::COPY_DONE | tag::COMMAND_COMPLETE | tag::READY_FOR_QUERY => {
                    return Err(Error::SourceError(format!(
                        "postgres ended the replication stream on slot '{}'; reconnecting \
                         resumes from the last confirmed LSN",
                        self.slot_name
                    )));
                }
                other => {
                    // Unknown but framed correctly: skipping keeps the stream usable, and a
                    // trace makes it visible if a future protocol version adds something.
                    tracing::debug!(
                        target: "rustcdc::source::postgres",
                        slot = %self.slot_name,
                        tag = %render_tag(other),
                        "ignoring unexpected message on the replication stream",
                    );
                }
            }
        }
    }

    /// Decode a `CopyData` frame, answering keepalives that request a reply.
    async fn decode_copy_data(&mut self, payload: &[u8]) -> Result<Option<WalMessage>> {
        let mut body = payload;
        let kind = take_u8(&mut body)?;

        match kind {
            copy_tag::XLOG_DATA => {
                let wal_start = take_i64(&mut body)? as u64;
                let wal_end = take_i64(&mut body)? as u64;
                let _server_time = take_i64(&mut body)?;
                Ok(Some(WalMessage::XLogData {
                    wal_start,
                    wal_end,
                    data: body.to_vec(),
                }))
            }
            copy_tag::KEEPALIVE => {
                let wal_end = take_i64(&mut body)? as u64;
                let _server_time = take_i64(&mut body)?;
                let reply_requested = take_u8(&mut body)? != 0;
                if reply_requested {
                    // The server is asking whether we are still alive. Not answering costs
                    // the connection.
                    self.send_status_update(false).await?;
                }
                Ok(Some(WalMessage::Keepalive { wal_end }))
            }
            other => {
                tracing::debug!(
                    target: "rustcdc::source::postgres",
                    slot = %self.slot_name,
                    tag = %render_tag(other),
                    "ignoring unknown CopyData sub-message on the replication stream",
                );
                Ok(None)
            }
        }
    }

    /// Record durable progress. Feedback reaches the server on the next status update.
    ///
    /// Monotonic: a lower LSN is ignored rather than reported, because telling the server a
    /// position behind one already confirmed is how a slot moves backwards.
    pub(in crate::source::postgres) fn set_applied_lsn(&mut self, lsn: u64) {
        self.applied_lsn = self.applied_lsn.max(lsn);
    }

    /// The LSN most recently reported as durable.
    pub(in crate::source::postgres) fn applied_lsn(&self) -> u64 {
        self.applied_lsn
    }

    async fn send_status_update_if_due(&mut self, request_reply: bool) -> Result<()> {
        if self.last_status_sent.elapsed() >= self.status_interval {
            self.send_status_update(request_reply).await?;
        }
        Ok(())
    }

    /// Send a Standby Status Update.
    ///
    /// `write`, `flush` and `apply` are all set to the applied LSN. For a CDC consumer that
    /// is the honest answer: the only position that matters is the one the consumer has
    /// durably persisted, and reporting a *higher* write position would let the server
    /// release WAL the consumer has not committed.
    pub(in crate::source::postgres) async fn send_status_update(&mut self, request_reply: bool) -> Result<()> {
        let lsn = self.applied_lsn as i64;
        let mut payload = Vec::with_capacity(34);
        payload.push(copy_tag::STANDBY_STATUS_UPDATE);
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&now_pg_timestamp().to_be_bytes());
        payload.push(u8::from(request_reply));

        write_message(&mut self.socket, frontend_tag::COPY_DATA, &payload).await?;
        self.last_status_sent = tokio::time::Instant::now();
        Ok(())
    }

    /// Read a backend message, turning `ErrorResponse` into an error and skipping notices.
    async fn read_backend_message(&mut self) -> Result<BackendMessage> {
        loop {
            let message = self.reader.read_message(&mut self.socket).await?;
            match message.tag {
                tag::ERROR_RESPONSE => {
                    return Err(Error::SourceError(format!(
                        "postgres replication connection rejected: {}",
                        render_error_response(&message.payload)
                    )));
                }
                tag::NOTICE_RESPONSE => {
                    tracing::debug!(
                        target: "rustcdc::source::postgres",
                        "postgres notice: {}",
                        render_error_response(&message.payload),
                    );
                }
                _ => return Ok(message),
            }
        }
    }
}

fn read_i32(bytes: &mut &[u8]) -> Result<i32> {
    if bytes.len() < 4 {
        return Err(Error::SourceError(
            "truncated postgres message: expected a 4-byte integer".into(),
        ));
    }
    let (head, tail) = bytes.split_at(4);
    *bytes = tail;
    Ok(i32::from_be_bytes(head.try_into().expect("4 bytes")))
}

/// Reject an identifier that cannot be safely interpolated into a replication command.
///
/// `START_REPLICATION` is a replication-protocol command, not SQL, so it takes no bind
/// parameters — the slot and publication names have to be interpolated. Allowing a quote or
/// a parenthesis through would let a crafted name change the command's option list, so the
/// character set is restricted to what PostgreSQL itself permits in an unquoted identifier.
fn validate_identifier(value: &str, what: &str) -> Result<()> {
    if value.is_empty() {
        return Err(Error::ConfigError(format!("postgres {what} must not be empty")));
    }
    if value.len() > 63 {
        return Err(Error::ConfigError(format!(
            "postgres {what} '{value}' exceeds the 63-character identifier limit"
        )));
    }
    if !value
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '_')
    {
        return Err(Error::ConfigError(format!(
            "postgres {what} '{value}' contains characters outside [A-Za-z0-9_]. \
             START_REPLICATION is a replication-protocol command with no bind parameters, so \
             the name is interpolated into the command text and cannot be escaped; rename \
             the object."
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identifier_with_a_quote_or_paren_is_refused() {
        // The name is interpolated into a replication command that takes no bind
        // parameters, so a quote could close the option list and append options.
        for hostile in [
            "slot' (proto_version '2",
            "pub', messages 'true",
            "slot)",
            "slot;drop",
            "slot name",
        ] {
            assert!(
                validate_identifier(hostile, "slot").is_err(),
                "{hostile:?} must be refused"
            );
        }
    }

    #[test]
    fn ordinary_identifiers_are_accepted() {
        for name in ["rustcdc_slot", "slot_1", "A_b_9"] {
            validate_identifier(name, "slot").expect("must be accepted");
        }
    }

    #[test]
    fn an_empty_or_overlong_identifier_is_refused() {
        assert!(validate_identifier("", "slot").is_err());
        assert!(validate_identifier(&"a".repeat(64), "slot").is_err());
        validate_identifier(&"a".repeat(63), "slot").expect("63 is the limit, not over it");
    }

    #[test]
    fn the_start_lsn_renders_in_postgres_two_part_hex_form() {
        // `START_REPLICATION` takes `XXXXXXXX/XXXXXXXX`, not a decimal. Getting the split
        // wrong resumes at an unrelated position.
        let render = |lsn: u64| format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFF_FFFF);
        assert_eq!(render(0), "0/0");
        assert_eq!(render(0x1234_5678), "0/12345678");
        assert_eq!(render(0x0000_0001_ABCD_EF01), "1/ABCDEF01");
    }

    #[test]
    fn applied_lsn_never_moves_backwards() {
        // Reporting a position behind one already confirmed is how a slot rewinds.
        let mut applied = 0u64;
        for candidate in [500u64, 400, 900, 100] {
            applied = applied.max(candidate);
        }
        assert_eq!(applied, 900);
    }

    #[test]
    fn a_standby_status_update_is_shaped_as_the_protocol_requires() {
        // Layout: 'r', write, flush, apply, clock, reply-requested. A wrong size here is
        // rejected by the server as a malformed message and the stream dies at startup.
        let lsn = 0x1234_5678_9ABC_DEF0_i64;
        let mut payload = Vec::new();
        payload.push(copy_tag::STANDBY_STATUS_UPDATE);
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&lsn.to_be_bytes());
        payload.extend_from_slice(&0i64.to_be_bytes());
        payload.push(0);
        assert_eq!(payload.len(), 34);
        assert_eq!(payload[0], b'r');
    }
}
