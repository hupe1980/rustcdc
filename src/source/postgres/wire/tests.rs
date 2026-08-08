//! End-to-end tests for the replication client against an in-process fake server.
//!
//! These cover what neither the byte-level unit tests nor the live-server suites can:
//!
//! * **The TLS path.** `tests/postgres_wal_transport_parity_integration.rs` runs against
//!   containers with `ssl = off`, because provisioning a server certificate with the
//!   ownership PostgreSQL demands is awkward inside a throwaway image. A fake server can
//!   present one in-process, so the SSLRequest exchange, the rustls handshake and the
//!   `Socket::Tls` read/write delegation are exercised together rather than assumed.
//! * **Cancel safety under a split frame.** The failure mode being guarded against is a poll
//!   budget expiring *between* a message's tag and its payload. Provoking that against a
//!   real server means winning a race; here the server simply writes half a message, waits,
//!   and writes the rest.
//! * **Protocol-level failures** — an `ErrorResponse` instead of `CopyBothResponse`, a server
//!   that declines TLS — which a healthy server will not produce on demand.

use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::core::{RustlsClientConfig, TransportConfig};

use super::{ReplicationParams, ReplicationStream, WalMessage};

/// What the fake server observed from the client, for assertions after the fact.
#[derive(Debug, Default)]
struct Observed {
    /// Raw startup packet parameters, as `key=value` strings.
    startup_params: Vec<String>,
    /// The `START_REPLICATION` command text.
    replication_query: String,
    /// Password payload the client sent, if authentication was requested.
    password_sent: Option<String>,
    /// Applied LSNs read out of Standby Status Update messages.
    status_updates: Vec<u64>,
}

/// A self-signed certificate plus a client config that trusts it.
struct TestTls {
    server: Arc<rustls::ServerConfig>,
    client: rustls::ClientConfig,
}

fn test_tls() -> TestTls {
    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .expect("generates a self-signed certificate");
    let cert = rustls::pki_types::CertificateDer::from(issued.cert.der().to_vec());
    let key = rustls::pki_types::PrivateKeyDer::try_from(issued.signing_key.serialize_der())
        .expect("PKCS#8 private key");

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let server = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .expect("server certificate");

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trusts the generated certificate");
    let client = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();

    TestTls {
        server: Arc::new(server),
        client,
    }
}

/// How the fake server should behave once the stream is established.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Behaviour {
    /// Send one XLogData carrying `payload`, then go quiet.
    SendOneRecord,
    /// Send a keepalive that demands a reply, then one XLogData.
    DemandKeepaliveReply,
    /// Write an XLogData in two TCP writes with a pause between them.
    SplitRecordAcrossWrites,
    /// Refuse `START_REPLICATION` with an `ErrorResponse`.
    RefuseReplication,
    /// Decline the TLS upgrade.
    DeclineTls,
}

/// Ask the client to authenticate with a cleartext password, or skip authentication.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Auth {
    None,
    Cleartext,
}

/// The pgoutput payload the fake server sends: an `Insert` message body.
///
/// Content does not matter to the transport — it forwards plugin bytes verbatim — so this is
/// just a recognisable, non-empty blob.
const PAYLOAD: &[u8] = b"I\x00\x00\x03\xe8N\x00\x01t\x00\x00\x00\x01x";

async fn write_tagged<S>(stream: &mut S, tag: u8, payload: &[u8])
where
    S: AsyncWriteExt + Unpin,
{
    let len = (payload.len() + 4) as i32;
    let mut framed = vec![tag];
    framed.extend_from_slice(&len.to_be_bytes());
    framed.extend_from_slice(payload);
    stream.write_all(&framed).await.expect("server writes");
    stream.flush().await.expect("server flushes");
}

/// Build an `XLogData` CopyData payload.
fn xlog_data(wal_start: u64, wal_end: u64, payload: &[u8]) -> Vec<u8> {
    let mut body = vec![b'w'];
    body.extend_from_slice(&(wal_start as i64).to_be_bytes());
    body.extend_from_slice(&(wal_end as i64).to_be_bytes());
    body.extend_from_slice(&0i64.to_be_bytes());
    body.extend_from_slice(payload);
    body
}

/// Read one tagged message from the client.
async fn read_tagged<S>(stream: &mut S) -> (u8, Vec<u8>)
where
    S: AsyncReadExt + Unpin,
{
    let tag = stream.read_u8().await.expect("client message tag");
    let len = stream.read_i32().await.expect("client message length");
    let mut payload = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut payload).await.expect("client payload");
    (tag, payload)
}

/// Run the server side of one connection.
async fn serve<S>(stream: &mut S, auth: Auth, behaviour: Behaviour, observed: &mut Observed)
where
    S: AsyncReadExt + AsyncWriteExt + Unpin,
{
    // ── Startup packet (untagged) ────────────────────────────────────────────
    let len = stream.read_i32().await.expect("startup length");
    let mut body = vec![0u8; (len - 4) as usize];
    stream.read_exact(&mut body).await.expect("startup body");
    let params: Vec<String> = body[4..]
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| String::from_utf8_lossy(part).into_owned())
        .collect();
    observed.startup_params = params
        .chunks(2)
        .map(|pair| format!("{}={}", pair[0], pair.get(1).cloned().unwrap_or_default()))
        .collect();

    // ── Authentication ───────────────────────────────────────────────────────
    if auth == Auth::Cleartext {
        // AuthenticationCleartextPassword
        write_tagged(stream, b'R', &3i32.to_be_bytes()).await;
        let (tag, payload) = read_tagged(stream).await;
        assert_eq!(tag, b'p', "the client must reply with a PasswordMessage");
        observed.password_sent = Some(
            String::from_utf8_lossy(payload.strip_suffix(&[0]).unwrap_or(&payload)).into_owned(),
        );
    }
    // AuthenticationOk
    write_tagged(stream, b'R', &0i32.to_be_bytes()).await;
    write_tagged(stream, b'S', b"server_version\x0016\x00").await;
    write_tagged(stream, b'K', &[0, 0, 0, 1, 0, 0, 0, 2]).await;
    write_tagged(stream, b'Z', b"I").await;

    // ── START_REPLICATION ────────────────────────────────────────────────────
    let (tag, payload) = read_tagged(stream).await;
    assert_eq!(tag, b'Q', "the client must issue a simple Query");
    observed.replication_query =
        String::from_utf8_lossy(payload.strip_suffix(&[0]).unwrap_or(&payload)).into_owned();

    if behaviour == Behaviour::RefuseReplication {
        let mut error = Vec::new();
        for (field, value) in [
            (b'S', "FATAL"),
            (b'C', "55006"),
            (b'M', "replication slot \"slot\" is active for PID 4711"),
        ] {
            error.push(field);
            error.extend_from_slice(value.as_bytes());
            error.push(0);
        }
        error.push(0);
        write_tagged(stream, b'E', &error).await;
        return;
    }

    write_tagged(stream, b'W', &[0, 0, 0]).await;

    // ── Stream body ──────────────────────────────────────────────────────────
    match behaviour {
        Behaviour::SendOneRecord => {
            write_tagged(stream, b'd', &xlog_data(0x1000, 0x2000, PAYLOAD)).await;
        }
        Behaviour::DemandKeepaliveReply => {
            let mut keepalive = vec![b'k'];
            keepalive.extend_from_slice(&0x5000i64.to_be_bytes());
            keepalive.extend_from_slice(&0i64.to_be_bytes());
            keepalive.push(1); // reply requested
            write_tagged(stream, b'd', &keepalive).await;

            // The client must answer the keepalive before anything else.
            let (tag, payload) = read_tagged(stream).await;
            assert_eq!(tag, b'd', "a keepalive reply travels as CopyData");
            assert_eq!(payload[0], b'r', "and is a Standby Status Update");
            observed
                .status_updates
                .push(i64::from_be_bytes(payload[1..9].try_into().unwrap()) as u64);

            write_tagged(stream, b'd', &xlog_data(0x6000, 0x7000, PAYLOAD)).await;
        }
        Behaviour::SplitRecordAcrossWrites => {
            let body = xlog_data(0x1000, 0x2000, PAYLOAD);
            let framed = {
                let mut framed = vec![b'd'];
                framed.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
                framed.extend_from_slice(&body);
                framed
            };
            // Tag and length only — the payload is withheld so the client's poll budget
            // expires mid-frame.
            stream.write_all(&framed[..5]).await.expect("partial write");
            stream.flush().await.expect("flush");
            tokio::time::sleep(Duration::from_millis(400)).await;
            stream.write_all(&framed[5..]).await.expect("rest of frame");
            stream.flush().await.expect("flush");
        }
        Behaviour::RefuseReplication | Behaviour::DeclineTls => unreachable!("handled above"),
    }

    // Hold the connection open so the client sees a *quiet* stream rather than a close, and
    // drain whatever it sends. Reading until EOF rather than sleeping means the task ends as
    // soon as the client drops its handle, so a test can await the observations instead of
    // waiting out a fixed sleep.
    let mut scratch = [0u8; 1024];
    while let Ok(read) = stream.read(&mut scratch).await {
        if read == 0 {
            break;
        }
    }
}

/// Start a fake server and return the port plus a handle to what it observes.
async fn spawn_server(
    tls: Option<TestTls>,
    auth: Auth,
    behaviour: Behaviour,
) -> (u16, Option<rustls::ClientConfig>, tokio::task::JoinHandle<Observed>) {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .expect("binds a loopback port");
    let port = listener.local_addr().expect("local address").port();
    let server_tls = tls.as_ref().map(|tls| Arc::clone(&tls.server));
    let client_tls = tls.map(|tls| tls.client);

    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accepts a connection");
        let mut observed = Observed::default();

        match server_tls {
            None => serve(&mut socket, auth, behaviour, &mut observed).await,
            Some(server_config) => {
                // The SSLRequest is untagged: an 8-byte packet whose "version" is magic.
                let len = socket.read_i32().await.expect("ssl request length");
                assert_eq!(len, 8, "an SSLRequest packet is exactly 8 bytes");
                let code = socket.read_i32().await.expect("ssl request code");
                assert_eq!(code, 80_877_103, "the magic SSLRequest code");

                if behaviour == Behaviour::DeclineTls {
                    socket.write_all(b"N").await.expect("declines");
                    socket.flush().await.expect("flush");
                    return observed;
                }

                socket.write_all(b"S").await.expect("accepts");
                socket.flush().await.expect("flush");
                let acceptor = tokio_rustls::TlsAcceptor::from(server_config);
                let mut tls_socket = acceptor.accept(socket).await.expect("tls handshake");
                serve(&mut tls_socket, auth, behaviour, &mut observed).await;
            }
        }

        observed
    });

    (port, client_tls, handle)
}

fn params<'a>(
    port: u16,
    transport: &'a TransportConfig,
    start_lsn: u64,
) -> ReplicationParams<'a> {
    ReplicationParams {
        // "localhost" rather than the literal address so the certificate's SAN matches.
        host: "localhost",
        port,
        user: "cdc",
        password: "s3cret",
        database: "app",
        slot_name: "rustcdc_slot",
        publication_name: "rustcdc_pub",
        transport,
        start_lsn,
        connect_timeout: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn a_tls_replication_stream_completes_the_upgrade_and_carries_wal() {
    // The whole TLS path in one test: SSLRequest, the rustls handshake, and reading a WAL
    // record back through `Socket::Tls`. A wrong branch in the read/write delegation, or
    // reading the SSLRequest reply as a framed message, fails here.
    let tls = test_tls();
    let (port, client_config, server) =
        spawn_server(Some(tls), Auth::Cleartext, Behaviour::SendOneRecord).await;
    let transport = TransportConfig::RustlsConfig {
        config: RustlsClientConfig(Arc::new(client_config.expect("client config"))),
    };

    let mut stream = ReplicationStream::connect(params(port, &transport, 0x900))
        .await
        .expect("replication stream starts over TLS");

    let message = stream
        .recv(Duration::from_secs(5))
        .await
        .expect("receives")
        .expect("a record arrives");
    match message {
        WalMessage::XLogData {
            wal_start,
            wal_end,
            data,
        } => {
            assert_eq!(wal_start, 0x1000);
            assert_eq!(wal_end, 0x2000);
            assert_eq!(
                data, PAYLOAD,
                "plugin bytes must be forwarded verbatim, so the shared pgoutput decoder \
                 cannot disagree between transports"
            );
        }
        other => panic!("expected XLogData, got {other:?}"),
    }

    drop(stream);
    server.abort();
}

#[tokio::test]
async fn the_startup_packet_and_replication_command_reach_the_server_intact() {
    let (port, _, server) = spawn_server(None, Auth::None, Behaviour::SendOneRecord).await;
    let transport = TransportConfig::plaintext();

    let mut stream = ReplicationStream::connect(params(port, &transport, 0x0001_0000_0000))
        .await
        .expect("replication stream starts");
    let _ = stream.recv(Duration::from_secs(5)).await.expect("receives");
    drop(stream);

    let observed = server.await.expect("server task");

    assert!(
        observed
            .startup_params
            .contains(&"replication=database".to_string()),
        "logical decoding is per-database, so the startup packet must ask for \
         `replication=database` rather than `true`: {:?}",
        observed.startup_params
    );
    assert!(observed.startup_params.contains(&"user=cdc".to_string()));
    assert!(observed.startup_params.contains(&"database=app".to_string()));

    let query = &observed.replication_query;
    assert!(
        query.contains("START_REPLICATION SLOT rustcdc_slot LOGICAL 1/0"),
        "the start LSN must be rendered in PostgreSQL's two-part hex form: {query}"
    );
    assert!(
        query.contains("proto_version '1'"),
        "the negotiated version must match what the decoder implements: {query}"
    );
    assert!(
        query.contains("publication_names 'rustcdc_pub'"),
        "the publication must be named: {query}"
    );
}

#[tokio::test]
async fn a_cleartext_password_is_sent_only_over_tls_and_arrives_unmodified() {
    // Cleartext is a legitimate configuration *under TLS*, and the password must reach the
    // server NUL-terminated and otherwise untouched — a stray length prefix or missing
    // terminator fails authentication with a message that blames the credentials.
    let tls = test_tls();
    let (port, client_config, server) =
        spawn_server(Some(tls), Auth::Cleartext, Behaviour::SendOneRecord).await;
    let transport = TransportConfig::RustlsConfig {
        config: RustlsClientConfig(Arc::new(client_config.expect("client config"))),
    };

    let mut stream = ReplicationStream::connect(params(port, &transport, 0))
        .await
        .expect("replication stream starts over TLS");
    let _ = stream.recv(Duration::from_secs(5)).await.expect("receives");
    drop(stream);

    let observed = server.await.expect("server task");
    assert_eq!(observed.password_sent.as_deref(), Some("s3cret"));
}

#[tokio::test]
async fn a_cleartext_password_request_over_a_plaintext_connection_is_refused() {
    // The server asking for a cleartext password does not make it safe to send one. Over an
    // unencrypted connection the password would go out in the clear, so this fails with a
    // configuration error naming both remedies rather than complying.
    let (port, _, server) = spawn_server(None, Auth::Cleartext, Behaviour::SendOneRecord).await;
    let transport = TransportConfig::plaintext();

    let rendered = match ReplicationStream::connect(params(port, &transport, 0)).await {
        Ok(_) => panic!("a cleartext request over plaintext must not be satisfied"),
        Err(error) => error.to_string(),
    };
    assert!(
        rendered.contains("in the clear"),
        "the error must say why it refused: {rendered}"
    );
    assert!(
        rendered.contains("scram-sha-256"),
        "and point at the fix: {rendered}"
    );

    server.abort();
}

#[tokio::test]
async fn a_keepalive_demanding_a_reply_is_answered_without_the_caller_noticing() {
    // Not answering costs the connection: the server concludes the client is gone once
    // `wal_sender_timeout` elapses. The reply must also carry the applied LSN, not zero,
    // or the server never learns it may release WAL.
    let (port, _, server) =
        spawn_server(None, Auth::None, Behaviour::DemandKeepaliveReply).await;
    let transport = TransportConfig::plaintext();

    let mut stream = ReplicationStream::connect(params(port, &transport, 0))
        .await
        .expect("replication stream starts");
    stream.set_applied_lsn(0x4242);

    // First the keepalive, then the record that follows it.
    let first = stream
        .recv(Duration::from_secs(5))
        .await
        .expect("receives")
        .expect("keepalive surfaces");
    assert!(
        matches!(first, WalMessage::Keepalive { wal_end } if wal_end == 0x5000),
        "the keepalive must surface with the server's WAL end so lag can be reported: \
         {first:?}"
    );

    let second = stream
        .recv(Duration::from_secs(5))
        .await
        .expect("receives")
        .expect("the record after the keepalive arrives");
    assert!(matches!(second, WalMessage::XLogData { .. }));

    drop(stream);
    let observed = server.await.expect("server task");
    assert_eq!(
        observed.status_updates,
        vec![0x4242],
        "the reply must report the applied LSN the consumer has durably persisted"
    );
}

#[tokio::test]
async fn a_frame_split_across_writes_survives_an_expired_poll_budget() {
    // The regression this guards: the poll budget expiring *between* a message's tag and its
    // payload. Reading fields straight off the socket under a timeout discards the bytes
    // already consumed, and every later read is misaligned — a permanent, silent
    // desynchronisation. Buffered framing means the partial frame simply waits.
    let (port, _, server) =
        spawn_server(None, Auth::None, Behaviour::SplitRecordAcrossWrites).await;
    let transport = TransportConfig::plaintext();

    let mut stream = ReplicationStream::connect(params(port, &transport, 0))
        .await
        .expect("replication stream starts");

    // The server writes five bytes, then waits 400 ms. This budget expires in between.
    let timed_out = stream
        .recv(Duration::from_millis(150))
        .await
        .expect("an expired budget is not an error");
    assert!(
        timed_out.is_none(),
        "the budget must expire without yielding a half-read frame"
    );

    // The rest of the frame arrives, and the message decodes correctly rather than as
    // garbage assembled from a misaligned stream.
    let message = stream
        .recv(Duration::from_secs(5))
        .await
        .expect("receives")
        .expect("the completed frame decodes");
    match message {
        WalMessage::XLogData {
            wal_start, data, ..
        } => {
            assert_eq!(wal_start, 0x1000, "the header must not be misread");
            assert_eq!(data, PAYLOAD, "the payload must be intact");
        }
        other => panic!("expected XLogData, got {other:?}"),
    }

    drop(stream);
    server.abort();
}

#[tokio::test]
async fn a_server_that_refuses_replication_surfaces_its_own_reason() {
    // "Slot is active" is the error an operator actually hits — a second pipeline on one
    // slot. The SQLSTATE and message must survive rather than be replaced by a guess about
    // missing privileges.
    let (port, _, server) =
        spawn_server(None, Auth::None, Behaviour::RefuseReplication).await;
    let transport = TransportConfig::plaintext();

    let rendered = match ReplicationStream::connect(params(port, &transport, 0)).await {
        Ok(_) => panic!("a refused START_REPLICATION must not yield a stream"),
        Err(error) => error.to_string(),
    };
    assert!(
        rendered.contains("is active for PID 4711"),
        "the server's own message must survive: {rendered}"
    );
    assert!(
        rendered.contains("55006"),
        "the SQLSTATE must survive, since callers key recoverability off it: {rendered}"
    );

    server.abort();
}

#[tokio::test]
async fn a_server_that_declines_tls_is_refused_rather_than_downgraded() {
    // Continuing unencrypted here is the silent-downgrade failure that `sslmode=prefer`
    // produced on the SQL connection. The replication transport must fail instead.
    let tls = test_tls();
    let (port, client_config, server) =
        spawn_server(Some(tls), Auth::None, Behaviour::DeclineTls).await;
    let transport = TransportConfig::RustlsConfig {
        config: RustlsClientConfig(Arc::new(client_config.expect("client config"))),
    };

    let rendered = match ReplicationStream::connect(params(port, &transport, 0)).await {
        Ok(_) => panic!("a declined TLS upgrade must not yield a stream"),
        Err(error) => error.to_string(),
    };
    assert!(
        rendered.contains("refused TLS"),
        "the error must name the downgrade attempt: {rendered}"
    );

    server.abort();
}

#[tokio::test]
async fn a_connect_timeout_is_reported_against_the_configured_budget() {
    // A listener that accepts and then says nothing. Without a timeout on the whole setup
    // sequence this hangs forever at startup, which looks identical to a slow database.
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("binds");
    let port = listener.local_addr().expect("addr").port();
    let silent = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.expect("accepts");
        tokio::time::sleep(Duration::from_secs(30)).await;
        drop(socket);
    });

    let transport = TransportConfig::plaintext();
    let mut parameters = params(port, &transport, 0);
    parameters.connect_timeout = Duration::from_millis(300);

    // The socket connects immediately; the hang is in the startup exchange that follows, so
    // this asserts the timeout covers more than `TcpStream::connect`.
    let started = std::time::Instant::now();
    let result =
        tokio::time::timeout(Duration::from_secs(5), ReplicationStream::connect(parameters)).await;
    assert!(
        result.is_ok(),
        "connect must give up on its own rather than hang for the outer timeout"
    );
    assert!(
        matches!(result, Ok(Err(_))),
        "and report an error rather than a stream"
    );
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "took {:?}",
        started.elapsed()
    );

    silent.abort();
}

#[tokio::test]
async fn connecting_to_a_closed_port_fails_with_an_actionable_message() {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("binds");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);

    let transport = TransportConfig::plaintext();
    let rendered = match ReplicationStream::connect(params(port, &transport, 0)).await {
        Ok(_) => panic!("connecting to a closed port must not yield a stream"),
        Err(error) => error.to_string(),
    };
    assert!(
        rendered.contains("localhost") && rendered.contains(&port.to_string()),
        "the error must name the endpoint it could not reach: {rendered}"
    );
}
