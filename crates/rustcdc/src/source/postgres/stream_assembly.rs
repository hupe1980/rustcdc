//! Reassembles pgoutput v2 streamed transactions into v1 transactions.
//!
//! Under `streaming 'on'` the server sends a large transaction while it is still running, in
//! `Stream Start`…`Stream Stop` blocks, and settles it later with `Stream Commit` or
//! `Stream Abort`. This module holds each streamed transaction until it settles and hands a
//! committed one back as `BEGIN`, its messages, `COMMIT` — the bytes v1 would have sent — so
//! the decoder, the commit barrier and the checkpoint never see an uncommitted change.
//!
//! Inside a block, the data messages carry an `Int32` xid after the tag: the subtransaction's,
//! for changes. Stripped, they are exactly v1's. A subtransaction abort truncates the held
//! messages at that subtransaction's first one; subtransactions nest as a stack, so everything
//! after that point belongs to it or to its children. PostgreSQL's own subscriber truncates
//! the same way.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::time::SystemTime;

use super::decoder::PgOutputXLogData;
use crate::{Error, Result};

/// `[u32 length][u64 lsn]` before each held message.
const RECORD_HEADER: usize = 12;

/// What the assembler hands on, in arrival order.
pub(super) enum Assembled {
    /// A message outside any stream block, unchanged.
    Message(PgOutputXLogData),
    /// A streamed transaction that committed, ready to be decoded as v1.
    Committed(CommittedStream),
}

/// Where a held transaction's messages live.
enum Store {
    Memory(Vec<u8>),
    File { writer: BufWriter<File>, len: u64 },
}

impl Store {
    fn len(&self) -> u64 {
        match self {
            Self::Memory(bytes) => bytes.len() as u64,
            Self::File { len, .. } => *len,
        }
    }

    fn append(&mut self, lsn: u64, message: &[u8]) -> std::io::Result<()> {
        let length = u32::try_from(message.len()).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "message over 4 GiB")
        })?;
        match self {
            Self::Memory(bytes) => {
                bytes.extend_from_slice(&length.to_be_bytes());
                bytes.extend_from_slice(&lsn.to_be_bytes());
                bytes.extend_from_slice(message);
            }
            Self::File { writer, len } => {
                writer.write_all(&length.to_be_bytes())?;
                writer.write_all(&lsn.to_be_bytes())?;
                writer.write_all(message)?;
                *len += (RECORD_HEADER + message.len()) as u64;
            }
        }
        Ok(())
    }

    fn truncate(&mut self, offset: u64) -> std::io::Result<()> {
        match self {
            Self::Memory(bytes) => bytes.truncate(offset as usize),
            Self::File { writer, len } => {
                writer.flush()?;
                let file = writer.get_mut();
                file.set_len(offset)?;
                file.seek(SeekFrom::End(0))?;
                *len = offset;
            }
        }
        Ok(())
    }

    fn reader(self) -> std::io::Result<Box<dyn Read + Send + Sync>> {
        match self {
            Self::Memory(bytes) => Ok(Box::new(std::io::Cursor::new(bytes))),
            Self::File { writer, .. } => {
                let mut file = writer.into_inner().map_err(|error| error.into_error())?;
                file.seek(SeekFrom::Start(0))?;
                Ok(Box::new(BufReader::new(file)))
            }
        }
    }
}

struct OpenStream {
    store: Store,
    /// Each subtransaction's first message: `(subxid, offset, changes before it)`.
    subxacts: Vec<(u32, u64, u64)>,
    changes: u64,
    started_at: SystemTime,
}

/// A committed streamed transaction.
pub(super) struct CommittedStream {
    pub(super) xid: u32,
    commit_lsn: u64,
    end_lsn: u64,
    commit_ts: i64,
    /// Where the `Stream Commit` arrived; the synthetic `BEGIN`/`COMMIT` are placed there.
    lsn: u64,
    store: Store,
}

impl CommittedStream {
    /// The `BEGIN` v1 would have sent: final LSN, commit timestamp, xid.
    pub(super) fn begin(&self) -> PgOutputXLogData {
        let mut data = Vec::with_capacity(21);
        data.push(b'B');
        data.extend_from_slice(&self.commit_lsn.to_be_bytes());
        data.extend_from_slice(&self.commit_ts.to_be_bytes());
        data.extend_from_slice(&self.xid.to_be_bytes());
        PgOutputXLogData {
            lsn: self.lsn,
            data,
        }
    }

    /// The `COMMIT` v1 would have sent: flags, commit LSN, end LSN, commit timestamp.
    pub(super) fn commit(&self) -> PgOutputXLogData {
        let mut data = Vec::with_capacity(26);
        data.push(b'C');
        data.push(0);
        data.extend_from_slice(&self.commit_lsn.to_be_bytes());
        data.extend_from_slice(&self.end_lsn.to_be_bytes());
        data.extend_from_slice(&self.commit_ts.to_be_bytes());
        PgOutputXLogData {
            lsn: self.lsn,
            data,
        }
    }

    /// The held messages, in order, read back without materialising them all.
    pub(super) fn messages(self) -> Result<HeldMessages> {
        let reader = self.store.reader().map_err(spill_error)?;
        Ok(HeldMessages { reader })
    }
}

pub(super) struct HeldMessages {
    reader: Box<dyn Read + Send + Sync>,
}

impl Iterator for HeldMessages {
    type Item = Result<PgOutputXLogData>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header = [0u8; RECORD_HEADER];
        match self.reader.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(error) => return Some(Err(spill_error(error))),
        }
        let length = u32::from_be_bytes(header[..4].try_into().expect("4 bytes")) as usize;
        let lsn = u64::from_be_bytes(header[4..].try_into().expect("8 bytes"));
        let mut data = vec![0u8; length];
        Some(
            self.reader
                .read_exact(&mut data)
                .map(|()| PgOutputXLogData { lsn, data })
                .map_err(spill_error),
        )
    }
}

/// The open streamed transaction an operator should hear about.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OpenStreamSummary {
    pub(super) xid: u32,
    pub(super) changes: u64,
    pub(super) bytes: u64,
    pub(super) spilled: bool,
    pub(super) started_at: SystemTime,
}

/// Totals across every open streamed transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct StreamBufferTotals {
    pub(super) open: u64,
    pub(super) memory_bytes: u64,
    pub(super) spilled_bytes: u64,
}

pub(super) struct StreamAssembler {
    open: HashMap<u32, OpenStream>,
    in_block: Option<u32>,
    memory_bytes: u64,
    memory_limit: u64,
    spill_directory: PathBuf,
}

impl StreamAssembler {
    pub(super) fn new(memory_limit: u64, spill_directory: PathBuf) -> Self {
        Self {
            open: HashMap::new(),
            in_block: None,
            memory_bytes: 0,
            memory_limit,
            spill_directory,
        }
    }

    pub(super) fn feed(&mut self, items: Vec<PgOutputXLogData>) -> Result<Vec<Assembled>> {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let Some(&tag) = item.data.first() else {
                return Err(Error::SourceError("empty pgoutput message".into()));
            };
            if let Some(xid) = self.in_block {
                match tag {
                    b'E' => self.in_block = None,
                    b'R' | b'Y' | b'I' | b'U' | b'D' | b'T' | b'M' => {
                        let subxid = read_u32(&item.data, 1, "streamed change")?;
                        let mut v1 = Vec::with_capacity(item.data.len() - 4);
                        v1.push(tag);
                        v1.extend_from_slice(&item.data[5..]);
                        self.hold(xid, subxid, item.lsn, &v1, tag)?;
                    }
                    // Sent once after the first Stream Start of a replicated transaction; v1
                    // sends it after BEGIN and the decoder already skips it.
                    b'O' => self.hold(xid, xid, item.lsn, &item.data, tag)?,
                    other => {
                        return Err(protocol_error(format!(
                            "message '{}' inside the stream block of transaction {xid}",
                            other as char
                        )));
                    }
                }
                continue;
            }
            match tag {
                b'S' => {
                    let xid = read_u32(&item.data, 1, "Stream Start")?;
                    let first = *item.data.get(5).ok_or_else(|| {
                        protocol_error("Stream Start without its first-segment flag".into())
                    })? == 1;
                    if first {
                        // A reconnect re-streams from the start: the new copy replaces the old.
                        self.drop_stream(xid);
                        self.open.insert(
                            xid,
                            OpenStream {
                                store: Store::Memory(Vec::new()),
                                subxacts: Vec::new(),
                                changes: 0,
                                started_at: SystemTime::now(),
                            },
                        );
                    } else if !self.open.contains_key(&xid) {
                        return Err(protocol_error(format!(
                            "a continuing stream segment for transaction {xid}, whose start \
                             this connection never received"
                        )));
                    }
                    self.in_block = Some(xid);
                }
                b'c' => {
                    let xid = read_u32(&item.data, 1, "Stream Commit")?;
                    let commit_lsn = read_u64(&item.data, 6, "Stream Commit")?;
                    let end_lsn = read_u64(&item.data, 14, "Stream Commit")?;
                    let commit_ts = read_u64(&item.data, 22, "Stream Commit")? as i64;
                    let stream = self.open.remove(&xid).ok_or_else(|| {
                        protocol_error(format!(
                            "Stream Commit for transaction {xid}, which was never streamed on \
                             this connection; its changes would be lost"
                        ))
                    })?;
                    if let Store::Memory(bytes) = &stream.store {
                        self.memory_bytes -= bytes.len() as u64;
                    }
                    out.push(Assembled::Committed(CommittedStream {
                        xid,
                        commit_lsn,
                        end_lsn,
                        commit_ts,
                        lsn: item.lsn,
                        store: stream.store,
                    }));
                }
                b'A' => {
                    let xid = read_u32(&item.data, 1, "Stream Abort")?;
                    let subxid = read_u32(&item.data, 5, "Stream Abort")?;
                    if subxid == xid {
                        self.drop_stream(xid);
                    } else {
                        self.abort_subtransaction(xid, subxid)?;
                    }
                }
                b'E' => return Err(protocol_error("Stream Stop outside a stream block".into())),
                _ => out.push(Assembled::Message(item)),
            }
        }
        Ok(out)
    }

    /// The open streamed transaction with the most changes.
    pub(super) fn open_transaction(&self) -> Option<OpenStreamSummary> {
        self.open
            .iter()
            .max_by_key(|(xid, stream)| (stream.changes, std::cmp::Reverse(**xid)))
            .map(|(xid, stream)| OpenStreamSummary {
                xid: *xid,
                changes: stream.changes,
                bytes: stream.store.len(),
                spilled: matches!(stream.store, Store::File { .. }),
                started_at: stream.started_at,
            })
    }

    pub(super) fn totals(&self) -> StreamBufferTotals {
        let spilled_bytes = self
            .open
            .values()
            .filter(|stream| matches!(stream.store, Store::File { .. }))
            .map(|stream| stream.store.len())
            .sum();
        StreamBufferTotals {
            open: self.open.len() as u64,
            memory_bytes: self.memory_bytes,
            spilled_bytes,
        }
    }

    fn hold(&mut self, xid: u32, subxid: u32, lsn: u64, message: &[u8], tag: u8) -> Result<()> {
        let record = (RECORD_HEADER + message.len()) as u64;
        let over_budget = self.memory_bytes + record > self.memory_limit;
        let stream = self
            .open
            .get_mut(&xid)
            .ok_or_else(|| protocol_error(format!("a block for unknown transaction {xid}")))?;
        if subxid != xid && !stream.subxacts.iter().any(|(known, ..)| *known == subxid) {
            stream
                .subxacts
                .push((subxid, stream.store.len(), stream.changes));
        }
        if over_budget && matches!(stream.store, Store::Memory(_)) {
            let Store::Memory(held) = std::mem::take(&mut stream.store) else {
                unreachable!("matched as memory above")
            };
            self.memory_bytes -= held.len() as u64;
            stream.store = spill(&self.spill_directory, &held).map_err(spill_error)?;
        }
        stream.store.append(lsn, message).map_err(spill_error)?;
        if let Store::Memory(_) = stream.store {
            self.memory_bytes += record;
        }
        if matches!(tag, b'I' | b'U' | b'D' | b'T' | b'M') {
            stream.changes += 1;
        }
        Ok(())
    }

    fn abort_subtransaction(&mut self, xid: u32, subxid: u32) -> Result<()> {
        let Some(stream) = self.open.get_mut(&xid) else {
            return Ok(());
        };
        let Some(index) = stream
            .subxacts
            .iter()
            .position(|(known, ..)| *known == subxid)
        else {
            // A subtransaction that wrote nothing published.
            return Ok(());
        };
        let (_, offset, changes) = stream.subxacts[index];
        if let Store::Memory(_) = stream.store {
            self.memory_bytes -= stream.store.len() - offset;
        }
        stream.store.truncate(offset).map_err(spill_error)?;
        stream.subxacts.truncate(index);
        stream.changes = changes;
        Ok(())
    }

    fn drop_stream(&mut self, xid: u32) {
        if let Some(stream) = self.open.remove(&xid)
            && let Store::Memory(bytes) = stream.store
        {
            self.memory_bytes -= bytes.len() as u64;
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::Memory(Vec::new())
    }
}

/// Move held bytes to an anonymous file: unlinked at creation, so any exit reclaims it.
fn spill(directory: &std::path::Path, held: &[u8]) -> std::io::Result<Store> {
    let mut writer = BufWriter::new(tempfile::tempfile_in(directory)?);
    writer.write_all(held)?;
    Ok(Store::File {
        writer,
        len: held.len() as u64,
    })
}

/// Check, before streaming starts, that a spill file can be created where configured.
pub(super) fn probe_spill_directory(directory: &std::path::Path) -> Result<()> {
    tempfile::tempfile_in(directory).map(drop).map_err(|error| {
        Error::SourceError(format!(
            "stream_spill_directory {} is not writable ({error}); large streamed transactions \
             spill there. Point stream_spill_directory at a writable directory, or set \
             stream_in_progress_transactions = false",
            directory.display()
        ))
    })
}

fn spill_error(error: std::io::Error) -> Error {
    Error::SourceError(format!(
        "spilling a streamed transaction to stream_spill_directory failed: {error}. Nothing was \
         dropped; the pipeline stops so the transaction is read again from the slot"
    ))
}

fn protocol_error(detail: String) -> Error {
    Error::SourceError(format!("pgoutput v2 protocol violation: {detail}"))
}

fn read_u32(data: &[u8], at: usize, what: &str) -> Result<u32> {
    data.get(at..at + 4)
        .map(|bytes| u32::from_be_bytes(bytes.try_into().expect("4 bytes")))
        .ok_or_else(|| protocol_error(format!("{what} is truncated")))
}

fn read_u64(data: &[u8], at: usize, what: &str) -> Result<u64> {
    data.get(at..at + 8)
        .map(|bytes| u64::from_be_bytes(bytes.try_into().expect("8 bytes")))
        .ok_or_else(|| protocol_error(format!("{what} is truncated")))
}

#[cfg(test)]
mod tests {
    use super::super::decoder::{PgOutputMessage, decode_pgoutput_message};
    use super::*;

    const TOP: u32 = 700;

    fn msg(data: Vec<u8>) -> PgOutputXLogData {
        PgOutputXLogData { lsn: 1, data }
    }

    fn start(xid: u32, first: bool) -> PgOutputXLogData {
        let mut data = vec![b'S'];
        data.extend_from_slice(&xid.to_be_bytes());
        data.push(u8::from(first));
        msg(data)
    }

    fn stop() -> PgOutputXLogData {
        msg(vec![b'E'])
    }

    /// A v1 insert of one text column into relation 16384.
    fn v1_insert(value: &str) -> Vec<u8> {
        let mut data = vec![b'I'];
        data.extend_from_slice(&16384u32.to_be_bytes());
        data.push(b'N');
        data.extend_from_slice(&1u16.to_be_bytes());
        data.push(b't');
        data.extend_from_slice(&(value.len() as u32).to_be_bytes());
        data.extend_from_slice(value.as_bytes());
        data
    }

    /// The same insert as v2 frames it inside a stream block.
    fn streamed_insert(subxid: u32, value: &str) -> PgOutputXLogData {
        let v1 = v1_insert(value);
        let mut data = vec![b'I'];
        data.extend_from_slice(&subxid.to_be_bytes());
        data.extend_from_slice(&v1[1..]);
        msg(data)
    }

    fn stream_commit(xid: u32) -> PgOutputXLogData {
        let mut data = vec![b'c'];
        data.extend_from_slice(&xid.to_be_bytes());
        data.push(0);
        data.extend_from_slice(&0x500u64.to_be_bytes());
        data.extend_from_slice(&0x528u64.to_be_bytes());
        data.extend_from_slice(&42i64.to_be_bytes());
        msg(data)
    }

    fn stream_abort(xid: u32, subxid: u32) -> PgOutputXLogData {
        let mut data = vec![b'A'];
        data.extend_from_slice(&xid.to_be_bytes());
        data.extend_from_slice(&subxid.to_be_bytes());
        msg(data)
    }

    fn assembler() -> StreamAssembler {
        StreamAssembler::new(64 * 1024 * 1024, std::env::temp_dir())
    }

    /// The committed transactions, each as the values it inserted.
    fn committed_values(out: Vec<Assembled>) -> Vec<(u32, Vec<String>)> {
        out.into_iter()
            .filter_map(|item| match item {
                Assembled::Committed(stream) => Some(stream),
                Assembled::Message(_) => None,
            })
            .map(|stream| {
                let xid = stream.xid;
                let values = stream
                    .messages()
                    .unwrap()
                    .map(
                        |held| match decode_pgoutput_message(&held.unwrap().data).unwrap() {
                            PgOutputMessage::Insert(insert) => match &insert.new_tuple[0] {
                                super::super::decoder::PgValue::Text(text) => text.clone(),
                                other => panic!("unexpected {other:?}"),
                            },
                            other => panic!("unexpected {other:?}"),
                        },
                    )
                    .collect();
                (xid, values)
            })
            .collect()
    }

    #[test]
    fn a_streamed_transaction_is_released_only_at_its_commit() {
        let mut assembler = assembler();
        let streamed = assembler
            .feed(vec![start(TOP, true), streamed_insert(TOP, "a"), stop()])
            .unwrap();
        assert!(
            streamed.is_empty(),
            "nothing may leave before Stream Commit"
        );
        assert_eq!(assembler.open_transaction().unwrap().changes, 1);

        let out = assembler.feed(vec![stream_commit(TOP)]).unwrap();
        assert_eq!(committed_values(out), vec![(TOP, vec!["a".to_string()])]);
        assert!(assembler.open_transaction().is_none());
    }

    #[test]
    fn a_held_message_is_the_v1_message() {
        let mut assembler = assembler();
        assembler
            .feed(vec![start(TOP, true), streamed_insert(TOP, "v"), stop()])
            .unwrap();
        let Assembled::Committed(stream) =
            assembler.feed(vec![stream_commit(TOP)]).unwrap().remove(0)
        else {
            panic!("expected a committed stream");
        };
        let held: Vec<_> = stream.messages().unwrap().map(Result::unwrap).collect();
        assert_eq!(held[0].data, v1_insert("v"));
    }

    #[test]
    fn the_synthetic_begin_and_commit_decode_as_v1() {
        let mut assembler = assembler();
        assembler.feed(vec![start(TOP, true), stop()]).unwrap();
        let Assembled::Committed(stream) =
            assembler.feed(vec![stream_commit(TOP)]).unwrap().remove(0)
        else {
            panic!("expected a committed stream");
        };
        match decode_pgoutput_message(&stream.begin().data).unwrap() {
            PgOutputMessage::Begin(begin) => {
                assert_eq!(
                    (begin.xid, begin.final_lsn, begin.commit_timestamp_us),
                    (TOP, 0x500, 42)
                );
            }
            other => panic!("unexpected {other:?}"),
        }
        match decode_pgoutput_message(&stream.commit().data).unwrap() {
            PgOutputMessage::Commit(commit) => {
                assert_eq!((commit.commit_lsn, commit.end_lsn), (0x500, 0x528));
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn a_top_level_abort_drops_the_transaction() {
        let mut assembler = assembler();
        let out = assembler
            .feed(vec![
                start(TOP, true),
                streamed_insert(TOP, "a"),
                stop(),
                stream_abort(TOP, TOP),
            ])
            .unwrap();
        assert!(out.is_empty());
        assert!(assembler.open_transaction().is_none());
        assert_eq!(assembler.totals(), StreamBufferTotals::default());
    }

    #[test]
    fn a_subtransaction_abort_drops_it_and_its_children_only() {
        let mut assembler = assembler();
        assembler
            .feed(vec![
                start(TOP, true),
                streamed_insert(TOP, "before"),
                streamed_insert(701, "released savepoint"),
                streamed_insert(702, "rolled back"),
                streamed_insert(703, "child of rolled back"),
                stop(),
                stream_abort(TOP, 703),
                stream_abort(TOP, 702),
                start(TOP, false),
                streamed_insert(TOP, "after"),
                stop(),
            ])
            .unwrap();
        assert_eq!(assembler.open_transaction().unwrap().changes, 3);
        let out = assembler.feed(vec![stream_commit(TOP)]).unwrap();
        assert_eq!(
            committed_values(out),
            vec![(
                TOP,
                vec![
                    "before".to_string(),
                    "released savepoint".to_string(),
                    "after".to_string()
                ]
            )]
        );
    }

    #[test]
    fn aborting_a_subtransaction_that_wrote_nothing_changes_nothing() {
        let mut assembler = assembler();
        assembler
            .feed(vec![start(TOP, true), streamed_insert(TOP, "a"), stop()])
            .unwrap();
        assembler.feed(vec![stream_abort(TOP, 999)]).unwrap();
        let out = assembler.feed(vec![stream_commit(TOP)]).unwrap();
        assert_eq!(committed_values(out), vec![(TOP, vec!["a".to_string()])]);
    }

    #[test]
    fn interleaved_streams_are_assembled_apart() {
        let mut assembler = assembler();
        let out = assembler
            .feed(vec![
                start(1, true),
                streamed_insert(1, "one"),
                stop(),
                start(2, true),
                streamed_insert(2, "two"),
                stop(),
                start(1, false),
                streamed_insert(1, "one again"),
                stop(),
                stream_commit(2),
                stream_commit(1),
            ])
            .unwrap();
        assert_eq!(
            committed_values(out),
            vec![
                (2, vec!["two".to_string()]),
                (1, vec!["one".to_string(), "one again".to_string()])
            ]
        );
    }

    #[test]
    fn a_first_segment_replaces_what_an_earlier_connection_held() {
        let mut assembler = assembler();
        assembler
            .feed(vec![
                start(TOP, true),
                streamed_insert(TOP, "stale"),
                stop(),
            ])
            .unwrap();
        let out = assembler
            .feed(vec![
                start(TOP, true),
                streamed_insert(TOP, "fresh"),
                stop(),
                stream_commit(TOP),
            ])
            .unwrap();
        assert_eq!(
            committed_values(out),
            vec![(TOP, vec!["fresh".to_string()])]
        );
    }

    #[test]
    fn a_segment_whose_start_was_never_received_is_an_error() {
        let error = assembler()
            .feed(vec![start(TOP, false)])
            .err()
            .expect("an orphan segment cannot be completed");
        assert!(error.to_string().contains("never received"), "{error}");
    }

    #[test]
    fn a_commit_for_a_transaction_never_streamed_is_an_error() {
        let error = assembler()
            .feed(vec![stream_commit(TOP)])
            .err()
            .expect("committing unseen changes would lose them");
        assert!(error.to_string().contains("never streamed"), "{error}");
    }

    #[test]
    fn messages_outside_a_stream_pass_through_unchanged() {
        let mut assembler = assembler();
        let begin = msg(vec![b'B', 1, 2, 3]);
        let out = assembler
            .feed(vec![
                begin,
                start(TOP, true),
                streamed_insert(TOP, "held"),
                stop(),
                msg(v1_insert("plain")),
            ])
            .unwrap();
        let passed: Vec<_> = out
            .into_iter()
            .map(|item| match item {
                Assembled::Message(message) => message.data,
                Assembled::Committed(_) => panic!("nothing committed"),
            })
            .collect();
        assert_eq!(passed, vec![vec![b'B', 1, 2, 3], v1_insert("plain")]);
    }

    #[test]
    fn past_the_memory_budget_a_transaction_spills_and_replays_from_disk() {
        let directory = tempfile::tempdir().unwrap();
        let mut assembler = StreamAssembler::new(64, directory.path().to_path_buf());
        let mut items = vec![start(TOP, true)];
        items.extend((0..20).map(|i| streamed_insert(TOP, &format!("row {i}"))));
        items.push(streamed_insert(701, "rolled back"));
        items.push(stop());
        items.push(stream_abort(TOP, 701));
        assembler.feed(items).unwrap();

        let open = assembler.open_transaction().unwrap();
        assert!(open.spilled, "{open:?}");
        assert_eq!(open.changes, 20);
        let totals = assembler.totals();
        assert_eq!((totals.open, totals.memory_bytes), (1, 0));
        assert!(totals.spilled_bytes > 0);
        assert_eq!(
            std::fs::read_dir(directory.path()).unwrap().count(),
            0,
            "a spill file is anonymous: nothing to clean up after a crash"
        );

        let out = assembler.feed(vec![stream_commit(TOP)]).unwrap();
        let expected: Vec<String> = (0..20).map(|i| format!("row {i}")).collect();
        assert_eq!(committed_values(out), vec![(TOP, expected)]);
    }

    #[test]
    fn a_spill_directory_that_cannot_be_written_is_an_error_not_a_loss() {
        let mut assembler = StreamAssembler::new(1, PathBuf::from("/nonexistent/rustcdc-spill"));
        let error = assembler
            .feed(vec![start(TOP, true), streamed_insert(TOP, "a")])
            .err()
            .expect("a failed spill must stop the pipeline");
        assert!(error.to_string().contains("Nothing was dropped"), "{error}");
        assert!(probe_spill_directory(std::path::Path::new("/nonexistent/rustcdc-spill")).is_err());
        assert!(probe_spill_directory(&std::env::temp_dir()).is_ok());
    }
}
