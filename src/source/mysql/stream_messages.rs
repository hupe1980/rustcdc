use crate::core::{Event, Operation, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION};

use super::{
    parser::format_mysql_source_offset, MysqlBinlogMessage, MysqlRowChange, MysqlStreamHandle,
};

impl MysqlStreamHandle {
    fn tx_meta(&self) -> Option<TransactionMetadata> {
        self.current_tx_id.map(|tx_id| TransactionMetadata {
            tx_id,
            total_events: 0,
            event_index: self.partial_tx_events.len() as u32,
        })
    }

    fn source_meta(&self) -> SourceMetadata {
        SourceMetadata {
            source_name: self.source_name.clone(),
            offset: format!("{}:{}", self.stream.binlog_file, self.stream.binlog_pos),
            timestamp: self.current_commit_ts,
        }
    }

    fn build_event(&self, op: Operation, change: MysqlRowChange) -> Event {
        Event {
            before: change.before,
            after: change.after,
            op,
            source: self.source_meta(),
            ts: self.current_commit_ts,
            schema: change.schema,
            table: change.table,
            primary_key: change.primary_key,
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
        }
    }

    fn commit_current_transaction(
        &mut self,
        tx_id: u64,
        timestamp_ms: u64,
        binlog_file: String,
        binlog_pos: u32,
        gtid: Option<String>,
    ) -> Vec<Event> {
        self.current_commit_ts = timestamp_ms;
        self.current_tx_id = Some(tx_id);
        let effective_gtid = gtid.clone().unwrap_or_else(|| self.stream.gtid.clone());
        let total = self.partial_tx_events.len() as u32;
        for (index, event) in self.partial_tx_events.iter_mut().enumerate() {
            if let Some(tx) = event.transaction.as_mut() {
                tx.total_events = total;
                tx.event_index = index as u32;
            }
            event.ts = timestamp_ms;
            event.source.timestamp = timestamp_ms;
            event.source.offset =
                format_mysql_source_offset(&binlog_file, binlog_pos, &effective_gtid);
        }

        self.stream.binlog_file = binlog_file;
        self.stream.binlog_pos = binlog_pos;
        self.stream.gtid = effective_gtid;

        self.events_polled = self.events_polled.saturating_add(u64::from(total));
        self.current_tx_id = None;
        self.current_commit_ts = 0;

        std::mem::take(&mut self.partial_tx_events)
    }

    pub(super) fn process_messages(&mut self, messages: Vec<MysqlBinlogMessage>) -> Vec<Event> {
        let mut committed = Vec::new();
        for message in messages {
            match message {
                MysqlBinlogMessage::Begin {
                    tx_id,
                    timestamp_ms,
                } => {
                    self.current_tx_id = Some(tx_id);
                    self.current_commit_ts = timestamp_ms;
                    self.partial_tx_events.clear();
                }
                MysqlBinlogMessage::WriteRows(change) => {
                    self.partial_tx_events
                        .push(self.build_event(Operation::Insert, change));
                }
                MysqlBinlogMessage::UpdateRows(change) => {
                    self.partial_tx_events
                        .push(self.build_event(Operation::Update, change));
                }
                MysqlBinlogMessage::DeleteRows(change) => {
                    self.partial_tx_events
                        .push(self.build_event(Operation::Delete, change));
                }
                MysqlBinlogMessage::Xid {
                    tx_id,
                    timestamp_ms,
                    binlog_file,
                    binlog_pos,
                    gtid,
                } => {
                    committed.extend(self.commit_current_transaction(
                        tx_id,
                        timestamp_ms,
                        binlog_file,
                        binlog_pos,
                        gtid,
                    ));
                }
                MysqlBinlogMessage::Rotate {
                    binlog_file,
                    binlog_pos,
                } => {
                    self.stream.binlog_file = binlog_file;
                    self.stream.binlog_pos = binlog_pos;
                }
                MysqlBinlogMessage::Gtid { gtid } => {
                    self.stream.gtid = gtid;
                }
                MysqlBinlogMessage::Ddl {
                    captured,
                    timestamp_ms,
                    binlog_file,
                    binlog_pos,
                } => {
                    if !self.partial_tx_events.is_empty() {
                        if let Some(tx_id) = self.current_tx_id {
                            committed.extend(self.commit_current_transaction(
                                tx_id,
                                timestamp_ms,
                                binlog_file.clone(),
                                binlog_pos,
                                None,
                            ));
                        } else {
                            self.partial_tx_events.clear();
                        }
                    }

                    self.stream.binlog_file = binlog_file;
                    self.stream.binlog_pos = binlog_pos;
                    let offset = format_mysql_source_offset(
                        &self.stream.binlog_file,
                        self.stream.binlog_pos,
                        &self.stream.gtid,
                    );
                    committed.push(captured.to_event(&self.source_name, offset, timestamp_ms));
                    self.events_polled = self.events_polled.saturating_add(1);
                }
                MysqlBinlogMessage::Heartbeat => {}
            }
        }
        committed
    }
}
