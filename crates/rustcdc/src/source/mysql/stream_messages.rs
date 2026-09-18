use crate::{
    core::{
        BeforeImage, EVENT_ENVELOPE_VERSION, Event, Operation, SourceMetadata, TransactionMetadata,
    },
    ddl_capture::{CapturedDdl, DDL_TYPE_READ_SCHEMA},
    source::{
        schema_catalog::{observed_statement, table_schema_from_catalog},
        table_is_allowed,
    },
};

use super::{
    MysqlBinlogMessage, MysqlRowChange, MysqlStreamHandle, parser::format_mysql_source_offset,
    query::merge_gtid_into_set,
};

impl MysqlStreamHandle {
    fn tx_meta(&self) -> Option<TransactionMetadata> {
        self.current_tx_id.map(|tx_id| TransactionMetadata {
            tx_id,
            total_events: None,
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

    fn source_meta_at(
        &self,
        binlog_file: &str,
        binlog_pos: u32,
        timestamp_ms: u64,
    ) -> SourceMetadata {
        SourceMetadata {
            source_name: self.source_name.clone(),
            offset: format_mysql_source_offset(binlog_file, binlog_pos, &self.stream.gtid),
            timestamp: timestamp_ms,
        }
    }

    fn build_event(&self, op: Operation, change: MysqlRowChange) -> Event {
        Event {
            // A row-based binlog event carries the whole prior row or none of it — MySQL
            // has no key-only pre-image, so there is no third case to distinguish here.
            before: change
                .before
                .map_or(BeforeImage::Unavailable, BeforeImage::full),
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
            unavailable_columns: Vec::new(),
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
                tx.total_events = Some(total);
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

    /// Announce a table's schema the first time this run sees a row for it.
    ///
    /// MySQL's schema-change events come from DDL statements parsed out of the binlog, so
    /// a table whose `CREATE TABLE` ran before capture started had no schema event at all —
    /// and its rows carry text values. This closes that, from the catalog read at stream
    /// start rather than from the table map; see
    /// [`query_database_column_types`](super::query::query_database_column_types).
    ///
    /// Returns `None` for a table the catalog has nothing for, which is a table created
    /// after the stream started. That case needs nothing: its `CREATE TABLE` is in the
    /// binlog and arrives as its own schema-change event.
    fn announce_table_if_new(&mut self, schema: Option<&str>, table: &str) -> Option<Event> {
        let database = schema?.to_string();
        let key = (database.clone(), table.to_string());
        if self.announced_tables.contains(&key) {
            return None;
        }
        let columns = self.catalog_columns.get(&key)?.clone();
        self.announced_tables.insert(key.clone());

        let primary_keys = self
            .catalog_primary_keys
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let ts_ms = if self.current_commit_ts == 0 {
            crate::source::helpers::now_millis()
        } else {
            self.current_commit_ts
        };
        let captured = CapturedDdl {
            ddl_type: DDL_TYPE_READ_SCHEMA.to_string(),
            schema: database.clone(),
            table: table.to_string(),
            statement: observed_statement(&database, table, "information_schema"),
            result_schema: Some(table_schema_from_catalog(
                &database,
                table,
                &columns,
                &primary_keys,
            )),
            schema_diff: None,
            ts: ts_ms,
        };
        let offset = format_mysql_source_offset(
            &self.stream.binlog_file,
            self.stream.binlog_pos,
            &self.stream.gtid,
        );
        let mut event = captured.to_event(&self.source_name, offset, ts_ms);
        if self.current_tx_id.is_some() {
            event.transaction = self.tx_meta();
        }
        Some(event)
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
                    if table_is_allowed(
                        change.schema.as_deref(),
                        &change.table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        // Before the row, never after: a consumer that receives the row
                        // first has already had to decide how to decode its text values.
                        if let Some(schema_event) =
                            self.announce_table_if_new(change.schema.as_deref(), &change.table)
                        {
                            self.partial_tx_events.push(schema_event);
                        }
                        self.partial_tx_events
                            .push(self.build_event(Operation::Insert, change));
                    }
                }
                MysqlBinlogMessage::UpdateRows(change) => {
                    if table_is_allowed(
                        change.schema.as_deref(),
                        &change.table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        // Before the row, never after: a consumer that receives the row
                        // first has already had to decide how to decode its text values.
                        if let Some(schema_event) =
                            self.announce_table_if_new(change.schema.as_deref(), &change.table)
                        {
                            self.partial_tx_events.push(schema_event);
                        }
                        self.partial_tx_events
                            .push(self.build_event(Operation::Update, change));
                    }
                }
                MysqlBinlogMessage::DeleteRows(change) => {
                    if table_is_allowed(
                        change.schema.as_deref(),
                        &change.table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        // Before the row, never after: a consumer that receives the row
                        // first has already had to decide how to decode its text values.
                        if let Some(schema_event) =
                            self.announce_table_if_new(change.schema.as_deref(), &change.table)
                        {
                            self.partial_tx_events.push(schema_event);
                        }
                        self.partial_tx_events
                            .push(self.build_event(Operation::Delete, change));
                    }
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
                    // Union, never overwrite — see `merge_gtid_into_set`. Replacing the
                    // executed set with a single GTID makes the checkpoint claim the
                    // replica has executed only that one transaction, so resuming from
                    // it replays everything before it.
                    self.stream.gtid = merge_gtid_into_set(&self.stream.gtid, &gtid);
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
                    // The position advances whether or not the statement is emitted — a
                    // filtered DDL still moved the binlog on, and a checkpoint that
                    // pretended otherwise would replay it.
                    //
                    // The event itself is subject to the same include/exclude lists as
                    // every row event. It used to bypass them, so an operator who
                    // allow-listed `app.orders` still received `ALTER TABLE` statements —
                    // full column lists included — for every other table on the server.
                    if table_is_allowed(
                        Some(captured.schema.as_str()),
                        &captured.table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        let offset = format_mysql_source_offset(
                            &self.stream.binlog_file,
                            self.stream.binlog_pos,
                            &self.stream.gtid,
                        );
                        committed.push(captured.to_event(&self.source_name, offset, timestamp_ms));
                        self.events_polled = self.events_polled.saturating_add(1);
                    }
                }
                MysqlBinlogMessage::Truncate {
                    schema,
                    table,
                    timestamp_ms,
                    binlog_file,
                    binlog_pos,
                } => {
                    // TRUNCATE is a DDL statement — it implicitly commits any open tx first.
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

                    self.stream.binlog_file = binlog_file.clone();
                    self.stream.binlog_pos = binlog_pos;

                    if table_is_allowed(
                        schema.as_deref(),
                        &table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        let source = self.source_meta_at(&binlog_file, binlog_pos, timestamp_ms);
                        committed.push(Event {
                            before: BeforeImage::Unavailable,
                            after: None,
                            op: Operation::Truncate,
                            source,
                            ts: timestamp_ms,
                            schema,
                            table,
                            primary_key: None,
                            snapshot: None,
                            transaction: None,
                            envelope_version: EVENT_ENVELOPE_VERSION,
                            unavailable_columns: Vec::new(),
                        });
                        self.events_polled = self.events_polled.saturating_add(1);
                    }
                }
                MysqlBinlogMessage::Heartbeat => {}
            }
        }
        committed
    }
}
