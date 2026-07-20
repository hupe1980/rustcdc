use crate::{
    core::{
        Error, Event, Operation, Result, SourceMetadata, TransactionMetadata,
        EVENT_ENVELOPE_VERSION,
    },
    ddl_capture::CapturedDdl,
    schema_history::{ColumnDef, TableSchema},
    source::{helpers::now_millis, table_is_allowed},
};

use super::decoder::{
    decode_pgoutput_message, PgDelete, PgInsert, PgOutputMessage, PgOutputXLogData, PgRelation,
    PgTruncate, PgUpdate, PgValue,
};
use super::{format_pg_lsn, pg_timestamp_to_millis, PostgresStreamHandle};

/// Resolve a PostgreSQL built-in type OID to its canonical type name.
///
/// Covers the ~50 most common built-in OIDs (from `pg_type` in PostgreSQL 16).
/// Unknown OIDs fall back to `"pg_type_oid:<N>"` so existing behaviour is preserved.
fn pg_type_name(oid: u32) -> String {
    match oid {
        16 => "bool".into(),
        17 => "bytea".into(),
        18 => "char".into(),
        19 => "name".into(),
        20 => "int8".into(),
        21 => "int2".into(),
        23 => "int4".into(),
        25 => "text".into(),
        26 => "oid".into(),
        700 => "float4".into(),
        701 => "float8".into(),
        790 => "money".into(),
        869 => "inet".into(),
        650 => "cidr".into(),
        829 => "macaddr".into(),
        774 => "macaddr8".into(),
        1000 => "_bool".into(),
        1001 => "_bytea".into(),
        1002 => "_char".into(),
        1005 => "_int2".into(),
        1007 => "_int4".into(),
        1009 => "_text".into(),
        1014 => "_bpchar".into(),
        1015 => "_varchar".into(),
        1016 => "_int8".into(),
        1017 => "_point".into(),
        1021 => "_float4".into(),
        1022 => "_float8".into(),
        1042 => "bpchar".into(),
        1043 => "varchar".into(),
        1082 => "date".into(),
        1083 => "time".into(),
        1114 => "timestamp".into(),
        1115 => "_timestamp".into(),
        1184 => "timestamptz".into(),
        1185 => "_timestamptz".into(),
        1186 => "interval".into(),
        1187 => "_interval".into(),
        1231 => "_numeric".into(),
        1266 => "timetz".into(),
        1560 => "bit".into(),
        1562 => "varbit".into(),
        1700 => "numeric".into(),
        2278 => "void".into(),
        2950 => "uuid".into(),
        2951 => "_uuid".into(),
        3802 => "jsonb".into(),
        3807 => "_jsonb".into(),
        114 => "json".into(),
        199 => "_json".into(),
        142 => "xml".into(),
        143 => "_xml".into(),
        3614 => "tsvector".into(),
        3615 => "tsquery".into(),
        600 => "point".into(),
        601 => "lseg".into(),
        602 => "path".into(),
        603 => "box".into(),
        604 => "polygon".into(),
        718 => "circle".into(),
        _ => format!("pg_type_oid:{oid}"),
    }
}

impl PostgresStreamHandle {
    /// Decode a pgoutput tuple into a JSON object.
    ///
    /// Returns `Err` rather than `None` for both failure modes. Both used to be
    /// silent: an unknown relation OID made the caller discard the whole event with
    /// no warning and no counter, and a column-count overflow collapsed every extra
    /// column onto one key. A missing RELATION is a protocol violation, not a
    /// filterable condition — pgoutput is required to send RELATION before any row
    /// referencing it — so the only safe response to either is to stop.
    /// Returns the decoded row plus the names of any columns the source could not
    /// supply (PostgreSQL unchanged-TOAST). Those columns are absent from the row, and
    /// the caller must surface them on the event so a consumer does not mistake
    /// "unavailable" for NULL and overwrite a value that never changed.
    fn tuple_to_json(
        &self,
        relation_oid: u32,
        values: &[PgValue],
    ) -> Result<(serde_json::Value, Vec<String>)> {
        let relation = self.relation_map.get(&relation_oid).ok_or_else(|| {
            Error::SourceError(format!(
                "postgres row event references relation oid {relation_oid} for which no \
                 RELATION message has been seen. pgoutput guarantees RELATION precedes \
                 any row referencing it, so the decoder state is inconsistent and the row \
                 cannot be attributed to a table. Dropping it would lose data silently. \
                 Restart the connector to rebuild the relation cache."
            ))
        })?;
        let mut map = serde_json::Map::new();
        let mut unavailable = Vec::new();
        for (i, value) in values.iter().enumerate() {
            // A tuple with more columns than the cached RELATION means our schema view
            // is stale — the table gained a column and we missed (or have not yet
            // processed) the new RELATION message.
            //
            // This used to fall back to the literal name `"?"`. Because the row is
            // assembled into a `serde_json::Map`, *every* overflow column collapsed
            // onto that one key and overwrote the previous one — silent, unlogged data
            // destruction. Failing is the only safe response: the alternative is
            // emitting a row that claims to be complete while having quietly discarded
            // columns.
            let Some(column) = relation.columns.get(i) else {
                return Err(Error::SourceError(format!(
                    "postgres tuple for relation '{}.{}' (oid {}) has {} values but the \
                     cached schema has only {} columns. The table's schema changed and \
                     this connector's RELATION cache is stale. Emitting the row would \
                     silently drop the extra columns. Restart the connector to re-read \
                     the relation metadata.",
                    relation.namespace,
                    relation.name,
                    relation.oid,
                    values.len(),
                    relation.columns.len()
                )));
            };
            let col_name = column.name.as_str();
            match value {
                PgValue::Null => {
                    map.insert(col_name.to_string(), serde_json::Value::Null);
                }
                PgValue::Text(text) => {
                    map.insert(
                        col_name.to_string(),
                        serde_json::Value::String(text.clone()),
                    );
                }
                PgValue::Unchanged => {
                    // Unchanged TOASTed value: PostgreSQL did not put it in the WAL, so
                    // we do not have it and cannot get it. Omit the key and record the
                    // column so the consumer can tell "absent because unavailable" from
                    // "absent because NULL".
                    unavailable.push(col_name.to_string());
                }
            }
        }
        Ok((serde_json::Value::Object(map), unavailable))
    }

    /// The **bare** table name, never schema-qualified.
    ///
    /// `Event::schema` carries the namespace separately and
    /// `Event::qualified_table_name()` joins the two. Embedding the namespace here as
    /// well produced `tenant2.tenant2.users` for any non-`public` schema — a name no
    /// route pattern an operator would write can ever match, so every event from a
    /// non-public schema fell through to the default sink or was dropped.
    fn relation_table_name(&self, relation_oid: u32) -> String {
        self.relation_map
            .get(&relation_oid)
            .map(|r| r.name.clone())
            .unwrap_or_else(|| format!("unknown_{relation_oid}"))
    }

    fn relation_schema(&self, relation_oid: u32) -> Option<String> {
        self.relation_map
            .get(&relation_oid)
            .map(|r| r.namespace.clone())
    }

    fn relation_primary_key(&self, relation_oid: u32) -> Option<Vec<String>> {
        let relation = self.relation_map.get(&relation_oid)?;
        let keys: Vec<String> = relation
            .columns
            .iter()
            .filter(|c| c.is_key())
            .map(|c| c.name.clone())
            .collect();
        if keys.is_empty() {
            None
        } else {
            Some(keys)
        }
    }

    fn tx_meta(&self) -> Option<TransactionMetadata> {
        self.current_xid.map(|xid| TransactionMetadata {
            tx_id: u64::from(xid),
            total_events: None,
            event_index: self.partial_tx_events.len() as u32,
        })
    }

    fn source_meta(&self, lsn: u64) -> SourceMetadata {
        SourceMetadata {
            source_name: self.source_name.clone(),
            offset: format_pg_lsn(lsn),
            timestamp: self.current_commit_ts,
        }
    }

    fn build_insert_event(&self, insert: &PgInsert, lsn: u64) -> Result<Event> {
        let (after, unavailable_columns) =
            self.tuple_to_json(insert.relation_oid, &insert.new_tuple)?;
        Ok(Event {
            before: None,
            after: Some(after),
            op: Operation::Insert,
            before_unavailable_columns: Vec::new(),
            source: self.source_meta(lsn),
            ts: self.current_commit_ts,
            schema: self.relation_schema(insert.relation_oid),
            table: self.relation_table_name(insert.relation_oid),
            primary_key: self.relation_primary_key(insert.relation_oid),
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only: false,
            unavailable_columns,
        })
    }

    fn build_update_event(&self, update: &PgUpdate, lsn: u64) -> Result<Event> {
        let (after, unavailable_columns) =
            self.tuple_to_json(update.relation_oid, &update.new_tuple)?;

        // If `old_tuple` is present we have the full pre-image (REPLICA IDENTITY FULL).
        // Otherwise fall back to `key_tuple` which contains only PK columns
        // (REPLICA IDENTITY DEFAULT). In the fallback case we set `before_is_key_only`
        // so consumers know not to treat the before image as a complete row.
        let (before, before_is_key_only, mut before_unavailable_columns) =
            match update.old_tuple.as_deref() {
                Some(tuple) => {
                    // With REPLICA IDENTITY FULL the before-image has TOAST holes of its
                    // own, and they are NOT the same set as the after-image's. A TOASTed
                    // column that *was* modified arrives present in `after` and `'u'` in
                    // `before`. Merging the two lists would mark that column unavailable,
                    // and a correct sink would then skip writing a value that genuinely
                    // changed — silent data loss. Keep them separate.
                    let (before, before_unavailable) =
                        self.tuple_to_json(update.relation_oid, tuple)?;
                    (Some(before), false, before_unavailable)
                }
                None => match update.key_tuple.as_deref() {
                    // A key-only before-image omits non-key columns by design. Reporting
                    // them as TOAST holes would conflate two different kinds of absence.
                    Some(tuple) => (
                        Some(self.tuple_to_json(update.relation_oid, tuple)?.0),
                        true,
                        Vec::new(),
                    ),
                    None => (None, false, Vec::new()),
                },
            };
        before_unavailable_columns.sort_unstable();
        before_unavailable_columns.dedup();

        Ok(Event {
            before,
            after: Some(after),
            op: Operation::Update,
            unavailable_columns,
            before_unavailable_columns,
            source: self.source_meta(lsn),
            ts: self.current_commit_ts,
            schema: self.relation_schema(update.relation_oid),
            table: self.relation_table_name(update.relation_oid),
            primary_key: self.relation_primary_key(update.relation_oid),
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only,
        })
    }

    fn build_delete_event(&self, delete: &PgDelete, lsn: u64) -> Result<Event> {
        // A DELETE carries no after-image, so every TOAST hole here belongs to `before`.
        // Reporting them in `unavailable_columns` would describe a payload that does not
        // exist, and hide the gap from the consumers that actually read the pre-image.
        let (before, before_is_key_only, before_unavailable_columns) =
            match delete.old_tuple.as_deref() {
                Some(tuple) => {
                    let (before, unavailable) = self.tuple_to_json(delete.relation_oid, tuple)?;
                    (Some(before), false, unavailable)
                }
                None => match delete.key_tuple.as_deref() {
                    Some(tuple) => (
                        Some(self.tuple_to_json(delete.relation_oid, tuple)?.0),
                        true,
                        Vec::new(),
                    ),
                    None => (None, false, Vec::new()),
                },
            };
        Ok(Event {
            before,
            after: None,
            op: Operation::Delete,
            unavailable_columns: Vec::new(),
            before_unavailable_columns,
            source: self.source_meta(lsn),
            ts: self.current_commit_ts,
            schema: self.relation_schema(delete.relation_oid),
            table: self.relation_table_name(delete.relation_oid),
            primary_key: self.relation_primary_key(delete.relation_oid),
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            before_is_key_only,
        })
    }

    fn build_truncate_events(&self, truncate: &PgTruncate, lsn: u64) -> Vec<Event> {
        truncate
            .relation_oids
            .iter()
            .map(|&oid| Event {
                before: None,
                after: None,
                op: Operation::Truncate,
                source: self.source_meta(lsn),
                ts: self.current_commit_ts,
                schema: self.relation_schema(oid),
                table: self.relation_table_name(oid),
                primary_key: None,
                snapshot: None,
                transaction: self.tx_meta(),
                envelope_version: EVENT_ENVELOPE_VERSION,
                before_is_key_only: false,
                unavailable_columns: Vec::new(),
                before_unavailable_columns: Vec::new(),
            })
            .collect()
    }

    fn relation_to_table_schema(relation: &PgRelation) -> TableSchema {
        let primary_keys: Vec<String> = relation
            .columns
            .iter()
            .filter(|column| column.is_key())
            .map(|column| column.name.clone())
            .collect();

        let columns = relation
            .columns
            .iter()
            .map(|column| {
                let mut constraints = Vec::new();
                if column.is_key() {
                    constraints.push("primary_key".to_string());
                }
                ColumnDef {
                    name: column.name.clone(),
                    data_type: pg_type_name(column.type_oid),
                    nullable: !column.is_key(),
                    constraints,
                }
            })
            .collect();

        TableSchema {
            schema: relation.namespace.clone(),
            table: relation.name.clone(),
            columns,
            primary_keys,
            version: 0,
        }
    }

    fn build_relation_schema_change_event(&self, relation: &PgRelation, lsn: u64) -> Event {
        let ts_ms = if self.current_commit_ts == 0 {
            now_millis()
        } else {
            self.current_commit_ts
        };
        let captured = CapturedDdl {
            ddl_type: "ALTER_TABLE".to_string(),
            schema: relation.namespace.clone(),
            table: relation.name.clone(),
            statement: format!(
                "ALTER TABLE {}.{} /* derived from pgoutput RELATION metadata */",
                relation.namespace, relation.name
            ),
            result_schema: Some(Self::relation_to_table_schema(relation)),
            schema_diff: None,
            ts: ts_ms,
        };
        captured.to_event(&self.source_name, format_pg_lsn(lsn), ts_ms)
    }

    pub(super) async fn process_messages(
        &mut self,
        xlog_data: Vec<PgOutputXLogData>,
    ) -> Result<Vec<Event>> {
        let mut committed: Vec<Event> = Vec::new();
        for item in xlog_data {
            let msg = decode_pgoutput_message(&item.data)?;
            match msg {
                PgOutputMessage::Begin(begin) => {
                    if self.current_xid.is_some() {
                        tracing::warn!(
                            target: "rustcdc::source::postgres",
                            prev_xid = ?self.current_xid,
                            new_xid = begin.xid,
                            partial_events_discarded = self.partial_tx_events.len(),
                            "received BEGIN while a transaction was already in-flight; \
                             discarding partial events — possible stream reset or protocol edge case",
                        );
                    }
                    self.current_xid = Some(begin.xid);
                    self.current_commit_ts = pg_timestamp_to_millis(begin.commit_timestamp_us);
                    self.partial_tx_events.clear();
                }
                PgOutputMessage::Commit(commit) => {
                    self.stream.lsn_position = commit.end_lsn;
                    let total = self.partial_tx_events.len() as u32;
                    for event in &mut self.partial_tx_events {
                        if let Some(tx) = event.transaction.as_mut() {
                            tx.total_events = Some(total);
                        }
                    }
                    self.events_polled += u64::from(total);
                    tracing::trace!(
                        target: "rustcdc::source::postgres",
                        tx_id = self.current_xid,
                        commit_lsn = commit.end_lsn,
                        event_count = total,
                        "postgres transaction committed",
                    );
                    committed.append(&mut self.partial_tx_events);
                    self.current_xid = None;
                    self.current_commit_ts = 0;
                }
                PgOutputMessage::Relation(rel) => {
                    let changed = self
                        .relation_map
                        .get(&rel.oid)
                        .map(|existing| existing != &rel)
                        .unwrap_or(false);

                    // Warn once per relation about a REPLICA IDENTITY that cannot
                    // identify a row.
                    //
                    // `replica_identity` was decoded and then read nowhere, so
                    // `NOTHING` went entirely undetected: UPDATE and DELETE arrive with
                    // no key and no old tuple, and the resulting event has
                    // `before: None, after: None` — it names a table but identifies no
                    // row, and a consumer cannot apply it to anything. PostgreSQL also
                    // treats `DEFAULT` on a table with no primary key as `NOTHING`.
                    if !self.warned_replica_identity.contains(&rel.oid) {
                        // pgoutput encodes this as the pg_class.relreplident char.
                        let has_key = rel.columns.iter().any(|column| column.is_key());
                        match rel.replica_identity {
                            b'n' => {
                                tracing::warn!(
                                    target: "rustcdc::source::postgres",
                                    table = %format!("{}.{}", rel.namespace, rel.name),
                                    "table has REPLICA IDENTITY NOTHING: UPDATE and DELETE \
                                     events will carry neither a key nor a before-image, so \
                                     they identify no row and cannot be applied downstream. \
                                     Fix with: ALTER TABLE {}.{} REPLICA IDENTITY FULL \
                                     (or DEFAULT with a primary key).",
                                    rel.namespace, rel.name,
                                );
                                self.warned_replica_identity.insert(rel.oid);
                            }
                            b'd' if !has_key => {
                                tracing::warn!(
                                    target: "rustcdc::source::postgres",
                                    table = %format!("{}.{}", rel.namespace, rel.name),
                                    "table has REPLICA IDENTITY DEFAULT but no primary key, \
                                     which PostgreSQL treats as NOTHING: UPDATE and DELETE \
                                     events will identify no row. Fix with: ALTER TABLE \
                                     {}.{} REPLICA IDENTITY FULL, or add a primary key.",
                                    rel.namespace, rel.name,
                                );
                                self.warned_replica_identity.insert(rel.oid);
                            }
                            _ => {}
                        }
                    }

                    self.relation_map.insert(rel.oid, rel.clone());

                    if changed {
                        let mut schema_event =
                            self.build_relation_schema_change_event(&rel, item.lsn);
                        if self.current_xid.is_some() {
                            schema_event.transaction = self.tx_meta();
                            self.partial_tx_events.push(schema_event);
                        } else {
                            self.events_polled = self.events_polled.saturating_add(1);
                            committed.push(schema_event);
                        }
                    }
                }
                PgOutputMessage::Insert(insert) => {
                    let schema = self.relation_schema(insert.relation_oid);
                    let table = self.relation_table_name(insert.relation_oid);
                    if table_is_allowed(
                        schema.as_deref(),
                        &table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        {
                            let event = self.build_insert_event(&insert, item.lsn)?;
                            self.partial_tx_events.push(event);
                        }
                    }
                }
                PgOutputMessage::Update(update) => {
                    let schema = self.relation_schema(update.relation_oid);
                    let table = self.relation_table_name(update.relation_oid);
                    if table_is_allowed(
                        schema.as_deref(),
                        &table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        {
                            let event = self.build_update_event(&update, item.lsn)?;
                            self.partial_tx_events.push(event);
                        }
                    }
                }
                PgOutputMessage::Delete(delete) => {
                    let schema = self.relation_schema(delete.relation_oid);
                    let table = self.relation_table_name(delete.relation_oid);
                    if table_is_allowed(
                        schema.as_deref(),
                        &table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        {
                            let event = self.build_delete_event(&delete, item.lsn)?;
                            self.partial_tx_events.push(event);
                        }
                    }
                }
                PgOutputMessage::Truncate(truncate) => {
                    let events = self.build_truncate_events(&truncate, item.lsn);
                    for event in events {
                        if table_is_allowed(
                            event.schema.as_deref(),
                            &event.table,
                            &self.table_include_list,
                            &self.table_exclude_list,
                        ) {
                            self.partial_tx_events.push(event);
                        }
                    }
                }
                PgOutputMessage::Unknown(tag) => {
                    // Not every unhandled tag is equally safe to skip.
                    //
                    // The connector negotiates `proto_version '1'`, under which the
                    // server must not send v2 streaming or v3 two-phase messages. If one
                    // arrives anyway, our view of transaction boundaries is wrong, and
                    // silently skipping is dangerous in a specific way: dropping a
                    // Stream Abort ('A') means we commit data the source rolled back.
                    // Treat those as protocol violations.
                    match tag {
                        // v2 streaming: Stream Start/Stop/Commit/Abort, Stream Prepare.
                        // v3 two-phase: Begin Prepare, Prepare, Commit Prepared,
                        // Rollback Prepared.
                        b'S' | b'E' | b'c' | b'A' | b'p' | b'b' | b'P' | b'K' | b'r' => {
                            return Err(Error::SourceError(format!(
                                "postgres sent pgoutput message '{}' (0x{tag:02x}), which \
                                 belongs to protocol version 2 or 3, but this connector \
                                 negotiated proto_version 1 and cannot interpret it. \
                                 Skipping it would misrepresent transaction boundaries — \
                                 and skipping a Stream Abort would commit data the source \
                                 rolled back. This indicates a server/plugin mismatch.",
                                tag as char
                            )));
                        }
                        // Informational tags that are genuinely safe to skip, but should
                        // not be silent: Origin ('O') matters for loop detection in
                        // bidirectional setups, Type ('Y') carries custom-type identity,
                        // and Message ('M') is `pg_logical_emit_message` output.
                        other => {
                            if self.warned_unknown_messages.insert(other) {
                                tracing::warn!(
                                    target: "rustcdc::source::postgres",
                                    tag = %(other as char),
                                    "ignoring unhandled pgoutput message type; \
                                     'O' = Origin (bidirectional loop detection), \
                                     'Y' = Type (custom type identity), \
                                     'M' = logical decoding message. These are not \
                                     surfaced as events.",
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(committed)
    }
}
