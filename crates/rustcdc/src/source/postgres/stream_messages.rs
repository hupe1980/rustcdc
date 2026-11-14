use crate::{
    core::{
        BeforeImage, EVENT_ENVELOPE_VERSION, Error, Event, Operation, Result, SourceMetadata,
        TransactionMetadata,
    },
    ddl_capture::CapturedDdl,
    schema_history::TableSchema,
    source::{
        helpers::now_millis,
        schema_catalog::{CatalogColumn, table_schema_from_catalog},
        table_is_allowed,
    },
};

use super::decoder::{
    PgDelete, PgInsert, PgLogicalMessage, PgOutputMessage, PgOutputXLogData, PgRelation,
    PgTruncate, PgUpdate, PgValue, decode_pgoutput_message,
};
use super::{PostgresStreamHandle, format_pg_lsn, pg_timestamp_to_millis};

/// Resolve a PostgreSQL built-in type OID to its canonical type name, **from the wire
/// alone**.
///
/// The fallback for a handle without a [`RelationCatalog`](super::relation_catalog::RelationCatalog),
/// which only unit tests build. It covers 60 built-in OIDs and nothing else, and it has no
/// access to the type modifier, so `numeric(12,4)` is reported as `numeric`.
///
/// An OID outside the table yields [`CatalogColumn::UNKNOWN_TYPE`] rather than
/// `pg_type_oid:<N>`. The OID was never usable by a consumer — enum, domain and extension
/// OIDs are installation-specific, so the number identifies nothing portable — and one
/// spelling for "the type could not be read" lets a consumer branch on it across every
/// connector instead of pattern-matching a per-source string.
fn wire_type_name(oid: u32) -> String {
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
        _ => CatalogColumn::UNKNOWN_TYPE.to_string(),
    }
}

impl PostgresStreamHandle {
    /// Decode a pgoutput tuple into a JSON object.
    ///
    /// Returns `Err` rather than `None` for both failure modes, because neither may be
    /// silent: an unknown relation OID would make the caller discard the whole event with
    /// no warning and no counter, and a column-count overflow would collapse every extra
    /// column onto one key. A missing RELATION is a protocol violation, not a filterable
    /// condition — pgoutput is required to send RELATION before any row referencing it —
    /// so the only safe response to either is to stop.
    ///
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
            // More columns than the cached RELATION means our schema view is stale. Fail:
            // any placeholder name would collapse the extra columns onto one JSON key and
            // emit a row that looks complete while it has lost values.
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

    /// The shape of the relation's latest `RELATION` message, built once when it arrived and
    /// shared by its rows.
    fn relation_shape(&self, relation_oid: u32) -> Option<std::sync::Arc<crate::RowShape>> {
        self.relation_shapes.get(&relation_oid).cloned()
    }

    fn relation_primary_key(&self, relation_oid: u32) -> Option<Vec<String>> {
        let relation = self.relation_map.get(&relation_oid)?;
        self.resolve_primary_key(relation)
    }

    /// The table's primary key, never merely its replica identity.
    ///
    /// pgoutput's column flag means "part of the replica identity". Under `DEFAULT` and `INDEX`
    /// that set *is* a row key — the primary key or the nominated unique index — and the flags are
    /// the best available answer. Under `FULL` PostgreSQL flags **every** column, so the flags
    /// describe the whole row rather than a key; the answer then comes from the catalog snapshot
    /// taken at stream start, which is also what the snapshot path uses, keeping one row's key
    /// identical across both phases.
    ///
    /// `None` when there is genuinely no key to report: `NOTHING`, `DEFAULT` on a table without a
    /// primary key, or `FULL` on a table without one. Reporting the full row instead would produce
    /// a "key" that changes with every column and cannot address a row across versions.
    fn resolve_primary_key(&self, relation: &PgRelation) -> Option<Vec<String>> {
        if relation.replica_identity == b'f' {
            let key = self
                .catalog_primary_keys
                .get(&(relation.namespace.clone(), relation.name.clone()))?;
            return (!key.is_empty()).then(|| key.clone());
        }

        let keys: Vec<String> = relation
            .columns
            .iter()
            .filter(|c| c.is_key())
            .map(|c| c.name.clone())
            .collect();
        if keys.is_empty() { None } else { Some(keys) }
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
            before: BeforeImage::Unavailable,
            after: Some(after),
            op: Operation::Insert,
            source: self.source_meta(lsn),
            ts: self.current_commit_ts,
            schema: self.relation_schema(insert.relation_oid),
            table: self.relation_table_name(insert.relation_oid),
            primary_key: self.relation_primary_key(insert.relation_oid),
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            shape: self.relation_shape(insert.relation_oid),
            unavailable_columns,
        })
    }

    fn build_update_event(&self, update: &PgUpdate, lsn: u64) -> Result<Event> {
        let (mut after, mut unavailable_columns) =
            self.tuple_to_json(update.relation_oid, &update.new_tuple)?;

        // The three shapes pgoutput can send map one-to-one onto `BeforeImage`, so the
        // replica identity of the table is read straight off the message rather than
        // inferred downstream from a nullable row plus a flag.
        let before = match update.old_tuple.as_deref() {
            // `O`: REPLICA IDENTITY FULL — a complete pre-image.
            //
            // It has TOAST holes of its own, and they are NOT the same set as the
            // after-image's. A TOASTed column that *was* modified arrives present in
            // `after` and `'u'` in `before`. Merging the two lists would mark that column
            // unavailable, and a correct sink would then skip writing a value that
            // genuinely changed — silent data loss. `BeforeImage` keeps them separate by
            // construction: only `Full` carries a hole list at all.
            //
            // A column `'u'` in the new tuple was not modified, so its new value *is* its old
            // value — and under FULL the old tuple carries that value, from the same WAL
            // record. Completing `after` from it is a copy, not a guess, so a consumer
            // upserting `after` writes the row as it is. A column that is a hole in both
            // images stays reported: there is no value anywhere in the record.
            Some(tuple) => {
                let (row, unavailable) = self.tuple_to_json(update.relation_oid, tuple)?;
                fill_unchanged_from_old_tuple(&mut after, &mut unavailable_columns, &row);
                BeforeImage::full_with_holes(row, unavailable)
            }
            None => match update.key_tuple.as_deref() {
                // `K`: REPLICA IDENTITY DEFAULT and the statement changed a key column.
                // A key-only pre-image omits non-key columns by design, so it carries no
                // hole list — reporting them as TOAST holes would conflate two different
                // kinds of absence.
                Some(tuple) => {
                    BeforeImage::key_only(self.tuple_to_json(update.relation_oid, tuple)?.0)
                }
                // Neither: REPLICA IDENTITY DEFAULT with no key column in the SET list.
                // There is genuinely no pre-image. This is the ordinary shape of an UPDATE
                // on a stock PostgreSQL table, not a defect — the envelope must be able to
                // say so without the runtime rejecting it.
                None => BeforeImage::Unavailable,
            },
        };

        Ok(Event {
            before,
            after: Some(after),
            op: Operation::Update,
            unavailable_columns,
            source: self.source_meta(lsn),
            ts: self.current_commit_ts,
            schema: self.relation_schema(update.relation_oid),
            table: self.relation_table_name(update.relation_oid),
            primary_key: self.relation_primary_key(update.relation_oid),
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            shape: self.relation_shape(update.relation_oid),
        })
    }

    fn build_delete_event(&self, delete: &PgDelete, lsn: u64) -> Result<Event> {
        // A DELETE carries no after-image, so every TOAST hole here belongs to `before`.
        // Reporting them in `unavailable_columns` would describe a payload that does not
        // exist, and hide the gap from the consumers that actually read the pre-image.
        let before = match delete.old_tuple.as_deref() {
            Some(tuple) => {
                let (row, unavailable) = self.tuple_to_json(delete.relation_oid, tuple)?;
                BeforeImage::full_with_holes(row, unavailable)
            }
            None => match delete.key_tuple.as_deref() {
                Some(tuple) => {
                    BeforeImage::key_only(self.tuple_to_json(delete.relation_oid, tuple)?.0)
                }
                // PostgreSQL refuses UPDATE and DELETE on a published table with
                // REPLICA IDENTITY NOTHING, so this arm is unreachable in practice —
                // but the envelope models it rather than inventing a pre-image.
                None => BeforeImage::Unavailable,
            },
        };
        Ok(Event {
            before,
            after: None,
            op: Operation::Delete,
            unavailable_columns: Vec::new(),
            source: self.source_meta(lsn),
            ts: self.current_commit_ts,
            schema: self.relation_schema(delete.relation_oid),
            table: self.relation_table_name(delete.relation_oid),
            primary_key: self.relation_primary_key(delete.relation_oid),
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            shape: self.relation_shape(delete.relation_oid),
        })
    }

    fn build_truncate_events(&self, truncate: &PgTruncate, lsn: u64) -> Vec<Event> {
        truncate
            .relation_oids
            .iter()
            .map(|&oid| Event {
                before: BeforeImage::Unavailable,
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
                // A truncate carries no row, so it names no shape — the rule on every connector.
                shape: None,
                unavailable_columns: Vec::new(),
            })
            .collect()
    }

    /// Fill what `relation_to_table_schema` reads from the server: the declared type of each
    /// of the relation's `(oid, typmod)` pairs, and — when the relation changed or the
    /// stream-start catalogue lacks the table — its nullability and primary key.
    ///
    /// The types come from the message itself, so they are the row's own even when the
    /// stream resumed before an `ALTER COLUMN … TYPE`. Nullability and key are the
    /// catalogue's at the moment of the re-read: pgoutput carries neither.
    async fn resolve_relation_catalog(
        &mut self,
        relation: &PgRelation,
        changed: bool,
    ) -> Result<()> {
        let Some(catalog) = self.relation_catalog.as_ref() else {
            return Ok(());
        };
        let key = (relation.namespace.clone(), relation.name.clone());
        if changed || !self.catalog_columns.contains_key(&key) {
            let (columns, primary_key) = catalog
                .columns_and_key(&relation.namespace, &relation.name)
                .await?;
            self.catalog_columns.insert(key.clone(), columns);
            self.catalog_primary_keys.insert(key, primary_key);
        }

        let mut missing: Vec<(u32, i32)> = Vec::new();
        for column in &relation.columns {
            let pair = (column.type_oid, column.type_modifier);
            if !self.type_names.contains_key(&pair) && !missing.contains(&pair) {
                missing.push(pair);
            }
        }
        if !missing.is_empty() {
            let names = catalog.type_names(&missing).await?;
            self.type_names.extend(missing.into_iter().zip(names));
        }
        Ok(())
    }

    /// Describe a relation: its columns in the message's order, each declared type from the
    /// message's `(oid, typmod)`, nullability from the catalogue.
    ///
    /// `primary_keys` is the resolved key from [`Self::resolve_primary_key`], not the raw
    /// replica-identity flags: under `REPLICA IDENTITY FULL` every column carries the flag.
    ///
    /// A column the catalogue does not describe is reported nullable — the weaker claim,
    /// because a consumer that relaxes a column is recoverable and one that tightens it
    /// rejects rows the source accepted. A type not resolved (no catalogue, unit tests only)
    /// falls back to the built-in OID map, [`CatalogColumn::UNKNOWN_TYPE`] beyond it.
    fn relation_to_table_schema(
        &self,
        relation: &PgRelation,
        primary_keys: Option<&Vec<String>>,
    ) -> TableSchema {
        let primary_keys: Vec<String> = primary_keys.cloned().unwrap_or_default();
        let catalog = self
            .catalog_columns
            .get(&(relation.namespace.clone(), relation.name.clone()));
        let columns: Vec<CatalogColumn> = relation
            .columns
            .iter()
            .map(|column| CatalogColumn {
                name: column.name.clone(),
                data_type: self
                    .type_names
                    .get(&(column.type_oid, column.type_modifier))
                    .cloned()
                    .unwrap_or_else(|| wire_type_name(column.type_oid)),
                nullable: catalog
                    .and_then(|columns| columns.iter().find(|c| c.name == column.name))
                    .is_none_or(|c| c.nullable),
            })
            .collect();
        table_schema_from_catalog(&relation.namespace, &relation.name, &columns, &primary_keys)
    }

    /// Build the event for a logical decoding message.
    ///
    /// # Identity
    ///
    /// The synthetic table is `<prefix>__messages`, mirroring `<table>__ddl_events`: the
    /// prefix is the only routing key a message has, so putting it in the table name is
    /// what lets an operator select messages by prefix with an ordinary glob. `schema` is
    /// `None` — a message belongs to no schema, and inventing `public` would let a route
    /// for `public.*` collect messages the operator did not ask for.
    ///
    /// # Content
    ///
    /// `content` is application bytes with no declared encoding. It is surfaced as a
    /// string when it is valid UTF-8 and base64 otherwise, with `content_encoding` saying
    /// which — so a consumer decodes on a field rather than on a guess, and a JSON payload
    /// (the common case) is readable without a decode step.
    ///
    /// # Offset
    ///
    /// The **transaction's** LSN, not the message's own. `resume_offset_for` checkpoints a
    /// transaction end position, and a message LSN sits inside the transaction; resuming
    /// from it would restart mid-transaction. The message's own LSN is carried in the
    /// payload for anyone who needs it.
    fn build_logical_message_event(&self, message: &PgLogicalMessage, lsn: u64) -> Event {
        let (content, content_encoding) = match std::str::from_utf8(&message.content) {
            Ok(text) => (text.to_string(), "utf8"),
            Err(_) => {
                use base64::{Engine as _, engine::general_purpose::STANDARD};
                (STANDARD.encode(&message.content), "base64")
            }
        };
        Event {
            before: BeforeImage::Unavailable,
            after: Some(serde_json::json!({
                "prefix": message.prefix,
                "content": content,
                "content_encoding": content_encoding,
                "transactional": message.transactional,
                "lsn": format_pg_lsn(message.lsn),
            })),
            op: Operation::Message,
            source: self.source_meta(lsn),
            ts: if self.current_commit_ts == 0 {
                now_millis()
            } else {
                self.current_commit_ts
            },
            schema: None,
            table: format!("{}__messages", message.prefix),
            primary_key: None,
            snapshot: None,
            transaction: self.tx_meta(),
            envelope_version: EVENT_ENVELOPE_VERSION,
            shape: None,
            unavailable_columns: Vec::new(),
        }
    }

    fn build_relation_schema_change_event(
        &self,
        relation: &PgRelation,
        result_schema: TableSchema,
        lsn: u64,
    ) -> Event {
        let ts_ms = if self.current_commit_ts == 0 {
            now_millis()
        } else {
            self.current_commit_ts
        };
        // pgoutput does not stamp a `RELATION` message with a position of its own, so the
        // frame's `wal_start` is routinely `0`. Inside a transaction `resume_offset_for` usually
        // answers for the event, but not once the transaction is past the tracking cap; outside
        // one it returns `None`. Either way the runtime falls back to this offset, and a
        // checkpoint at `0/00000000` would resume from the beginning of the slot.
        //
        // The resume floor is the answer: the highest position a checkpoint of this stream can
        // already hold, so it cannot rewind, and never past a transaction not yet delivered.
        // `lsn_position` is not: without a checkpoint it starts at the WAL head, past any
        // transaction the slot already holds, and a checkpoint there would skip its rows.
        let lsn = if lsn == 0 { self.resume_floor } else { lsn };
        let captured = CapturedDdl {
            ddl_type: "ALTER_TABLE".to_string(),
            schema: relation.namespace.clone(),
            table: relation.name.clone(),
            statement: format!(
                "ALTER TABLE {}.{} /* derived from pgoutput RELATION metadata */",
                relation.namespace, relation.name
            ),
            result_schema: Some(result_schema),
            schema_diff: None,
            ts: ts_ms,
        };
        captured.to_event(&self.source_name, format_pg_lsn(lsn), ts_ms)
    }

    /// A standalone event — outside any transaction — checkpoints at its own offset, so no
    /// later resume position may fall behind it. Read from the offset the event carries, the
    /// one the runtime stores, rather than recomputed.
    fn raise_resume_floor_to(&mut self, event: &Event) {
        if let Ok(lsn) = super::parser::parse_pg_lsn(&event.source.offset) {
            self.resume_floor = self.resume_floor.max(lsn);
        }
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
                    self.current_tx_start = self.resume_floor;
                    self.partial_tx_events.clear();
                }
                PgOutputMessage::Commit(commit) => {
                    self.stream.lsn_position = commit.end_lsn;
                    self.resume_floor = self.resume_floor.max(commit.end_lsn);
                    // Remember where this transaction starts and ends: its events resume from
                    // the start until its last one. See `PostgresStreamHandle::resume_offset_for`.
                    if let Some(xid) = self.current_xid {
                        self.committed_tx_ends.push_back((
                            u64::from(xid),
                            self.current_tx_start,
                            commit.end_lsn,
                        ));
                        while self.committed_tx_ends.len() > super::MAX_TRACKED_TX_ENDS {
                            self.committed_tx_ends.pop_front();
                        }
                    }
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
                    let previous = self.relation_map.get(&rel.oid);
                    let first_sight = previous.is_none();
                    let changed = previous.is_some_and(|existing| existing != &rel);

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
                            // FULL identifies a row by its whole before-image, which is not a
                            // key. When the table has no primary key in the catalog there is
                            // nothing to report, and a consumer that keys on `primary_key` needs
                            // to hear that from the log rather than infer it from absent keys.
                            b'f' if self.resolve_primary_key(&rel).is_none() => {
                                tracing::warn!(
                                    target: "rustcdc::source::postgres",
                                    table = %format!("{}.{}", rel.namespace, rel.name),
                                    "table has REPLICA IDENTITY FULL but no primary key, so \
                                     events carry no key: pgoutput flags every column as replica \
                                     identity and the whole row is not usable as one. Match on \
                                     the before-image, or add a primary key.",
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
                    // pgoutput re-sends an unchanged RELATION every session; only a new or
                    // changed one can change the shape its rows are captured under.
                    if !(first_sight || changed) {
                        continue;
                    }
                    self.resolve_relation_catalog(&rel, changed).await?;
                    let table_schema = self
                        .relation_to_table_schema(&rel, self.resolve_primary_key(&rel).as_ref());
                    match crate::RowShape::from_table_schema(
                        &rel.namespace,
                        &rel.name,
                        &table_schema,
                    ) {
                        Some(shape) => {
                            self.relation_shapes
                                .insert(rel.oid, std::sync::Arc::new(shape));
                        }
                        None => {
                            self.relation_shapes.remove(&rel.oid);
                        }
                    }

                    // The caches above hold *every* relation, filtered or not: the decoder
                    // needs them to attribute any row it later sees. The schema-change event
                    // carries the table's column list to the sink, so an excluded table must
                    // not produce one.
                    let emit_schema_event = changed
                        && table_is_allowed(
                            Some(rel.namespace.as_str()),
                            &rel.name,
                            &self.table_include_list,
                            &self.table_exclude_list,
                        );

                    if emit_schema_event {
                        let mut schema_event =
                            self.build_relation_schema_change_event(&rel, table_schema, item.lsn);
                        if self.current_xid.is_some() {
                            schema_event.transaction = self.tx_meta();
                            self.partial_tx_events.push(schema_event);
                        } else {
                            self.events_polled = self.events_polled.saturating_add(1);
                            self.raise_resume_floor_to(&schema_event);
                            committed.push(schema_event);
                        }
                    }
                }
                PgOutputMessage::LogicalMessage(message) => {
                    // Filtered by prefix through the same include/exclude lists the row
                    // events use, matched against the synthetic `<prefix>__messages` name.
                    // A message carries no table, so without this an operator who
                    // allow-listed one table would still receive every message on the
                    // instance that reaches this slot.
                    let table = format!("{}__messages", message.prefix);
                    if table_is_allowed(
                        None,
                        &table,
                        &self.table_include_list,
                        &self.table_exclude_list,
                    ) {
                        let event = self.build_logical_message_event(&message, item.lsn);
                        // A transactional message belongs to the open transaction and is
                        // released with it. A non-transactional one was written to the log
                        // outside any transaction's fate, so holding it until a commit
                        // that may never come would strand it.
                        if message.transactional && self.current_xid.is_some() {
                            self.partial_tx_events.push(event);
                        } else {
                            self.events_polled = self.events_polled.saturating_add(1);
                            self.raise_resume_floor_to(&event);
                            committed.push(event);
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
                    // Not every unhandled tag is equally safe to skip. Stream messages
                    // never reach the decoder: the streaming transport's assembler consumes
                    // them, and `sql_peek` negotiates v1. One arriving here means our view of
                    // transaction boundaries is wrong, and skipping a Stream Abort ('A') would
                    // commit data the source rolled back.
                    match tag {
                        // v2 streaming: Stream Start/Stop/Commit/Abort, Stream Prepare.
                        // v3 two-phase: Begin Prepare, Prepare, Commit Prepared,
                        // Rollback Prepared.
                        b'S' | b'E' | b'c' | b'A' | b'p' | b'b' | b'P' | b'K' | b'r' => {
                            return Err(Error::SourceError(format!(
                                "postgres pgoutput message '{}' (0x{tag:02x}) reached the \
                                 decoder outside a stream it can assemble. Skipping it would \
                                 misrepresent transaction boundaries — and skipping a Stream \
                                 Abort would commit data the source rolled back. This \
                                 indicates a server/plugin mismatch.",
                                tag as char
                            )));
                        }
                        // Informational tags that are genuinely safe to skip, but should
                        // not be silent: Origin ('O') matters for loop detection in
                        // bidirectional setups, and Type ('Y') carries custom-type
                        // identity.
                        //
                        // `Y` is safe to skip for a specific reason rather than by
                        // default: pgoutput sends it ahead of a row whose column uses a
                        // non-built-in type, to name that type. Values arrive as **text**
                        // under this decoder — the column type's own output form — so
                        // nothing downstream needs the OID-to-name mapping `Y` provides.
                        // The *declared* type a consumer does need is `format_type` of the
                        // RELATION's OID and modifier, which resolves enums and domains by
                        // name.
                        //
                        // `M` is not here: it is decoded when `capture_logical_messages` is
                        // set, and the server does not send it otherwise.
                        other => {
                            if self.warned_unknown_messages.insert(other) {
                                tracing::warn!(
                                    target: "rustcdc::source::postgres",
                                    tag = %(other as char),
                                    "ignoring unhandled pgoutput message type; \
                                     'O' = Origin (bidirectional loop detection), \
                                     'Y' = Type (custom type identity, not needed because \
                                     values are text and declared types come from \
                                     format_type). These are not surfaced as events.",
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

/// Move each after-image hole that the full old tuple carries into `after`.
///
/// Only for `REPLICA IDENTITY FULL`. A key-only pre-image omits non-key columns by design,
/// so it cannot be used: absence there means "not logged", not "unchanged".
fn fill_unchanged_from_old_tuple(
    after: &mut serde_json::Value,
    unavailable_columns: &mut Vec<String>,
    old_row: &serde_json::Value,
) {
    let (Some(after), Some(old_row)) = (after.as_object_mut(), old_row.as_object()) else {
        return;
    };
    unavailable_columns.retain(|column| match old_row.get(column) {
        Some(value) => {
            after.insert(column.clone(), value.clone());
            false
        }
        None => true,
    });
}
