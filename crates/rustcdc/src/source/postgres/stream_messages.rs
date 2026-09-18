use crate::{
    core::{
        BeforeImage, EVENT_ENVELOPE_VERSION, Error, Event, Operation, Result, SourceMetadata,
        TransactionMetadata,
    },
    ddl_capture::CapturedDdl,
    schema_history::{ColumnDef, TableSchema},
    source::{
        helpers::now_millis,
        schema_catalog::{
            CatalogColumn, DDL_TYPE_READ_SCHEMA, observed_statement, table_schema_from_catalog,
        },
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
/// This is the fallback used when the catalog read at stream start has nothing for a
/// table — a table added to the publication mid-stream. It is deliberately not the primary
/// path: it covers 60 built-in OIDs and nothing else, and it has no access to the type
/// modifier, so `numeric(12,4)` is reported as `numeric`.
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
            unavailable_columns,
        })
    }

    fn build_update_event(&self, update: &PgUpdate, lsn: u64) -> Result<Event> {
        let (after, unavailable_columns) =
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
            Some(tuple) => {
                let (row, unavailable) = self.tuple_to_json(update.relation_oid, tuple)?;
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
                unavailable_columns: Vec::new(),
            })
            .collect()
    }

    /// Describe a relation using the catalog read at stream start, falling back to the
    /// wire when the catalog has nothing for it.
    ///
    /// `primary_keys` is the resolved key from [`Self::resolve_primary_key`], not the raw
    /// replica-identity flags: under `REPLICA IDENTITY FULL` every column carries the flag, and
    /// deriving the schema from it published a table whose every column was a non-nullable primary
    /// key.
    ///
    /// # Why the catalog rather than the RELATION message
    ///
    /// The wire carries a type OID, a type modifier and no nullability. Used alone it
    /// reports `numeric` for `numeric(12,4)`, `pg_type_oid:16385` for every enum and
    /// domain, and a `nullable` flag invented from the primary key. The catalog answers
    /// all three; see
    /// [`query_publication_column_types`](super::query::query_publication_column_types).
    ///
    /// # The fallback, and why it is marked
    ///
    /// A table added to the publication after stream start is not in the map. Its columns
    /// are then described from the wire — the same OID map as before, and
    /// [`CatalogColumn::UNKNOWN_TYPE`] for an OID this crate does not know, rather than
    /// `pg_type_oid:<N>`, so "we could not read the type" has one spelling across every
    /// connector. Nullability is not guessed in that case: it is reported `true`, the
    /// weaker claim, because a consumer that relaxes a column is recoverable and one that
    /// tightens it rejects rows the source accepted.
    fn relation_to_table_schema(
        &self,
        relation: &PgRelation,
        primary_keys: Option<&Vec<String>>,
    ) -> TableSchema {
        let primary_keys: Vec<String> = primary_keys.cloned().unwrap_or_default();

        if let Some(catalog) = self
            .catalog_columns
            .get(&(relation.namespace.clone(), relation.name.clone()))
        {
            // Project the catalog through the relation's own column list and order: a
            // publication can publish a column subset (`FOR TABLE t (a, b)`), and the
            // events carry exactly the columns pgoutput sent. Describing columns the
            // stream will never deliver would be a schema no row matches.
            let columns: Vec<CatalogColumn> = relation
                .columns
                .iter()
                .map(|column| {
                    catalog
                        .iter()
                        .find(|candidate| candidate.name == column.name)
                        .cloned()
                        .unwrap_or_else(|| CatalogColumn {
                            name: column.name.clone(),
                            data_type: wire_type_name(column.type_oid),
                            nullable: true,
                        })
                })
                .collect();
            return table_schema_from_catalog(
                &relation.namespace,
                &relation.name,
                &columns,
                &primary_keys,
            );
        }

        let columns = relation
            .columns
            .iter()
            .map(|column| {
                let is_primary_key = primary_keys.contains(&column.name);
                ColumnDef {
                    name: column.name.clone(),
                    data_type: wire_type_name(column.type_oid),
                    nullable: true,
                    constraints: if is_primary_key {
                        vec!["primary_key".to_string()]
                    } else {
                        Vec::new()
                    },
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

    /// Build the schema event for a relation, as either an observation or a change.
    ///
    /// `first_sight` picks the `ddl_type`, and the distinction is the reason a consumer can
    /// use either. `READ_SCHEMA` says "this is the shape of the table, before its first
    /// row"; `ALTER_TABLE` says "this table changed". Reusing `ALTER_TABLE` for both would
    /// tell every consumer that every table was altered on every pipeline restart.
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
            unavailable_columns: Vec::new(),
        }
    }

    fn build_relation_schema_change_event(
        &self,
        relation: &PgRelation,
        lsn: u64,
        first_sight: bool,
    ) -> Event {
        let ts_ms = if self.current_commit_ts == 0 {
            now_millis()
        } else {
            self.current_commit_ts
        };
        let (ddl_type, statement) = if first_sight {
            (
                DDL_TYPE_READ_SCHEMA,
                observed_statement(&relation.namespace, &relation.name, "pgoutput RELATION"),
            )
        } else {
            (
                "ALTER_TABLE",
                format!(
                    "ALTER TABLE {}.{} /* derived from pgoutput RELATION metadata */",
                    relation.namespace, relation.name
                ),
            )
        };
        let captured =
            CapturedDdl {
                ddl_type: ddl_type.to_string(),
                schema: relation.namespace.clone(),
                table: relation.name.clone(),
                statement,
                result_schema: Some(self.relation_to_table_schema(
                    relation,
                    self.resolve_primary_key(relation).as_ref(),
                )),
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
                    // Remember where this transaction *ends*, which is the only position a
                    // restart may resume from. See `PostgresStreamHandle::resume_offset_for`.
                    if let Some(xid) = self.current_xid {
                        self.committed_tx_ends
                            .push_back((u64::from(xid), commit.end_lsn));
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
                    // **First sight emits too**, and that is the whole point of the split.
                    //
                    // pgoutput sends RELATION before the first row of each table in a
                    // session, so the first sighting is exactly the moment a consumer
                    // needs the schema — and it used to be the one case that emitted
                    // nothing (`unwrap_or(false)`). On a fresh start, after a restart, or
                    // for any table that never undergoes DDL, rows arrived with no type
                    // information in the stream at all, and column values are text.
                    //
                    // The two cases stay distinguishable at the consumer through
                    // `ddl_type`, and the schema history de-duplicates an unchanged
                    // observation so a restart does not append a version per table.
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

                    // The cache above is updated for *every* relation, filtered or not:
                    // the decoder needs it to attribute any row it later sees. The
                    // schema-change **event** is a different matter — it carries the
                    // table's full column list to the sink, so an excluded table must not
                    // produce one. This path used to bypass the include/exclude lists
                    // entirely, which meant an operator who allow-listed one table still
                    // received the schema of every other table in the publication.
                    let emit_schema_event = (first_sight || changed)
                        && table_is_allowed(
                            Some(rel.namespace.as_str()),
                            &rel.name,
                            &self.table_include_list,
                            &self.table_exclude_list,
                        );

                    if emit_schema_event {
                        let mut schema_event =
                            self.build_relation_schema_change_event(&rel, item.lsn, first_sight);
                        if self.current_xid.is_some() {
                            schema_event.transaction = self.tx_meta();
                            self.partial_tx_events.push(schema_event);
                        } else {
                            self.events_polled = self.events_polled.saturating_add(1);
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
                        // bidirectional setups, and Type ('Y') carries custom-type
                        // identity.
                        //
                        // `Y` is safe to skip for a specific reason rather than by
                        // default: pgoutput sends it ahead of a row whose column uses a
                        // non-built-in type, to name that type. Values arrive as **text**
                        // under this decoder — the column type's own output form — so
                        // nothing downstream needs the OID-to-name mapping `Y` provides.
                        // The *declared* type a consumer does need comes from the catalog
                        // read at stream start, which resolves enums and domains by name.
                        //
                        // `M` is no longer here: it is decoded when
                        // `capture_logical_messages` is set, and the server does not send
                        // it otherwise.
                        other => {
                            if self.warned_unknown_messages.insert(other) {
                                tracing::warn!(
                                    target: "rustcdc::source::postgres",
                                    tag = %(other as char),
                                    "ignoring unhandled pgoutput message type; \
                                     'O' = Origin (bidirectional loop detection), \
                                     'Y' = Type (custom type identity, not needed because \
                                     values are text and declared types come from the \
                                     catalog). These are not surfaced as events.",
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
