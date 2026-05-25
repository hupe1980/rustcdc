use crate::{
    core::{Event, Operation, Result, SourceMetadata, TransactionMetadata, EVENT_ENVELOPE_VERSION},
    ddl_capture::CapturedDdl,
    schema_history::{ColumnDef, TableSchema},
    source::helpers::now_millis,
};

use super::
{
    decode_pgoutput_message, format_pg_lsn, pg_timestamp_to_millis, PgDelete, PgInsert,
    PgOutputMessage, PgOutputXLogData, PgRelation, PgTruncate, PgUpdate, PgValue,
    PostgresStreamHandle,
};

impl PostgresStreamHandle {
    fn tuple_to_json(&self, relation_oid: u32, values: &[PgValue]) -> Option<serde_json::Value> {
        let relation = self.relation_map.get(&relation_oid)?;
        let mut map = serde_json::Map::new();
        for (i, value) in values.iter().enumerate() {
            let col_name = relation
                .columns
                .get(i)
                .map(|c| c.name.as_str())
                .unwrap_or("?");
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
                    // TOAST value unchanged from previous row version - omit from JSON.
                }
            }
        }
        Some(serde_json::Value::Object(map))
    }

    fn relation_table_name(&self, relation_oid: u32) -> String {
        self.relation_map
            .get(&relation_oid)
            .map(|r| {
                if r.namespace.is_empty() || r.namespace == "public" {
                    r.name.clone()
                } else {
                    format!("{}.{}", r.namespace, r.name)
                }
            })
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
            total_events: 0,
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

    fn build_insert_event(&self, insert: &PgInsert, lsn: u64) -> Option<Event> {
        let after = self.tuple_to_json(insert.relation_oid, &insert.new_tuple)?;
        Some(Event {
            before: None,
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
        })
    }

    fn build_update_event(&self, update: &PgUpdate, lsn: u64) -> Option<Event> {
        let after = self.tuple_to_json(update.relation_oid, &update.new_tuple)?;
        let before = update
            .old_tuple
            .as_deref()
            .and_then(|t| self.tuple_to_json(update.relation_oid, t))
            .or_else(|| {
                update
                    .key_tuple
                    .as_deref()
                    .and_then(|t| self.tuple_to_json(update.relation_oid, t))
            });
        Some(Event {
            before,
            after: Some(after),
            op: Operation::Update,
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

    fn build_delete_event(&self, delete: &PgDelete, lsn: u64) -> Option<Event> {
        let before = delete
            .old_tuple
            .as_deref()
            .and_then(|t| self.tuple_to_json(delete.relation_oid, t))
            .or_else(|| {
                delete
                    .key_tuple
                    .as_deref()
                    .and_then(|t| self.tuple_to_json(delete.relation_oid, t))
            });
        Some(Event {
            before,
            after: None,
            op: Operation::Delete,
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
                    data_type: format!("pg_type_oid:{}", column.type_oid),
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
                    self.current_xid = Some(begin.xid);
                    self.current_commit_ts = pg_timestamp_to_millis(begin.commit_timestamp_us);
                    self.partial_tx_events.clear();
                }
                PgOutputMessage::Commit(commit) => {
                    self.stream.lsn_position = commit.end_lsn;
                    let total = self.partial_tx_events.len() as u32;
                    for event in &mut self.partial_tx_events {
                        if let Some(tx) = event.transaction.as_mut() {
                            tx.total_events = total;
                        }
                    }
                    self.events_polled += u64::from(total);
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
                    if let Some(event) = self.build_insert_event(&insert, item.lsn) {
                        self.partial_tx_events.push(event);
                    }
                }
                PgOutputMessage::Update(update) => {
                    if let Some(event) = self.build_update_event(&update, item.lsn) {
                        self.partial_tx_events.push(event);
                    }
                }
                PgOutputMessage::Delete(delete) => {
                    if let Some(event) = self.build_delete_event(&delete, item.lsn) {
                        self.partial_tx_events.push(event);
                    }
                }
                PgOutputMessage::Truncate(truncate) => {
                    let events = self.build_truncate_events(&truncate, item.lsn);
                    self.partial_tx_events.extend(events);
                }
                PgOutputMessage::Unknown(_) => {}
            }
        }
        Ok(committed)
    }
}
