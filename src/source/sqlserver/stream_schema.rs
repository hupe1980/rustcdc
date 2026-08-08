use crate::{
    core::{Event, Result},
    ddl_capture::CapturedDdl,
    schema_history::{ColumnDef, TableSchema},
    source::helpers::now_millis,
};

use super::{
    load_capture_metas_for_config, lsn_bytes_to_hex, CaptureInstanceMeta, SqlServerStreamHandle,
};

impl SqlServerStreamHandle {
    async fn load_capture_metas(&self) -> Result<Vec<CaptureInstanceMeta>> {
        load_capture_metas_for_config(&self.config, "sqlserver stream", false, false).await
    }

    fn table_schema_from_meta(meta: &CaptureInstanceMeta) -> TableSchema {
        let columns = meta
            .captured_columns
            .iter()
            .map(|name| {
                let mut constraints = Vec::new();
                if meta.primary_key.iter().any(|pk| pk == name) {
                    constraints.push("primary_key".to_string());
                }
                ColumnDef {
                    name: name.clone(),
                    data_type: "sqlserver_captured".to_string(),
                    nullable: !meta.primary_key.iter().any(|pk| pk == name),
                    constraints,
                }
            })
            .collect();

        TableSchema {
            schema: meta.schema.clone(),
            table: meta.table.clone(),
            columns,
            primary_keys: meta.primary_key.clone(),
            version: 0,
        }
    }

    fn build_schema_event_for_meta(
        &self,
        ddl_type: &str,
        meta: &CaptureInstanceMeta,
        statement: String,
    ) -> Event {
        let result_schema = if ddl_type == "DROP_TABLE" {
            None
        } else {
            Some(Self::table_schema_from_meta(meta))
        };
        let captured = CapturedDdl {
            ddl_type: ddl_type.to_string(),
            schema: meta.schema.clone(),
            table: meta.table.clone(),
            statement,
            result_schema,
            schema_diff: None,
            ts: now_millis(),
        };
        captured.to_event(
            "sqlserver",
            lsn_bytes_to_hex(&self.stream.lsn_end),
            now_millis(),
        )
    }

    pub(super) fn compute_schema_events_for_meta_refresh(
        &self,
        refreshed: &[CaptureInstanceMeta],
    ) -> Vec<Event> {
        let mut events = Vec::new();
        let current: std::collections::HashMap<&str, &CaptureInstanceMeta> = self
            .metas
            .iter()
            .map(|meta| (meta.capture_instance.as_str(), meta))
            .collect();
        let next: std::collections::HashMap<&str, &CaptureInstanceMeta> = refreshed
            .iter()
            .map(|meta| (meta.capture_instance.as_str(), meta))
            .collect();

        for (capture_instance, old_meta) in &current {
            if !next.contains_key(capture_instance) {
                events.push(self.build_schema_event_for_meta(
                    "DROP_TABLE",
                    old_meta,
                    format!(
                        "DROP TABLE {}.{} /* capture instance '{}' removed */",
                        old_meta.schema, old_meta.table, old_meta.capture_instance
                    ),
                ));
            }
        }

        for (capture_instance, new_meta) in &next {
            match current.get(capture_instance) {
                None => events.push(self.build_schema_event_for_meta(
                    "CREATE_TABLE",
                    new_meta,
                    format!(
                        "CREATE TABLE {}.{} /* capture instance '{}' discovered */",
                        new_meta.schema, new_meta.table, new_meta.capture_instance
                    ),
                )),
                Some(old_meta)
                    if old_meta.schema != new_meta.schema
                        || old_meta.table != new_meta.table
                        || old_meta.primary_key != new_meta.primary_key
                        || old_meta.captured_columns != new_meta.captured_columns =>
                {
                    events.push(self.build_schema_event_for_meta(
                        "ALTER_TABLE",
                        new_meta,
                        format!(
                            "ALTER TABLE {}.{} /* capture instance '{}' metadata updated */",
                            new_meta.schema, new_meta.table, new_meta.capture_instance
                        ),
                    ));
                }
                _ => {}
            }
        }

        events
    }

    pub(super) async fn refresh_metas_and_collect_schema_events(&mut self) -> Result<Vec<Event>> {
        let mut refreshed = self.load_capture_metas().await?;
        Self::retain_known_capture_floors(&self.metas, &mut refreshed);
        let events = self.compute_schema_events_for_meta_refresh(&refreshed);
        self.metas = refreshed;
        Ok(events)
    }

    /// Keep the floor this stream first observed for every instance it already knows.
    ///
    /// A refresh re-reads `sys.fn_cdc_get_min_lsn`, which the cleanup job moves forward
    /// as it purges. Adopting the new value for a known instance would clamp the poll to
    /// it and silently step over changes this connector had not read yet — the exact data
    /// loss [`super::SqlServerStreamHandle::classify_cdc_window_error`] exists to refuse.
    /// Only genuinely new instances take their floor from the refresh, which is what
    /// makes adding a table to a running pipeline work.
    fn retain_known_capture_floors(
        known: &[CaptureInstanceMeta],
        refreshed: &mut [CaptureInstanceMeta],
    ) {
        for meta in refreshed.iter_mut() {
            if let Some(existing) = known
                .iter()
                .find(|candidate| candidate.capture_instance == meta.capture_instance)
            {
                meta.capture_floor = existing.capture_floor;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CaptureInstanceMeta, SqlServerStreamHandle};

    fn meta(capture_instance: &str, floor: u8) -> CaptureInstanceMeta {
        let mut capture_floor = [0u8; 10];
        capture_floor[9] = floor;
        CaptureInstanceMeta {
            capture_instance: capture_instance.to_string(),
            schema: "dbo".into(),
            table: capture_instance.trim_start_matches("dbo_").to_string(),
            primary_key: vec!["id".into()],
            captured_columns: vec!["id".into()],
            capture_floor,
        }
    }

    #[test]
    fn a_known_instance_keeps_the_floor_it_was_first_seen_with() {
        // Cleanup moves `fn_cdc_get_min_lsn` forward. Adopting the newer value here would
        // clamp the poll to it and step over changes this connector had not read — which
        // is the data loss the window classifier exists to refuse, arriving by a route
        // that never reaches the classifier at all.
        let known = vec![meta("dbo_orders", 0x10)];
        let mut refreshed = vec![meta("dbo_orders", 0x90)];

        SqlServerStreamHandle::retain_known_capture_floors(&known, &mut refreshed);

        assert_eq!(
            refreshed[0].capture_floor, known[0].capture_floor,
            "a purge must stay visible, not be clamped away by a refresh"
        );
    }

    #[test]
    fn a_newly_added_instance_takes_the_floor_from_the_refresh() {
        // This is what makes `sp_cdc_enable_table` on a running pipeline work: the new
        // instance's floor is later than the current window, and it must be honoured so
        // the poll starts there instead of asking for changes that never existed.
        let known = vec![meta("dbo_orders", 0x10)];
        let mut refreshed = vec![meta("dbo_orders", 0x10), meta("dbo_shipments", 0x90)];

        SqlServerStreamHandle::retain_known_capture_floors(&known, &mut refreshed);

        assert_eq!(refreshed[0].capture_floor[9], 0x10);
        assert_eq!(
            refreshed[1].capture_floor[9], 0x90,
            "an instance the stream has never read must start at its own floor"
        );
    }
}
