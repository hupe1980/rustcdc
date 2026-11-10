use crate::{
    core::{Event, Result},
    ddl_capture::CapturedDdl,
    schema_history::TableSchema,
    source::{helpers::now_millis, schema_catalog::table_schema_from_catalog},
};

use super::{
    CaptureInstanceMeta, SqlServerStreamHandle, load_capture_metas_for_config, lsn_bytes_to_hex,
};

impl SqlServerStreamHandle {
    async fn load_capture_metas(&self) -> Result<Vec<CaptureInstanceMeta>> {
        load_capture_metas_for_config(&self.config, "sqlserver stream", false, false).await
    }

    /// Describe a capture instance's table from the metadata read at stream start.
    ///
    /// Every column used to be published with the literal type `"sqlserver_captured"` and
    /// a `nullable` flag derived from the primary key — a placeholder and an inference,
    /// where a consumer decoding text values needs the declaration. Both now come from
    /// `sys.columns` through the capture instance; see
    /// [`load_captured_columns_for_instance`](super::load_captured_columns_for_instance).
    fn table_schema_from_meta(meta: &CaptureInstanceMeta) -> TableSchema {
        table_schema_from_catalog(
            &meta.schema,
            &meta.table,
            &meta.captured_columns,
            &meta.primary_key,
        )
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
            shape: None,
        }
    }

    /// A stream row carries the capture instance's columns, declared types, nullability and
    /// key, and shares the instance's one `Arc` rather than building its own.
    #[test]
    fn sqlserver_rows_carry_the_capture_shape() {
        use crate::source::schema_catalog::CatalogColumn;

        let meta = CaptureInstanceMeta {
            capture_instance: "dbo_orders".into(),
            schema: "dbo".into(),
            table: "orders".into(),
            primary_key: vec!["id".into()],
            captured_columns: vec![
                CatalogColumn {
                    name: "id".into(),
                    data_type: "bigint".into(),
                    nullable: false,
                },
                CatalogColumn {
                    name: "note".into(),
                    data_type: "nvarchar(255)".into(),
                    nullable: true,
                },
            ],
            capture_floor: [0u8; 10],
            shape: None,
        }
        .with_shape();

        let row = super::super::stream_window::build_sqlserver_event_for_tests(&meta);
        let shape = row
            .shape
            .as_ref()
            .expect("a captured row carries its shape");
        assert!(std::sync::Arc::ptr_eq(shape, meta.shape.as_ref().unwrap()));
        assert_eq!((shape.schema(), shape.table()), ("dbo", "orders"));
        let columns: Vec<_> = shape
            .columns()
            .unwrap()
            .iter()
            .map(|column| {
                (
                    column.name.as_str(),
                    column.data_type.as_str(),
                    column.nullable,
                )
            })
            .collect();
        assert_eq!(
            columns,
            [("id", "bigint", false), ("note", "nvarchar(255)", true)]
        );
        assert_eq!(shape.primary_key().unwrap(), ["id".to_string()]);
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
