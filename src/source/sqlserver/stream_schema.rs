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
        let refreshed = self.load_capture_metas().await?;
        let events = self.compute_schema_events_for_meta_refresh(&refreshed);
        self.metas = refreshed;
        Ok(events)
    }
}
