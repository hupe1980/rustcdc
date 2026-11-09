use super::*;
use crate::schema_history::{ColumnDef, DDLEvent, TableSchema};

impl CdcRuntime {
    /// Record a row's shape in the schema history when it is not the table's latest entry.
    ///
    /// Rows are the only carrier of a table's shape, so a table that never undergoes DDL is
    /// in the history only through this. The last recorded id per table is cached; on a miss
    /// (a fresh process) the row is compared with `latest_schema`, so a restart records
    /// nothing for an unchanged table. The identity is `shape:<schema>.<table>:<offset>` —
    /// per occurrence, so A → B → A records A again, and stable under replay.
    ///
    /// Every error here is [`Error::Unrecoverable`]: the batch is already taken from the
    /// stream, and a poll retried on a transient error would read past it. A restart
    /// redelivers it from the checkpoint.
    pub(crate) async fn record_row_shape(&mut self, event: &Event) -> Result<()> {
        let Some(shape) = event.shape.as_deref() else {
            return Ok(());
        };
        let (Some(columns), Some(primary_key)) = (shape.columns(), shape.primary_key()) else {
            return Ok(());
        };
        let key = format!("{}.{}", shape.schema(), shape.table());
        if self.recorded_shapes.get(&key).map(String::as_str) == Some(shape.id()) {
            return Ok(());
        }
        let terminal = |step: &str, error: Error| {
            Error::Unrecoverable(format!(
                "{step} the row shape of {key} at offset {} in the schema history failed: {}. \
                 The batch was already taken from the source, so a retried poll would skip \
                 it; restart the pipeline to redeliver it from the checkpoint.",
                event.source.offset,
                error.report()
            ))
        };

        let latest = self
            .config
            .schema_history
            .latest_schema(&key)
            .await
            .map_err(|error| terminal("reading", error))?;
        let unchanged = latest.as_ref().is_some_and(|latest| {
            crate::RowShape::from_table_schema(shape.schema(), shape.table(), latest)
                .is_some_and(|recorded| recorded.id() == shape.id())
        });
        if !unchanged {
            let table_schema = TableSchema {
                schema: shape.schema().to_string(),
                table: shape.table().to_string(),
                columns: columns
                    .iter()
                    .map(|column| ColumnDef {
                        name: column.name.clone(),
                        data_type: column.data_type.clone(),
                        nullable: column.nullable,
                        constraints: if primary_key.contains(&column.name) {
                            vec!["primary_key".to_string()]
                        } else {
                            Vec::new()
                        },
                    })
                    .collect(),
                primary_keys: primary_key.to_vec(),
                version: 0,
            };
            let identity = format!("shape:{key}:{}", event.source.offset);
            self.config
                .schema_history
                .record_ddl(&identity, DDLEvent::CreateTable(table_schema))
                .await
                .map_err(|error| terminal("recording", error))?;
            if let Some(retention) = self.config.options.schema_history_retention {
                self.config
                    .schema_history
                    .apply_retention(retention)
                    .await
                    .map_err(|error| terminal("pruning after", error))?;
            }
        }
        self.recorded_shapes.insert(key, shape.id().to_string());
        Ok(())
    }

    pub(crate) fn forget_recorded_shape(&mut self, schema: &str, table: &str) {
        self.recorded_shapes.remove(&format!("{schema}.{table}"));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use crate::checkpoint::InMemoryCheckpoint;
    use crate::core::{BeforeImage, EVENT_ENVELOPE_VERSION, Operation, SourceMetadata};
    use crate::ddl_capture::CapturedDdl;
    use crate::schema_history::{
        DDLEvent, InMemorySchemaHistory, SchemaHistory, SchemaHistoryRetention, TableSchema,
    };
    use crate::{RowShape, ShapeColumn};

    use super::super::{CdcRuntime, RuntimeConfig, RuntimeSourceConfig};
    use crate::core::{Error, Event, Result};

    fn shape(columns: &[(&str, &str)]) -> Arc<RowShape> {
        Arc::new(
            RowShape::new(
                "public",
                "users",
                columns
                    .iter()
                    .map(|(name, data_type)| ShapeColumn::new(*name, *data_type, *name != "id"))
                    .collect(),
                vec!["id".to_string()],
            )
            .unwrap(),
        )
    }

    fn row(offset: &str, shape: &Arc<RowShape>) -> Event {
        Event {
            before: BeforeImage::Unavailable,
            after: Some(serde_json::json!({"id": "1"})),
            op: Operation::Insert,
            source: SourceMetadata {
                source_name: "test".into(),
                offset: offset.into(),
                timestamp: 1,
            },
            ts: 1,
            schema: Some("public".into()),
            table: "users".into(),
            primary_key: Some(vec!["id".into()]),
            snapshot: None,
            transaction: None,
            envelope_version: EVENT_ENVELOPE_VERSION,
            shape: Some(Arc::clone(shape)),
            unavailable_columns: Vec::new(),
        }
    }

    async fn runtime_over(history: impl SchemaHistory + 'static) -> CdcRuntime {
        let config = RuntimeConfig::new(
            RuntimeSourceConfig::Disabled,
            InMemoryCheckpoint::default(),
            history,
        );
        let mut runtime = CdcRuntime::new(config).unwrap();
        runtime.start().await.unwrap();
        runtime
    }

    /// Delivers `events` the way a source batch arrives: through a poll.
    async fn poll(runtime: &mut CdcRuntime, events: Vec<Event>) -> Result<()> {
        for event in events {
            runtime.enqueue_event(event).unwrap();
        }
        let batch = runtime.poll_event_batch().await?;
        runtime.commit_ack(batch.ack_mode()).await.unwrap();
        Ok(())
    }

    async fn latest(history: &InMemorySchemaHistory) -> Option<TableSchema> {
        history.latest_schema("public.users").await.unwrap()
    }

    fn id_of(schema: &TableSchema) -> String {
        RowShape::from_table_schema("public", "users", schema)
            .unwrap()
            .id()
            .to_string()
    }

    #[tokio::test]
    async fn history_records_a_shape_seen_only_on_rows() {
        let history = InMemorySchemaHistory::default();
        let mut runtime = runtime_over(history.clone()).await;
        let a = shape(&[("id", "bigint"), ("name", "text")]);

        poll(&mut runtime, vec![row("1", &a), row("2", &a)])
            .await
            .unwrap();

        let recorded = latest(&history)
            .await
            .expect("a table seen only on rows is in the history");
        assert_eq!(id_of(&recorded), a.id());
        assert_eq!(recorded.version, 1, "one shape, one version");
    }

    #[tokio::test]
    async fn a_to_b_to_a_records_a_again() {
        let history = InMemorySchemaHistory::default();
        let mut runtime = runtime_over(history.clone()).await;
        let a = shape(&[("id", "bigint")]);
        let b = shape(&[("id", "bigint"), ("email", "text")]);

        poll(&mut runtime, vec![row("1", &a), row("2", &b), row("3", &a)])
            .await
            .unwrap();

        let recorded = latest(&history).await.unwrap();
        assert_eq!(
            id_of(&recorded),
            a.id(),
            "the table is back to A, so A is the latest entry"
        );
        assert_eq!(recorded.version, 3);
    }

    #[tokio::test]
    async fn a_restart_does_not_record_an_unchanged_shape() {
        let history = InMemorySchemaHistory::default();
        let a = shape(&[("id", "bigint")]);
        let mut first = runtime_over(history.clone()).await;
        poll(&mut first, vec![row("1", &a)]).await.unwrap();
        drop(first);

        let mut restarted = runtime_over(history.clone()).await;
        poll(&mut restarted, vec![row("9", &a)]).await.unwrap();

        assert_eq!(
            latest(&history).await.unwrap().version,
            1,
            "a restart sees the shape the history already holds and records nothing"
        );
    }

    /// A statement moves the history's latest schema, so the next row is compared with the
    /// history rather than with the id this process last recorded.
    #[tokio::test]
    async fn a_row_after_a_statement_is_compared_with_the_history() {
        let history = InMemorySchemaHistory::default();
        let mut runtime = runtime_over(history.clone()).await;
        let a = shape(&[("id", "bigint")]);
        let alter = CapturedDdl {
            ddl_type: "ALTER_TABLE".into(),
            schema: "public".into(),
            table: "users".into(),
            statement: "ALTER TABLE users ADD COLUMN email text".into(),
            result_schema: Some(TableSchema {
                schema: "public".into(),
                table: "users".into(),
                columns: vec![crate::schema_history::ColumnDef {
                    name: "email".into(),
                    data_type: "text".into(),
                    nullable: true,
                    constraints: Vec::new(),
                }],
                primary_keys: Vec::new(),
                version: 0,
            }),
            schema_diff: None,
            ts: 1,
        }
        .to_event("test", "2".into(), 1);

        poll(&mut runtime, vec![row("1", &a), alter, row("3", &a)])
            .await
            .unwrap();

        assert_eq!(
            id_of(&latest(&history).await.unwrap()),
            a.id(),
            "the row after the statement carries A, so A is the latest entry"
        );
    }

    /// A store whose next `record_ddl` fails with a *transient* error.
    #[derive(Clone, Default)]
    struct FlakyHistory {
        inner: InMemorySchemaHistory,
    }

    #[async_trait]
    impl SchemaHistory for FlakyHistory {
        async fn record_ddl(&mut self, _ddl_id: &str, _ddl: DDLEvent) -> Result<u32> {
            Err(Error::TimeoutError("schema history store timed out".into()))
        }
        async fn get_schema_at_version(
            &self,
            table: &str,
            version: u32,
        ) -> Result<Option<TableSchema>> {
            self.inner.get_schema_at_version(table, version).await
        }
        async fn get_schema_at_timestamp(
            &self,
            table: &str,
            ts: u64,
        ) -> Result<Option<TableSchema>> {
            self.inner.get_schema_at_timestamp(table, ts).await
        }
        async fn latest_schema(&self, table: &str) -> Result<Option<TableSchema>> {
            self.inner.latest_schema(table).await
        }
        async fn apply_retention(&mut self, retention: SchemaHistoryRetention) -> Result<usize> {
            self.inner.apply_retention(retention).await
        }
    }

    /// A transient store error must not be retried as a poll: the batch is already off the
    /// stream, so the retry would read past it.
    #[tokio::test]
    async fn a_history_failure_on_a_row_shape_is_terminal() {
        let mut runtime = runtime_over(FlakyHistory::default()).await;
        let a = shape(&[("id", "bigint")]);

        let error = poll(&mut runtime, vec![row("1", &a)])
            .await
            .expect_err("the record failed");

        assert!(
            matches!(error.root_cause(), Error::Unrecoverable(_)),
            "got {error:?}"
        );
        assert!(!error.is_recoverable());
        assert!(
            error.to_string().contains("public.users"),
            "the error names the table: {error}"
        );
    }
}
