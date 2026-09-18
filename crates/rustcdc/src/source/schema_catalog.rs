//! Declared column types, read from the source's own catalogue.
//!
//! # Why this exists
//!
//! Column values are text on every connector and every capture path, deliberately: a JSON
//! number is an IEEE-754 double downstream, and one representation is what lets a snapshot
//! row and a stream row agree character for character.
//!
//! The other half of a text representation is the type. Given
//! `{"id": "9", "flag": "f", "tags": "{alpha,beta}", "amount": "12345.6789"}` a consumer
//! cannot tell whether `"9"` is a `bigint` or a `text` holding a digit, whether `"f"` is a
//! `boolean` or a `char(1)`, whether `"{alpha,beta}"` is a `text[]` to parse or a string
//! that happens to contain braces, or what precision to give the target column for
//! `"12345.6789"`. The published guidance is "read with `value.as_str()` and parse", and
//! parsing requires knowing what to parse it as.
//!
//! So every connector announces a table's schema **before that table's first row**, in both
//! the snapshot and the stream, and the declared type is read from the catalogue rather
//! than inferred from the replication protocol. What the protocols carry is not enough:
//!
//! | Source | What the log gives | What it costs |
//! |---|---|---|
//! | PostgreSQL | a type OID and a type modifier | pgoutput carries no nullability at all, and an OID alone loses `numeric(12,4)` to `numeric` |
//! | MySQL | a protocol type code plus a metadata block | `MYSQL_TYPE_NEWDECIMAL`, not `decimal(12,4)`; reconstructing the spelling would produce a *second* spelling beside the one the DDL parser already emits |
//! | SQL Server | `cdc.captured_columns.column_type` | the base type name only — that table carries no precision, scale or length |
//!
//! One rule follows and it is the point of this module: **a declared type is read, never
//! inferred.** Where the catalogue cannot be reached the connector says so rather than
//! guessing ([`CatalogColumn::UNKNOWN_TYPE`]).

#[cfg(any(feature = "postgres", feature = "mysql"))]
use std::collections::HashMap;

use crate::schema_history::{ColumnDef, TableSchema};

/// One column as the source's catalogue declares it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CatalogColumn {
    /// Column name.
    pub(crate) name: String,
    /// The declared type **in the source's own syntax**, complete with its modifier:
    /// `numeric(12,4)`, `character varying(64)`, `decimal(12,4) unsigned`,
    /// `nvarchar(255)`.
    ///
    /// Source-native rather than normalised, because a normalised vocabulary is a mapping
    /// this crate would then own and have to keep correct for every type of every engine,
    /// and because a consumer that knows which engine it is reading already knows the
    /// syntax.
    pub(crate) data_type: String,
    /// Whether the column accepts `NULL`, from the catalogue's own answer.
    ///
    /// Never derived from whether the column is part of the key. pgoutput and the SQL
    /// Server capture tables both carry no nullability, and inferring it from the primary
    /// key marks every `NOT NULL` non-key column nullable — a consumer building a target
    /// schema from this then accepts rows the source would have rejected.
    pub(crate) nullable: bool,
}

impl CatalogColumn {
    /// The `data_type` used when the catalogue could not be read for a table.
    ///
    /// A distinguishable marker rather than a plausible-looking guess: a consumer can
    /// branch on it, and it cannot be mistaken for a real declaration the way `"text"` or
    /// an empty string could.
    ///
    /// Only two connectors can produce it. PostgreSQL falls back to the wire type OID for
    /// a table added to the publication after stream start, and SQL Server cannot resolve
    /// a captured column whose source column was dropped. MySQL reads `COLUMN_TYPE`, which
    /// `information_schema` never leaves null, so under a MySQL-only build this is dead.
    ///
    /// `test` is in the gate because the test helpers below construct a column with it
    /// whatever connector is compiled.
    #[cfg(any(feature = "postgres", feature = "sqlserver", test))]
    pub(crate) const UNKNOWN_TYPE: &'static str = "unknown";
}

/// A column named but not described.
///
/// Test-only, so a fixture that cares about column *names* does not have to invent types
/// it is not asserting on. It is `#[cfg(test)]` on purpose: production code that has a
/// name and no type has failed to read a catalogue, and should say
/// [`CatalogColumn::UNKNOWN_TYPE`] deliberately rather than reach for a conversion that
/// makes a string look like a column.
#[cfg(test)]
impl From<&str> for CatalogColumn {
    fn from(name: &str) -> Self {
        Self {
            name: name.to_string(),
            data_type: Self::UNKNOWN_TYPE.to_string(),
            nullable: true,
        }
    }
}

/// Declared columns per `(schema, table)`, read once and reused.
///
/// PostgreSQL reads a publication and MySQL a database, so both need the map. SQL Server
/// reads per capture instance and carries the columns on its own metadata, so under a
/// SQL-Server-only build this alias has no user.
#[cfg(any(feature = "postgres", feature = "mysql"))]
pub(crate) type CatalogSchemas = HashMap<(String, String), Vec<CatalogColumn>>;

/// Build a [`TableSchema`] from catalogue columns and the resolved primary key.
///
/// `primary_keys` is the key resolved from the catalogue, not from a replication-protocol
/// key flag: under PostgreSQL's `REPLICA IDENTITY FULL` pgoutput flags *every* column as
/// part of the replica identity, and treating that as the key published a table whose
/// every column was a non-nullable primary key.
pub(crate) fn table_schema_from_catalog(
    schema: &str,
    table: &str,
    columns: &[CatalogColumn],
    primary_keys: &[String],
) -> TableSchema {
    TableSchema {
        schema: schema.to_string(),
        table: table.to_string(),
        columns: columns
            .iter()
            .map(|column| ColumnDef {
                name: column.name.clone(),
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                constraints: if primary_keys.iter().any(|key| key == &column.name) {
                    vec!["primary_key".to_string()]
                } else {
                    Vec::new()
                },
            })
            .collect(),
        primary_keys: primary_keys.to_vec(),
        // Assigned by the schema history when the event is recorded; the connector does
        // not know the version and must not invent one.
        version: 0,
    }
}

/// The statement text carried by an observation event.
///
/// Not SQL that was run — nothing ran. It is a comment naming the source of the
/// description, so a reader of a dead-letter payload or an audit trail can tell an
/// observation from a captured statement without consulting `ddl_type`.
pub(crate) fn observed_statement(schema: &str, table: &str, source: &str) -> String {
    format!("/* schema of {schema}.{table} observed from {source} */")
}

/// Attach the snapshot metadata a snapshot-path schema event must carry.
///
/// A schema event built from [`CapturedDdl`](crate::ddl_capture::CapturedDdl) has
/// `snapshot: None`, and the runtime reads that as "this is a stream event": it then parses
/// the event's offset as a *stream* position to build a checkpoint from. A snapshot has no
/// such position — its progress is persisted by
/// [`SnapshotHandle::checkpoint`](crate::source::SnapshotHandle) — so the parse fails and
/// takes the poll with it, or worse, writes a stream checkpoint over a snapshot one.
///
/// Marking the event as part of the snapshot is what routes it the same way as the rows it
/// precedes. Every connector's snapshot path must call this; the alternative, hand-crafting
/// an offset each connector's parser happens to accept, is three chances to get it wrong.
#[cfg(any(feature = "postgres", feature = "mysql", feature = "sqlserver"))]
pub(crate) fn mark_as_snapshot_event(
    event: &mut crate::core::Event,
    snapshot_id: &str,
    chunk_index: u32,
) {
    event.snapshot = Some(crate::core::SnapshotMetadata {
        snapshot_id: snapshot_id.to_string(),
        chunk_index,
        is_last_chunk: false,
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, data_type: &str, nullable: bool) -> CatalogColumn {
        CatalogColumn {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
        }
    }

    #[test]
    fn the_primary_key_becomes_a_constraint_without_touching_nullability() {
        // The regression this pins: `nullable` used to be `!is_primary_key`, so a
        // `NOT NULL` non-key column was published as nullable and a nullable key was
        // impossible to express.
        let columns = vec![
            column("id", "bigint", false),
            column("email", "character varying(320)", false),
            column("nickname", "text", true),
        ];
        let schema = table_schema_from_catalog("public", "users", &columns, &["id".to_string()]);

        assert_eq!(schema.columns[0].constraints, vec!["primary_key"]);
        assert!(!schema.columns[0].nullable);
        assert!(schema.columns[1].constraints.is_empty());
        assert!(
            !schema.columns[1].nullable,
            "a NOT NULL non-key column must not be published as nullable"
        );
        assert!(schema.columns[2].nullable);
        assert_eq!(schema.primary_keys, vec!["id".to_string()]);
        assert_eq!(schema.version, 0);
    }

    #[test]
    fn the_declared_type_keeps_its_modifier() {
        let columns = vec![column("amount", "numeric(12,4)", true)];
        let schema = table_schema_from_catalog("public", "orders", &columns, &[]);
        assert_eq!(schema.columns[0].data_type, "numeric(12,4)");
    }

    /// A snapshot-path schema event must be marked as a snapshot event.
    ///
    /// Without it the runtime parses the event's offset as a stream position. That is not
    /// hypothetical: a MySQL snapshot announcement carrying `mysql-bin.000003:205202:schema`
    /// failed `parse_mysql_stream_offset` and took a crash-recovery suite down, and the SQL
    /// Server equivalent wrote a `sqlserver` checkpoint where a `sqlserver_snapshot` one
    /// belonged.
    #[test]
    fn a_snapshot_schema_event_is_marked_as_one() {
        use crate::ddl_capture::{CapturedDdl, DDL_TYPE_READ_SCHEMA};

        let captured = CapturedDdl {
            ddl_type: DDL_TYPE_READ_SCHEMA.to_string(),
            schema: "public".into(),
            table: "orders".into(),
            statement: observed_statement("public", "orders", "a catalogue"),
            result_schema: None,
            schema_diff: None,
            ts: 1,
        };
        let mut event = captured.to_event("postgres", "0/00000064".into(), 1);
        assert!(
            event.snapshot.is_none(),
            "the builder does not set it; the snapshot path must"
        );

        mark_as_snapshot_event(&mut event, "snap-1", 7);

        let snapshot = event.snapshot.expect("marked");
        assert_eq!(snapshot.snapshot_id, "snap-1");
        assert_eq!(snapshot.chunk_index, 7);
        assert!(!snapshot.is_last_chunk);
    }

    #[test]
    fn an_unknown_type_is_marked_rather_than_guessed() {
        let columns = vec![column("mystery", CatalogColumn::UNKNOWN_TYPE, true)];
        let schema = table_schema_from_catalog("public", "t", &columns, &[]);
        assert_eq!(schema.columns[0].data_type, "unknown");
    }
}
