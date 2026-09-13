//! Re-reading unchanged TOASTed values from the source.
//!
//! PostgreSQL omits an unchanged out-of-line value from the WAL, so pgoutput sends `'u'` and
//! the connector records the column in `Event::unavailable_columns` rather than inventing a
//! value. With
//! [`reselect_unavailable_columns`](super::PostgresSourceConfig::reselect_unavailable_columns)
//! set, this module reads those values back by row key and fills the after-image.
//!
//! # Why the after-image only
//!
//! A `before` image is a statement about the past, and reading the row now cannot recover
//! what a column held before the update. Filling holes there would be a wrong answer with no
//! bound, rather than the narrow one below.
//!
//! # What bounds the staleness
//!
//! `'u'` means the statement did **not** modify that column, so the row still holds the value
//! the event was written with — unless a *later* transaction changed it before this query
//! ran. A later `DELETE` leaves nothing to read, and the columns stay absent.
//!
//! # Type fidelity
//!
//! The projection is [`row_as_text_json`], the same one the snapshot path uses, so a
//! reselected value is byte-identical to what pgoutput would have sent. A plain `::text` cast
//! would not be: PostgreSQL renders `bool` as `true` where pgoutput emits `t`.

use std::collections::HashMap;

use tokio_postgres::Client;

use crate::core::{Error, Event, Result};

use super::parser::quote_pg_identifier;
use super::query::row_as_text_json;

/// Declared type of every column, per `(schema, table)`, read once per table per stream.
///
/// The catalog lookup is one round trip and the answer only changes under DDL, which
/// restarts the stream — so caching it turns a per-event query into a per-table one.
pub(super) type ColumnTypeCache = HashMap<(String, String), HashMap<String, String>>;

/// What one reselect pass did, for logging.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReselectStats {
    /// Events that had at least one hole filled.
    pub(super) events_filled: usize,
    /// Individual column values recovered.
    pub(super) columns_filled: usize,
    /// Events left with holes because the row could not be read or addressed.
    pub(super) events_unresolved: usize,
}

/// Fill after-image TOAST holes in `events`, in place.
///
/// Never fails the pipeline. A reselect that cannot run leaves the columns absent, which is
/// precisely the state the event was already in — so degrading is strictly no worse than
/// having the feature switched off, and turning a recoverable read failure into a halted
/// change stream would be.
pub(super) async fn reselect_events(
    client: &Client,
    events: &mut [Event],
    column_types: &mut ColumnTypeCache,
) -> ReselectStats {
    let mut stats = ReselectStats::default();

    for event in events.iter_mut() {
        if event.unavailable_columns.is_empty() {
            continue;
        }
        match reselect_one(client, event, column_types).await {
            Ok(0) => stats.events_unresolved += 1,
            Ok(filled) => {
                stats.events_filled += 1;
                stats.columns_filled += filled;
            }
            Err(error) => {
                stats.events_unresolved += 1;
                tracing::warn!(
                    target: "rustcdc::source::postgres",
                    schema = event.schema.as_deref().unwrap_or_default(),
                    table = %event.table,
                    offset = %event.source.offset,
                    "could not reselect unavailable columns; they stay absent in this event, \
                     which is the same state as with reselect disabled: {error}",
                );
            }
        }
    }

    stats
}

/// Returns how many columns were filled. `0` means the row was not addressable or no longer
/// exists — both ordinary outcomes, not errors.
async fn reselect_one(
    client: &Client,
    event: &mut Event,
    column_types: &mut ColumnTypeCache,
) -> Result<usize> {
    // Without a schema the table cannot be qualified, and relying on `search_path` would
    // address whichever table the connection happens to resolve first.
    let Some(schema) = event.schema.clone() else {
        return Ok(0);
    };
    let key = (schema.clone(), event.table.clone());

    // The event's own key, not a fresh catalog read. It is already the right answer for
    // every replica identity: the primary key under `FULL`, and the nominated unique index
    // under `INDEX` — which addresses a row just as well, and is what a second lookup of
    // *the primary key* would have contradicted.
    let Some(key_columns) = event.primary_key.clone().filter(|key| !key.is_empty()) else {
        // A keyless table cannot be addressed, and cannot start being addressable later in
        // the same stream.
        return Ok(0);
    };

    let Some(after) = event.after.as_ref().and_then(|row| row.as_object()) else {
        return Ok(0);
    };

    // The key values come from the after-image, where pgoutput put them as text. A key
    // column that is itself absent means the row cannot be addressed — which happens when a
    // key column is TOASTed, an unusual but legal schema.
    let mut key_values = Vec::with_capacity(key_columns.len());
    for column in &key_columns {
        match after.get(column) {
            Some(serde_json::Value::String(value)) => key_values.push(value.clone()),
            // A NULL key column cannot occur in a row key, and any other JSON shape would
            // mean the after-image was rewritten by something upstream of here.
            _ => return Ok(0),
        }
    }

    let holes: Vec<String> = event.unavailable_columns.clone();

    if !column_types.contains_key(&key) {
        let types = query_column_types(client, &schema, &event.table).await?;
        column_types.insert(key.clone(), types);
    }
    let types = &column_types[&key];

    let mut key_types = Vec::with_capacity(key_columns.len());
    for column in &key_columns {
        let Some(pg_type) = types.get(column) else {
            return Err(Error::SourceError(format!(
                "reselect: key column '{column}' of '{schema}.{}' is not in the catalog; the \
                 table's shape changed under the stream",
                event.table
            )));
        };
        key_types.push(pg_type.clone());
    }

    let predicate = key_columns
        .iter()
        .zip(key_types.iter())
        .enumerate()
        .map(|(index, (column, pg_type))| {
            // Bound as text and cast in SQL to the column's own type, so one code path
            // serves every key type without a per-type `ToSql` match — and the comparison
            // still happens in the column's type, so the primary-key index is usable.
            format!(
                "t.{} = ${}::text::{pg_type}",
                quote_pg_identifier(column),
                index + 1
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    let sql = format!(
        "SELECT {projection} FROM {schema}.{table} t WHERE {predicate}",
        projection = row_as_text_json(&holes),
        schema = quote_pg_identifier(&schema),
        table = quote_pg_identifier(&event.table),
    );

    let params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = key_values
        .iter()
        .map(|value| value as &(dyn tokio_postgres::types::ToSql + Sync))
        .collect();

    let rows = client.query(&sql, &params).await.map_err(|error| {
        Error::SourceError(format!(
            "reselect query failed for '{schema}.{}': {error}",
            event.table
        ))
    })?;

    // The row has been deleted since the event was written. Leaving the holes is the only
    // answer that is not a guess.
    let Some(row) = rows.first() else {
        return Ok(0);
    };

    let recovered: serde_json::Value =
        serde_json::from_str(row.get::<usize, &str>(0)).map_err(|error| {
            Error::SourceError(format!(
                "reselect returned a projection that is not JSON for '{schema}.{}': {error}",
                event.table
            ))
        })?;
    let Some(recovered) = recovered.as_object() else {
        return Ok(0);
    };

    let mut filled = Vec::new();
    if let Some(after) = event.after.as_mut().and_then(|row| row.as_object_mut()) {
        for column in &holes {
            if let Some(value) = recovered.get(column) {
                after.insert(column.clone(), value.clone());
                filled.push(column.clone());
            }
        }
    }

    // The list must stop describing a hole the row no longer has: a sink reading
    // `unavailable_columns` decides whether it may write the column, and leaving a filled
    // column listed would suppress the value that was just recovered.
    event
        .unavailable_columns
        .retain(|column| !filled.contains(column));

    Ok(filled.len())
}

/// Declared type of every live column of a table, by name.
///
/// Read for the whole table rather than for the key columns alone: it is the same single
/// round trip either way, and the result is cached per table, so asking once for everything
/// avoids a second lookup the first time a different key shape appears.
async fn query_column_types(
    client: &Client,
    schema: &str,
    table: &str,
) -> Result<HashMap<String, String>> {
    let rows = client
        .query(
            "SELECT attribute.attname, \
                    pg_catalog.format_type(attribute.atttypid, attribute.atttypmod) \
             FROM pg_catalog.pg_attribute attribute \
             JOIN pg_catalog.pg_class class_def ON class_def.oid = attribute.attrelid \
             JOIN pg_catalog.pg_namespace namespace_def \
               ON namespace_def.oid = class_def.relnamespace \
             WHERE namespace_def.nspname = $1 \
               AND class_def.relname = $2 \
               AND attribute.attnum > 0 \
               AND NOT attribute.attisdropped",
            &[&schema, &table],
        )
        .await
        .map_err(|error| {
            Error::SourceError(format!(
                "reselect: failed reading column types for '{schema}.{table}': {error}"
            ))
        })?;

    Ok(rows
        .iter()
        .map(|row| (row.get::<usize, String>(0), row.get::<usize, String>(1)))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The projection must ask for the holes and nothing else.
    ///
    /// Reselecting the whole row would read every TOASTed column on the table, including
    /// ones this event already carries — which is the cost the feature exists to bound.
    #[test]
    fn the_projection_covers_exactly_the_missing_columns() {
        let sql = row_as_text_json(&["body".to_string(), "attachment".to_string()]);
        assert!(sql.contains("'body'"), "{sql}");
        assert!(sql.contains("'attachment'"), "{sql}");
        assert!(
            !sql.contains("'id'"),
            "a column that is present must not be re-read: {sql}"
        );
    }

    /// `format('%s', …)` rather than `::text`, so a reselected value is byte-identical to
    /// what pgoutput would have sent. The `bool` case is the one that diverges: the cast
    /// yields `true`, the output function yields `t`.
    #[test]
    fn the_projection_uses_the_types_own_output_function() {
        let sql = row_as_text_json(&["flag".to_string()]);
        assert!(sql.contains("format('%s'"), "{sql}");
        assert!(
            !sql.contains("::text)"),
            "a ::text cast disagrees with pgoutput for bool: {sql}"
        );
    }
}
