use mysql_async::{prelude::Queryable, Pool as MySqlPool};

use super::parser::decode_stream_resume_position;
use crate::core::{Error, Offset, Result};

pub(super) struct MysqlStreamStartPosition {
    pub(super) binlog_file: String,
    pub(super) binlog_pos: u32,
    pub(super) gtid: String,
}

pub(super) async fn resolve_stream_start_position(
    pool: &MySqlPool,
    source_type: &str,
    resume_from: Option<&dyn Offset>,
) -> Result<MysqlStreamStartPosition> {
    let (mut binlog_file, mut binlog_pos_u64): (String, u64) = {
        let mut conn = pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!(
                "failed to acquire mysql connection for stream: {error}"
            ))
        })?;
        let mut row: mysql_async::Row = match conn.query_first("SHOW MASTER STATUS").await {
            Ok(Some(row)) => row,
            Ok(None) => {
                return Err(Error::SourceError(
                    "mysql master status unavailable for stream start".into(),
                ));
            }
            Err(primary_error) => conn
                .query_first("SHOW BINARY LOG STATUS")
                .await
                .map_err(|fallback_error| {
                    Error::SourceError(format!(
                        "failed to read mysql binary log status for stream start (SHOW MASTER STATUS error: {primary_error}; SHOW BINARY LOG STATUS error: {fallback_error})"
                    ))
                })?
                .ok_or_else(|| {
                    Error::SourceError(
                        "mysql binary log status unavailable for stream start".into(),
                    )
                })?,
        };
        (row.take(0).unwrap_or_default(), row.take(1).unwrap_or(4))
    };
    let mut gtid: String = {
        let mut conn = pool.get_conn().await.map_err(|error| {
            Error::SourceError(format!(
                "failed to acquire mysql connection for gtid query: {error}"
            ))
        })?;
        conn.query_first("SELECT @@GLOBAL.GTID_EXECUTED")
            .await
            .ok()
            .flatten()
            .unwrap_or_default()
    };

    if let Some(offset) = resume_from {
        let resumed = decode_stream_resume_position(source_type, offset)?;

        // Before resuming, verify the server has not purged binlogs we still need.
        //
        // Without this the server errors with a generic "could not find first log file"
        // (MySQL 1236) that says nothing about how much was lost, or — worse, on some
        // configurations — silently starts elsewhere. Checking first turns an
        // unrecoverable gap into an actionable message naming the missing transactions.
        if !resumed.gtid.is_empty() {
            verify_gtid_position_still_available(pool, &resumed.gtid).await?;
        }

        binlog_file = resumed.binlog_file;
        binlog_pos_u64 = u64::from(resumed.binlog_pos);
        if !resumed.gtid.is_empty() {
            gtid = resumed.gtid;
        }
    }

    let binlog_pos = u32::try_from(binlog_pos_u64).map_err(|_| {
        Error::SourceError(format!(
            "mysql stream start binlog pos exceeds u32: {binlog_pos_u64}"
        ))
    })?;

    Ok(MysqlStreamStartPosition {
        binlog_file,
        binlog_pos,
        gtid,
    })
}

/// Verify the server still retains every transaction the resume position has not consumed.
///
/// The correct test is `GTID_SUBSET(gtid_purged, my_position)`: every transaction the
/// server has purged must already be one we consumed. If it is, the binlogs still hold
/// everything we have yet to read.
///
/// The inverted form — `GTID_SUBSET(my_position, gtid_executed)` — is the intuitive one
/// and is wrong, because it **fails open**: it reports "available" in precisely the gap
/// case. That is why this is a named function with a test rather than an inline query.
async fn verify_gtid_position_still_available(pool: &MySqlPool, position: &str) -> Result<()> {
    let mut conn = pool.get_conn().await.map_err(|error| {
        Error::SourceError(format!(
            "failed to acquire mysql connection for GTID availability check: {error}"
        ))
    })?;

    // MariaDB has neither GTID_SUBSET nor gtid_purged; its GTID model is
    // domain-server-sequence and unrelated. Skip rather than fail there.
    let purged: Option<String> = match conn.query_first("SELECT @@GLOBAL.gtid_purged").await {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };
    let Some(purged) = purged.filter(|value| !value.trim().is_empty()) else {
        // Nothing purged: everything ever written is still available.
        return Ok(());
    };

    let still_available: Option<i64> = conn
        .exec_first("SELECT GTID_SUBSET(?, ?)", (&purged, position))
        .await
        .map_err(|error| {
            Error::SourceError(format!("mysql GTID availability check failed: {error}"))
        })?;

    if still_available == Some(1) {
        return Ok(());
    }

    // Name the exact transactions that were purged before we read them.
    let missing: Option<String> = conn
        .exec_first("SELECT GTID_SUBTRACT(?, ?)", (&purged, position))
        .await
        .ok()
        .flatten();

    Err(Error::Unrecoverable(format!(
        "mysql binary logs no longer contain the transactions required to resume from the \
         checkpointed position. The server has purged transactions this connector had not yet \
         read, so resuming would silently skip them.\n  \
         checkpoint position: {position}\n  \
         server gtid_purged:  {purged}\n  \
         missing (purged but unread): {}\n\
         Operator action required: re-snapshot the affected tables and restart from a fresh \
         checkpoint. To prevent recurrence, raise binlog_expire_logs_seconds so retention \
         comfortably exceeds the maximum expected connector downtime.",
        missing.as_deref().unwrap_or("<unavailable>")
    )))
}
