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
