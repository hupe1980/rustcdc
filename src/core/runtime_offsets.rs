#[cfg(any(feature = "postgres", feature = "mysql", test))]
use super::{Error, Result};

#[cfg(any(feature = "postgres", test))]
pub(super) fn parse_postgres_lsn(value: &str) -> Result<u64> {
    let (high, low) = value.split_once('/').ok_or_else(|| {
        Error::CheckpointError(format!(
            "invalid postgres LSN offset '{value}'; expected HEX/HEX"
        ))
    })?;
    let high = u64::from_str_radix(high, 16).map_err(|error| {
        Error::CheckpointError(format!(
            "invalid postgres LSN high bits in '{value}': {error}"
        ))
    })?;
    let low = u64::from_str_radix(low, 16).map_err(|error| {
        Error::CheckpointError(format!(
            "invalid postgres LSN low bits in '{value}': {error}"
        ))
    })?;
    Ok((high << 32) | low)
}

#[cfg(feature = "mysql")]
pub(super) fn parse_mysql_stream_offset(value: &str) -> Result<(String, u32, String)> {
    let (file, rest) = value.split_once(':').ok_or_else(|| {
        Error::CheckpointError(format!(
            "invalid mysql offset '{value}'; expected <binlog_file>:<binlog_pos>[#gtid=<gtid>]"
        ))
    })?;

    let (position_part, gtid) = rest.split_once("#gtid=").map_or((rest, ""), |parts| parts);

    let binlog_pos = position_part.parse::<u32>().map_err(|error| {
        Error::CheckpointError(format!(
            "invalid mysql binlog position in '{value}': {error}"
        ))
    })?;

    Ok((file.to_string(), binlog_pos, gtid.to_string()))
}
