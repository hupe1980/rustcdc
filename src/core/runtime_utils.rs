pub(super) fn format_capability_metric(name: &str, enabled: bool) -> String {
    format!(
        "cdc_runtime_source_capability{{capability=\"{name}\"}} {}\n",
        if enabled { 1 } else { 0 }
    )
}

pub(super) fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or_default()
}

pub(super) fn normalize_source_timestamp_ms(raw: u64) -> u64 {
    // Normalize common units used by connectors:
    // - seconds (<= year 2100 in epoch seconds) => milliseconds
    // - microseconds (very large epoch values) => milliseconds
    if raw <= 4_102_444_800 {
        raw.saturating_mul(1000)
    } else if raw >= 10_000_000_000_000 {
        raw / 1000
    } else {
        raw
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_source_timestamp_ms;

    #[test]
    fn normalize_source_timestamp_ms_converts_seconds_to_millis() {
        assert_eq!(
            normalize_source_timestamp_ms(1_700_000_000),
            1_700_000_000_000
        );
    }

    #[test]
    fn normalize_source_timestamp_ms_converts_micros_to_millis() {
        assert_eq!(
            normalize_source_timestamp_ms(1_700_000_000_123_456),
            1_700_000_000_123
        );
    }

    #[test]
    fn normalize_source_timestamp_ms_keeps_millis_as_is() {
        assert_eq!(
            normalize_source_timestamp_ms(1_700_000_000_123),
            1_700_000_000_123
        );
    }
}
