//! Control-plane signals and their notification fan-out.
//!
//! Split out of `admin/mod.rs` on the same reasoning as `admin/tests.rs`: this is one
//! self-contained concern — the wire shapes an operator sends, the lifecycle states a
//! signal moves through, and the CloudEvents rendering that reports them — and it is the
//! part a reader most often wants to follow end to end without the health probes,
//! authentication and metrics in between.
//!
//! `super::` still resolves to `admin`, so `AdminState`'s own signal methods stay where
//! their sibling state lives and reach these types unchanged.

use super::*;

#[derive(Debug, Clone, Serialize)]
pub(super) struct ControlNotification {
    pub(super) id: u64,
    pub(super) at: DateTime<Utc>,
    pub(super) signal_id: String,
    pub(super) correlation_id: String,
    pub(super) action_type: String,
    pub(super) state: String,
    pub(super) traceparent: Option<String>,
    pub(super) detail: serde_json::Value,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum SignalActionType {
    LogMarker,
    ExecuteSnapshot,
    PauseSnapshot,
    ResumeSnapshot,
    StopSnapshot,
}

impl SignalActionType {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::LogMarker => "log_marker",
            Self::ExecuteSnapshot => "execute_snapshot",
            Self::PauseSnapshot => "pause_snapshot",
            Self::ResumeSnapshot => "resume_snapshot",
            Self::StopSnapshot => "stop_snapshot",
        }
    }

    pub(super) fn started_audit_action(self) -> &'static str {
        match self {
            Self::LogMarker => "signal_log_marker_started",
            Self::ExecuteSnapshot => "signal_execute_snapshot_started",
            Self::PauseSnapshot => "signal_pause_snapshot_started",
            Self::ResumeSnapshot => "signal_resume_snapshot_started",
            Self::StopSnapshot => "signal_stop_snapshot_started",
        }
    }

    pub(super) fn in_progress_audit_action(self) -> Option<&'static str> {
        match self {
            Self::ExecuteSnapshot => Some("signal_execute_snapshot_in_progress"),
            _ => None,
        }
    }

    pub(super) fn queue_rejected_audit_action(self) -> &'static str {
        match self {
            Self::LogMarker => "signal_log_marker_aborted",
            Self::ExecuteSnapshot => "signal_execute_snapshot_aborted",
            Self::PauseSnapshot => "signal_pause_snapshot_aborted",
            Self::ResumeSnapshot => "signal_resume_snapshot_aborted",
            Self::StopSnapshot => "signal_stop_snapshot_aborted",
        }
    }

    pub(super) fn terminal_audit_action(self) -> &'static str {
        match self {
            Self::LogMarker => "signal_log_marker_completed",
            Self::ExecuteSnapshot => "signal_execute_snapshot_completed",
            Self::PauseSnapshot => "signal_pause_snapshot_paused",
            Self::ResumeSnapshot => "signal_resume_snapshot_resumed",
            Self::StopSnapshot => "signal_stop_snapshot_aborted",
        }
    }

    pub(super) fn terminal_state(self) -> &'static str {
        match self {
            Self::LogMarker => "COMPLETED",
            Self::ExecuteSnapshot => "COMPLETED",
            Self::PauseSnapshot => "PAUSED",
            Self::ResumeSnapshot => "RESUMED",
            Self::StopSnapshot => "ABORTED",
        }
    }

    pub(super) fn terminal_result(self) -> &'static str {
        match self {
            Self::LogMarker | Self::ExecuteSnapshot => "completed",
            Self::PauseSnapshot => "paused",
            Self::ResumeSnapshot => "resumed",
            Self::StopSnapshot => "aborted",
        }
    }
}

/// `deny_unknown_fields` matches [`SignalIngressRecord`], deliberately.
///
/// Without it the documented `{"action_type":"execute_snapshot","tables":[...]}` payload
/// was **accepted over HTTP with `tables` silently dropped** and **rejected** by the file
/// and Kafka ingress paths, which do deny unknown fields — the same request understood
/// two different ways depending on how it arrived. A typo in a table name is exactly the
/// mistake this catches.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SignalActionRequest {
    pub(super) signal_id: Option<String>,
    pub(super) correlation_id: Option<String>,
    pub(super) action_type: SignalActionType,
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) additional_data: Option<serde_json::Value>,
    /// Fully-qualified `"schema.table"` names for `execute_snapshot`.
    #[serde(default)]
    pub(super) tables: Option<Vec<String>>,
    /// Per-table row filter for this request, keyed by `"schema.table"`.
    ///
    /// Debezium's `additional-conditions` on the same signal. Overrides
    /// `incremental_snapshot.table_conditions` for the tables it names; a table without an
    /// override keeps its configured filter, so static configuration stays meaningful.
    ///
    /// **Raw SQL, and trusted input.** It is interpolated into the chunk `SELECT`, carries
    /// the same trust level as the connection string, and is **not** a tenancy boundary.
    /// The write-scope token that reaches this endpoint is already trusted with
    /// `stop_snapshot`; treat it accordingly and do not proxy an end user's input into it.
    #[serde(default)]
    pub(super) conditions: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SignalIngressRecord {
    pub(super) signal_id: Option<String>,
    pub(super) correlation_id: Option<String>,
    pub(super) action_type: SignalActionType,
    pub(super) message: Option<String>,
    #[serde(default)]
    pub(super) additional_data: Option<serde_json::Value>,
    #[serde(default)]
    pub(super) tables: Option<Vec<String>>,
    /// See [`SignalActionRequest::conditions`].
    #[serde(default)]
    pub(super) conditions: Option<BTreeMap<String, String>>,
    #[serde(default)]
    pub(super) traceparent: Option<String>,
}

/// Normalise and check the table list a signal carries.
///
/// `execute_snapshot` without tables has nothing to do, and accepting it would produce a
/// `COMPLETED` record for a no-op. Every other action carries no tables at all.
pub(super) fn validate_signal_tables(
    action_type: SignalActionType,
    tables: Option<Vec<String>>,
) -> Result<Vec<String>, String> {
    let tables: Vec<String> = tables
        .unwrap_or_default()
        .into_iter()
        .map(|table| table.trim().to_string())
        .filter(|table| !table.is_empty())
        .collect();

    match action_type {
        SignalActionType::ExecuteSnapshot if tables.is_empty() => Err(
            "action_type=execute_snapshot requires a non-empty \"tables\" list of \
             fully-qualified \"schema.table\" names"
                .to_string(),
        ),
        SignalActionType::ExecuteSnapshot => Ok(tables),
        _ if !tables.is_empty() => Err(format!(
            "\"tables\" is only meaningful for action_type=execute_snapshot, not {}",
            action_type.as_str()
        )),
        _ => Ok(Vec::new()),
    }
}

/// Normalise and check a request's per-table row filters against its table list.
///
/// A condition keyed to a table the request does not snapshot is silently inert — and
/// inert in the dangerous direction, because the backfill then reads the *whole* table
/// while the operator believes it is scoped. The mistake is almost always a typo in the
/// key, so it is rejected here rather than accepted and ignored.
///
/// A blank expression is rejected for the same reason: it reads as "filter this" and does
/// nothing.
pub(super) fn validate_signal_conditions(
    action_type: SignalActionType,
    tables: &[String],
    conditions: Option<BTreeMap<String, String>>,
) -> Result<BTreeMap<String, String>, String> {
    let conditions = conditions.unwrap_or_default();
    if conditions.is_empty() {
        return Ok(BTreeMap::new());
    }

    if !matches!(action_type, SignalActionType::ExecuteSnapshot) {
        return Err(format!(
            "\"conditions\" is only meaningful for action_type=execute_snapshot, not {}",
            action_type.as_str()
        ));
    }

    let mut normalized = BTreeMap::new();
    for (table, condition) in conditions {
        let table = table.trim().to_string();
        let condition = condition.trim().to_string();
        if condition.is_empty() {
            return Err(format!(
                "\"conditions\" entry for '{table}' is empty; omit the entry instead of \
                 sending a blank filter"
            ));
        }
        if !tables.contains(&table) {
            return Err(format!(
                "\"conditions\" names table '{table}', which is not in this request's \
                 \"tables\" list, so the filter would never be applied and the whole table \
                 would be backfilled. Check the spelling."
            ));
        }
        normalized.insert(table, condition);
    }
    Ok(normalized)
}

pub(super) fn normalize_optional_id(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

pub(super) fn generated_signal_id() -> String {
    format!("signal-{}", Utc::now().timestamp_micros())
}

pub(super) fn is_control_lifecycle_state(state: &str) -> bool {
    matches!(
        state,
        "STARTED"
            | "IN_PROGRESS"
            | "TABLE_SCAN_COMPLETED"
            | "PAUSED"
            | "RESUMED"
            | "COMPLETED"
            | "ABORTED"
            | "SKIPPED"
    )
}

pub(super) fn is_control_action_type(action_type: &str) -> bool {
    matches!(
        action_type,
        "log_marker" | "execute_snapshot" | "pause_snapshot" | "resume_snapshot" | "stop_snapshot"
    )
}

pub(super) fn notification_from_audit_entry(
    entry: &AuditTrailEntry,
) -> Option<ControlNotification> {
    if !entry.action.starts_with("signal_") {
        return None;
    }

    let detail = serde_json::from_str::<serde_json::Value>(&entry.detail).ok()?;
    let signal_id = detail.get("signal_id")?.as_str()?.to_string();
    let correlation_id = detail
        .get("correlation_id")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(signal_id.as_str())
        .to_string();
    let action_type = detail
        .get("action_type")
        .and_then(serde_json::Value::as_str)?;
    if !is_control_action_type(action_type) {
        return None;
    }
    let state = detail
        .get("state")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_string();
    if !is_control_lifecycle_state(&state) {
        return None;
    }
    let traceparent = detail
        .get("traceparent")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string);

    Some(ControlNotification {
        id: entry.sequence,
        at: entry.at,
        signal_id,
        correlation_id,
        action_type: action_type.to_string(),
        state,
        traceparent,
        detail,
    })
}

pub(super) fn collect_control_notifications(data: &AdminStateData) -> Vec<ControlNotification> {
    data.audit_recent_entries
        .iter()
        .filter_map(notification_from_audit_entry)
        .collect()
}

pub(super) fn notification_to_cloudevent(notification: &ControlNotification) -> serde_json::Value {
    notification_to_cloudevent_with_source(notification, "urn:cdc-server:admin:notifications")
}

pub(super) fn notification_to_cloudevent_with_source(
    notification: &ControlNotification,
    source: &str,
) -> serde_json::Value {
    let mut event = serde_json::Map::new();
    event.insert(
        "id".to_string(),
        serde_json::Value::String(format!("{}", notification.id)),
    );
    event.insert(
        "source".to_string(),
        serde_json::Value::String(source.to_string()),
    );
    event.insert(
        "type".to_string(),
        serde_json::Value::String(format!(
            "cdc.signal.{}",
            notification.state.to_ascii_lowercase()
        )),
    );
    event.insert(
        "specversion".to_string(),
        serde_json::Value::String("1.0".to_string()),
    );
    event.insert(
        "time".to_string(),
        serde_json::Value::String(notification.at.to_rfc3339()),
    );
    event.insert(
        "datacontenttype".to_string(),
        serde_json::Value::String("application/json".to_string()),
    );
    event.insert(
        "signalid".to_string(),
        serde_json::Value::String(notification.signal_id.clone()),
    );
    event.insert(
        "correlationid".to_string(),
        serde_json::Value::String(notification.correlation_id.clone()),
    );
    event.insert(
        "actiontype".to_string(),
        serde_json::Value::String(notification.action_type.clone()),
    );
    if let Some(traceparent) = &notification.traceparent {
        event.insert(
            "traceparent".to_string(),
            serde_json::Value::String(traceparent.clone()),
        );
    }

    event.insert(
        "data".to_string(),
        serde_json::json!({
            "signal_id": notification.signal_id,
            "correlation_id": notification.correlation_id,
            "action_type": notification.action_type,
            "state": notification.state,
            "detail": notification.detail,
        }),
    );

    serde_json::Value::Object(event)
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SignalNotificationHealth {
    pub(super) without_terminal_total: u64,
    pub(super) duplicate_terminal_total: u64,
    pub(super) lag_seconds: f64,
}

pub(super) fn signal_terminal_state(state: &str) -> bool {
    matches!(
        state,
        "COMPLETED" | "ABORTED" | "SKIPPED" | "PAUSED" | "RESUMED"
    )
}

pub(super) fn parse_signal_lifecycle_detail(detail: &str) -> Option<(String, String, String)> {
    let parsed = serde_json::from_str::<serde_json::Value>(detail).ok()?;
    let signal_id = parsed
        .get("signal_id")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    let action_type = parsed
        .get("action_type")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    if !is_control_action_type(&action_type) {
        return None;
    }
    let state = parsed
        .get("state")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    Some((signal_id, action_type, state))
}

/// Lifecycle aggregate per `(signal_id, action_type)`: earliest STARTED
/// timestamp (if any) and count of terminal-state entries.
type SignalLifecycleAggregate = (Option<DateTime<Utc>>, u64);

pub(super) fn signal_notification_health(
    data: &AdminStateData,
    now: DateTime<Utc>,
) -> SignalNotificationHealth {
    let mut lifecycle_by_signal: HashMap<(String, String), SignalLifecycleAggregate> =
        HashMap::new();
    let mut lag_samples = Vec::new();

    for entry in &data.audit_recent_entries {
        if !entry.action.starts_with("signal_") {
            continue;
        }

        let Some((signal_id, action_type, state)) = parse_signal_lifecycle_detail(&entry.detail)
        else {
            continue;
        };
        if !is_control_lifecycle_state(&state) {
            continue;
        }

        let key = (signal_id, action_type);
        let lifecycle = lifecycle_by_signal.entry(key).or_insert((None, 0));
        if state == "STARTED" {
            lifecycle.0 = Some(match lifecycle.0 {
                Some(started_at) => started_at.min(entry.at),
                None => entry.at,
            });
        }
        if signal_terminal_state(&state) {
            lifecycle.1 = lifecycle.1.saturating_add(1);
            if let Some(started_at) = lifecycle.0 {
                let lag = (entry.at - started_at).num_milliseconds().max(0) as f64 / 1000.0;
                lag_samples.push(lag);
            }
        }
    }

    let without_terminal_total = lifecycle_by_signal
        .values()
        .filter(|(started_at, terminal_count)| {
            started_at
                .map(|ts| (now - ts).num_seconds() > SIGNAL_TERMINAL_TIMEOUT_SECONDS)
                .unwrap_or(false)
                && *terminal_count == 0
        })
        .count() as u64;

    let duplicate_terminal_total = lifecycle_by_signal
        .values()
        .filter(|(_, terminal_count)| *terminal_count > 1)
        .count() as u64;

    let lag_seconds = lag_samples.into_iter().fold(0.0_f64, f64::max);

    SignalNotificationHealth {
        without_terminal_total,
        duplicate_terminal_total,
        lag_seconds,
    }
}

pub(super) fn latest_signal_action_state(
    data: &AdminStateData,
    signal_id: &str,
    action_type: &str,
) -> Option<String> {
    data.audit_recent_entries.iter().rev().find_map(|entry| {
        if !entry.action.starts_with("signal_") {
            return None;
        }

        let Ok(detail) = serde_json::from_str::<serde_json::Value>(&entry.detail) else {
            return None;
        };

        let entry_signal_id = detail
            .get("signal_id")
            .and_then(serde_json::Value::as_str)?;
        let entry_action_type = detail
            .get("action_type")
            .and_then(serde_json::Value::as_str)?;
        let state = detail.get("state").and_then(serde_json::Value::as_str)?;

        if entry_signal_id == signal_id && entry_action_type == action_type {
            return Some(state.to_string());
        }

        None
    })
}

pub(super) fn signal_action_requires_async_worker(action_type: SignalActionType) -> bool {
    !matches!(action_type, SignalActionType::LogMarker)
}

pub(super) async fn signal_action(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(request): Json<SignalActionRequest>,
) -> Response {
    let Some(actor_token_id) = admin.authorize_write_token_id(&headers) else {
        return unauthorized_response();
    };

    let message = request.message.unwrap_or_default().trim().to_string();
    if matches!(request.action_type, SignalActionType::LogMarker) && message.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "message must not be empty for action_type=log_marker",
        )
            .into_response();
    }

    let tables = match validate_signal_tables(request.action_type, request.tables) {
        Ok(tables) => tables,
        Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
    };

    let signal_id =
        normalize_optional_id(request.signal_id.as_deref()).unwrap_or_else(generated_signal_id);
    let correlation_id = normalize_optional_id(request.correlation_id.as_deref())
        .unwrap_or_else(|| signal_id.clone());
    let traceparent = headers
        .get("traceparent")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| normalize_optional_id(Some(value)));
    let conditions =
        match validate_signal_conditions(request.action_type, &tables, request.conditions) {
            Ok(conditions) => conditions,
            Err(error) => return (StatusCode::BAD_REQUEST, error).into_response(),
        };

    let envelope = SignalActionEnvelope {
        signal_id,
        correlation_id,
        action_type: request.action_type,
        message,
        traceparent,
        additional_data: request.additional_data.unwrap_or(serde_json::Value::Null),
        tables,
        conditions,
        actor_source_ip: Some(peer_addr.ip().to_string()),
        actor_token_id: actor_token_id.clone(),
    };

    match admin.execute_signal_action_envelope(envelope).await {
        SignalActionExecutionResult::ExistingState {
            signal_id,
            correlation_id,
            action_type,
            current_state,
            expected_terminal_state,
        } => Json(serde_json::json!({
            "api_version": "v1",
            "signal_id": signal_id,
            "correlation_id": correlation_id,
            "action_type": action_type,
            "current_state": current_state,
            "expected_terminal_state": expected_terminal_state,
            "idempotent_replay": true,
        }))
        .into_response(),
        SignalActionExecutionResult::Started {
            signal_id,
            correlation_id,
            action_type,
            expected_terminal_state,
        } => {
            tracing::info!(
                target: "rustcdc_audit",
                action = "signal_action",
                signal_id = %signal_id,
                correlation_id = %correlation_id,
                action_type = %action_type,
                token_id = %actor_token_id,
                result = "accepted_async",
                "typed signal queued for async completion"
            );

            Json(serde_json::json!({
                "api_version": "v1",
                "signal_id": signal_id,
                "correlation_id": correlation_id,
                "action_type": action_type,
                "current_state": "STARTED",
                "expected_terminal_state": expected_terminal_state,
                "idempotent_replay": false,
            }))
            .into_response()
        }
        SignalActionExecutionResult::Terminal {
            signal_id,
            correlation_id,
            action_type,
            current_state,
            expected_terminal_state,
        } => {
            tracing::info!(
                target: "rustcdc_audit",
                action = "signal_action",
                signal_id = %signal_id,
                correlation_id = %correlation_id,
                action_type = %action_type,
                token_id = %actor_token_id,
                result = "accepted",
                "typed signal processed"
            );

            Json(serde_json::json!({
                "api_version": "v1",
                "signal_id": signal_id,
                "correlation_id": correlation_id,
                "action_type": action_type,
                "current_state": current_state,
                "expected_terminal_state": expected_terminal_state,
                "idempotent_replay": false,
            }))
            .into_response()
        }
        SignalActionExecutionResult::Aborted {
            signal_id,
            correlation_id,
            action_type,
            expected_terminal_state,
            error,
            retry_after_seconds,
        } => {
            let mut headers = HeaderMap::new();
            if let Some(retry_after) = retry_after_seconds {
                if let Ok(value) = HeaderValue::from_str(&retry_after.to_string()) {
                    headers.insert(RETRY_AFTER, value);
                }
            }

            (
                StatusCode::SERVICE_UNAVAILABLE,
                headers,
                Json(serde_json::json!({
                    "api_version": "v1",
                    "signal_id": signal_id,
                    "correlation_id": correlation_id,
                    "action_type": action_type,
                    "current_state": "ABORTED",
                    "expected_terminal_state": expected_terminal_state,
                    "idempotent_replay": false,
                    "error": error,
                })),
            )
                .into_response()
        }
    }
}

pub(super) async fn notifications_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("notifications");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    Json(serde_json::json!({
        "api_version": "v1",
        "notifications_total": notifications.len(),
        "notifications": notifications,
    }))
    .into_response()
}

pub(super) async fn notifications_cloudevents_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("notifications_cloudevents");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    let data = admin.data.read().await;
    let notifications = collect_control_notifications(&data);
    let events = notifications
        .iter()
        .map(notification_to_cloudevent)
        .collect::<Vec<_>>();

    Json(serde_json::json!({
        "api_version": "v1",
        "format": "cloudevents",
        "events_total": events.len(),
        "events": events,
    }))
    .into_response()
}

pub(super) async fn notifications_stream_authed(
    State(admin): State<AdminState>,
    ConnectInfo(peer_addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let (allowed, decision_latency) =
        admin.allow_abuse_scope(AbuseLimitScope::Status, &headers, Some(peer_addr));
    admin
        .record_rate_limiter_decision(AbuseLimitScope::Status, decision_latency)
        .await;
    if !allowed {
        admin
            .record_rate_limited_request(AbuseLimitScope::Status)
            .await;
        return rate_limited_response("notifications_stream");
    }

    if !admin.authorize_read(&headers) {
        return unauthorized_response();
    }

    // Subscribe *before* reading the backlog. The other order has a hole: a notification
    // raised between the snapshot and the subscription belongs to neither, and the client
    // never sees it.
    let live = admin.notification_broadcast.subscribe();

    // `Last-Event-ID` is how EventSource resumes after a dropped connection — the browser
    // replays it automatically. Ignoring it, as the previous handler did, meant every
    // reconnect re-delivered the entire ring buffer.
    let resume_after = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<u64>().ok());

    let backlog: Vec<ControlNotification> = {
        let data = admin.data.read().await;
        collect_control_notifications(&data)
            .into_iter()
            .filter(|notification| resume_after.is_none_or(|after| notification.id > after))
            .collect()
    };

    let stream = futures::stream::unfold(
        (backlog.into_iter(), live, resume_after),
        |(mut backlog, mut live, mut last_id)| async move {
            if let Some(notification) = backlog.next() {
                last_id = Some(notification.id);
                let event = notification_sse_event(&notification);
                return Some((
                    Ok::<_, std::convert::Infallible>(event),
                    (backlog, live, last_id),
                ));
            }

            loop {
                match live.recv().await {
                    Ok(notification) => {
                        // The backlog and the live channel overlap by construction: a
                        // notification raised between the subscribe and the snapshot read
                        // appears in both. Suppressing anything not newer than the last id
                        // emitted is what makes the stream exactly-once for the client.
                        if last_id.is_some_and(|seen| notification.id <= seen) {
                            continue;
                        }
                        last_id = Some(notification.id);
                        let event = notification_sse_event(&notification);
                        return Some((Ok(event), (backlog, live, last_id)));
                    }
                    // Tell the client it has a gap rather than let it believe the sequence
                    // is complete. A silent gap in a control-plane feed is worse than a
                    // slow one.
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(missed)) => {
                        let event = axum::response::sse::Event::default()
                            .event("lagged")
                            .data(missed.to_string());
                        return Some((Ok(event), (backlog, live, last_id)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    axum::response::sse::Sse::new(stream)
        .keep_alive(axum::response::sse::KeepAlive::new().interval(NOTIFICATION_STREAM_KEEPALIVE))
        .into_response()
}

/// Render one notification as an SSE event.
///
/// The `id` is the audit sequence, which is what makes `Last-Event-ID` resumption work:
/// it is monotonic, gapless and already the identity the audit trail uses.
pub(super) fn notification_sse_event(
    notification: &ControlNotification,
) -> axum::response::sse::Event {
    axum::response::sse::Event::default()
        .id(notification.id.to_string())
        .event("notification")
        .json_data(notification_to_cloudevent(notification))
        .unwrap_or_else(|error| {
            axum::response::sse::Event::default()
                .event("error")
                .data(format!("failed to encode notification: {error}"))
        })
}
