+++
title = "Runbook"
description = "Incident procedures for rustcdc: replication slot growth, checkpoint corruption, quarantined events, lease conflicts, disaster recovery, upgrade and rollback."
weight = 70
+++

Operational procedures for rustcdc. Written to be usable at 3 a.m. by someone who did
not build it.

Each entry states the **symptom** you will actually see, what it means, and what to do.
Diagnosis first, action second, and where an action loses data it says so in the step
rather than in a footnote.

> **One rule above all others.** Never hand-edit a checkpoint file. It carries a
> `content_checksum` and the runtime refuses to start on a mismatch — by design. A
> checkpoint that parses but is wrong resumes capture from the wrong position and skips
> events with no error anywhere.


## 1. First response

Three commands, in this order. They separate "the process is unhealthy" from "the
process is fine and the data is not".

```bash
# 1. Is it alive and does it think it is healthy?
curl -sk https://<host>:8080/livez        # 200 = running, 503 = Error/degraded

# 2. What does it think is wrong? (needs the read token)
curl -sk -H "Authorization: Bearer $READ_TOKEN" https://<host>:8080/status | jq '{
  state, last_terminal_reason_code, checkpoint_age_seconds, slo
}'

# 3. What do the numbers say?
curl -sk -H "Authorization: Bearer $READ_TOKEN" https://<host>:8080/metrics \
  | grep -E 'rustcdc_(runtime_health|slo_checkpoint_age_seconds|dlq_events_total|source_consecutive_poll_errors)'
```

`rustcdc_runtime_health` is a one-hot gauge — exactly one `verdict` label is `1`:

| Verdict | Meaning | Alertable |
|---|---|---|
| `healthy` | Progressing | — |
| `idle` | Connected, no changes upstream | **No.** A quiet database is not an incident |
| `stalled` | Running but not progressing | **Yes.** Go to §3 |
| `degraded` | Recoverable errors accumulating | Yes. Go to §7 |

`last_terminal_reason_code` in `/status` is the single most useful field after a crash:
it names the subsystem that ended the run (`batch_delivery_error`,
`checkpoint_barrier_commit_error`, `runtime_poll_error`, …) without needing logs.


## 2. Pipeline will not start

### `another cdc-server process (PID N) is already running against state directory`

Local PID lock. Either a second process really is running, or a previous one was killed
without cleanup.

```bash
ps -p <PID> -o pid,comm,args     # is it actually a rustcdc process?
# If it is gone, the lock is stale and the next start overwrites it automatically.
# If the file is orphaned on a shared volume:
rm <state-dir>/.cdc-server.lock
```

### `<backend> state is already owned by '<host:pid:nonce>'`

Remote owner lease for the `redis` / `postgresql` state backends. Another instance holds
it.

**The overwhelmingly common cause is a Kubernetes rolling update.** A `Deployment`
without `strategy: Recreate` starts the new pod before stopping the old one, so both
claim the state. Fix the Deployment — see
[operations.md §11](@/docs/operations.md#11-kubernetes-deployment). This is not optional.

If the previous owner is genuinely gone, the lease expires by itself after **60 s**;
just wait. Do not delete the lease key to hurry it along unless you have confirmed the
other process is dead — that is precisely the check being bypassed.

### `failed to initialize the kafka state producer's transactional state`

The Kafka state backend fences by producer epoch. This means another instance took the
`transactional.id`, or the broker is unreachable. Same cause and same fix as above; check
broker connectivity first with the same `brokers` value from the config.

### `429 Too Many Requests` from the admin API

Rate limiting runs **before** authentication, so a `429` says nothing about your
credentials. Buckets are per client IP (or per `X-Forwarded-For` client behind a trusted
proxy), default 20 rps with a burst of 40 per endpoint.

A first-time client gets the full configured burst. If you are seeing `429` at low
request rates, check whether the limiter is tracking a very large number of distinct
client keys — under that pressure new keys are deliberately admitted with a single token
to stop an address-rotating attacker multiplying their allowance. That is the attack
signature, not a misconfiguration.

### `source password must use a deferred secret reference`

By design. Put the credential in a secret manager and reference it:
`password = { env = "POSTGRES_PASSWORD" }`.

### `unrecognised configuration key(s): ...`

A typo, or a key under the wrong table. The message names the full path. This is
deliberate: a misspelled `table_include_lst` leaves the include list empty, which
captures **every table in the database**.

### `postgres at '<host>' refused TLS (the server replied 'N')`

The connector is configured with `transport.mode = "tls"` and the server has
`ssl = off`. It now fails instead of silently continuing unencrypted — previously
this connection downgraded to plaintext with no error and no warning, detectable only
with a packet capture.

Enable TLS on the server, or state the trade-off explicitly:

```toml
[source.postgres.transport]
mode = "plaintext"   # credentials and change data in the clear
```

### `replication slot "<slot>" is active for PID <n>`

An out-of-band `pg_replication_slot_advance` or `pg_drop_replication_slot` was run
against a slot a live pipeline holds. Under the default
`wal_transport = "streaming_replication"` a walsender holds the slot for the life of
the stream, and PostgreSQL refuses both operations on an active slot.

Stop the pipeline first, run the operation, then start it again. This did not apply
under `sql_peek`, where nothing held the slot persistently — an operator script
carried over from that transport is the usual source.


## 3. Checkpoint is not advancing

**Symptom:** `rustcdc_slo_checkpoint_age_seconds` climbing;
`rustcdc_runtime_health{verdict="stalled"} 1`.

Work down this list — it is ordered by how often each is the cause.

1. **Is anything happening upstream?** `verdict="idle"` means no changes to capture.
   Confirm with a write to a captured table.
2. **Is the sink accepting?** Check `rustcdc_sink_send_ops_total` for movement and
   `rustcdc_runtime_batch_delivery_latency_seconds` for a p99 blowout. A sink that has
   stopped acknowledging stalls the pipeline by design — the checkpoint must not
   advance past undelivered data.
3. **Is the source connection degraded?** `rustcdc_source_consecutive_poll_errors > 0`.
4. **Is the breaker open?** `rustcdc_runtime_recoverable_breaker_open_consecutive > 0`
   → §7.

```bash
# What position is actually stored?
rustcdc inspect-checkpoint --config-file /etc/rustcdc/config.toml
```

If the checkpoint is advancing but downstream is behind, the pipeline is fine and the
consumer is the problem — check lag on the sink side, not here.

### `refusing checkpoint write ... the stream position moved backwards`

The connector offered a resume position **behind** the one already stored while the
committed-event count kept rising. That is not a replay: a replay forgets progress,
and this reports progress while recording a position before data the sink has already
committed. The write is refused rather than accepted, so the pipeline halts loudly
instead of resuming from a position the stream never reached.

Three causes, in order of likelihood:

1. **The source was repointed or rebuilt.** A different server, a restored backup, or
   a `pg_resetwal`. The stored position describes a log that no longer exists. Clear
   the checkpoint directory and re-snapshot — see §9.
2. **A failover on MySQL/MariaDB without GTID.** Binlog file+position is server-local
   and a promoted replica's coordinates are routinely lower. Enable
   `gtid_mode_enabled`: with a GTID set the coordinates are not compared at all,
   because the GTID is what resumes the stream.
3. **A connector defect.** If neither of the above applies, the message names both
   positions — report it with that pair.

The guard is deliberately narrow and does not fire on legitimate movement: PostgreSQL
LSNs go backwards routinely under concurrent writers (pgoutput emits in *commit*
order while each change keeps its own WAL position), so only a zero LSN is caught
there.


## 4. Replication slot growth (PostgreSQL)

**Symptom:** `rustcdc_runtime_replication_slot_lag_bytes` growing steadily.

This is the failure that takes the **source database** down, so it outranks almost
everything else. An unconsumed slot pins WAL forever; the disk fills; PostgreSQL stops
accepting writes.

```sql
SELECT slot_name, active, restart_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained
FROM pg_replication_slots;
```

- **`active = false` and the pipeline is running** → it is not connected. Go to §2.
- **`active = true` and `retained` still growing** → the pipeline is connected but not
  confirming. Go to §3; the cause is almost always a stalled sink.
- **The pipeline is decommissioned** → drop the slot. Nothing else releases the WAL:

  ```sql
  SELECT pg_drop_replication_slot('cdc_slot');
  ```

  Dropping the slot **discards every change since the last confirmed position**. If the
  pipeline will return, it will need a fresh snapshot.

Set `max_slot_wal_keep_size` on the server so PostgreSQL invalidates a runaway slot
rather than exhausting the volume. An invalidated slot loses data and needs a
re-snapshot — bad, but survivable, unlike a full WAL volume.


## 5. Events are being quarantined

**Symptom:** `rustcdc_dlq_events_total` increasing;
`RUSTCDCEventsQuarantined` firing.

**Every one of these is an event that was not delivered and whose checkpoint advanced
anyway.** It is recorded data loss. Treat it as a data incident, not a warning.

```bash
# What was dropped and why?
tail -n 50 /var/lib/rustcdc/dlq.jsonl | jq -r '[.ts_ms, .table, .source_offset, .error] | @tsv'

# Which tables and causes dominate?
jq -r '.error' /var/lib/rustcdc/dlq.jsonl | sort | uniq -c | sort -rn
```

With the `kafka` target the same questions are answerable from the record headers, without
deserialising anything — the record key is the source table, and
`__rustcdc.dlq.source.table`, `__rustcdc.dlq.source.offset`, `__rustcdc.dlq.sink` and
`__rustcdc.dlq.exception.message` carry the rest:

```bash
kafka-console-consumer --bootstrap-server kafka:9092 --topic cdc.dlq \
  --from-beginning --property print.headers=true --property print.key=true
```

Common causes and their fixes:

| Error contains | Cause | Fix |
|---|---|---|
| `max_event_bytes` | Encoded payload over the limit | Raise `runtime.max_event_bytes` to just under the broker's `max.message.bytes`, or exclude the offending column with a transform |
| `codec` / `schema` | Registry rejected the schema | Reconcile the subject's compatibility mode; check the registry is reachable |
| `ValidationError` | Envelope contract violation after a transform | Inspect the transform rule that touched the table |

**Replaying after the fix.** The DLQ is JSONL of full events, which is what `replay`
consumes:

```bash
jq -c '.event' /var/lib/rustcdc/dlq.jsonl > /tmp/replay.jsonl
rustcdc replay /tmp/replay.jsonl --config-file /etc/rustcdc/config.toml
```

Replay delivers to the configured sink and does **not** move the checkpoint. Under
`at_least_once` a replayed event that had partially succeeded may duplicate; that is
within contract. Truncate the DLQ only after you have confirmed the replay landed.


## 6. Sink is failing

**Symptom:** batch delivery errors in the log; the pipeline retries and may terminate.

Recoverable sink failures are retried under `[runtime]`'s backoff and circuit-breaker
policy — the *same* policy the source uses. A terminal exit means either the breaker
escalated (§7) or the failure was classified non-recoverable.

Non-recoverable means "the same attempt will fail identically": an oversized event, a
config error. Those go to the DLQ if `[dlq]` is configured (§5) and halt the pipeline if
it is not.

```bash
# Is it the sink or the network?
rustcdc dry-run --config-file /etc/rustcdc/config.toml --events 10
```

`dry-run` pushes synthetic events through the real sink. If it succeeds, the sink is
reachable and the problem is data-shaped, not connectivity.

**Known limitation.** A transient failure surfacing during a sink *flush* rather than a
send is currently treated as terminal, because the router flattens per-sink flush errors
into one string and loses the classification. The pipeline restarts and resumes from the
checkpoint; no data is lost, but the restart is avoidable noise.


## 7. Circuit breaker keeps opening

**Symptom:** `rustcdc_runtime_recoverable_breaker_open_consecutive` rising; repeated
`degraded` verdicts.

The breaker exists to convert an endless retry loop into a loud failure. Repeated
opening means the underlying dependency is genuinely unhealthy — the answer is upstream,
not in these settings.

Widen the policy only when the dependency is *known* to be slow-but-recovering (a
failover in progress, say):

```toml
[runtime]
recoverable_error_breaker_consecutive_threshold = 30     # was 10
recoverable_error_breaker_cooldown_ms           = 60000  # was 30000
recoverable_error_breaker_max_open_cycles       = 10     # was 3
```

Widening it while the dependency is actually down just delays the page.


## 8. Duplicate events downstream

Expected under `at_least_once` — that is the contract. Consumers must be idempotent.

**Not** expected under `effectively_once`. The batch's records and its checkpoint commit
in one Kafka transaction, so a crash discards both or keeps both; there is no window that
replays a committed batch. See [delivery contracts](@/docs/concepts.md#3-delivery-contracts).
If you are seeing duplicates under that contract, check first:

1. Is the consumer reading `read_committed`? A `read_uncommitted` consumer sees records
   from aborted transactions, and no producer-side guarantee prevents that. This is by far
   the most common cause.
2. Is the duplicate actually a *retry* of an aborted batch? Compare the producer epoch in
   the record headers; an aborted attempt and its replay are distinct transactions and only
   the second is committed.

For either contract, a duplicate burst that is **not** explained by a restart is
different. Check:

1. Did two instances run concurrently? Look for lease-conflict errors around the burst
   (§2). This is the signature of a rolling update without `strategy: Recreate`.
2. Did the checkpoint move backwards? Compare `inspect-checkpoint` against the last
   known position from your logs.


## 9. Disaster recovery

### Lost state directory, source intact

The pipeline resumes from the *live head* if there is no checkpoint, which **silently
skips everything since the last durable position**. That is almost never what you want.

Deliberate choice, in order of preference:

1. **Restore the state directory from backup.** Fastest and loses nothing after the
   backup point. Take state backups.
2. **Re-snapshot.** Correct and complete; costs a full table read and duplicates
   everything the sink already has. Consumers must be idempotent.
   ```bash
   # Remove state, then start with snapshot_tables configured.
   rustcdc run --config-file /etc/rustcdc/config.toml --snapshot-table public.orders
   ```
3. **Accept the gap.** Only with an explicit, written decision about which window of
   changes is being abandoned.

### Corrupt checkpoint

```
Checkpoint error: … integrity check …
```

Working as designed — the checksum caught a damaged file rather than resuming from a
wrong position. Do not edit it. Restore from backup, or re-snapshot as above.

### Lost source (database rebuilt / failed over)

A logical replication slot does **not** survive a rebuild, and on most managed platforms
does not survive a failover either. After either, the old checkpoint refers to a position
that no longer exists.

1. Stop the pipeline.
2. Recreate the publication and slot on the new primary.
3. Clear the state directory — the old LSN is meaningless against a new timeline.
4. Start with a snapshot.


## 10. Upgrade and rollback

### Upgrade

1. Read the release notes for state-format changes.
2. Update the image tag.
3. `kubectl rollout restart deployment/rustcdc`.

**What CI already checked for you.** `tests/state_compatibility.rs` holds frozen state
artefacts — a real checkpoint file with its `content_checksum`, snapshot state in the shape
a previous release wrote, every version of the Kafka state-topic record, the `local_fs`
owner lease, and the processed-signal ledger — and asserts this build reads all of them
with the right *values*, not merely that they parse. A release that could not resume an
existing pipeline fails the build rather than the deploy.

That covers the forward direction only. It cannot cover rollback, because the old binary is
not present to test against; see below.

With `strategy: Recreate` (**required** — see
[operations.md §11](@/docs/operations.md#11-kubernetes-deployment)) the old pod stops before
the new one starts. Capture pauses for `terminationGracePeriodSeconds` plus startup;
the source retains the changes.

Confirm success: `rustcdc_slo_checkpoint_age_seconds` returns to baseline and
`rustcdc_runtime_health{verdict="healthy"}` is `1`.

### Rollback

Roll the image tag back and restart. Safe **as long as the state format did not
change** — checkpoints are forward-compatible within a format version, not backward.

The asymmetry is deliberate and worth understanding: a new build reading old state fills in
absent fields with documented defaults, and CI proves it does. An *old* build reading new
state has no such rule — it sees fields it was never taught, and depending on the struct it
either ignores them (losing whatever they recorded) or refuses the file. A concrete example
from 0.12: snapshot state gained `stopped`. Roll back to a build that predates it and a
backfill an operator deliberately stopped starts again from row zero on the next restart,
because the field carrying that decision is one the old build cannot see.

If the release notes flagged a state-format change, rolling back requires the state as
it was *before* the upgrade:

1. Stop the pipeline.
2. Restore the state directory from the pre-upgrade backup.
3. Deploy the old image.

**Take a state backup immediately before any upgrade whose notes mention state.** There
is no downgrade path for state written by a newer format, and no tool that reconstructs
it.

### Config change

`validate-config` parses and validates without starting — use it in CI:

```bash
rustcdc validate-config --config-file cdc.toml --print-json
```

It resolves environment variables and prints the redacted result, so it also confirms
your secret references actually resolve in the target environment.


## 11. Credential rotation

### Source password

Referenced by env var, so rotation is a restart, not a config change:

1. Update the secret.
2. Restart the pod. Capture pauses for the restart and resumes from the checkpoint.

### Admin tokens

Static tokens (`admin.read_token_env` / `write_token_env`) require a restart. For
rotation without downtime use a signed token manifest
(`admin.token_manifest_file`), which is reloaded in place and supports overlapping
validity and revocation.

### Audit signing key

Rotating `admin.audit_signing_key_env` invalidates signatures on **previously exported**
audit trails — verify and archive before rotating. Confirm the new key is in effect:
an unset or malformed key logs at `warn` on startup and exports go out **unsigned**.

### Masking HMAC key

Rotating a `mask_hash` key changes every pseudonym it has ever produced, so
previously-emitted values will no longer join to newly-emitted ones. Treat it as a
data-model change, not a credential rotation.
