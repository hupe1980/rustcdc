#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./scripts/runbook-force-reset-checklist.sh \
  --change-ticket <id> \
  --snapshot-backed-up \
  --downstream-paused \
  --slot-impact-reviewed \
  --rollback-plan-ready

Purpose:
  Guardrail checklist before executing destructive force-reset recovery steps
  from docs/runbook.md. This script does not perform any mutation.

Required acknowledgements:
  --change-ticket <id>      Incident/change record for audit traceability.
  --snapshot-backed-up      Confirm current checkpoint/offset snapshot was archived.
  --downstream-paused       Confirm downstream consumers are paused or dedup-safe.
  --slot-impact-reviewed    Confirm replication-slot/binlog retention impact reviewed.
  --rollback-plan-ready     Confirm rollback and operator-on-call plan is prepared.
USAGE
}

change_ticket=""
ack_snapshot=0
ack_downstream=0
ack_slot=0
ack_rollback=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --change-ticket)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --change-ticket" >&2
        exit 2
      fi
      change_ticket="$2"
      shift 2
      ;;
    --snapshot-backed-up)
      ack_snapshot=1
      shift
      ;;
    --downstream-paused)
      ack_downstream=1
      shift
      ;;
    --slot-impact-reviewed)
      ack_slot=1
      shift
      ;;
    --rollback-plan-ready)
      ack_rollback=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$change_ticket" ]]; then
  echo "missing required --change-ticket <id>" >&2
  exit 2
fi

missing=0
if [[ "$ack_snapshot" -eq 0 ]]; then
  echo "checklist failed: --snapshot-backed-up is required" >&2
  missing=1
fi
if [[ "$ack_downstream" -eq 0 ]]; then
  echo "checklist failed: --downstream-paused is required" >&2
  missing=1
fi
if [[ "$ack_slot" -eq 0 ]]; then
  echo "checklist failed: --slot-impact-reviewed is required" >&2
  missing=1
fi
if [[ "$ack_rollback" -eq 0 ]]; then
  echo "checklist failed: --rollback-plan-ready is required" >&2
  missing=1
fi

if [[ "$missing" -ne 0 ]]; then
  echo
  echo "DO NOT RUN FORCE RESET. Complete all checklist acknowledgements first." >&2
  exit 1
fi

cat <<SUMMARY
Force-reset preflight checklist passed.
Change ticket: $change_ticket
Timestamp: $(date -u +"%Y-%m-%dT%H:%M:%SZ")
Summary: destructive recovery preconditions acknowledged.
SUMMARY
