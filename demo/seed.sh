#!/usr/bin/env bash
# =============================================================================
# seed.sh — continuously generates INSERT / UPDATE / DELETE events
#
# Runs inside the postgres:16 container (has psql available).
# PGPASSWORD is injected via the compose environment.
# =============================================================================
set -euo pipefail

HOST="${PGHOST:-postgres}"
USER="${PGUSER:-postgres}"
DB="${PGDATABASE:-demo}"

q() { psql -h "$HOST" -U "$USER" -d "$DB" -q "$@"; }

echo "→ Waiting for PostgreSQL to be ready..."
until q -c "SELECT 1" > /dev/null 2>&1; do sleep 1; done
echo "→ PostgreSQL ready."

echo "→ Giving rustcdc 6 s to complete the initial snapshot..."
sleep 6

echo "→ Seeder running — new events every 2 s. Press Ctrl-C to stop."

# Start above the highest-numbered email suffix already in the DB so we never
# collide with rows from a previous run. ON CONFLICT below provides a safety
# net in case of any remaining race.
n=$(q -t -c "SELECT COALESCE(MAX(CAST(regexp_replace(email,'[^0-9]','','g') AS BIGINT)),9)+1 FROM customers WHERE email ~ '^user[0-9]+@example[.]com$';" | tr -d ' \n')
while true; do
    # ── INSERT: new customer ─────────────────────────────────────────────────
    q -c "INSERT INTO customers (name, email, tier)
          VALUES (
              'User $n',
              'user$n@example.com',
              CASE WHEN $n % 3 = 0 THEN 'pro' ELSE 'free' END
          ) ON CONFLICT (email) DO NOTHING;"

    # ── INSERT: new order for that customer ──────────────────────────────────
    q -c "INSERT INTO orders (customer_id, sku, quantity, total_cents)
          SELECT id,
                 CASE WHEN $n % 2 = 0 THEN 'WGT-001' ELSE 'GDG-002' END,
                 ($n % 4) + 1,
                 999 * (($n % 4) + 1)
          FROM   customers
          ORDER  BY id DESC
          LIMIT  1;"

    # ── UPDATE: ship the oldest pending order ────────────────────────────────
    q -c "UPDATE orders
          SET    status = 'shipped'
          WHERE  id = (
              SELECT id FROM orders
              WHERE  status = 'pending'
              ORDER  BY id
              LIMIT  1
          );"

    # ── UPDATE: deliver the oldest shipped order ─────────────────────────────
    q -c "UPDATE orders
          SET    status = 'delivered'
          WHERE  id = (
              SELECT id FROM orders
              WHERE  status = 'shipped'
              ORDER  BY id
              LIMIT  1
          );"

    # ── DELETE: remove old delivered orders (keep table tidy) ───────────────
    q -c "DELETE FROM orders
          WHERE  status = 'delivered'
          AND    id < (SELECT COALESCE(MAX(id) - 20, 0) FROM orders);"

    n=$(( n + 1 ))
    sleep 2
done
