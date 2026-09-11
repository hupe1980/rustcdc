-- =============================================================================
-- rustcdc demo — PostgreSQL bootstrap
-- Executed once by the postgres container on first start.
-- =============================================================================

-- ── Tables ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS customers (
    id         SERIAL       PRIMARY KEY,
    name       TEXT         NOT NULL,
    email      TEXT         NOT NULL UNIQUE,
    tier       TEXT         NOT NULL DEFAULT 'free',
    created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS products (
    id          SERIAL   PRIMARY KEY,
    sku         TEXT     NOT NULL UNIQUE,
    name        TEXT     NOT NULL,
    price_cents INTEGER  NOT NULL,
    stock       INTEGER  NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL       PRIMARY KEY,
    customer_id INTEGER      NOT NULL REFERENCES customers(id),
    sku         TEXT         NOT NULL,
    quantity    INTEGER      NOT NULL DEFAULT 1,
    total_cents INTEGER      NOT NULL,
    status      TEXT         NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ── Replica identity ──────────────────────────────────────────────────────────
-- FULL: capture complete before/after row images on UPDATE and DELETE.

ALTER TABLE customers REPLICA IDENTITY FULL;
ALTER TABLE products  REPLICA IDENTITY FULL;
ALTER TABLE orders    REPLICA IDENTITY FULL;

-- ── Dedicated CDC user (minimal privileges) ───────────────────────────────────
-- REPLICATION: create/drop replication slots, start logical replication.
-- pg_monitor: read pg_replication_slots, call pg_logical_slot_peek_binary_changes.
-- SELECT on all tables: required for initial snapshot reads.

CREATE USER cdc_user WITH REPLICATION LOGIN PASSWORD 'cdc_password';
GRANT pg_monitor TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_user;

-- ── Publication ───────────────────────────────────────────────────────────────

CREATE PUBLICATION cdc_demo_pub FOR TABLE customers, products, orders;

-- ── Seed data ─────────────────────────────────────────────────────────────────
-- These rows will appear as "read" op events during the initial snapshot.

INSERT INTO customers (name, email, tier) VALUES
    ('Alice Johnson', 'alice@example.com', 'pro'),
    ('Bob Smith',     'bob@example.com',   'free'),
    ('Carol White',   'carol@example.com', 'pro');

INSERT INTO products (sku, name, price_cents, stock) VALUES
    ('WGT-001', 'Widget',     999, 200),
    ('GDG-002', 'Gadget',    4999,  50),
    ('DOO-003', 'Doohickey',  299, 500);

INSERT INTO orders (customer_id, sku, quantity, total_cents, status) VALUES
    (1, 'WGT-001', 2, 1998, 'delivered'),
    (2, 'GDG-002', 1, 4999, 'shipped'),
    (3, 'DOO-003', 5, 1495, 'pending');
