-- rustcdc docker-compose example: initial database setup
-- Creates a demo table, enables logical replication for it, and creates the
-- replication slot + publication that the cdc-rs container expects.

CREATE TABLE IF NOT EXISTS public.users (
    id      BIGSERIAL PRIMARY KEY,
    name    TEXT      NOT NULL,
    email   TEXT      NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE public.users REPLICA IDENTITY FULL;

-- Seed a few rows so the snapshot phase has data to emit.
INSERT INTO public.users (name, email) VALUES
    ('Alice',   'alice@example.com'),
    ('Bob',     'bob@example.com'),
    ('Charlie', 'charlie@example.com');

-- Publication consumed by the cdc-rs container (matches CDC_RS_PUBLICATION env var).
DROP PUBLICATION IF EXISTS cdc_rs_example_pub;
CREATE PUBLICATION cdc_rs_example_pub FOR TABLE public.users;
