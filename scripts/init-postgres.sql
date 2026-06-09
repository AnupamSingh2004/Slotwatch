-- This script runs automatically when the Postgres container starts for the first time.
-- It sets up everything Slotwatch needs — you never need to run SQL manually for the demo.

-- Create the slotwatch replication user
CREATE USER slotwatch REPLICATION LOGIN PASSWORD 'secret';
GRANT CONNECT ON DATABASE myapp TO slotwatch;
GRANT USAGE ON SCHEMA public TO slotwatch;

-- Demo tables — replace these with your own schema for real use
CREATE TABLE IF NOT EXISTS orders (
    id         SERIAL PRIMARY KEY,
    item       TEXT NOT NULL,
    qty        INT  NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    email TEXT NOT NULL
);

-- Grant Slotwatch read access (required for logical replication)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO slotwatch;

-- Publication tells Postgres which tables to include in the WAL stream.
-- Add any table you want Slotwatch to capture here.
CREATE PUBLICATION slotwatch_pub FOR TABLE public.orders, public.users;

-- Seed a few rows so the dashboard has something to show immediately
INSERT INTO users (name, email) VALUES
    ('alice', 'alice@example.com'),
    ('bob',   'bob@example.com');

INSERT INTO orders (item, qty) VALUES
    ('keyboard', 1),
    ('monitor',  2);
