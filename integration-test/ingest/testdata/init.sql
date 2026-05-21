-- Schema used by the /ipc/ingest integration tests.
-- A single events table with a mix of scalar types and an autogen primary key
-- so the tests can also exercise "default value" behaviour (omitting the PK
-- from the Arrow stream).

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
