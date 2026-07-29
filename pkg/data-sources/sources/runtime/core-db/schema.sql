{{ if isPostgres }}CREATE EXTENSION IF NOT EXISTS vector;{{ end }}

CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}"version" AS SELECT '0.0.20' AS "version";

CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}catalog_sources (
    name VARCHAR NOT NULL PRIMARY KEY,
    type VARCHAR NOT NULL,
    description VARCHAR,
    path VARCHAR NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}data_sources (
    name VARCHAR NOT NULL PRIMARY KEY,
    type VARCHAR NOT NULL,
    description VARCHAR,
    prefix VARCHAR NOT NULL,
    as_module BOOLEAN NOT NULL DEFAULT false,
    path VARCHAR NOT NULL,
    disabled BOOLEAN NOT NULL DEFAULT false,
    self_defined BOOLEAN NOT NULL DEFAULT FALSE,
    read_only BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}data_source_catalogs (
    data_source_name VARCHAR NOT NULL,
    catalog_name VARCHAR NOT NULL,
    PRIMARY KEY (data_source_name, catalog_name)
);


CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}roles (
    name VARCHAR NOT NULL PRIMARY KEY,
    description VARCHAR,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    can_impersonate BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO {{ if isAttachedDuckdb }}core.{{ end }}roles (name, description, can_impersonate)
VALUES ('admin', 'Admin role', TRUE), ('public', 'Public role', FALSE), ('readonly', 'Readonly role', FALSE);

CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}permissions (
    role VARCHAR NOT NULL,
    type_name VARCHAR NOT NULL,
    field_name VARCHAR NOT NULL,
    hidden BOOLEAN NOT NULL DEFAULT FALSE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    filter JSON,
    data JSON,
    PRIMARY KEY (role, type_name, field_name)
);

INSERT INTO {{ if isAttachedDuckdb }}core.{{ end }}permissions (role, type_name, field_name, hidden, disabled)
VALUES
    ('readonly', 'Mutation', '*', false, true);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}api_keys (
    name VARCHAR PRIMARY KEY,
    key VARCHAR NOT NULL UNIQUE,
    description VARCHAR,
    default_role VARCHAR NOT NULL,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    is_temporal BOOLEAN NOT NULL DEFAULT FALSE,
    expires_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    headers {{if isPostgres }} JSONB {{ else }} JSON {{ end }},
    claims {{if isPostgres }} JSONB {{ else }} JSON {{ end }},
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- _schema_settings is NOT compiled-schema storage despite the name: it carries
-- the schema_version counter cluster workers poll and the catalog storage
-- reads and writes. The eleven tables that WERE the compiled-schema provider
-- went with it in design-036 — a source's logical model lives in catalog.*
-- (hugr_catalog.sql) and the GraphQL schema is generated from it on read.

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_settings (
    key VARCHAR NOT NULL PRIMARY KEY,
    value {{if isPostgres }} JSONB {{ else }} JSON {{ end }} NOT NULL
);

-- Cluster node registry. Each node UPSERTs on startup, updates last_heartbeat periodically.
CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_cluster_nodes (
    name VARCHAR NOT NULL PRIMARY KEY,
    url VARCHAR NOT NULL,
    role VARCHAR NOT NULL,
    version VARCHAR,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_heartbeat TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    error VARCHAR
);

-- Schema version counter for cluster change detection.
INSERT INTO {{ if isAttachedDuckdb }}core.{{ end }}_schema_settings (key, value)
VALUES ('schema_version', '"0"')
ON CONFLICT (key) DO NOTHING;

-- Seed vec_size so ensureVectorSize() sees the correct stored dimension on first boot.
{{ if gt .VectorSize 0 }}
INSERT INTO {{ if isAttachedDuckdb }}core.{{ end }}_schema_settings (key, value)
VALUES ('config', '{"vec_size": {{ .VectorSize }}}')
ON CONFLICT (key) DO UPDATE SET value = '{"vec_size": {{ .VectorSize }}}';
{{ end }}
