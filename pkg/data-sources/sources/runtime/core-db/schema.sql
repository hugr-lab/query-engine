{{ if isPostgres }}CREATE EXTENSION IF NOT EXISTS vector;{{ end }}

CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}"version" AS SELECT '0.0.12' AS "version";

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
    disabled BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO {{ if isAttachedDuckdb }}core.{{ end }}roles (name, description)
VALUES ('admin', 'Admin role'), ('public', 'Public role'), ('readonly', 'Readonly role');

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

-- Schema storage tables for DB-backed schema provider.
-- FK constraints are intentionally omitted for DuckDB compatibility;
-- referential integrity is maintained at the application level.

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_catalogs (
    name VARCHAR NOT NULL PRIMARY KEY,
    version VARCHAR NOT NULL DEFAULT '',
    description VARCHAR NOT NULL DEFAULT '',
    long_description VARCHAR NOT NULL DEFAULT '',
    source_type VARCHAR NOT NULL DEFAULT '',
    prefix VARCHAR NOT NULL DEFAULT '',
    as_module BOOLEAN NOT NULL DEFAULT FALSE,
    read_only BOOLEAN NOT NULL DEFAULT FALSE,
    is_summarized BOOLEAN NOT NULL DEFAULT FALSE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    suspended BOOLEAN NOT NULL DEFAULT FALSE,
    vec {{if isPostgres }} vector({{ .VectorSize }}) {{ else }} FLOAT[{{ .VectorSize }}] {{ end }}
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_catalog_dependencies (
    catalog_name VARCHAR NOT NULL,
    depends_on VARCHAR NOT NULL,
    PRIMARY KEY (catalog_name, depends_on)
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_types (
    name VARCHAR NOT NULL PRIMARY KEY,
    kind VARCHAR NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    long_description VARCHAR NOT NULL DEFAULT '',
    hugr_type VARCHAR NOT NULL DEFAULT '',
    module VARCHAR NOT NULL DEFAULT '',
    catalog VARCHAR,
    directives {{if isPostgres }} JSONB {{ else }} JSON {{ end }} NOT NULL DEFAULT '[]',
    interfaces VARCHAR NOT NULL DEFAULT '',
    union_types VARCHAR NOT NULL DEFAULT '',
    is_summarized BOOLEAN NOT NULL DEFAULT FALSE,
    vec {{if isPostgres }} vector({{ .VectorSize }}) {{ else }} FLOAT[{{ .VectorSize }}] {{ end }}
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_fields (
    type_name VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    field_type VARCHAR NOT NULL,
    field_type_name VARCHAR NOT NULL DEFAULT '',
    description VARCHAR NOT NULL DEFAULT '',
    long_description VARCHAR NOT NULL DEFAULT '',
    hugr_type VARCHAR NOT NULL DEFAULT '',
    catalog VARCHAR,
    dependency_catalog VARCHAR,
    directives {{if isPostgres }} JSONB {{ else }} JSON {{ end }} NOT NULL DEFAULT '[]',
    is_pk BOOLEAN NOT NULL DEFAULT FALSE,
    is_summarized BOOLEAN NOT NULL DEFAULT FALSE,
    vec {{if isPostgres }} vector({{ .VectorSize }}) {{ else }} FLOAT[{{ .VectorSize }}] {{ end }},
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (type_name, name)
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_arguments (
    type_name VARCHAR NOT NULL,
    field_name VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    arg_type VARCHAR NOT NULL,
    arg_type_name VARCHAR NOT NULL DEFAULT '',
    default_value VARCHAR,
    description VARCHAR NOT NULL DEFAULT '',
    directives {{if isPostgres }} JSONB {{ else }} JSON {{ end }} NOT NULL DEFAULT '[]',
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (type_name, field_name, name)
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_enum_values (
    type_name VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL DEFAULT '',
    directives {{if isPostgres }} JSONB {{ else }} JSON {{ end }} NOT NULL DEFAULT '[]',
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (type_name, name)
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_directives (
    name VARCHAR NOT NULL PRIMARY KEY,
    description VARCHAR NOT NULL DEFAULT '',
    locations VARCHAR NOT NULL DEFAULT '', -- pipe-separated: e.g. "FIELD_DEFINITION|ARGUMENT_DEFINITION"
    is_repeatable BOOLEAN NOT NULL DEFAULT FALSE,
    arguments VARCHAR NOT NULL DEFAULT '[]' -- JSON array of argument definitions
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_modules (
    name VARCHAR NOT NULL PRIMARY KEY,
    description VARCHAR NOT NULL DEFAULT '',
    long_description VARCHAR NOT NULL DEFAULT '',
    query_root VARCHAR,
    mutation_root VARCHAR,
    function_root VARCHAR,
    mut_function_root VARCHAR,
    is_summarized BOOLEAN NOT NULL DEFAULT FALSE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    vec {{if isPostgres }} vector({{ .VectorSize }}) {{ else }} FLOAT[{{ .VectorSize }}] {{ end }}
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_module_type_catalogs (
    module_name VARCHAR NOT NULL,
    type_name VARCHAR NOT NULL,
    catalog_name VARCHAR NOT NULL,
    PRIMARY KEY (type_name, catalog_name)
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_data_objects (
    name VARCHAR NOT NULL PRIMARY KEY,
    filter_type_name VARCHAR,
    args_type_name VARCHAR
);

CREATE TABLE IF NOT EXISTS {{ if isAttachedDuckdb }}core.{{ end }}_schema_data_object_queries (
    name VARCHAR NOT NULL,
    object_name VARCHAR NOT NULL,
    query_root VARCHAR NOT NULL,
    query_type VARCHAR NOT NULL,
    PRIMARY KEY (name, object_name)
);

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

-- Non-PK indexes for query performance (both DuckDB and PostgreSQL).

-- _schema_types: frequent filters in type loading CTE and reconcile queries
CREATE INDEX IF NOT EXISTS idx_schema_types_catalog   ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_types (catalog);
CREATE INDEX IF NOT EXISTS idx_schema_types_hugr_type ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_types (hugr_type);
CREATE INDEX IF NOT EXISTS idx_schema_types_kind      ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_types (kind);

-- _schema_fields: FK-like lookups on type_name, field filtering, dependency detection
CREATE INDEX IF NOT EXISTS idx_schema_fields_type_name          ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_fields (type_name);
CREATE INDEX IF NOT EXISTS idx_schema_fields_catalog            ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_fields (catalog);
CREATE INDEX IF NOT EXISTS idx_schema_fields_hugr_type          ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_fields (hugr_type);
CREATE INDEX IF NOT EXISTS idx_schema_fields_dependency_catalog ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_fields (dependency_catalog);

-- _schema_arguments: FK-like lookups on type_name and (type_name, field_name)
CREATE INDEX IF NOT EXISTS idx_schema_args_type_name  ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_arguments (type_name);
CREATE INDEX IF NOT EXISTS idx_schema_args_type_field ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_arguments (type_name, field_name);

-- _schema_enum_values: FK-like lookups on type_name
CREATE INDEX IF NOT EXISTS idx_schema_enumvals_type_name ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_enum_values (type_name);

-- _schema_module_type_catalogs: catalog_name-only lookups for reconcile/cascade
CREATE INDEX IF NOT EXISTS idx_schema_mtc_catalog_name ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_module_type_catalogs (catalog_name);

-- _schema_data_object_queries: object_name-only lookups for cleanup DELETEs
CREATE INDEX IF NOT EXISTS idx_schema_doq_object_name ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_data_object_queries (object_name);

-- _schema_catalog_dependencies: reverse lookup by depends_on
CREATE INDEX IF NOT EXISTS idx_schema_catdeps_depends_on ON {{ if isAttachedDuckdb }}core.{{ end }}_schema_catalog_dependencies (depends_on);

{{ if isPostgres }}
-- PostgreSQL-specific: vector similarity indexes (HNSW)
CREATE INDEX IF NOT EXISTS _schema_catalogs_vec_idx ON _schema_catalogs USING hnsw (vec vector_cosine_ops);
CREATE INDEX IF NOT EXISTS _schema_types_vec_idx ON _schema_types USING hnsw (vec vector_cosine_ops);
CREATE INDEX IF NOT EXISTS _schema_fields_vec_idx ON _schema_fields USING hnsw (vec vector_cosine_ops);
CREATE INDEX IF NOT EXISTS _schema_modules_vec_idx ON _schema_modules USING hnsw (vec vector_cosine_ops);
{{ end }}