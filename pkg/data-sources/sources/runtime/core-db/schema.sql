CREATE TABLE {{ if isAttachedDuckdb }}core.{{ end }}"version" AS SELECT '0.0.8' AS "version";

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