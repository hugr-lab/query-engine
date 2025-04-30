CREATE TABLE "version" (version VARCHAR NOT NULL PRIMARY KEY);
INSERT INTO "version" VALUES ('0.0.5');

CREATE TABLE catalog_sources (
    name TEXT NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    description TEXT,
    path TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE data_sources (
    name TEXT NOT NULL PRIMARY KEY,
    type TEXT NOT NULL,
    description TEXT,
    prefix TEXT NOT NULL,
    as_module BOOLEAN NOT NULL DEFAULT false,
    path TEXT NOT NULL,
    disabled BOOLEAN NOT NULL DEFAULT false,
    self_defined BOOLEAN NOT NULL DEFAULT FALSE,
    read_only BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);

CREATE TABLE data_source_catalogs (
    data_source_name TEXT NOT NULL,
    catalog_name TEXT NOT NULL,
    PRIMARY KEY (data_source_name, catalog_name)
);


CREATE TABLE roles (
    name TEXT NOT NULL PRIMARY KEY,
    description TEXT,
    disabled BOOLEAN NOT NULL DEFAULT FALSE
);

INSERT INTO roles (name, description)
VALUES ('admin', 'Admin role'), ('public', 'Public role'), ('readonly', 'Readonly role');

CREATE TABLE permissions (
    role TEXT NOT NULL,
    type_name TEXT NOT NULL,
    field_name TEXT NOT NULL,
    hidden BOOLEAN NOT NULL DEFAULT FALSE,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    filter JSONB,
    data JSONB,
    PRIMARY KEY (role, type_name, field_name)
);

INSERT INTO permissions (role, type_name, field_name, hidden, disabled)
VALUES
    ('readonly', 'Mutation', '*', false, true);
