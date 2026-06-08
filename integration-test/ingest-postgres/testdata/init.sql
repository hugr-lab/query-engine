-- Schema used by the /ipc/ingest integration tests.
-- A single events table with a mix of scalar types and an autogen primary key
-- so the tests can also exercise "default value" behaviour (omitting the PK
-- from the Arrow stream).

CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE events (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT true,
    payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    geom GEOMETRY(Point, 4326),
    geom_wkt GEOMETRY(LineString, 4326),
    geom_geojson GEOMETRY(Polygon, 4326),
    geom_wkb GEOMETRY(Point, 4326),
    geom_line GEOMETRY(LineString, 4326),
    geom_polygon_native GEOMETRY(Polygon, 4326),
    geom_multipoint GEOMETRY(MultiPoint, 4326),
    geom_multiline GEOMETRY(MultiLineString, 4326),
    geom_multipolygon GEOMETRY(MultiPolygon, 4326)
);
