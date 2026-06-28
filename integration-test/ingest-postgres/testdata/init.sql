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
    owner_id BIGINT,
    payload JSONB,
    payload_large_string JSONB,
    payload_string_view JSONB,
    payload_binary JSONB,
    payload_large_binary JSONB,
    payload_binary_view JSONB,
    payload_struct JSONB,
    payload_list JSONB,
    payload_large_list JSONB,
    payload_fixed_size_list JSONB,
    payload_list_view JSONB,
    payload_large_list_view JSONB,
    payload_map JSONB,
    payload_scalar JSONB,
    payload_arrow_json JSONB,
    payload_geo_point JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    geom GEOMETRY(Point, 0),
    geom_4326 GEOMETRY(Point, 4326),
    geom_wkt GEOMETRY(LineString, 0),
    geom_wkt_4326 GEOMETRY(LineString, 4326),
    geom_geojson GEOMETRY(Polygon, 0),
    geom_hugr_geojson GEOMETRY(Polygon, 0),
    geom_plain_geojson GEOMETRY(Polygon, 0),
    geom_geojson_struct GEOMETRY(Polygon, 0),
    geom_geojson_arrow_json GEOMETRY(Polygon, 0),
    geom_wkb GEOMETRY(Point, 0),
    geom_hexwkb GEOMETRY(Point, 0),
    geom_line GEOMETRY(LineString, 0),
    geom_polygon_native GEOMETRY(Polygon, 0),
    geom_multipoint GEOMETRY(MultiPoint, 0),
    geom_multiline GEOMETRY(MultiLineString, 0),
    geom_multipolygon GEOMETRY(MultiPolygon, 0)
);

-- This table intentionally contains only binary-COPY-compatible PostgreSQL
-- types. The integration suite uses it to verify that duckdb-postgres selects
-- FORMAT BINARY rather than falling back to text because of a JSONB column.
CREATE TABLE binary_events (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    geom GEOMETRY(Point, 0)
);

-- Permissive geometry table for ingest edge-case coverage: NULL, 3D (Z),
-- EMPTY and GEOMETRYCOLLECTION values. The column is a bare `geometry`
-- (no type/SRID typmod) so it accepts whatever the native
-- DuckDB GEOMETRY -> PostGIS bridge produces, letting the test assert
-- whether the bridge preserves these non-trivial geometries faithfully.
CREATE TABLE geom_edge (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    geom GEOMETRY
);
