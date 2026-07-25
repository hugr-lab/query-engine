# Arrow IPC Ingest

This note records the type contract for Arrow IPC ingest. It is intentionally
about semantics, not about every HTTP or client detail.

## Contract

Arrow IPC ingest receives an Arrow stream and a Hugr data object name. The
planner matches Arrow columns to Hugr fields by field name. The Hugr schema is
the source of truth for the logical type of every inserted value.

The ingest pipeline has three distinct steps:

1. Read the Arrow column from the temporary DuckDB Arrow view.
2. Convert the Arrow representation to the canonical Hugr logical value in the
   DuckDB staging SELECT.
3. Optionally adapt that canonical value to the physical representation expected
   by the target source.

The important boundary is:

```text
Arrow representation -> Hugr logical type -> source storage representation
```

Do not skip the Hugr logical type.

## Read Mappings

These tables describe the Arrow-table IPC path: data is read from a source,
planned as Hugr fields, and returned as Arrow. JSON/object responses may encode
the same Hugr value differently for JSON transport, for example `Geometry` as
GeoJSON.

### DuckDB Read

| DB field type | Hugr field type | Arrow field type |
| --- | --- | --- |
| `INTEGER`, `SMALLINT`, `TINYINT`, `INT4`, `INT2` | `Int` | Arrow integer, preserving the source width where possible |
| `BIGINT`, `INT8`, `LONG`, `UINTEGER` | `BigInt` | Arrow integer, usually `int64`/`uint64` |
| `FLOAT`, `REAL`, `DOUBLE`, `DECIMAL`, `NUMERIC` | `Float` | Arrow floating-point or decimal source representation |
| `BOOLEAN`, `BOOL`, `LOGICAL` | `Boolean` | Arrow boolean |
| `VARCHAR`, `TEXT`, `CHAR`, `STRING`, `UUID`, `BIT`, `BITSTRING` | `String` | Arrow UTF-8/string-like value |
| `BLOB`, `BINARY`, `VARBINARY` | `String` | Arrow binary storage; Hugr exposes it as `String` |
| `DATE` | `Date` | Arrow date |
| `TIME` | `Time` | Arrow time |
| `TIMESTAMP`, `DATETIME` | `DateTime` | Arrow timestamp without timezone semantics |
| `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `Timestamp` | Arrow timestamp with timezone semantics |
| `INTERVAL` | `Interval` | Arrow interval/string-compatible representation |
| `JSON` | `JSON` | Arrow JSON/string-compatible value; `RecordToJSON` embeds it as JSON |
| `GEOMETRY`, `GEOGRAPHY`, `WKB_BLOB` | `Geometry` | Arrow WKB/binary geometry with Hugr/GeoArrow geometry metadata on IPC output |

### Postgres Read

| DB field type | Hugr field type | Arrow field type |
| --- | --- | --- |
| `INTEGER`, `INT4`, `SMALLINT`, `INT2`, `TINYINT` | `Int` | Arrow integer, preserving the source width where possible |
| `BIGINT`, `INT8`, `LONG` | `BigInt` | Arrow integer, usually `int64` |
| `FLOAT`, `REAL`, `DOUBLE`, `FLOAT4`, `FLOAT8`, `DECIMAL`, `NUMERIC` | `Float` | Arrow floating-point or decimal source representation |
| `BOOLEAN`, `BOOL` | `Boolean` | Arrow boolean |
| `VARCHAR`, `TEXT`, `CHAR`, `BPCHAR`, `STRING`, `UUID`, `BIT`, `BITSTRING` | `String` | Arrow UTF-8/string-like value |
| `BYTEA`, `BLOB`, `BINARY`, `VARBINARY` | `String` | Arrow binary storage; Hugr exposes it as `String` |
| `DATE` | `Date` | Arrow date |
| `TIME` | `Time` | Arrow time |
| `TIMESTAMP`, `DATETIME` | `DateTime` | Arrow timestamp without timezone semantics |
| `TIMESTAMPTZ`, `TIMESTAMP WITH TIME ZONE` | `Timestamp` | Arrow timestamp with timezone semantics |
| `INTERVAL` | `Interval` | Arrow interval/string-compatible representation |
| `JSON`, `JSONB` when exposed by catalog as JSON | `JSON` | Arrow JSON/string-compatible value; `RecordToJSON` embeds it as JSON |
| `GEOMETRY`, `GEOGRAPHY`, `WKB_BLOB` | `Geometry` | PostGIS value is converted through DuckDB geometry/WKB for Arrow IPC geometry output |

## Write Mappings

These tables describe `/ipc/ingest`: data is read from Arrow, resolved against
the Hugr schema, and inserted into the source. For `JSON` and `Geometry`, the
Hugr field type decides which Arrow encodings are valid.

### DuckDB Write

| Arrow field type | Hugr field type | DB field type |
| --- | --- | --- |
| Arrow integer | `Int` | `INTEGER`-compatible target column |
| Arrow integer | `BigInt` | `BIGINT`-compatible target column |
| Arrow floating-point or decimal | `Float` | `FLOAT`/`DOUBLE`/numeric-compatible target column |
| Arrow boolean | `Boolean` | `BOOLEAN` target column |
| Arrow UTF-8/string-like value | `String` | `VARCHAR`/text-compatible target column |
| Arrow date | `Date` | `DATE` target column |
| Arrow time | `Time` | `TIME` target column |
| Arrow timestamp | `DateTime` or `Timestamp` | `TIMESTAMP`/`TIMESTAMPTZ` target column |
| Arrow serialized JSON (`STRING`, `LARGE_STRING`, `STRING_VIEW`, binary variants) | `JSON` | `JSON` target column |
| `arrow.json` with canonical string storage (`STRING`, `LARGE_STRING`, `STRING_VIEW`) | `JSON` | `JSON` target column |
| Arrow nested JSON-compatible value (`STRUCT`, `LIST`, `MAP`, list-view variants) without geometry metadata | `JSON` | `JSON` target column via `to_json(...)` |
| `geoarrow.geojson`, `hugr.geojson`, `geojson` | `JSON` | `JSON` target column; GeoJSON is preserved as JSON because the Hugr schema asked for JSON |
| `geoarrow.wkb` with `BINARY`, `LARGE_BINARY`, or `BINARY_VIEW` storage | `Geometry` | `GEOMETRY`/`WKB_BLOB` target column |
| `geoarrow.wkt` | `Geometry` | `GEOMETRY` target column via `ST_GeomFromText(...)` |
| `geoarrow.geojson`, `hugr.geojson`, `geojson` | `Geometry` | `GEOMETRY` target column; GeoJSON is parsed to native geometry via `ST_GeomFromGeoJSON(...)` |
| concrete GeoArrow coordinate layouts (`geoarrow.point`, `geoarrow.linestring`, `geoarrow.polygon`, etc.) | `Geometry` | `GEOMETRY` target column via DuckDB spatial constructors |

### Postgres Write

| Arrow field type | Hugr field type | DB field type |
| --- | --- | --- |
| Arrow integer | `Int` | `INTEGER`-compatible target column |
| Arrow integer | `BigInt` | `BIGINT`-compatible target column |
| Arrow floating-point or decimal | `Float` | `FLOAT`/`DOUBLE PRECISION`/numeric-compatible target column |
| Arrow boolean | `Boolean` | `BOOLEAN` target column |
| Arrow UTF-8/string-like value | `String` | `VARCHAR`/text-compatible target column |
| Arrow date | `Date` | `DATE` target column |
| Arrow time | `Time` | `TIME` target column |
| Arrow timestamp | `DateTime` or `Timestamp` | `TIMESTAMP`/`TIMESTAMPTZ` target column |
| Arrow serialized JSON (`STRING`, `LARGE_STRING`, `STRING_VIEW`, binary variants) | `JSON` | `JSON`/`JSONB` target column |
| `arrow.json` with canonical string storage (`STRING`, `LARGE_STRING`, `STRING_VIEW`) | `JSON` | `JSON`/`JSONB` target column |
| Arrow nested JSON-compatible value (`STRUCT`, `LIST`, `MAP`, list-view variants) without geometry metadata | `JSON` | `JSON`/`JSONB` target column via DuckDB staging JSON |
| `geoarrow.geojson`, `hugr.geojson`, `geojson` | `JSON` | `JSON`/`JSONB` target column; GeoJSON is preserved as JSON because the Hugr schema asked for JSON |
| `geoarrow.wkb` with `BINARY`, `LARGE_BINARY`, or `BINARY_VIEW` storage | `Geometry` | PostGIS `GEOMETRY`/`GEOGRAPHY` target column |
| `geoarrow.wkt` | `Geometry` | PostGIS `GEOMETRY`/`GEOGRAPHY` target column via DuckDB `ST_GeomFromText(...)` staging |
| `geoarrow.geojson`, `hugr.geojson`, `geojson` | `Geometry` | PostGIS `GEOMETRY`/`GEOGRAPHY` target column; GeoJSON is parsed to DuckDB `GEOMETRY` in staging before insert |
| concrete GeoArrow coordinate layouts (`geoarrow.point`, `geoarrow.linestring`, `geoarrow.polygon`, etc.) | `Geometry` | PostGIS `GEOMETRY`/`GEOGRAPHY` target column via canonical DuckDB `GEOMETRY` staging |

For the GeoJSON rows above, `Geometry` is not JSON with a different label.
GeoJSON is an interchange encoding; `ST_GeomFromGeoJSON(...)` deserializes it
into a spatial value that the target geometry column stores and indexes as
geometry. If a future source stores Hugr `Geometry` physically as JSON, that is
a source-specific storage adapter case, not this default geometry-column path.

## Read/Write Consistency Check

The mappings are consistent at the Hugr logical type boundary, not necessarily
at the original physical storage boundary:

- Scalar values round-trip by Hugr type (`Int`, `BigInt`, `Float`, `Boolean`,
  `String`, dates/times) while preserving source-native Arrow widths where the
  driver exposes them.
- `JSON` is symmetric by semantics: read exposes Hugr `JSON` as JSON-compatible
  Arrow/string data; write accepts JSON-compatible Arrow/string/nested data for
  Hugr `JSON`.
- `Geometry` is symmetric by semantics: read exposes source geometry as Hugr
  `Geometry` on the Arrow IPC path, while write accepts explicit GeoArrow/Hugr
  geometry encodings for Hugr `Geometry`.
- GeoJSON is intentionally dual-use: `geoarrow.geojson`, `hugr.geojson`, and
  `geojson` may target either Hugr `Geometry` or Hugr `JSON`, and the Hugr schema
  decides which logical type is inserted.
- Other geometry encodings must not cross into Hugr `JSON`: `geoarrow.wkb`,
  `geoarrow.wkt`, concrete GeoArrow coordinate layouts, and `arrow.json` as
  `Geometry` are rejected.
- Physical binary columns are a known non-bijective edge: catalog introspection
  maps generic `BLOB`/`BYTEA`/`BINARY`/`VARBINARY` to Hugr `String`, while
  geometry-aware binary storage must be surfaced as `WKB_BLOB` or another
  geometry-aware source type to become Hugr `Geometry`.

## Geometry And JSON

The Hugr schema decides whether GeoJSON is inserted as `Geometry` or as `JSON`.
This is the only geometry-related dual-use encoding: GeoJSON is both a geometry
interchange format and a JSON document.

`geoarrow.geojson` is treated as a Hugr compatibility alias, not as a canonical
GeoArrow 0.2 extension name. The supported GeoJSON extension names are
`hugr.geojson`, `geojson`, and `geoarrow.geojson`.

Other GeoArrow encodings are geometry-only. A column such as `geoarrow.point`,
`geoarrow.wkb`, `geoarrow.wkt`, or `geoarrow.polygon` must not be silently
accepted for a Hugr `JSON` field by converting it to GeoJSON.

Allowed examples:

```text
Arrow geoarrow.point -> Hugr Geometry -> DuckDB GEOMETRY
Arrow geoarrow.wkb   -> Hugr Geometry -> DuckDB GEOMETRY
Arrow geoarrow.geojson -> Hugr Geometry -> DuckDB GEOMETRY
Arrow geoarrow.geojson -> Hugr JSON -> DuckDB JSON
Arrow arrow.json     -> Hugr JSON     -> DuckDB JSON
```

Rejected example:

```text
Arrow geoarrow.point -> Hugr JSON -> ST_AsGeoJSON(...) -> JSON
Arrow geoarrow.wkb -> Hugr JSON -> ST_AsGeoJSON(ST_GeomFromWKB(...)) -> JSON
Arrow arrow.json -> Hugr Geometry -> ST_GeomFromGeoJSON(...)
```

If a client wants to store a GeoJSON document as ordinary Hugr `JSON`, it may
send it as plain Arrow JSON-compatible data, as `arrow.json`, or with
`geoarrow.geojson`/`hugr.geojson`/`geojson` metadata. If the same payload should
become spatial data, the target Hugr field must be `Geometry`.

`arrow.json` by itself has JSON semantics, not geometry semantics; a client that
wants GeoJSON to become Hugr `Geometry` must use `geoarrow.geojson`,
`hugr.geojson`, or `geojson` metadata.

## Source-Specific Storage Encoding

Some sources may store a Hugr logical `Geometry` in a physical representation
that is not a native geometry column. For example, a future source may need WKB,
hex WKB, or GeoJSON text/JSON as its storage format.

That conversion belongs after the value has already become a canonical Hugr
`Geometry` value:

```text
Arrow geoarrow.point
  -> Hugr Geometry
  -> canonical DuckDB GEOMETRY
  -> source adapter
  -> ST_AsWKB(...), ST_AsHEXWKB(...), ST_AsGeoJSON(...), etc.
```

So `Geometry -> JSON` can be valid, but only as a source storage encoding for a
Hugr `Geometry` field. It is not a reason to allow every Arrow geometry encoding
to target Hugr `JSON`; only GeoJSON can do that because its source
representation is already JSON.

The source adapter is the place for this target-specific choice. It should not
change the Hugr logical type contract; it should only adapt the already canonical
staging expression to the representation required by the target source.

## Capability Checks

If a source declares ingest support but lists a Hugr logical type in
`UnsupportedTypes`, planning ingest into that type must fail before SQL is built.

For example, if a source does not support Hugr `Geometry`, then ingest into a
`Geometry` field must fail even if some binary storage representation could
technically hold WKB bytes. The source must first expose a reliable read/write
mapping that preserves Hugr `Geometry` semantics.
