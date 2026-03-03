# Data Types

## Scalar Types

- `String`, `Int`, `Float`, `Boolean` — standard GraphQL scalars
- `BigInt` — 64-bit integer
- `Timestamp`, `Date`, `Time` — temporal types
- `Interval` — time intervals
- `Geometry` — spatial type (WKT/GeoJSON)
- `JSON` — arbitrary JSON data
- `H3Cell` — H3 hexagonal grid cell
- `Vector` — float array for embeddings
- `Range` types: `IntRange`, `BigIntRange`, `TimestampRange`

## Extra Calculated Fields

Fields automatically generated for certain scalar types:

- **Timestamp/DateTime**: `_year`, `_month`, `_day`, `_hour`, `_minute`, `_second`, `_day_of_week`, `_day_of_year`
- **Geometry**: `_area`, `_length`, `_centroid`, `_envelope`, `_geojson`, `_h3_cells`

## Arguments for Parameterized Objects

For parameterized objects (views with args), pass arguments as an input object `args` (if defined).
