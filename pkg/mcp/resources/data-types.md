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

- **Date/Timestamp**: `_<field>_part` (type `BigInt`) with args:
  - `extract`: epoch, minute, hour, day, doy, dow, iso_dow, week, month, year, iso_year, quarter
  - `extract_divide`: divider for the extracted part
- **Geometry**: `_<field>_measurement` (type `Float`) with args:
  - `type`: Area, AreaSpheroid, Length, LengthSpheroid, Perimeter, PerimeterSpheroid
  - `transform`: Boolean (apply transforms before measurement)
  - `from`/`to`: SRID for coordinate transform
- **Vector**: `_<field>_distance` (type `Float`) with args:
  - `vector`: Vector to compare against
  - `distance`: L2, Cosine, Inner

## Generated Arguments Per Type

Certain field types receive arguments for value transformation:

- **Date/Timestamp**: `bucket` (minute, hour, day, week, month, quarter, year)
- **Timestamp**: `bucket_interval` (accepts Interval, e.g. `"15 minutes"`)
- **JSON**: `struct` (JSON object defining extraction shape, e.g. `{"field1": "string", "field2": {"sub": "int"}}`)
- **Geometry**: `transforms` (list of GeometryTransform), `from`/`to` (SRID), `buffer` (Float), `simplify_factor` (Float)

## Arguments for Parameterized Objects

For parameterized objects (views with args), pass arguments as an input object `args` (if defined).
