# HUGR IPC Protocol

The HUGR IPC (Inter-Process Communication) protocol is designed to handle GraphQL queries and return results in a streaming format. It uses a multipart/mixed content type to transmit data and metadata efficiently.

## Request Format

The client sends an HTTP POST request with the following structure:

- **Headers**:
  - `Content-Type`: `application/json`
- **Body**:
  - A JSON payload containing:
    - `query`: The GraphQL query string.
    - `variables`: (Optional) A JSON object with variables for the query.

Example:

```json
{
  "query": "query($limit: BigInt) { users(limit: $limit) { id name } }",
  "variables": { "limit": 10 }
}
```

## Response Format

The server responds with a `multipart/mixed` content type. Each part contains a specific type of data or metadata.

### Headers

- `Content-Type`: `multipart/mixed; boundary=HUGR`

### Parts

Each part in the response has its own MIME headers and content. The parts include:

1. **Data Part**:
   - `Content-Type`: `application/vnd.apache.arrow.stream` (for Arrow tables) or `application/json` (for JSON objects).
   - `X-Hugr-Part-Type`: `data`, `extensions`, or `error`.
   - `X-Hugr-Path`: The path of the data in the query.
   - `X-Hugr-Format`: `table` (for Arrow tables) or `object` (for JSON objects).
   - `X-Hugr-Chunk`: (Optional) The chunk number for streaming data.

2. **Extensions Part**:
   - `Content-Type`: `application/json`
   - `X-Hugr-Part-Type`: `extensions`
   - `X-Hugr-Path`: (Optional) The path for extensions.
   - `X-Hugr-Format`: `object`

### Example Response

```text
--HUGR
Content-Type: application/vnd.apache.arrow.stream
X-Hugr-Part-Type: data
X-Hugr-Path: data.users
X-Hugr-Format: table

<Arrow table data>
--HUGR
Content-Type: application/json
X-Hugr-Part-Type: extensions
X-Hugr-Path: extensions
X-Hugr-Format: object

{"timing": {"query": 123}}
--HUGR--
```

## Error Handling

If an error occurs, the server responds with an appropriate HTTP status code and a JSON error message.

Example:

```json
{
  "error": "Invalid query syntax"
}
```

## Geometry Metadata

For queries involving geometry fields, additional headers are included:

- `X-Hugr-Geometry-Fields`: A JSON object describing the geometry fields.
- `X-Hugr-Geometry`: `true` if geometry data is present.

Example:

```json
{
  "geometry": {
    "field": "location",
    "srid": "4326",
    "format": "GeoJSON"
  }
}
```

## GraphQL Nature and Result Encoding

GraphQL queries can inherently return results from multiple data requests. The encoding of results depends on the type of data being returned:

### Data Results

1. **Array of Objects (Tables and Views)**:
   - Results are transmitted as `ArrowTable` (arrow RecordBatch) for efficient handling of tabular data.

2. **Scalar Values or Single Objects**:
   - Results are transmitted as `JSON`.
   - This includes queries for unique records or function results.

### Geometry Fields

1. **In Tables**:
   - Geometry fields at the top level are encoded as `WKB` (Well-Known Binary).
   - Nested geometry fields are encoded as string values in `GeoJSON` format.

2. **In Single Objects**:
   - Geometry fields are encoded as `GeoJSON` at all levels of nesting.

### Data Path Prefix

- All results from the `data` field have a path prefix of `data`.
- The path corresponds to the query path in the data request.

### Extensions

- Extensions are transmitted as `JSON` objects.
- If the query includes multiple named operations, multiple `extensions` objects are returned.
- The paths for these objects include the operation names, e.g., `extensions.name`.
