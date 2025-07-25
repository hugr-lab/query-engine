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
   - `X-Hugr-Empty`: `true` if the table part is empty.

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

## HUGR IPC WebSocket Stream Protocol

The HUGR IPC stream protocol allows clients to execute GraphQL queries and receive results as Arrow streams over a WebSocket connection. This is designed for efficient, real-time, and large-scale data transfer.

### WebSocket Connection

- The client connects to the server using a WebSocket with the subprotocol `hugr-ipc-ws`.
- The server upgrades the connection and maintains it for streaming data and control messages.

### Message Types

All messages are JSON objects with a `type` field:

- `query_object`: Request to stream a table or view. Fields:
  - `data_object`: Name of the data object (table/view).
  - `selected_fields`: Array of field names to select.
  - `variables`: (Optional) Query variables.
- `query`: Request to execute a raw GraphQL query. Fields:
  - `query`: The GraphQL query string.
  - `variables`: (Optional) Query variables.
- `cancel`: Cancel the current active query.
- `error`: Error message from the server.
- `complete`: Indicates the end of the stream.

### Example Client Request

```json
{
  "type": "query_object",
  "data_object": "users",
  "selected_fields": ["id", "name"],
  "variables": {"limit": 100}
}
```

### Server Stream Response

- Data is sent as binary WebSocket messages containing Arrow IPC RecordBatches.
- Control messages (`error`, `complete`) are sent as JSON.
- The server may send multiple Arrow RecordBatches for large results.

### Streaming Error Handling

- If an error occurs, the server sends a message:

```json
{
  "type": "error",
  "error": "Description of the error"
}
```

### Completion

- When the stream is finished, the server sends:

```json
{
  "type": "complete"
}
```

### Keep-Alive

- The server sends WebSocket ping frames every 30 seconds to keep the connection alive.
- The client should respond with pong frames.

### Cancellation

- The client can send a `cancel` message to abort the current query and close the stream.

### Notes

- Only one active query per connection is allowed.
- If a new query is sent while another is active, the server responds with an error.
- The protocol is designed for streaming large tabular results efficiently using Arrow IPC.
