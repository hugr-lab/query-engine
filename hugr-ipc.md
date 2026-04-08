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

All text messages are JSON objects with a `type` field.

#### Query Messages

- `query_object`: Request to stream a table or view. Fields:
  - `data_object`: Name of the data object (table/view).
  - `selected_fields`: Array of field names to select.
  - `variables`: (Optional) Query variables.
- `query`: Request to execute a raw GraphQL query. Fields:
  - `query`: The GraphQL query string.
  - `variables`: (Optional) Query variables.
- `cancel`: Cancel the current active query.
- `error`: Error message from the server.
- `complete`: Indicates the end of the query stream.

#### Subscription Messages

- `subscribe`: Start a GraphQL subscription. Fields:
  - `subscription_id`: Unique ID for this subscription (client-generated).
  - `query`: The GraphQL subscription query string.
  - `variables`: (Optional) Query variables.
- `unsubscribe`: Cancel an active subscription. Fields:
  - `subscription_id`: ID of the subscription to cancel.
- `part_complete`: Server signals that all Arrow batches for a data path are sent. Fields:
  - `subscription_id`: ID of the subscription.
  - `path`: The data object path that completed.
- `subscription_complete`: Server signals the subscription has ended. Fields:
  - `subscription_id`: ID of the completed subscription.
- `subscription_error`: Server signals an error on a subscription. Fields:
  - `subscription_id`: ID of the failed subscription.
  - `error`: Error description.

### Query Streaming

- Only one active query per connection is allowed.
- Data is sent as binary WebSocket messages containing Arrow IPC RecordBatches.
- Control messages (`error`, `complete`) are sent as JSON text frames.
- If a new query is sent while another is active, the server responds with an error.

### Subscription Streaming

Multiple subscriptions can be active simultaneously on the same connection. Subscriptions coexist with regular queries.

#### Arrow Metadata Routing

Each Arrow IPC binary frame contains routing metadata in the Arrow schema:

- `subscription_id`: Identifies which subscription this batch belongs to.
- `path`: The data object path (e.g., `core.data_sources`).

This eliminates the need for text frame markers before binary frames and prevents interleave races when multiple subscriptions stream concurrently.

#### Subscription Lifecycle

```
Client → Server: subscribe
  {"type": "subscribe", "subscription_id": "s1",
   "query": "subscription { query { core { data_sources { name } } } }"}

Server → Client: Arrow binary frames (with metadata)
  Binary: Arrow IPC RecordBatch
    Schema metadata: {"subscription_id": "s1", "path": "core.data_sources"}

Server → Client: part_complete (all batches for a path sent)
  {"type": "part_complete", "subscription_id": "s1", "path": "core.data_sources"}

Server → Client: subscription_complete (subscription ended)
  {"type": "subscription_complete", "subscription_id": "s1"}
```

#### Multiple Subscriptions

```
Client → subscribe s1 (query streaming)
Client → subscribe s2 (LLM streaming)

Server → binary frame (s1 metadata in Arrow schema)
Server → binary frame (s2 metadata in Arrow schema)
Server → binary frame (s1 metadata)
Server → part_complete s1
Server → subscription_complete s1
Server → binary frame (s2 metadata)
Server → subscription_complete s2
```

Binary frames from different subscriptions may interleave freely. The client routes each frame by reading `subscription_id` from the Arrow schema metadata.

#### Periodic Subscriptions

```
Client → subscribe with interval
  {"type": "subscribe", "subscription_id": "s1",
   "query": "subscription { query(interval: \"10s\", count: 3) { ... } }"}

Server → binary frames (tick 1)
Server → part_complete s1
Server → binary frames (tick 2, after 10s)
Server → part_complete s1
Server → binary frames (tick 3, after 10s)
Server → part_complete s1
Server → subscription_complete s1
```

#### Cancellation

```
Client → unsubscribe
  {"type": "unsubscribe", "subscription_id": "s1"}

Server → subscription_complete
  {"type": "subscription_complete", "subscription_id": "s1"}
```

### Keep-Alive

- The server sends WebSocket ping frames every 30 seconds to keep the connection alive.
- The client should respond with pong frames.

### Notes

- Only one active query per connection is allowed, but multiple subscriptions can run simultaneously.
- Subscriptions and queries coexist on the same connection.
- Arrow schema metadata is used for subscription routing — no text frame markers needed for binary data.
- The protocol is designed for streaming large tabular results and real-time subscription events efficiently using Arrow IPC.

## GraphQL Subscriptions via graphql-ws

In addition to IPC, subscriptions are available via the standard [graphql-ws protocol](https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md) at the `/subscribe` endpoint.

- **Format**: JSON (Arrow records converted to individual row objects).
- **Multiplexing**: Multiple subscriptions per connection via unique `id`.
- **Concurrency**: Up to 10 concurrent subscriptions per connection.

Each Arrow row becomes a separate `next` message:

```json
{"type": "next", "id": "1", "payload": {
  "data": {"core": {"data_sources": {"name": "pg", "type": "postgresql"}}}
}}
```

This endpoint is compatible with standard GraphQL clients (Apollo, Relay, gql-python, graphql-ws npm).
