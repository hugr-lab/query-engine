# Hugr Query Engine: Request-level Tracing & Structured Logging

## Problem

Today hugr does not make it clear what happened while handling a specific request. Logging is a mix of `log.Printf` (when `Debug=true`) and scattered `slog.Warn/Error` calls with no request correlation. On HTTP data source errors (non-200 responses), logs omit the response body and which GraphQL request caused the call. In a cluster, logs from different nodes cannot be correlated.

## Solution

Lightweight built-in tracing with no external dependencies (not OpenTelemetry), based on `context.Context` (already threaded through the call chain) and the standard library `log/slog`.

### Core: two context levels

`context.Context` holds two independent values:

1. **Logger with trace_id** — always created in middleware. Wraps `slog.Default()` with an attached `trace_id`. Cheap; correlates logs per request even in production.

2. **TraceInfo (with spans)** — created when tracing is active: either global level <= DEBUG (`POST /admin/log-level`), or the `@trace` directive on the request. Holds a span tree.

Each span is tied to the goroutine’s context (not a single shared pointer), which works correctly with `AllowParallel=true` — parallel fields build their own subtrees independently, while a mutex only protects `append` to the parent span’s `Children` array.

The context is available at every layer, including DuckDB UDF callbacks (duckdb-go keeps `ctx` from `QueryContext` and passes it to `RowContextExecutor`).

When tracing is off (production, no global debug), `TraceInfo` is not created. Instrumentation overhead at each site is one `ctx.Value() == nil` check (~5 ns).

### Data structures

```go
package trace

// TraceInfo is the root structure, one per request. Stored in context.Context.
type TraceInfo struct {
    TraceID   string       // unique request UUID
    Level     slog.Level   // log level for this request
    Root      *Span        // root span (virtual; holds children)
    mu        sync.Mutex   // protects Children append under parallel fields
    StartTime time.Time
}

// Span is one unit of work. Organized as a tree via Children.
type Span struct {
    Name     string         `json:"name"`
    Duration string         `json:"duration,omitempty"`
    Children []*Span        `json:"children,omitempty"`
    attrs    map[string]any // server log only (not serialized to client)
    start    time.Time      // not serialized
    parent   *Span          // back-reference to parent
}
```

The logger with `trace_id` is stored separately in the context (via `ContextWithLogger`), not inside `TraceInfo`, so every request can have a `trace_id` logger even when `TraceInfo` is not created (production).

The current span is bound to the goroutine’s `context.Context`, not a shared pointer in `TraceInfo`. That keeps parallel field execution (`AllowParallel=true`) correct: each goroutine builds its subtree via its own `ctx`, and the mutex only guards appending a child span to the parent.

### `pkg/trace` API

```go
// ContextWithLogger puts a trace_id logger in the context (middleware, always).
func ContextWithLogger(ctx context.Context, logger *slog.Logger, traceID string) context.Context

// ContextWithTrace puts TraceInfo in the context (when tracing is active).
func ContextWithTrace(ctx context.Context, info *TraceInfo) context.Context

// FromContext returns TraceInfo or nil (tracing off).
func FromContext(ctx context.Context) *TraceInfo

// TraceIDFromContext returns the trace ID from context, or "" (for propagation in a cluster).
func TraceIDFromContext(ctx context.Context) string

// LoggerFromContext returns the trace_id logger (always available) or slog.Default().
func LoggerFromContext(ctx context.Context) *slog.Logger

// StartSpan creates a child span under the current span (from ctx) and returns a new ctx.
// If tracing is off — returns ctx unchanged.
func StartSpan(ctx context.Context, name string, attrs ...any) context.Context

// EndSpan ends the current span (from ctx), records duration. No-op if tracing is off.
func EndSpan(ctx context.Context)
```

Typical hugr usage — check plus two lines:

```go
if ti := trace.FromContext(ctx); ti != nil {
    ctx = trace.StartSpan(ctx, "planner.plan", "field", field.Name)
    defer trace.EndSpan(ctx)
}
```

Checking `FromContext` before `StartSpan` avoids variadic allocations when tracing is off (zero cost in production). `LoggerFromContext` is always available — with `trace_id`, regardless of whether tracing is active.

### Three control modes

| Mode | Activation | Logger with trace_id | Spans | Spans in extensions |
|---|---|---|---|---|
| Production (quiet) | Default | Yes | No | No |
| Global debug | `POST /admin/log-level` (no restart) | Yes | **Yes** (server log) | No |
| Per-request trace | `@trace` directive on the GraphQL request | Yes | **Yes** (server log) | **Yes** (to client) |

`POST /admin/log-level` changes the global level via `slog.LevelVar` (atomic) — instantly, no restart. At DEBUG, middleware creates `TraceInfo` with spans for every request — the span tree is written to the server log but **not** sent to the client. The `@trace(level: DEBUG)` directive additionally emits spans in the response `extensions`.

### @trace directive

```graphql
directive @trace(level: LogLevel = DEBUG) on QUERY | MUTATION
enum LogLevel @system { ERROR WARN INFO DEBUG }
```

Example:

```graphql
query GetData @trace(level: DEBUG) {
  catalog1 { users { id name } }
}
```

After parsing the request (`ParseQuery`), the engine checks for `@trace` and, if present, creates `TraceInfo` (production without global debug) or adjusts the level (if `TraceInfo` already exists from global debug).

### Integration points: flow from request to HTTP/Airport

```
HTTP Request
│
▼
┌──────────────────────────────────────────────────────────────────┐
│ (1) traceMiddleware                                              │
│     • Generates TraceID (UUID) or reads X-Trace-Id from header   │
│     • Creates logger with trace_id (always)                      │
│     • If global level <= DEBUG — creates TraceInfo with spans    │
│     • Sets X-Trace-Id on the response header                     │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (2) ProcessQuery                                                 │
│     • span "query.parse" (if TraceInfo exists)                   │
│     • Parses @trace directive → creates TraceInfo (if missing)   │
│       or switches level (if already present from global debug)   │
│     • logger.Debug("trace.spans") — writes tree to server log    │
│     • ext["trace"] = ti.Result() — only with @trace              │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (3) processDataQuery (inside dataFunc)                           │
│     • span "planner.plan" — build query plan                     │
│     • logger.Debug("planner.sql", "sql", plan.Log())             │
│     • logger.Debug("query.user", "user", ..., "role", ...)       │
│     • span "db.execute" — run SQL in DuckDB                      │
│     • logger.Error on planning and execution errors              │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (4) db.Pool / SQL execution                                      │
│     • ctx stored in duckdb-go contextStore by connId             │
│     • On UDF call — ctx is retrieved and passed to callback      │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (5) Data Sources                                                 │
│                                                                  │
│ HTTP (pkg/data-sources/service.go → HttpRequest):                │
│   • span "http.request"                                          │
│   • logger.Warn("http.request.error") — on request error         │
│   • On non-200: read body preview (up to 512 bytes)              │
│   • logger.Warn("http.response.error",                           │
│       "status", code, "body_preview", preview)                   │
│   • logger.Warn("http.decode.error") — on JSON decode error      │
│                                                                  │
│ OAuth (sources/http/client.go):                                  │
│   • logger.Warn("oauth.token.request.error") — request error   │
│   • logger.Warn("oauth.token.unauthorized") — 401 response     │
│   • logger.Warn("oauth.token.error") — non-200 with body preview │
│                                                                  │
│ Airport (DuckDB ATTACH → gRPC):                                  │
│   • Errors surface as DuckDB errors, logged with trace_id        │
│   • gRPC metadata is not controlled from Go                      │
└──────────────────────┬───────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────┐
│ (6) Response                                                     │
│   • X-Trace-Id header on HTTP response (always)                  │
│   • Spans to server log (global debug and @trace)               │
│   • Spans to client extensions — only with @trace                │
│   • extensions: names and timings only (security)               │
└──────────────────────────────────────────────────────────────────┘
```

At each point, `trace.LoggerFromContext(ctx)` is used — a logger with `trace_id`. If tracing is off, `slog.Default()` is returned.

### Implemented code

**1. Middleware (middlewares.go) — logger with trace_id + TraceInfo when debug:**

```go
func traceMiddleware(level *slog.LevelVar) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            traceID := r.Header.Get("X-Trace-Id")
            if traceID == "" {
                traceID = uuid.NewString()
            }
            w.Header().Set("X-Trace-Id", traceID)

            logger := slog.Default().With("trace_id", traceID)
            ctx := trace.ContextWithLogger(r.Context(), logger, traceID)

            if level.Level() <= slog.LevelDebug {
                info := trace.NewTraceInfo(traceID, level.Level())
                ctx = trace.ContextWithTrace(ctx, info)
            }

            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}
```

**2. ProcessQuery (engine.go) — @trace handling and assembling the result:**

```go
func (s *Service) ProcessQuery(ctx context.Context, req types.Request) types.Response {
    start := time.Now()
    logger := trace.LoggerFromContext(ctx)

    if ti := trace.FromContext(ctx); ti != nil {
        ctx = trace.StartSpan(ctx, "query.parse")
    }
    op, err := s.schema.ParseQuery(ctx, req.Query, req.Variables, req.OperationName)
    trace.EndSpan(ctx)
    if err != nil {
        return types.ErrResponse(err)
    }
    parseDuration := time.Since(start)

    hasTraceDirective := op.Definition.Directives.ForName(base.TraceDirectiveName) != nil
    if hasTraceDirective {
        d := op.Definition.Directives.ForName(base.TraceDirectiveName)
        ti := trace.FromContext(ctx)
        if ti == nil {
            ti = trace.NewTraceInfo(trace.TraceIDFromContext(ctx), parseLogLevel(d))
            ctx = trace.ContextWithTrace(ctx, ti)
        } else {
            ti.SetLevel(parseLogLevel(d))
        }
    }

    // ... ValidateOnly, ProcessOperation, @stats ...

    if ti := trace.FromContext(ctx); ti != nil {
        logger.Debug("trace.spans", "spans", ti.Result())
        if hasTraceDirective {
            ext["trace"] = ti.Result()
        }
    }

    if len(ext) > 0 {
        res.Extensions = ext
    }
    return res
}
```

**3. processDataQuery (query.go) — spans and structured logging:**

```go
func (s *Service) processDataQuery(ctx context.Context, provider catalog.Provider,
    query base.QueryRequest, vars map[string]any) (data any, ext map[string]any, err error) {
    // ...
    logger := trace.LoggerFromContext(ctx)

    dataFunc := func() (any, error) {
        if ti := trace.FromContext(ctx); ti != nil {
            ctx = trace.StartSpan(ctx, "planner.plan", "field", query.Field.Name)
        }
        plan, err := s.planner.Plan(ctx, provider, query.Field, vars)
        trace.EndSpan(ctx)
        if err != nil {
            logger.Error("planner.plan.error", "field", query.Field.Name, "error", err)
            return nil, err
        }

        // compile (errors logged; no separate span — compile is synchronous and fast)
        err = plan.Compile()
        if err != nil {
            logger.Error("planner.compile.error", "field", query.Field.Name, "error", err)
            return nil, err
        }

        logger.Debug("planner.sql", "field", query.Field.Name,
            "alias", query.Field.Alias, "sql", plan.Log())

        if ai := auth.AuthInfoFromContext(ctx); ai != nil {
            logger.Debug("query.user", "user", ai.UserName, "role", ai.Role,
                "field", query.Field.Name)
        }

        if ti := trace.FromContext(ctx); ti != nil {
            ctx = trace.StartSpan(ctx, "db.execute", "field", query.Field.Name)
        }
        result, err := plan.Execute(ctx, s.db)
        trace.EndSpan(ctx)
        if err != nil {
            logger.Error("db.execute.error", "field", query.Field.Name, "error", err)
        }
        return result, err
    }

    // ... cache logic, @stats ...
}
```

**4. HttpRequest (pkg/data-sources/service.go) — HTTP error logging:**

```go
func (s *Service) HttpRequest(ctx context.Context, source, path, method,
    headers, params, body, jqq string) (any, error) {

    logger := trace.LoggerFromContext(ctx)

    if ti := trace.FromContext(ctx); ti != nil {
        ctx = trace.StartSpan(ctx, "http.request", "source", source, "path", path, "method", method)
        defer trace.EndSpan(ctx)
    }

    // ... resolve data source ...

    res, err := httpDs.Request(ctx, path, method, headers, params, body)
    if err != nil {
        logger.Warn("http.request.error", "source", source, "path", path, "error", err)
        return nil, err
    }
    defer res.Body.Close()

    if res.StatusCode != 200 {
        preview, _ := io.ReadAll(io.LimitReader(res.Body, 512))
        logger.Warn("http.response.error",
            "source", source, "path", path, "method", method,
            "status", res.StatusCode, "body_preview", string(preview))
        return nil, fmt.Errorf("request failed with status code %d: %s", res.StatusCode, res.Status)
    }

    var data any
    if err := json.NewDecoder(res.Body).Decode(&data); err != nil {
        logger.Warn("http.decode.error", "source", source, "path", path, "error", err)
        return nil, err
    }
    return data, nil
}
```

**5. Server log — sample output:**

```
level=WARN msg="http.response.error" trace_id=550e8400-... source=ext_api path=/users method=GET status=502 body_preview="Bad Gateway"
level=DEBUG msg="planner.sql" trace_id=550e8400-... field=users sql="SELECT u.id, u.name FROM catalog1.users u"
level=ERROR msg="db.execute.error" trace_id=550e8400-... field=orders error="connection refused"
```

All lines carry `trace_id` — filter one request: `grep 550e8400`.

### Log vs extensions split

To avoid leaking data to the client (SQL, response bodies, internal structure):

- **Server log (stderr):** full detail — SQL, body preview, headers, parameters
- **GraphQL extensions (to client):** span names and timings only, no sensitive fields

```json
{
  "extensions": {
    "trace": {
      "trace_id": "550e8400-...",
      "total_time": "42ms",
      "spans": [
        { "name": "query.parse", "duration": "1ms" },
        { "name": "planner.plan", "duration": "3ms" },
        { "name": "db.execute", "duration": "37ms" },
        { "name": "http.request", "duration": "28ms" }
      ]
    }
  }
}
```

### Operating in a cluster

User requests run entirely on one node — tracing uses in-process `context.Context`. For cluster operations (`Broadcast`, `ForwardToManagement`), `X-Trace-Id` is added to outbound HTTP headers via `client.WithTraceID(traceID)`; middleware on the receiving node picks it up instead of generating a new one. One operation — one `trace_id` across nodes.

### Airport (hugr-app)

DuckDB calls Airport catalogs over gRPC via its C++ extension — Go does not control those calls. Passing `trace_id` in gRPC metadata would require changes to the DuckDB extension. From the engine side, the SQL against the Airport catalog and any DuckDB error are logged.

## Implemented changes

| File / package | What was implemented |
|---|---|
| `pkg/trace/` (new) | `TraceInfo`, `Span`, context helpers (`ContextWithLogger`, `ContextWithTrace`, `StartSpan`, `EndSpan`, `FromContext`, `LoggerFromContext`, `TraceIDFromContext`), tests (21 tests) |
| `query_directives.graphql` | `@trace` directive + `LogLevel` enum |
| `constants.go`, `directives.go` | Register `trace` as a query-side directive |
| `middlewares.go` | `traceMiddleware`: TraceID, `X-Trace-Id`, base `TraceInfo` |
| `engine.go` | `Service.logLevel *slog.LevelVar`, `GET/POST /admin/log-level`; in `ProcessQuery` — `@trace` handling, merge trace into extensions, `parseLogLevel` |
| `query.go` | Replace `log.Printf` (debug) with `trace.StartSpan` + `trace.LoggerFromContext`: spans `planner.plan`, `db.execute`; structured logging for SQL, errors, user info |
| `stream.go` | Replace `log.Printf` with `trace.LoggerFromContext` for stream requests |
| `subscription.go` | Replace `log.Printf` with `trace.LoggerFromContext` for subscription requests |
| `pkg/data-sources/service.go` | `HttpRequest`: span `http.request`, structured logging for non-200 with body preview (up to 512 bytes), request errors, JSON decode errors |
| `pkg/data-sources/sources/http/client.go` | OAuth: structured logging `oauth.token.request.error`, `oauth.token.unauthorized`, `oauth.token.error` with body preview |
| `client/client.go` | `WithTraceID` option + `traceIDTransport` to propagate `X-Trace-Id` in HTTP headers |
| `pkg/cluster/coordinator.go` | `Broadcast` propagates `trace_id` via `client.WithTraceID` |
| `pkg/cluster/worker.go` | `ForwardToManagement` propagates `trace_id` via `client.WithTraceID` |

### Not implemented in this iteration

- Replacing `log.Printf` in `graphql-ws.go` and `ipc-stream.go` — these are IPC/WebSocket lifecycle logs (connect, ping/pong, close), not tied to a specific GraphQL request; they need a separate approach to pass trace context over the WS session
- A dedicated span for `planner.compile` — compile is synchronous and fast; SQL logging via `logger.Debug("planner.sql")` already covers it

## Out of scope for v1

- Restricting `@trace` by role/permissions
- Sampling under global debug at high load
- OpenTelemetry integration (can be added later on top of this design)
- Passing trace context into Airport/gRPC (DuckDB limitation)
- Trace-aware logging for IPC/WebSocket lifecycle

## Separate bug (found during audit)

In `pkg/planner/node_select_vector.go` (line 153), `queries.CreateEmbedding` uses `context.Background()` instead of the request context — request cancellation does not reach the embedding sub-query. Fix: pass `ctx` from the closure.
