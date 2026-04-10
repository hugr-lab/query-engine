# hugr-app SDK (`client/app`)

Build pluggable applications for the [Hugr](https://github.com/hugr-lab) data mesh platform. Apps connect via DuckDB Airport extension and publish functions, tables, and views into hugr's shared GraphQL schema.

## Quick Start

```go
package main

import (
    "context"
    "net"
    "os/signal"
    "syscall"

    "github.com/hugr-lab/airport-go/catalog"
    "github.com/hugr-lab/query-engine/client"
    "github.com/hugr-lab/query-engine/client/app"
)

type MyApp struct{}

func (a *MyApp) Info() app.AppInfo {
    return app.AppInfo{
        Name:    "my_app",
        Version: "1.0.0",
        URI:     "grpc://localhost:50051",
    }
}

func (a *MyApp) Listner() (net.Listener, error) {
    return net.Listen("tcp", "localhost:50051")
}

func (a *MyApp) Init(ctx context.Context) error    { return nil }
func (a *MyApp) Shutdown(ctx context.Context) error { return nil }

func (a *MyApp) Catalog(ctx context.Context) (catalog.Catalog, error) {
    mux := app.New()

    // Register a scalar function
    mux.HandleFunc("default", "add", func(w *app.Result, r *app.Request) error {
        return w.Set(r.Int64("a") + r.Int64("b"))
    }, app.Arg("a", app.Int64), app.Arg("b", app.Int64), app.Return(app.Int64))

    // Register a table function
    mux.HandleTableFunc("default", "search", func(w *app.Result, r *app.Request) error {
        w.Append(int64(1), "Alice")
        w.Append(int64(2), "Bob")
        return nil
    }, app.Arg("query", app.String), app.ColPK("id", app.Int64), app.Col("name", app.String))

    return mux, nil
}

func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    c := client.NewClient("http://localhost:15100/ipc")
    c.RunApplication(ctx, &MyApp{}, client.WithSecretKey("my-secret"))
}
```

After connecting, your app is queryable:

```graphql
{ function { my_app { add(a: 1, b: 2) } } }
# → {"data": {"function": {"my_app": {"add": 3}}}}

{ my_app { search(query: "test") { id name } } }
# → {"data": {"my_app": {"search": [{"id": 1, "name": "Alice"}, ...]}}}
```

## CatalogMux API

| Method | Description |
| ------ | ----------- |
| `HandleFunc(schema, name, handler, opts...)` | Register scalar function |
| `HandleTableFunc(schema, name, handler, opts...)` | Register table function |
| `Table(schema, table, opts...)` | Register catalog.Table |
| `TableRef(schema, ref, opts...)` | Register catalog.TableRef |
| `ScalarFunc(schema, fn)` | Register catalog.ScalarFunction directly |
| `TableFunc(schema, fn)` | Register catalog.TableFunction directly |
| `TableFuncInOut(schema, fn)` | Register catalog.TableFunctionInOut directly |
| `WithSDL(sdl)` | Override all auto-generated SDL with custom SDL |

**Per-item SDL wrapping**: `app.WithSDL(table, sdl)`, `app.WithScalarFuncSDL(fn, sdl)`, `app.WithTableFuncSDL(fn, sdl)`, `app.WithTableRefSDL(ref, sdl)`

## Struct Return and Input Types

For functions that return or accept structured data, use the `Struct` / `InputStruct` builders. The wire format is JSON; the planner inlines complex argument values automatically and casts JSON returns to typed GraphQL structures via `@function(json_cast: true)`.

```go
// Output struct with @field_source rename and a list field
weather := app.Struct("weather_result").
    Desc("Current weather snapshot").
    Field("temp", app.Float64).
    Field("humidity", app.Int64).
    FieldFromSource("city", app.String, "city_name").
    FieldList("conditions", app.String) // [String!]!

// Input struct
location := app.InputStruct("location_input").
    Field("lat", app.Float64).
    Field("lon", app.Float64)

mux.HandleFunc("default", "get_weather",
    func(w *app.Result, r *app.Request) error {
        var loc struct{ Lat, Lon float64 }
        if err := r.JSON("location", &loc); err != nil {
            return err
        }
        return w.SetJSON(map[string]any{
            "temp":       22.5,
            "humidity":   60,
            "city_name":  "Berlin",
            "conditions": []string{"sunny", "windy"},
        })
    },
    app.Arg("location", location.AsType()),
    app.Return(weather.AsType()),
)
```

Generated SDL:

```graphql
input location_input {
    lat: Float!
    lon: Float!
}

"""Current weather snapshot"""
type weather_result {
    temp: Float!
    humidity: BigInt!
    city: String! @field_source(field: "city_name")
    conditions: [String!]!
}

extend type Function {
    get_weather(location: location_input!): weather_result
        @function(name: "...", json_cast: true)
}
```

**Helpers**:

- `Result.SetJSON(v)` — marshals `v` to JSON and stores as the return value. Runtime check ensures the function's return type accepts JSON.
- `Result.SetJSONValue(v)` — accepts `string`, `[]byte`, or any value (marshaled). Most flexible variant.
- `Request.JSON(name, &out)` — unmarshals the named arg's JSON string value into `out`.

**Raw JSON without struct definition**: `app.Return(app.JSON)` for opaque JSON returns; `app.Arg(name, app.JSON)` for opaque JSON inputs.

**Lists of scalars**: `app.ReturnList(app.String)` for `[String!]`-style returns. Uses native Arrow LIST as the wire format (not JSON), so the handler returns a Go slice via `Set`: `[]string`, `[]int64`, `[]float64`, `[]bool`, and other scalar slice types are supported (`[]any` works as a fallback). The outer list is nullable, matching the scalar-return convention — elements are non-null.

**Lists of structs are NOT supported as scalar function returns** — use `HandleTableFunc` instead.

**Type deduplication**: the same `StructType` (by name) used in multiple functions is emitted once in SDL. Conflicting registrations (same name, different fields) fail at registration time.

**Thread safety**: `HandleFunc` / `HandleTableFunc` (and the other registration methods) are **not** safe to call concurrently on the same `CatalogMux`. Build the catalog from a single goroutine (typically inside `Application.Catalog`), then publish it — after the catalog is handed to the runtime, all reads are safe for any number of goroutines.

## Options

**Function options**: `Arg(name, type)`, `ArgDesc(name, type, desc)`, `ArgFromContext(name, type, placeholder)`, `Return(type)`, `ReturnList(type)`, `Desc(description)`, `Mutation()`

`ArgFromContext()` declares a server-injected function argument bound to a context placeholder. The argument is hidden from the GraphQL schema — clients cannot see or set it. At handler execution time, read it via `r.String(name)` / `r.Int64(name)` like any other argument.

```go
mux.HandleFunc("default", "my_orders",
    func(w *app.Result, r *app.Request) error {
        userID := r.String("user_id") // injected from auth context
        limit := r.Int64("limit")
        // ... fetch orders for userID ...
        return w.Set("ok")
    },
    app.Arg("limit", app.Int64),
    app.ArgFromContext("user_id", app.String, app.AuthUserID),
    app.Return(app.String),
)

// GraphQL: { function { my_app { my_orders(limit: 10) } } }
// Note: user_id is NOT in the schema — server-injected.
```

**Available placeholders** (typed `app.ContextPlaceholder` constants):

| Constant | Resolves to |
|----------|-------------|
| `app.AuthUserID` | Authenticated user ID (string) |
| `app.AuthUserIDInt` | Authenticated user ID parsed as integer |
| `app.AuthUserName` | Authenticated user display name |
| `app.AuthRole` | Authenticated role |
| `app.AuthType` | Auth method (`apiKey`, `jwt`, `oidc`, etc.) |
| `app.AuthProvider` | Auth provider name |
| `app.AuthImpersonatedByRole` | Original role when impersonating |
| `app.AuthImpersonatedByUserID` | Original user ID when impersonating |
| `app.AuthImpersonatedByUserName` | Original user name when impersonating |
| `app.Catalog` | Current catalog name |

`Mutation()` marks a scalar function as a GraphQL mutation. Without it, the function is exposed as a query (extends `Function`); with it, the function extends `MutationFunction` and must be called via `mutation { ... }`. Use it for operations with side effects.

```go
mux.HandleFunc("default", "send_email",
    func(w *app.Result, r *app.Request) error {
        return w.Set(fmt.Sprintf("sent to %s", r.String("to")))
    },
    app.Arg("to", app.String),
    app.Arg("body", app.String),
    app.Return(app.String),
    app.Mutation(),
)

// GraphQL: mutation { function { my_app { send_email(to: "alice", body: "hi") } } }
```

**Table column options**: `Col(name, type)`, `ColPK(name, type)`, `ColDesc(name, type, desc)`, `ColNullable(name, type)`

**Schema options**: `WithPK(fields...)`, `WithDescription(desc)`, `WithReferences(...)`, `WithFieldReferences(...)`, `WithM2M()`, `WithFilterRequired(fields...)`

## Types

`Boolean`, `Int8`, `Int16`, `Int32`, `Int64`, `Uint8`...`Uint64`, `Float32`, `Float64`, `String`, `Binary`, `Timestamp`, `Date`, `Geometry`

## Schemas

- `"default"` schema → objects at app's root module level
- Named schemas → nested modules (e.g., schema `"reports"` → `{ my_app { reports { ... } } }`)
- `"_mount"` — reserved for system metadata

## Application Interfaces

| Interface | Required | Description |
| --------- | -------- | ----------- |
| `Application` | Yes | Info, Catalog, Init, Shutdown, Listener |
| `DataSourceUser` | No | Declare database dependencies |
| `ApplicationDBInitializer` | No | Provide SQL for schema initialization |
| `ApplicationDBMigrator` | No | Provide SQL for versioned migrations |
| `MultiCatalogProvider` | No | Dynamic catalog registration at runtime |
| `TLSConfigProvider` | No | TLS configuration for gRPC |

## Documentation

Full documentation: [hugr-lab.github.io/docs/hugr-apps](https://hugr-lab.github.io/docs/hugr-apps/)

Examples: [`examples/hugr-app/`](../../examples/hugr-app/)
