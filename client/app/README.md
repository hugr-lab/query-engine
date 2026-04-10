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

## Options

**Function options**: `Arg(name, type)`, `ArgDesc(name, type, desc)`, `Return(type)`, `Desc(description)`, `Mutation()`

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
