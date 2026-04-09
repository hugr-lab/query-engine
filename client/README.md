# Hugr Go Client

Go client for the [Hugr](https://github.com/hugr-lab/hugr) Data Mesh platform. Execute GraphQL queries and subscriptions with results delivered as Apache Arrow record readers over the IPC protocol.

## Installation

```bash
go get github.com/hugr-lab/query-engine/client
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/hugr-lab/query-engine/client"
)

func main() {
    c := client.NewClient("http://localhost:15000/ipc",
        client.WithApiKey("your-api-key"),
    )

    resp, err := c.Query(context.Background(),
        `{ core { data_sources { name type } } }`, nil,
    )
    if err != nil {
        log.Fatal(err)
    }
    defer resp.Close()

    fmt.Println(resp.Data)
}
```

## Client Options

### Authentication

| Option | Description |
|--------|-------------|
| `WithApiKey(key)` | API key via `x-hugr-api-key` header |
| `WithApiKeyCustomHeader(key, header)` | API key with custom header name |
| `WithSecretKeyAuth(key)` | Admin secret key via `x-hugr-secret-key` header. Enables impersonation via `AsUser` |
| `WithToken(token)` | Bearer token via `Authorization` header (for JWT/OIDC) |

### Identity

| Option | Description |
|--------|-------------|
| `WithUserRole(role)` | Set `x-hugr-role` header |
| `WithUserRoleCustomHeader(role, header)` | Custom role header name |
| `WithUserInfo(id, name)` | Set `x-hugr-user-id` and `x-hugr-name` headers |
| `WithUserInfoCustomHeader(id, name, idH, nameH)` | Custom user info header names |

### Connection

| Option | Description |
|--------|-------------|
| `WithTimeout(d)` | HTTP request timeout (default: 5 minutes) |
| `WithTransport(rt)` | Custom `http.RoundTripper` |
| `WithHttpUrl(url)` | Override HTTP query URL |
| `WithJQQueryUrl(url)` | Override JQ query URL |
| `WithSubscriptionPool(max, idle)` | WebSocket pool for subscriptions (default: 1/1) |

### Data Format

| Option | Description |
|--------|-------------|
| `WithTimezone(tz)` | Set `X-Hugr-Timezone` header (auto-detected by default) |
| `WithoutTimezone()` | Disable auto timezone detection |
| `WithArrowStructFlatten()` | Flatten Arrow struct fields in responses |

### Example

```go
c := client.NewClient("http://localhost:15000/ipc",
    client.WithSecretKeyAuth("my-secret-key"),
    client.WithTimeout(30 * time.Second),
    client.WithSubscriptionPool(10, 5),
)
```

## Queries

```go
resp, err := c.Query(ctx, `
    query($limit: Int!) {
        devices(limit: $limit) { id name status }
    }
`, map[string]any{"limit": 100})
if err != nil {
    log.Fatal(err)
}
defer resp.Close()

// Scan into struct
var devices []Device
err = resp.ScanData("devices", &devices)

// Or access raw data
fmt.Println(resp.Data)
```

### Validate Without Executing

```go
err := c.ValidateQuery(ctx, query, vars)
```

### JQ Transform

```go
result, err := c.QueryJSON(ctx, types.JQRequest{
    Query: types.Request{Query: graphqlQuery},
    JQ:    ".devices[] | {id, name}",
})
```

## Subscriptions

Subscriptions use WebSocket connections via the `hugr-ipc-ws` protocol with Apache Arrow IPC binary frames.

### Pooled Connections

```go
c := client.NewClient(url,
    client.WithApiKey("key"),
    client.WithSubscriptionPool(10, 5),
)

sub, err := c.Subscribe(ctx, `
    subscription {
        query(interval: "5s") {
            devices { id status }
        }
    }
`, nil)
if err != nil {
    log.Fatal(err)
}

for event := range sub.Events {
    fmt.Printf("Path: %s\n", event.Path)
    for event.Reader.Next() {
        batch := event.Reader.RecordBatch()
        fmt.Printf("  %d rows\n", batch.NumRows())
    }
    event.Reader.Release()
}
```

### Dedicated Connections

For long-running subscriptions or full WebSocket lifecycle control:

```go
conn, err := c.NewSubscriptionConn(ctx)
if err != nil {
    log.Fatal(err)
}
defer conn.Close()

sub, err := conn.Subscribe(ctx, query, nil)
```

### Cancel Subscription

```go
sub.Cancel() // cancels one subscription
c.CloseSubscriptions() // closes all pooled connections
```

## Impersonation (AsUser)

Admin clients authenticated via `WithSecretKeyAuth` can execute queries and subscriptions on behalf of any user with any role. The impersonated user's role permissions, field access rules, and row-level security filters are enforced.

### Setup

```go
c := client.NewClient("http://localhost:15000/ipc",
    client.WithSecretKeyAuth("admin-secret-key"),
)

// Optional: verify admin status at startup
if err := c.VerifyAdmin(ctx); err != nil {
    log.Fatal(err)
}
```

### Query as User

```go
// types.AsUser or client.AsUser — both work
ctx := types.AsUser(ctx, "user-123", "John Doe", "viewer")
resp, err := c.Query(ctx, `{ devices { id name } }`, nil)
// Response contains only data the "viewer" role can see
```

### Subscribe as User

```go
ctx := types.AsUser(ctx, "user-456", "Jane Smith", "editor")
sub, err := c.Subscribe(ctx, `
    subscription {
        query(interval: "5s") {
            devices { id status }
        }
    }
`, nil)
// Subscription events are filtered by "editor" role permissions
```

Multiple subscriptions for different users can coexist on the same pooled connection — each subscription independently enforces its user's permissions.

### Introspect Impersonated Identity

```go
ctx := types.AsUser(ctx, "user-123", "John", "viewer")
resp, _ := c.Query(ctx, `{
    function { core { auth { me {
        user_id
        role
        auth_type
        impersonated_by_user_id
        impersonated_by_user_name
    } } } }
}`, nil)
// Returns:
//   user_id: "user-123"
//   role: "viewer"
//   auth_type: "impersonation"
//   impersonated_by_user_id: "api"
//   impersonated_by_user_name: "api"
```

### Security

- Only `WithSecretKeyAuth` clients can impersonate. Other auth methods (JWT, OIDC, anonymous, regular API keys) have override headers silently ignored (HTTP) or rejected with an error (IPC subscriptions).
- Row-level security filters use the impersonated user's identity: `[$auth.user_id]` resolves to the impersonated user's ID.
- The original admin identity is tracked via `impersonated_by_*` fields for audit logging.

## Data Source Management

```go
// Register
err := c.RegisterDataSource(ctx, types.DataSource{
    Name: "my_source",
    Type: "postgres",
    URI:  "postgresql://...",
})

// Load / Unload
err = c.LoadDataSource(ctx, "my_source")
err = c.UnloadDataSource(ctx, "my_source")
err = c.UnloadDataSource(ctx, "my_source", types.WithHardUnload())

// Status
status, err := c.DataSourceStatus(ctx, "my_source")

// Describe schema
sdl, err := c.DescribeDataSource(ctx, "my_source", true)
```

## Hugr Applications

The client supports running pluggable applications that register tables and functions:

```go
c := client.NewClient("http://localhost:15000/ipc",
    client.WithApiKey("app-key"),
)

err := c.RunApplication(ctx, myApp,
    client.WithSecretKey("admin-secret"),
    client.WithStartupTimeout(30 * time.Second),
)
```

See the `client/app` package for the application framework API.

## See Also

- [Hugr Documentation](https://hugr-lab.github.io/) — full platform documentation
- [Go Client Docs](https://hugr-lab.github.io/docs/querying/go-client) — online documentation
- [WebSocket Subscriptions](https://hugr-lab.github.io/docs/querying/websocket-subscriptions) — wire protocol details
- [Access Control](https://hugr-lab.github.io/docs/engine-configuration/access-control) — roles, permissions, RLS
- [Authentication](https://hugr-lab.github.io/docs/deployment/auth) — auth providers configuration
