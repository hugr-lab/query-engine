# pkg/jq

A jq transformer built on [gojq](https://github.com/itchyny/gojq) with custom functions for time manipulation, authentication, and query engine integration.

## Usage

```go
t, err := jq.NewTransformer(ctx, `.name`, jq.WithQuerier(qe))
if err != nil {
    return err
}
result, err := t.Transform(ctx, map[string]any{"name": "alice"}, nil)
// result = "alice"
```

### Options

| Option | Description |
| --- | --- |
| `WithQuerier(qe)` | Enables `queryHugr`, `authInfo`, and time functions |
| `WithVariables(vars)` | Registers variables accessible as `$name` in queries |
| `WithCollectStat()` | Enables execution statistics collection via `Stats()` |

### Variables

Variables can be provided at compile time and optionally overridden per `Transform` call:

```go
t, _ := jq.NewTransformer(ctx, `.value + $offset`, jq.WithVariables(map[string]any{"offset": 10}))

// uses compile-time variable
result, _ := t.Transform(ctx, map[string]any{"value": 5}, nil)
// result = 15

// runtime override
result, _ = t.Transform(ctx, map[string]any{"value": 5}, map[string]any{"offset": 20})
// result = 25
```

## Custom Functions

### Time

#### `localTime`

Returns the current local time.

```jq
localTime | roundTime("day") | datePart("hour")
# 0
```

#### `utcTime`

Returns the current UTC time.

```jq
utcTime | roundTime("month") | datePart("day")
# 1
```

#### `unixTime`

Converts a time value to Unix timestamp in seconds. When the input is `null`, returns the current UTC Unix timestamp.

```jq
"2024-06-15T14:30:45Z" | unixTime
# 1718457045

"2024-06-15T14:30:45Z" | roundTime("day"; "UTC") | unixTime
# 1718409600

null | unixTime
# (current UTC unix timestamp)
```

#### `roundTime(period)` / `roundTime(period; timezone)`

Truncates or rounds a time value to the given period. The optional `timezone` parameter is an IANA timezone string (e.g. `"America/New_York"`).

**Named periods:**

| Period | Result |
| --- | --- |
| `"second"` / `"seconds"` | Round to nearest second |
| `"minute"` / `"minutes"` | Round to nearest minute |
| `"hour"` / `"hours"` | Truncate to start of hour |
| `"day"` | Truncate to start of day |
| `"month"` / `"months"` | Truncate to first of the month |
| `"year"` / `"years"` | Truncate to January 1st |
| `"monday"` .. `"saturday"` | Truncate to the given weekday |

Any Go `time.Duration` string (e.g. `"15m"`, `"2h30m"`) is also accepted and uses `time.Round`.

```jq
"2024-06-15T14:30:45Z" | roundTime("day")
# 2024-06-15T00:00:00Z

"2024-06-15T14:30:45Z" | roundTime("month"; "UTC")
# 2024-06-01T00:00:00Z

"2024-06-15T14:30:45Z" | roundTime("monday")
# 2024-06-10T00:00:00Z (previous Monday)

"2024-06-15T14:30:45Z" | roundTime("15m")
# 2024-06-15T14:30:00Z
```

#### `timeAdd(duration)` / `timeAdd(duration; timezone)`

Adds a Go duration string to a time value.

```jq
"2024-06-15T14:30:45Z" | timeAdd("2h")
# 2024-06-15T16:30:45Z

"2024-06-15T14:30:45Z" | timeAdd("-30m"; "UTC")
# 2024-06-15T14:00:45Z
```

#### `datePart(part)` / `datePart(part; timezone)`

Extracts a component from a time value as an integer.

| Part | Returns |
| --- | --- |
| `"second"` | 0-59 |
| `"minute"` | 0-59 |
| `"hour"` | 0-23 |
| `"day"` | 1-31 |
| `"month"` | 1-12 |
| `"year"` | e.g. 2024 |
| `"weekday"` | 1 (Monday) - 7 (Sunday) |
| `"yearday"` | 1-366 |

```jq
"2024-06-15T14:30:45Z" | datePart("hour")
# 14

"2024-06-15T14:30:45Z" | datePart("weekday")
# 6 (Saturday)
```

#### Time input formats

All time functions accept:

- **RFC 3339 string**: `"2024-06-15T14:30:45Z"`
- **Unix timestamp** (float64): `1718457045` -- timezone parameter applies only to this format
- **time.Time** value: from `localTime`, `utcTime`, or another time function

#### Chaining

Time functions can be piped together:

```jq
"2024-06-15T14:30:45Z" | roundTime("day") | timeAdd("9h") | {
    hour: datePart("hour"),
    day:  datePart("day")
}
# {"hour": 9, "day": 15}

utcTime | roundTime("year"; "UTC") | {
    month: datePart("month"; "UTC"),
    day:   datePart("day"; "UTC")
}
# {"month": 1, "day": 1}
```

### Query Engine

#### `queryHugr(query)` / `queryHugr(query; variables)`

Executes a GraphQL query against the hugr query engine. Requires `WithQuerier` option.

```jq
queryHugr("{ users { name } }").users
# [{"name": "alice"}, {"name": "bob"}]

queryHugr("{ user(id: $id) { name } }"; {"id": "u1"}).user.name
# "alice"
```

Can be bound to a variable with `as`:

```jq
queryHugr("{ config { multiplier } }") as $cfg | .value * $cfg.config.multiplier
```

### Authentication

#### `authInfo`

Returns the current authentication context, or `null` if not authenticated. Requires `WithQuerier` option.

```jq
authInfo
# {"userId": "user-123", "userName": "alice", "role": "admin"}

authInfo.role
# "admin"
```
