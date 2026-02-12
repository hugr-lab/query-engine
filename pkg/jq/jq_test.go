package jq

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockQuerier struct {
	queryFunc func(ctx context.Context, query string, vars map[string]any) (*types.Response, error)
}

func (m *mockQuerier) Query(ctx context.Context, query string, vars map[string]any) (*types.Response, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query, vars)
	}
	return &types.Response{}, nil
}

func (m *mockQuerier) RegisterDataSource(ctx context.Context, ds types.DataSource) error { return nil }
func (m *mockQuerier) LoadDataSource(ctx context.Context, name string) error             { return nil }
func (m *mockQuerier) UnloadDataSource(ctx context.Context, name string) error           { return nil }
func (m *mockQuerier) DataSourceStatus(ctx context.Context, name string) (string, error) {
	return "", nil
}
func (m *mockQuerier) DescribeDataSource(ctx context.Context, name string, self bool) (string, error) {
	return "", nil
}

func TestNewTransformer(t *testing.T) {
	tt := []struct {
		Name    string
		Query   string
		Opts    []Option
		WantErr bool
	}{
		{
			Name:  "identity query",
			Query: ".",
		},
		{
			Name:  "select field",
			Query: ".foo",
		},
		{
			Name:  "pipe query",
			Query: ".foo | .bar",
		},
		{
			Name:    "invalid query syntax",
			Query:   ".foo ||| bar",
			WantErr: true,
		},
		{
			Name:  "with variables option",
			Query: "$x",
			Opts:  []Option{WithVariables(map[string]any{"x": 1})},
		},
		{
			Name:  "with collect stat option",
			Query: ".",
			Opts:  []Option{WithCollectStat()},
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			tr, err := NewTransformer(context.Background(), tc.Query, tc.Opts...)
			if tc.WantErr {
				require.Error(t, err)
				assert.Nil(t, tr)
				return
			}
			require.NoError(t, err)
			assert.NotNil(t, tr)
		})
	}
}

func TestTransform(t *testing.T) {
	tt := []struct {
		Name    string
		Query   string
		Input   any
		Want    any
		WantErr bool
	}{
		{
			Name:  "identity",
			Query: ".",
			Input: map[string]any{"a": 1},
			Want:  map[string]any{"a": float64(1)},
		},
		{
			Name:  "select field",
			Query: ".name",
			Input: map[string]any{"name": "alice"},
			Want:  "alice",
		},
		{
			Name:  "nested field",
			Query: ".a.b",
			Input: map[string]any{"a": map[string]any{"b": 42}},
			Want:  float64(42),
		},
		{
			Name:  "array index",
			Query: ".[1]",
			Input: []any{10, 20, 30},
			Want:  float64(20),
		},
		{
			Name:  "arithmetic",
			Query: ".a + .b",
			Input: map[string]any{"a": 3, "b": 4},
			Want:  float64(7),
		},
		{
			Name:  "null input field",
			Query: ".missing",
			Input: map[string]any{"a": 1},
			Want:  nil,
		},
		{
			Name:  "multiple outputs returns array",
			Query: ".[]",
			Input: []any{1, 2, 3},
			Want:  []any{float64(1), float64(2), float64(3)},
		},
		{
			Name:  "object construction",
			Query: `{x: .a, y: .b}`,
			Input: map[string]any{"a": 1, "b": 2},
			Want:  map[string]any{"x": float64(1), "y": float64(2)},
		},
		{
			Name:  "string interpolation",
			Query: `"Hello, \(.name)!"`,
			Input: map[string]any{"name": "world"},
			Want:  "Hello, world!",
		},
		{
			Name:    "type error in query",
			Query:   ".foo | .bar",
			Input:   map[string]any{"foo": "not_an_object"},
			WantErr: true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			tr, err := NewTransformer(context.Background(), tc.Query)
			require.NoError(t, err)

			got, err := tr.Transform(context.Background(), tc.Input, nil)
			if tc.WantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Want, got)
		})
	}
}

func TestTransformWithVariables(t *testing.T) {
	tt := []struct {
		Name        string
		Query       string
		Input       any
		CompileVars map[string]any
		RuntimeVars map[string]any
		Want        any
		WantErr     bool
	}{
		{
			Name:        "single variable",
			Query:       "$x",
			Input:       nil,
			CompileVars: map[string]any{"x": "hello"},
			Want:        "hello",
		},
		{
			Name:        "variable in expression",
			Query:       ".value + $offset",
			Input:       map[string]any{"value": 10},
			CompileVars: map[string]any{"offset": 5},
			Want:        float64(15),
		},
		{
			Name:        "multiple variables",
			Query:       `{a: $x, b: $y}`,
			Input:       nil,
			CompileVars: map[string]any{"x": "foo", "y": "bar"},
			Want:        map[string]any{"a": "foo", "b": "bar"},
		},
		{
			Name:        "runtime vars override compile vars",
			Query:       "$x",
			Input:       nil,
			CompileVars: map[string]any{"x": "compile_value"},
			RuntimeVars: map[string]any{"x": "runtime_value"},
			Want:        "runtime_value",
		},
		{
			Name:        "runtime vars nil uses compile vars",
			Query:       "$x",
			Input:       nil,
			CompileVars: map[string]any{"x": "compile_value"},
			RuntimeVars: nil,
			Want:        "compile_value",
		},
		{
			Name:        "variable with object input",
			Query:       `{name: .name, greeting: $msg}`,
			Input:       map[string]any{"name": "alice"},
			CompileVars: map[string]any{"msg": "hi"},
			Want:        map[string]any{"name": "alice", "greeting": "hi"},
		},
		{
			Name:        "variable with number",
			Query:       `. * $factor`,
			Input:       float64(7),
			CompileVars: map[string]any{"factor": 3},
			Want:        float64(21),
		},
		{
			Name:        "variable with null value",
			Query:       `$x`,
			Input:       nil,
			CompileVars: map[string]any{"x": nil},
			Want:        nil,
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			tr, err := NewTransformer(context.Background(), tc.Query, WithVariables(tc.CompileVars))
			require.NoError(t, err)

			got, err := tr.Transform(context.Background(), tc.Input, tc.RuntimeVars)
			if tc.WantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Want, got)
		})
	}
}

func TestAuthInfoFunction(t *testing.T) {
	tt := []struct {
		Name  string
		Query string
		Auth  *auth.AuthInfo
		Want  any
	}{
		{
			Name:  "no auth info returns null",
			Query: "authInfo",
			Auth:  nil,
			Want:  nil,
		},
		{
			Name:  "auth info returns user fields",
			Query: "authInfo",
			Auth: &auth.AuthInfo{
				UserId:   "user-123",
				UserName: "alice",
				Role:     "admin",
			},
			Want: map[string]any{
				"userId":   "user-123",
				"userName": "alice",
				"role":     "admin",
			},
		},
		{
			Name:  "auth info select userId",
			Query: "authInfo.userId",
			Auth: &auth.AuthInfo{
				UserId:   "u-1",
				UserName: "bob",
				Role:     "viewer",
			},
			Want: "u-1",
		},
		{
			Name:  "auth info select role",
			Query: "authInfo.role",
			Auth: &auth.AuthInfo{
				UserId:   "u-2",
				UserName: "carol",
				Role:     "editor",
			},
			Want: "editor",
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			ctx := context.Background()
			if tc.Auth != nil {
				ctx = auth.ContextWithAuthInfo(ctx, tc.Auth)
			}

			qe := &mockQuerier{}
			tr, err := NewTransformer(ctx, tc.Query, WithQuerier(qe))
			require.NoError(t, err)

			got, err := tr.Transform(ctx, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.Want, got)
		})
	}
}

func TestQueryHugrFunction(t *testing.T) {
	tt := []struct {
		Name     string
		Query    string
		Input    any
		MockResp *types.Response
		MockErr  error
		Want     any
		WantErr  bool
	}{
		{
			Name:  "simple query returns data",
			Query: `queryHugr("{ users { name } }")`,
			Input: nil,
			MockResp: &types.Response{
				Data: map[string]any{
					"users": []any{
						map[string]any{"name": "alice"},
					},
				},
			},
			Want: map[string]any{
				"users": []any{
					map[string]any{"name": "alice"},
				},
			},
		},
		{
			Name:  "query with variables",
			Query: `queryHugr("{ user(id: $id) { name } }"; {"id": "u1"})`,
			Input: nil,
			MockResp: &types.Response{
				Data: map[string]any{
					"user": map[string]any{"name": "bob"},
				},
			},
			Want: map[string]any{
				"user": map[string]any{"name": "bob"},
			},
		},
		{
			Name:    "query returns error from querier",
			Query:   `queryHugr("bad query")`,
			Input:   nil,
			MockErr: fmt.Errorf("query failed"),
			WantErr: true,
		},
		{
			Name:  "queryHugr as variable and select field",
			Query: `queryHugr("{ users { name } }") as $res | $res.users`,
			Input: nil,
			MockResp: &types.Response{
				Data: map[string]any{
					"users": []any{
						map[string]any{"name": "alice"},
						map[string]any{"name": "bob"},
					},
				},
			},
			Want: []any{
				map[string]any{"name": "alice"},
				map[string]any{"name": "bob"},
			},
		},
		{
			Name:  "queryHugr as variable and nested access",
			Query: `queryHugr("{ user { name age } }") as $res | {n: $res.user.name, a: $res.user.age}`,
			Input: nil,
			MockResp: &types.Response{
				Data: map[string]any{
					"user": map[string]any{"name": "carol", "age": float64(30)},
				},
			},
			Want: map[string]any{"n": "carol", "a": float64(30)},
		},
		{
			Name:  "queryHugr as variable with vars and select",
			Query: `queryHugr("{ user(id: $id) { name } }"; {"id": "u2"}) as $res | $res.user.name`,
			Input: nil,
			MockResp: &types.Response{
				Data: map[string]any{
					"user": map[string]any{"name": "dave"},
				},
			},
			Want: "dave",
		},
		{
			Name:  "queryHugr as variable combined with input",
			Query: `queryHugr("{ config { multiplier } }") as $cfg | .value * $cfg.config.multiplier`,
			Input: map[string]any{"value": 5},
			MockResp: &types.Response{
				Data: map[string]any{
					"config": map[string]any{"multiplier": float64(3)},
				},
			},
			Want: float64(15),
		},
		{
			Name:  "queryHugr as variable select array element",
			Query: `queryHugr("{ items { id } }") as $res | $res.items[0].id`,
			Input: nil,
			MockResp: &types.Response{
				Data: map[string]any{
					"items": []any{
						map[string]any{"id": "first"},
						map[string]any{"id": "second"},
					},
				},
			},
			Want: "first",
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			qe := &mockQuerier{
				queryFunc: func(ctx context.Context, query string, vars map[string]any) (*types.Response, error) {
					if tc.MockErr != nil {
						return nil, tc.MockErr
					}
					return tc.MockResp, nil
				},
			}

			ctx := context.Background()
			tr, err := NewTransformer(ctx, tc.Query, WithQuerier(qe))
			require.NoError(t, err)

			got, err := tr.Transform(ctx, tc.Input, nil)
			if tc.WantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.Want, got)
		})
	}
}

func TestTimeFunctions(t *testing.T) {
	refTime := "2024-06-15T14:30:45Z"
	// Unix timestamp for timezone tests: parseTime uses tz only for float64 input
	refUnix := float64(time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC).Unix())
	ny := mustLoadLocation("America/New_York")

	tt := []struct {
		Name  string
		Query string
		Input any
		Want  any
	}{
		{
			Name: "roundTime all periods with tz",
			Query: `{
				second: roundTime("second"; "UTC"),
				minute: roundTime("minute"; "UTC"),
				hour:   roundTime("hour"; "UTC"),
				day:    roundTime("day"; "UTC"),
				month:  roundTime("month"; "UTC"),
				year:   roundTime("year"; "UTC"),
				monday: roundTime("monday"; "UTC"),
				custom: roundTime("15m"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"second": time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC),
				"minute": time.Date(2024, 6, 15, 14, 31, 0, 0, time.UTC),
				"hour":   time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
				"day":    time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
				"month":  time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				"year":   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				"monday": time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC),
				"custom": time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
			},
		},
		{
			Name: "roundTime all periods without tz",
			Query: `{
				day:    roundTime("day"),
				hour:   roundTime("hour"),
				month:  roundTime("month"),
				monday: roundTime("monday"),
				custom: roundTime("15m")
			}`,
			Input: refTime,
			Want: map[string]any{
				"day":    time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
				"hour":   time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
				"month":  time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
				"monday": time.Date(2024, 6, 10, 0, 0, 0, 0, time.UTC),
				"custom": time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
			},
		},
		{
			Name: "datePart all parts with tz",
			Query: `{
				year:    datePart("year"; "UTC"),
				month:   datePart("month"; "UTC"),
				day:     datePart("day"; "UTC"),
				hour:    datePart("hour"; "UTC"),
				minute:  datePart("minute"; "UTC"),
				second:  datePart("second"; "UTC"),
				weekday: datePart("weekday"; "UTC"),
				yearday: datePart("yearday"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"year":    2024,
				"month":   6,
				"day":     15,
				"hour":    14,
				"minute":  30,
				"second":  45,
				"weekday": 6,
				"yearday": 167,
			},
		},
		{
			Name: "datePart all parts without tz",
			Query: `{
				year:    datePart("year"),
				month:   datePart("month"),
				day:     datePart("day"),
				hour:    datePart("hour"),
				weekday: datePart("weekday")
			}`,
			Input: refTime,
			Want: map[string]any{
				"year":    2024,
				"month":   6,
				"day":     15,
				"hour":    14,
				"weekday": 6,
			},
		},
		{
			Name: "timeAdd positive and negative with tz",
			Query: `{
				plus2h:   timeAdd("2h"; "UTC"),
				minus30m: timeAdd("-30m"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"plus2h":   time.Date(2024, 6, 15, 16, 30, 45, 0, time.UTC),
				"minus30m": time.Date(2024, 6, 15, 14, 0, 45, 0, time.UTC),
			},
		},
		{
			Name: "timeAdd without tz",
			Query: `{
				plus2h:  timeAdd("2h"),
				minus1h: timeAdd("-1h")
			}`,
			Input: refTime,
			Want: map[string]any{
				"plus2h":  time.Date(2024, 6, 15, 16, 30, 45, 0, time.UTC),
				"minus1h": time.Date(2024, 6, 15, 13, 30, 45, 0, time.UTC),
			},
		},
		{
			Name: "mixed functions in object with tz",
			Query: `{
				day:     roundTime("day"; "UTC"),
				added:   timeAdd("3h"; "UTC"),
				year:    datePart("year"; "UTC"),
				weekday: datePart("weekday"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"day":     time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
				"added":   time.Date(2024, 6, 15, 17, 30, 45, 0, time.UTC),
				"year":    2024,
				"weekday": 6,
			},
		},
		{
			Name: "mixed functions in object without tz",
			Query: `{
				day:     roundTime("day"),
				added:   timeAdd("3h"),
				year:    datePart("year"),
				weekday: datePart("weekday")
			}`,
			Input: refTime,
			Want: map[string]any{
				"day":     time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
				"added":   time.Date(2024, 6, 15, 17, 30, 45, 0, time.UTC),
				"year":    2024,
				"weekday": 6,
			},
		},
		{
			Name: "timezone via unix all functions",
			Query: `{
				day:   roundTime("day"; "America/New_York"),
				added: timeAdd("1h30m"; "America/New_York"),
				hour:  datePart("hour"; "America/New_York")
			}`,
			Input: refUnix,
			Want: map[string]any{
				"day":   time.Date(2024, 6, 15, 0, 0, 0, 0, ny),
				"added": time.Date(2024, 6, 15, 12, 0, 45, 0, ny),
				"hour":  10,
			},
		},
		{
			Name: "chained funcs round to day then add 9h then extract parts",
			Query: `roundTime("day"; "UTC") | timeAdd("9h"; "UTC") | {
				hour:    datePart("hour"; "UTC"),
				day:     datePart("day"; "UTC"),
				weekday: datePart("weekday"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"hour":    9,
				"day":     15,
				"weekday": 6,
			},
		},
		{
			Name: "chained funcs round to monday then add 2h then extract",
			Query: `roundTime("monday"; "UTC") | timeAdd("2h"; "UTC") | {
				hour:    datePart("hour"; "UTC"),
				day:     datePart("day"; "UTC"),
				weekday: datePart("weekday"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"hour":    2,
				"day":     10,
				"weekday": 1,
			},
		},
		{
			Name: "chained funcs without tz round to month add 48h extract",
			Query: `roundTime("month") | timeAdd("48h") | {
				day:  datePart("day"),
				hour: datePart("hour")
			}`,
			Input: refTime,
			Want: map[string]any{
				"day":  3,
				"hour": 0,
			},
		},
		{
			Name: "chained funcs with timezone via unix round add extract",
			Query: `roundTime("day"; "America/New_York") | timeAdd("5h30m"; "America/New_York") | {
				hour:    datePart("hour"; "America/New_York"),
				weekday: datePart("weekday"; "America/New_York")
			}`,
			Input: refUnix,
			Want: map[string]any{
				"hour":    5,
				"weekday": 6,
			},
		},
		{
			Name: "chained round to year add duration round to month",
			Query: `roundTime("year"; "UTC") | timeAdd("744h"; "UTC") | roundTime("month"; "UTC") | {
				month: datePart("month"; "UTC"),
				day:   datePart("day"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"month": 2,
				"day":   1,
			},
		},
		{
			Name: "utcTime chained round to day gives zero time parts",
			Query: `utcTime | roundTime("day"; "UTC") | {
				hour:   datePart("hour"; "UTC"),
				minute: datePart("minute"; "UTC"),
				second: datePart("second"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"hour":   0,
				"minute": 0,
				"second": 0,
			},
		},
		{
			Name: "utcTime chained round to month gives first day",
			Query: `utcTime | roundTime("month"; "UTC") | {
				day:  datePart("day"; "UTC"),
				hour: datePart("hour"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"day":  1,
				"hour": 0,
			},
		},
		{
			Name: "utcTime chained round to year gives jan first",
			Query: `utcTime | roundTime("year"; "UTC") | {
				month: datePart("month"; "UTC"),
				day:   datePart("day"; "UTC"),
				hour:  datePart("hour"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"month": 1,
				"day":   1,
				"hour":  0,
			},
		},
		{
			Name: "localTime chained round to day gives zero time parts",
			Query: `localTime | roundTime("day") | {
				hour:   datePart("hour"),
				minute: datePart("minute"),
				second: datePart("second")
			}`,
			Input: nil,
			Want: map[string]any{
				"hour":   0,
				"minute": 0,
				"second": 0,
			},
		},
		{
			Name: "localTime chained round to month add 25h extract",
			Query: `localTime | roundTime("month") | timeAdd("25h") | {
				day:  datePart("day"),
				hour: datePart("hour")
			}`,
			Input: nil,
			Want: map[string]any{
				"day":  2,
				"hour": 1,
			},
		},
		{
			Name: "now chained round to day via UTC gives zero hour",
			Query: `now | roundTime("day"; "UTC") | {
				hour:   datePart("hour"; "UTC"),
				minute: datePart("minute"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"hour":   0,
				"minute": 0,
			},
		},
		{
			Name: "now chained round to year gives jan first",
			Query: `now | roundTime("year"; "UTC") | {
				month: datePart("month"; "UTC"),
				day:   datePart("day"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"month": 1,
				"day":   1,
			},
		},
		{
			Name: "now chained round to hour gives zero minutes",
			Query: `now | roundTime("hour"; "UTC") | {
				minute: datePart("minute"; "UTC"),
				second: datePart("second"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"minute": 0,
				"second": 0,
			},
		},
		{
			Name: "utcTime round to day add 9h30m then extract",
			Query: `utcTime | roundTime("day"; "UTC") | timeAdd("9h30m"; "UTC") | {
				hour:   datePart("hour"; "UTC"),
				minute: datePart("minute"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"hour":   9,
				"minute": 30,
			},
		},
		{
			Name: "now round to month add 48h extract day and hour",
			Query: `now | roundTime("month"; "UTC") | timeAdd("48h"; "UTC") | {
				day:  datePart("day"; "UTC"),
				hour: datePart("hour"; "UTC")
			}`,
			Input: nil,
			Want: map[string]any{
				"day":  3,
				"hour": 0,
			},
		},
		{
			Name:  "unixTime converts RFC3339 string to unix seconds",
			Query: `unixTime`,
			Input: refTime,
			Want:  time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC).Unix(),
		},
		{
			Name:  "unixTime converts time from roundTime",
			Query: `roundTime("day"; "UTC") | unixTime`,
			Input: refTime,
			Want:  time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC).Unix(),
		},
		{
			Name: "unixTime in object with other time funcs",
			Query: `{
				unix:     unixTime,
				day_unix: (roundTime("day"; "UTC") | unixTime),
				hour:     datePart("hour"; "UTC")
			}`,
			Input: refTime,
			Want: map[string]any{
				"unix":     time.Date(2024, 6, 15, 14, 30, 45, 0, time.UTC).Unix(),
				"day_unix": time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC).Unix(),
				"hour":     14,
			},
		},
		{
			Name:  "unixTime chained with timeAdd",
			Query: `timeAdd("1h"; "UTC") | unixTime`,
			Input: refTime,
			Want:  time.Date(2024, 6, 15, 15, 30, 45, 0, time.UTC).Unix(),
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			qe := &mockQuerier{}
			tr, err := NewTransformer(context.Background(), tc.Query, WithQuerier(qe))
			require.NoError(t, err)

			got, err := tr.Transform(context.Background(), tc.Input, nil)
			require.NoError(t, err)
			assert.Equal(t, tc.Want, got)
		})
	}

	t.Run("unixTime on null input returns current UTC unix", func(t *testing.T) {
		qe := &mockQuerier{}
		tr, err := NewTransformer(context.Background(), `null | unixTime`, WithQuerier(qe))
		require.NoError(t, err)

		before := time.Now().UTC().Unix()
		got, err := tr.Transform(context.Background(), nil, nil)
		after := time.Now().UTC().Unix()
		require.NoError(t, err)

		unix, ok := got.(int64)
		require.True(t, ok, "expected int64, got %T", got)
		assert.GreaterOrEqual(t, unix, before)
		assert.LessOrEqual(t, unix, after)
	})
}

func TestTimeFunctionErrors(t *testing.T) {
	tt := []struct {
		Name  string
		Query string
		Input any
	}{
		{
			Name:  "roundTime invalid duration string",
			Query: `roundTime("invalid_dur"; "UTC")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "roundTime invalid timezone",
			Query: `roundTime("day"; "Invalid/TZ")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "timeAdd invalid duration",
			Query: `timeAdd("not_a_duration"; "UTC")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "timeAdd invalid timezone",
			Query: `timeAdd("1h"; "Bad/Zone")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "datePart invalid part",
			Query: `datePart("century"; "UTC")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "datePart invalid timezone",
			Query: `datePart("hour"; "No/Where")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "roundTime null timezone",
			Query: `roundTime("day"; null)`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "timeAdd null timezone",
			Query: `timeAdd("1h"; null)`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "datePart null timezone",
			Query: `datePart("hour"; null)`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "roundTime non-string duration",
			Query: `roundTime(123; "UTC")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "timeAdd non-string duration",
			Query: `timeAdd(123; "UTC")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "datePart non-string part",
			Query: `datePart(123; "UTC")`,
			Input: "2024-06-15T14:30:45Z",
		},
		{
			Name:  "roundTime invalid time input",
			Query: `roundTime("day"; "UTC")`,
			Input: true,
		},
		{
			Name:  "timeAdd invalid time input",
			Query: `timeAdd("1h"; "UTC")`,
			Input: true,
		},
		{
			Name:  "datePart invalid time input",
			Query: `datePart("hour"; "UTC")`,
			Input: true,
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			qe := &mockQuerier{}
			tr, err := NewTransformer(context.Background(), tc.Query, WithQuerier(qe))
			require.NoError(t, err)

			_, err = tr.Transform(context.Background(), tc.Input, nil)
			require.Error(t, err)
		})
	}
}

func TestStats(t *testing.T) {
	tt := []struct {
		Name          string
		RunCount      int
		WantRuns      int
		WantTransform int
	}{
		{
			Name:          "single run",
			RunCount:      1,
			WantRuns:      1,
			WantTransform: 1,
		},
		{
			Name:          "multiple runs",
			RunCount:      5,
			WantRuns:      5,
			WantTransform: 5,
		},
	}

	for _, tc := range tt {
		t.Run(tc.Name, func(t *testing.T) {
			tr, err := NewTransformer(context.Background(), ".a", WithCollectStat())
			require.NoError(t, err)

			for i := 0; i < tc.RunCount; i++ {
				_, err := tr.Transform(context.Background(), map[string]any{"a": i}, nil)
				require.NoError(t, err)
			}

			stats := tr.Stats()
			assert.Equal(t, tc.WantRuns, stats.Runs)
			assert.Equal(t, tc.WantTransform, stats.Transformed)
			assert.Greater(t, stats.CompilerTime, time.Duration(0))
			assert.Greater(t, stats.SerializationTime, time.Duration(0))
			assert.Greater(t, stats.ExecutionTime, time.Duration(0))
		})
	}
}

func mustLoadLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		panic(err)
	}
	return loc
}
