//go:build duckdb_arrow

package timezone_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/db"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
)

// ── helpers ────────────────────────────────────────────────────────────────

func setupEngine(t *testing.T, timezone string) *hugr.Service {
	t.Helper()
	ctx := context.Background()

	service, err := hugr.New(hugr.Config{
		DB: db.Config{
			Path: "",
			Settings: db.Settings{
				Timezone: timezone,
			},
		},
		CoreDB: coredb.New(coredb.Config{}),
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{
					Allowed: true,
					Role:    "admin",
				}),
			},
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })

	err = service.Init(ctx)
	require.NoError(t, err)

	return service
}

// ── Connection-level timezone tests ────────────────────────────────────────

func TestTimezone_ConnectionSetsTimezone(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := db.ContextWithTimezone(context.Background(), "America/New_York")
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var tz string
	err = conn.QueryRow(ctx, "SELECT current_setting('TimeZone')").Scan(&tz)
	require.NoError(t, err)
	assert.Equal(t, "America/New_York", tz)
}

func TestTimezone_PoolIsolation(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	// Request 1: set Tokyo
	ctx1 := db.ContextWithTimezone(context.Background(), "Asia/Tokyo")
	conn1, err := pool.Conn(ctx1)
	require.NoError(t, err)
	conn1.Close()

	// Request 2: no timezone — must NOT inherit Tokyo
	ctx2 := context.Background()
	conn2, err := pool.Conn(ctx2)
	require.NoError(t, err)
	defer conn2.Close()

	var tz string
	err = conn2.QueryRow(ctx2, "SELECT current_setting('TimeZone')").Scan(&tz)
	require.NoError(t, err)
	assert.NotEqual(t, "Asia/Tokyo", tz, "timezone leaked between connections")
}

func TestTimezone_DefaultTimezone(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{
		Path:     "",
		Settings: db.Settings{Timezone: "Europe/Moscow"},
	})
	require.NoError(t, err)
	defer pool.Close()

	conn, err := pool.Conn(context.Background())
	require.NoError(t, err)
	defer conn.Close()

	var tz string
	err = conn.QueryRow(context.Background(), "SELECT current_setting('TimeZone')").Scan(&tz)
	require.NoError(t, err)
	assert.Equal(t, "Europe/Moscow", tz)
}

func TestTimezone_RequestOverridesDefault(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{
		Path:     "",
		Settings: db.Settings{Timezone: "Europe/Moscow"},
	})
	require.NoError(t, err)
	defer pool.Close()

	ctx := db.ContextWithTimezone(context.Background(), "US/Pacific")
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var tz string
	err = conn.QueryRow(ctx, "SELECT current_setting('TimeZone')").Scan(&tz)
	require.NoError(t, err)
	assert.Equal(t, "US/Pacific", tz)
}

func TestTimezone_InvalidTimezoneError(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := db.ContextWithTimezone(context.Background(), "Not/A/Timezone")
	_, err = pool.Conn(ctx)
	assert.Error(t, err)
}

func TestTimezone_ArrowConnection(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := db.ContextWithTimezone(context.Background(), "Europe/Berlin")
	ar, err := pool.Arrow(ctx)
	require.NoError(t, err)
	defer ar.Close()

	reader, err := ar.QueryContext(ctx, "SELECT current_setting('TimeZone') AS tz")
	require.NoError(t, err)
	defer reader.Release()

	require.True(t, reader.Next())
	assert.Equal(t, "Europe/Berlin", reader.Record().Column(0).ValueStr(0))
}

func TestTimezone_ConcurrentIsolation(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()
	pool.SetMaxOpenConns(5)

	zones := []string{"America/New_York", "Europe/London", "Asia/Tokyo", "Australia/Sydney", "US/Pacific"}

	var wg sync.WaitGroup
	errors := make(chan string, len(zones))

	for _, zone := range zones {
		wg.Add(1)
		go func(tz string) {
			defer wg.Done()
			ctx := db.ContextWithTimezone(context.Background(), tz)
			conn, err := pool.Conn(ctx)
			if err != nil {
				errors <- "Conn: " + err.Error()
				return
			}
			defer conn.Close()

			var got string
			if err := conn.QueryRow(ctx, "SELECT current_setting('TimeZone')").Scan(&got); err != nil {
				errors <- "Query: " + err.Error()
				return
			}
			if got != tz {
				errors <- "expected " + tz + ", got " + got
			}
		}(zone)
	}

	wg.Wait()
	close(errors)

	for e := range errors {
		t.Error(e)
	}
}

// ── TIMESTAMP vs TIMESTAMPTZ behavior ──────────────────────────────────────

func TestTimezone_TimestampTZAffected_TimestampNot(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()
	_, err = pool.Exec(ctx, `
		CREATE TABLE tz_test (
			id INT,
			aware TIMESTAMPTZ,
			naive TIMESTAMP
		);
		INSERT INTO tz_test VALUES (1, '2025-06-15 12:00:00+00', '2025-06-15 12:00:00');
	`)
	require.NoError(t, err)

	ctx = db.ContextWithTimezone(context.Background(), "Europe/Moscow")
	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	var awareStr, naiveStr string
	err = conn.QueryRow(ctx, "SELECT aware::VARCHAR, naive::VARCHAR FROM tz_test WHERE id = 1").Scan(&awareStr, &naiveStr)
	require.NoError(t, err)

	// TIMESTAMPTZ shifted to Moscow (UTC+3)
	assert.Equal(t, "2025-06-15 15:00:00+03", awareStr)
	// TIMESTAMP (naive) stays unchanged
	assert.Equal(t, "2025-06-15 12:00:00", naiveStr)
}

func TestTimezone_NaiveTimestampUnaffectedByAnyTimezone(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()
	_, err = pool.Exec(ctx, `
		CREATE TABLE naive_test (id INT, ts TIMESTAMP);
		INSERT INTO naive_test VALUES (1, '2025-06-15 12:00:00');
	`)
	require.NoError(t, err)

	for _, zone := range []string{"UTC", "America/New_York", "Asia/Tokyo", "Europe/Moscow"} {
		t.Run(zone, func(t *testing.T) {
			ctx := db.ContextWithTimezone(context.Background(), zone)
			conn, err := pool.Conn(ctx)
			require.NoError(t, err)
			defer conn.Close()

			var ts string
			err = conn.QueryRow(ctx, "SELECT ts::VARCHAR FROM naive_test WHERE id = 1").Scan(&ts)
			require.NoError(t, err)
			assert.Equal(t, "2025-06-15 12:00:00", ts, "zone=%s", zone)
		})
	}
}

// ── HTTP endpoint tests ────────────────────────────────────────────────────

func TestTimezone_QueryEndpoint_WithHeader(t *testing.T) {
	service := setupEngine(t, "")

	body, _ := json.Marshal(map[string]any{"query": `{ __typename }`})
	req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hugr-Timezone", "America/New_York")

	w := httptest.NewRecorder()
	service.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestTimezone_QueryEndpoint_TimeZoneHeaderFallback(t *testing.T) {
	service := setupEngine(t, "")

	// Test Time-Zone header (fallback, GitHub convention)
	body, _ := json.Marshal(map[string]any{"query": `{ __typename }`})
	req := httptest.NewRequest("POST", "/query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Time-Zone", "Asia/Tokyo") // fallback header

	w := httptest.NewRecorder()
	service.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestTimezone_JQEndpoint_WithTimezone(t *testing.T) {
	service := setupEngine(t, "")
	body, _ := json.Marshal(map[string]any{
		"jq": `.`,
		"query": map[string]any{
			"query": `{ __typename }`,
		},
	})
	req := httptest.NewRequest("POST", "/jq-query", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hugr-Timezone", "Europe/Berlin")

	w := httptest.NewRecorder()
	service.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestTimezone_IPCEndpoint_WithTimezone(t *testing.T) {
	service := setupEngine(t, "")
	// Test /ipc endpoint accepts timezone header
	body, _ := json.Marshal(map[string]any{"query": `{ __typename }`})
	req := httptest.NewRequest("POST", "/ipc", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Hugr-Timezone", "US/Pacific")

	w := httptest.NewRecorder()
	service.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()
	// IPC may return different content type but should not error due to timezone
	assert.NotEqual(t, http.StatusInternalServerError, resp.StatusCode)
}

// ── DateTime/Timestamp type integration ────────────────────────────────────

func TestTimezone_DatePartOnTimestamp(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()
	_, err = pool.Exec(ctx, `
		CREATE TABLE dp_test (id INT, ts TIMESTAMP);
		INSERT INTO dp_test VALUES (1, '2025-06-15 14:30:45');
	`)
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	tests := []struct {
		part     string
		expected int
	}{
		{"year", 2025}, {"month", 6}, {"day", 15}, {"hour", 14}, {"minute", 30},
	}
	for _, tt := range tests {
		t.Run(tt.part, func(t *testing.T) {
			var val int
			err := conn.QueryRow(ctx, "SELECT date_part('"+tt.part+"', ts)::INT FROM dp_test WHERE id = 1").Scan(&val)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, val)
		})
	}
}

func TestTimezone_BucketAggregation(t *testing.T) {
	pool, err := db.Connect(context.Background(), db.Config{Path: ""})
	require.NoError(t, err)
	defer pool.Close()

	ctx := context.Background()
	_, err = pool.Exec(ctx, `
		CREATE TABLE bucket_test (id INT, ts TIMESTAMP);
		INSERT INTO bucket_test VALUES (1, '2025-03-15 10:00:00'), (2, '2025-03-15 14:30:00'), (3, '2025-03-16 08:00:00');
	`)
	require.NoError(t, err)

	conn, err := pool.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	rows, err := conn.Query(ctx, "SELECT date_trunc('day', ts)::VARCHAR, COUNT(*) FROM bucket_test GROUP BY 1 ORDER BY 1")
	require.NoError(t, err)
	defer rows.Close()

	results := map[string]int{}
	for rows.Next() {
		var bucket string
		var cnt int
		require.NoError(t, rows.Scan(&bucket, &cnt))
		results[bucket] = cnt
	}

	assert.Equal(t, 2, results["2025-03-15 00:00:00"])
	assert.Equal(t, 1, results["2025-03-16 00:00:00"])
}
