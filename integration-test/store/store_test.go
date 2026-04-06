//go:build duckdb_arrow

package store_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

var testService *hugr.Service

func TestMain(m *testing.M) {
	ctx := context.Background()

	service, err := hugr.New(hugr.Config{
		Debug:  true,
		DB:     db.Config{},
		CoreDB: coredb.New(coredb.Config{}),
		Auth:   &auth.Config{},
	})
	if err != nil {
		panic(err)
	}
	if err := service.Init(ctx); err != nil {
		panic(err)
	}
	testService = service

	// Register Redis DS
	if redisURL := os.Getenv("REDIS"); redisURL != "" {
		// Ensure redis:// scheme for go-redis ParseURL
		if !strings.HasPrefix(redisURL, "redis://") {
			redisURL = "redis://" + redisURL + "/0"
		}
		mustQuery(ctx, service, `mutation($data: core_data_sources_mut_input_data!) {
			core { insert_data_sources(data: $data) { name } }
		}`, map[string]any{
			"data": map[string]any{
				"name": "redis", "type": "redis",
				"prefix": "redis", "as_module": false, "path": redisURL,
			},
		})
		loadRes, loadErr := service.Query(ctx, `mutation { function { core { load_data_source(name: "redis") { success message } } } }`, nil)
		if loadErr != nil {
			panic(fmt.Sprintf("load_data_source query error: %v", loadErr))
		}
		var loadResult struct {
			Success bool   `json:"success"`
			Message string `json:"message"`
		}
		if err := loadRes.ScanData("function.core.load_data_source", &loadResult); err != nil {
			panic(fmt.Sprintf("load_data_source scan error: %v, errors: %v", err, loadRes.Errors))
		}
		loadRes.Close()
		if !loadResult.Success {
			panic(fmt.Sprintf("load_data_source failed: %s", loadResult.Message))
		}
		_ = loadResult // Redis loaded
	}

	code := m.Run()
	service.Close()
	os.Exit(code)
}

func mustQuery(ctx context.Context, s *hugr.Service, q string, vars map[string]any) {
	res, err := s.Query(ctx, q, vars)
	if err != nil {
		panic(fmt.Sprintf("query error: %v\nquery: %s", err, q))
	}
	if len(res.Errors) > 0 {
		panic(fmt.Sprintf("graphql errors: %v\nquery: %s", res.Errors, q))
	}
	res.Close()
}

func query(t *testing.T, q string, vars map[string]any) *types.Response {
	t.Helper()
	res, err := testService.Query(context.Background(), q, vars)
	require.NoError(t, err)
	return res
}

// --- US1: Store Operations ---

func TestStore_SetAndGet(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	// Set
	res := query(t, `mutation { function { core { store {
		set(store: "redis", key: "test:greeting", value: "hello world", ttl: 60) { success message }
	} } } }`, nil)
	defer res.Close()
	var setResult struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
	}
	err := res.ScanData("function.core.store.set", &setResult)
	require.NoError(t, err)
	assert.True(t, setResult.Success)

	// Get
	res2 := query(t, `{ function { core { store {
		get(store: "redis", key: "test:greeting")
	} } } }`, nil)
	defer res2.Close()
	var val string
	err = res2.ScanData("function.core.store.get", &val)
	require.NoError(t, err)
	assert.Equal(t, "hello world", val)
}

func TestStore_GetNotFound(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	res := query(t, `{ function { core { store {
		get(store: "redis", key: "test:nonexistent_key_12345")
	} } } }`, nil)
	defer res.Close()
	var val any
	_ = res.ScanData("function.core.store.get", &val)
	assert.Nil(t, val, "nonexistent key should return null")
}

func TestStore_Del(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	// Set first
	res := query(t, `mutation { function { core { store {
		set(store: "redis", key: "test:todel", value: "temp") { success }
	} } } }`, nil)
	res.Close()

	// Delete
	res = query(t, `mutation { function { core { store {
		del(store: "redis", key: "test:todel") { success }
	} } } }`, nil)
	defer res.Close()
	var delResult struct{ Success bool `json:"success"` }
	err := res.ScanData("function.core.store.del", &delResult)
	require.NoError(t, err)
	assert.True(t, delResult.Success)

	// Verify deleted
	res2 := query(t, `{ function { core { store { get(store: "redis", key: "test:todel") } } } }`, nil)
	defer res2.Close()
	var val any
	_ = res2.ScanData("function.core.store.get", &val)
	assert.Nil(t, val)
}

func TestStore_Incr(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	// Delete first to ensure clean state
	res := query(t, `mutation { function { core { store {
		del(store: "redis", key: "test:counter") { success }
	} } } }`, nil)
	res.Close()

	// Incr twice
	res = query(t, `mutation { function { core { store {
		incr(store: "redis", key: "test:counter")
	} } } }`, nil)
	defer res.Close()
	var val1 int64
	err := res.ScanData("function.core.store.incr", &val1)
	require.NoError(t, err)
	assert.Equal(t, int64(1), val1)

	res2 := query(t, `mutation { function { core { store {
		incr(store: "redis", key: "test:counter")
	} } } }`, nil)
	defer res2.Close()
	var val2 int64
	err = res2.ScanData("function.core.store.incr", &val2)
	require.NoError(t, err)
	assert.Equal(t, int64(2), val2)
}

func TestStore_Expire(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	// Set key
	res := query(t, `mutation { function { core { store {
		set(store: "redis", key: "test:expirable", value: "temp") { success }
	} } } }`, nil)
	res.Close()

	// Set expire
	res = query(t, `mutation { function { core { store {
		expire(store: "redis", key: "test:expirable", ttl: 300) { success }
	} } } }`, nil)
	defer res.Close()
	var result struct{ Success bool `json:"success"` }
	err := res.ScanData("function.core.store.expire", &result)
	require.NoError(t, err)
	assert.True(t, result.Success)
}

func TestStore_Keys(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	// Set some keys
	for _, k := range []string{"test:keys:a", "test:keys:b", "test:keys:c"} {
		res := query(t, `mutation { function { core { store {
			set(store: "redis", key: "`+k+`", value: "v") { success }
		} } } }`, nil)
		res.Close()
	}

	// List
	res := query(t, `{ function { core { store {
		keys(store: "redis", pattern: "test:keys:*")
	} } } }`, nil)
	defer res.Close()

	var keys []string
	err := res.ScanData("function.core.store.keys", &keys)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(keys), 3)
	t.Logf("keys: %d found", len(keys))
}

func TestStore_NotFound(t *testing.T) {
	res, err := testService.Query(context.Background(),
		`{ function { core { store { get(store: "nonexistent", key: "x") } } } }`, nil)
	if err != nil {
		t.Logf("expected error: %v", err)
		return
	}
	defer res.Close()
	assert.True(t, len(res.Errors) > 0 || err != nil, "should error for nonexistent store")
}

func TestStore_KeysEmpty(t *testing.T) {
	if os.Getenv("REDIS") == "" {
		t.Skip("REDIS not set")
	}
	res := query(t, `{ function { core { store {
		keys(store: "redis", pattern: "nonexistent_prefix_xyz:*")
	} } } }`, nil)
	defer res.Close()

	var keys []string
	err := res.ScanData("function.core.store.keys", &keys)
	require.NoError(t, err)
	assert.Empty(t, keys)
}
