//go:build duckdb_arrow

package entity_test

import (
	"context"
	"net/http"
	"testing"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/require"
)

// The meta path is what every catalog-* MCP tool reads, so its cost is the
// cost of the whole structural half. Run:
//
//	CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow -run=NONE \
//	    -bench=BenchmarkMetaPath ./integration-test/catalog/entity/
//
// This exists because the numbers move and the shapes differ sharply: the store
// answers a module listing with one grouped query, while a point lookup
// (DescribeObject) costs it SQL where a map would be free. Read the rows
// separately — enumeration and lookup are not the same workload.
//
// It used to run both catalog storages side by side; the compiled arm went away
// with pkg/catalog/db.

var metaPathQueries = []struct {
	name  string
	query string
}{
	{"ModulesOneLevel", `{ _module(name: "") { name modules { name } } }`},
	{"ModulesTwoLevels", `{ _module(name: "") { name modules { name modules { name } } } }`},
	{"ModuleWalkWithPayload", `{ _module(name: "") { name dataObjects{name} functions{name}
		modules { name dataObjects{name} functions{name}
			modules { name dataObjects{name} functions{name}
				modules { name dataObjects{name} functions{name} modules { name } } } } } }`},
	{"DataSources", `{ _dataSources { name engine modules } }`},
	{"DescribeObject", `{ _dataObject(name: "core_data_sources") {
		name type moduleName primaryKey
		queries { name type rootTypeName }
		relations { name direction kind }
		fields { name }
	} }`},
}

func benchStorage(b *testing.B) (*hugr.Service, context.Context) {
	b.Helper()
	service, err := hugr.New(hugr.Config{
		DB:     db.Config{Path: ""},
		CoreDB: coredb.New(coredb.Config{}),
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{Allowed: true, Role: "admin"}),
			},
		},
	})
	require.NoError(b, err)
	b.Cleanup(func() { service.Close() })
	require.NoError(b, service.Init(context.Background()))
	return service, auth.ContextWithAuthInfo(context.Background(),
		&auth.AuthInfo{Role: "admin", UserId: "bench"})
}

func BenchmarkMetaPath(b *testing.B) {
	s, ctx := benchStorage(b)
	for _, q := range metaPathQueries {
		b.Run(q.name, func(b *testing.B) {
			// Warm the caches — the interesting number is the steady state,
			// not the first resolve.
			res, err := s.Query(ctx, q.query, nil)
			require.NoError(b, err)
			require.NoError(b, res.Err())
			res.Close()

			b.ResetTimer()
			for b.Loop() {
				res, err := s.Query(ctx, q.query, nil)
				if err != nil {
					b.Fatal(err)
				}
				if err := res.Err(); err != nil {
					b.Fatal(err)
				}
				res.Close()
			}
		})
	}
}

// BenchmarkCatalogTools measures the MCP tools themselves, on the entity
// storage. catalog-search is the expensive one and the only one whose cost is
// not obvious: it embeds the query, scans the index, and then spends round
// trips re-resolving candidates in the caller's context to filter them.
//
// The vector arm needs a live embedder (EMBEDDER_URL + EMBEDDER_VECTOR_SIZE)
// and skips without one; the lexical arm always runs, so the two can be
// compared on the same corpus:
//
//	set -a && source .local/.env && set +a
//	CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow -run=NONE \
//	    -bench=BenchmarkCatalogTools ./integration-test/catalog/entity/
func BenchmarkCatalogTools(b *testing.B) {
	run := func(b *testing.B, h http.Handler, tool string, args map[string]any) {
		mcpCall(b, h, tool, args) // warm
		b.ResetTimer()
		for b.Loop() {
			mcpCall(b, h, tool, args)
		}
	}

	b.Run("lexical", func(b *testing.B) {
		h := mcpHandler(b, 0)
		b.Run("list_modules", func(b *testing.B) {
			run(b, h, "catalog-list", map[string]any{"kind": "module"})
		})
		b.Run("list_data_objects", func(b *testing.B) {
			run(b, h, "catalog-list", map[string]any{"kind": "data_object", "limit": 200})
		})
		b.Run("describe_object", func(b *testing.B) {
			run(b, h, "catalog-describe", map[string]any{
				"kind": "data_object", "names": []string{"core_data_sources"},
			})
		})
		b.Run("search", func(b *testing.B) {
			run(b, h, "catalog-search", map[string]any{"query": "data sources", "limit": 10})
		})
	})

	b.Run("vector", func(b *testing.B) {
		url, size := liveEmbedder(b)
		h := mcpHandlerWithEmbedder(b, size, url)
		b.Run("search", func(b *testing.B) {
			run(b, h, "catalog-search", map[string]any{
				"query": "where are the attached databases described", "limit": 10,
			})
		})
		b.Run("search_fields_only", func(b *testing.B) {
			run(b, h, "catalog-search", map[string]any{
				"query": "connection string to reach the database",
				"kinds": []string{"field"}, "limit": 10,
			})
		})
	})
}
