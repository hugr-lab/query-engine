//go:build duckdb_arrow

package entity_test

import (
	"context"
	"testing"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/require"
)

// The meta path is what every catalog-* MCP tool reads, so its cost is the
// cost of the whole structural half — and it differs sharply between the two
// catalog storages. Run:
//
//	CGO_CFLAGS="-O1 -g" go test -tags=duckdb_arrow -run=NONE \
//	    -bench=BenchmarkMetaPath ./integration-test/catalog/entity/
//
// This exists because the numbers move: the compiled provider resolves a
// definition per root field (a DB round trip each), while the store answers a
// module listing with one grouped query, and both sides keep changing. A
// throwaway probe would have to be rewritten every time the question comes up.
//
// The "db" arm goes away with pkg/catalog/db itself when the entity storage
// becomes the default — delete it there, together with the storage loop, and
// leave the queries.
//
// The two storages are not ordered: the store wins by an order of magnitude on
// ENUMERATION, which is what it was built for and what the MCP catalog tools
// lean on, and loses badly on a single point lookup (DescribeObject), where the
// compiled provider answers from an in-memory map instead of SQL. Read the
// numbers per row, not as a verdict.

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

func benchStorage(b *testing.B, storage hugr.CatalogStorage) (*hugr.Service, context.Context) {
	b.Helper()
	service, err := hugr.New(hugr.Config{
		DB:             db.Config{Path: ""},
		CoreDB:         coredb.New(coredb.Config{}),
		CatalogStorage: storage,
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
	for _, storage := range []hugr.CatalogStorage{
		hugr.CatalogStorageCompiled, hugr.CatalogStorageEntity,
	} {
		b.Run(string(storage), func(b *testing.B) {
			s, ctx := benchStorage(b, storage)
			for _, q := range metaPathQueries {
				b.Run(q.name, func(b *testing.B) {
					// Warm the caches — the interesting number is the steady
					// state, not the first resolve.
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
		})
	}
}
