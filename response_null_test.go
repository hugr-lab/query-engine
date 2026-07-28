//go:build duckdb_arrow

package hugr

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

// A field the engine resolves to NULL must appear in the response as an
// explicit null. It used to vanish: the sequential path dropped a nil result
// before it reached the response, and the collector then collapsed a
// single-key null response to a nil data map — so the same query answered
// "wrong data path" or "no data" depending on AllowParallel, and neither said
// anything about the field that was asked for.
//
// Both paths are covered here, because they failed differently.
func TestNullFieldReachesTheResponse(t *testing.T) {
	for _, parallel := range []bool{false, true} {
		name := "sequential"
		if parallel {
			name = "parallel"
		}
		t.Run(name, func(t *testing.T) {
			service, err := New(Config{
				DB:            db.Config{Path: ""},
				CoreDB:        coredb.New(coredb.Config{}),
				Auth:          &auth.Config{},
				AllowParallel: parallel,
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx := auth.ContextWithFullAccess(context.Background())
			if err := service.Init(ctx); err != nil {
				t.Fatal(err)
			}
			defer service.Close()

			// One root field, resolved to null: the shape that used to lose it
			// entirely. A meta lookup is the cheapest way to produce one.
			res, err := service.Query(ctx, `{ _dataObject(name: "no_such_object") { name } }`, nil)
			if err != nil {
				t.Fatalf("query: %v", err)
			}
			if rerr := res.Err(); rerr != nil {
				t.Fatalf("response error: %v", rerr)
			}
			defer res.Close()

			if res.Data == nil {
				t.Fatal("data is nil — a null field collapsed the whole response")
			}
			v, ok := res.Data["_dataObject"]
			if !ok {
				t.Fatalf("the field is missing from the response entirely: %v", res.Data)
			}
			if v != nil {
				t.Fatalf("_dataObject = %v, want null", v)
			}

			// Scanning INTO the null still reports no data — that is what
			// ScanData means by a nil value, and callers that need to tell
			// "served null" from "no answer" read the parent instead.
			var node struct {
				Name string `json:"name"`
			}
			if err := res.ScanData("_dataObject", &node); err == nil {
				t.Fatal("scanning into a null must not succeed")
			} else if !isNoData(err) {
				t.Fatalf("scanning into a null: got %v, want ErrNoData", err)
			}
		})
	}
}

func isNoData(err error) bool {
	return err == types.ErrNoData
}
