//go:build duckdb_arrow

package dssource

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatalogSchemaModule compiles the runtime source's SDL and pins the
// curation surface to the core.catalog module — the same module the CoreDB
// source contributes its catalog views to. The names here are the stable
// GraphQL surface; the `_schema_*` fields in the core module are the same UDFs
// under their legacy names and stay until the compiled-schema provider goes.
func TestCatalogSchemaModule(t *testing.T) {
	ctx := context.Background()
	cat, err := New(nil).Catalog(ctx)
	require.NoError(t, err)
	target, err := static.New()
	require.NoError(t, err)

	out, err := compiler.New(compiler.GlobalRules()...).Compile(ctx, target, cat, cat.CompileOptions())
	require.NoError(t, err)

	root := sdl.ModuleTypeName("core.catalog", base.ModuleMutationFunction)
	var fields []string
	for def := range out.Extensions(ctx) {
		if def.Name != root {
			continue
		}
		for _, f := range def.Fields {
			fields = append(fields, f.Name)
		}
	}
	assert.Equal(t, []string{
		"annotate_module", "annotate_data_source", "annotate_data_object",
		"annotate_field", "annotate_type", "annotate_function",
		"annotate_gql_type", "annotate_gql_field", "annotate_gql_argument",
		"remove_data_source_schema", "reset_data_source_version", "reindex_embeddings",
	}, fields, "core.catalog mutation-function module")
}
