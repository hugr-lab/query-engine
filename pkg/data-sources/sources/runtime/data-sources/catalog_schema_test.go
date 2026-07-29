//go:build duckdb_arrow

package dssource

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/ingest"
	"github.com/hugr-lab/query-engine/pkg/catalog/static"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCatalogSchemaModule compiles the runtime source's SDL and pins the
// curation surface to the core.catalog module — the same module the CoreDB
// source contributes its catalog views to. The names here are the stable
// GraphQL surface; the `_schema_*` fields in the core module are the same UDFs
// under their legacy names.
//
// The module ROOT type (_module_core_catalog_mutation_function) is no longer a
// compilation artifact — design-036 deleted the ASSEMBLE rules and the catalog
// storage synthesizes those roots on READ. So the assignment is read where it
// is actually declared: @module on each field of `extend type MutationFunction`,
// which is what the storage records and reconstructs the root from.
func TestCatalogSchemaModule(t *testing.T) {
	ctx := context.Background()
	cat, err := New(nil).Catalog(ctx)
	require.NoError(t, err)
	target, err := static.New()
	require.NoError(t, err)

	_, err = ingest.New(ingest.Default()...).Compile(ctx, target, cat, cat.CompileOptions())
	require.NoError(t, err)

	es, ok := cat.(base.ExtensionsSource)
	require.True(t, ok, "the runtime source carries extensions")

	var fields []string
	for def := range es.Extensions(ctx) {
		if def.Name != base.FunctionMutationTypeName {
			continue
		}
		for _, f := range def.Fields {
			if base.DirectiveArgString(f.Directives.ForName(base.ModuleDirectiveName), base.ArgName) == "core.catalog" {
				fields = append(fields, f.Name)
			}
		}
	}
	assert.Equal(t, []string{
		"annotate_module", "annotate_data_source", "annotate_data_object",
		"annotate_field", "annotate_type", "annotate_function",
		"annotate_gql_type", "annotate_gql_field", "annotate_gql_argument",
		"remove_data_source_schema", "reset_data_source_version", "reindex_embeddings",
	}, fields, "core.catalog mutation-function module")
}
