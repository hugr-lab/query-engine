package sdl

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// queryDef is the root type the walker sets on a top-level selection. A
// meta-field is a field OF Query, and the classifier tests that as well as the
// name — nothing named like one elsewhere is routed to the metadata path.
var queryDef = &ast.Definition{Kind: ast.Object, Name: base.QueryBaseName}

// metaField builds a selected field the way the walker leaves it: enriched
// with its parent definition.
func metaField(name string) *ast.Field {
	return &ast.Field{Name: name, Alias: name, ObjectDefinition: queryDef}
}

func TestQueryRequestInfo_MetaQueries(t *testing.T) {
	// GraphQL's own meta-fields are never declared by hugr — gqlparser appends
	// them while validating the system schema — so the "__" prefix is what
	// recognises them, wherever they appear.
	introspection := []string{
		MetadataSchemaQuery,
		MetadataTypeQuery,
		MetadataTypeNameQuery,
	}
	// hugr's logical-model meta-fields are recognised by name, on Query.
	logical := []string{
		MetadataCatalogQuery,
		MetadataModuleQuery,
		MetadataDataObjectQuery,
		MetadataFunctionQuery,
		MetadataTypesQuery,
		MetadataDataSourceQuery,
		MetadataDataSourcesQuery,
		MetadataSearchQuery,
	}

	var want []string
	ss := ast.SelectionSet{}
	for _, name := range introspection {
		ss = append(ss, &ast.Field{Name: name, Alias: name})
		want = append(want, name)
	}
	for _, name := range logical {
		ss = append(ss, metaField(name))
		want = append(want, name)
	}
	// A non-meta field without an object definition is skipped by
	// classification — it must not be reported as meta.
	ss = append(ss, &ast.Field{Name: "some_data_query", Alias: "some_data_query"})
	// A field named like a meta query but hanging off something else is a
	// data field: the exemption belongs to Query, not to the name.
	ss = append(ss, &ast.Field{
		Name: MetadataCatalogQuery, Alias: "impostor",
		ObjectDefinition: &ast.Definition{Kind: ast.Object, Name: "some_module_query"},
	})

	rr, qt := QueryRequestInfo(ss)

	if qt&QueryTypeMeta == 0 {
		t.Fatalf("QueryRequestInfo() type = %v, want QueryTypeMeta bit set", qt)
	}
	if len(rr) != len(want) {
		t.Fatalf("QueryRequestInfo() returned %d requests, want %d", len(rr), len(want))
	}
	for i, r := range rr {
		if r.QueryType != QueryTypeMeta {
			t.Errorf("request %q classified as %v, want QueryTypeMeta", r.Name, r.QueryType)
		}
		if r.Name != want[i] {
			t.Errorf("request[%d] name = %q, want %q", i, r.Name, want[i])
		}
		if r.Field == nil || r.Field.Name != want[i] {
			t.Errorf("request %q lost its field reference", r.Name)
		}
	}
}
