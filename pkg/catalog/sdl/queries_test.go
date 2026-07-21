package sdl

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestQueryRequestInfo_MetaQueries(t *testing.T) {
	metaNames := []string{
		MetadataSchemaQuery,
		MetadataTypeQuery,
		MetadataTypeNameQuery,
		MetadataCatalogQuery,
		MetadataModuleQuery,
		MetadataDataObjectQuery,
		MetadataFunctionQuery,
	}

	ss := ast.SelectionSet{}
	for _, name := range metaNames {
		ss = append(ss, &ast.Field{Name: name, Alias: name})
	}
	// A non-meta field without an object definition is skipped by
	// classification — it must not be reported as meta.
	ss = append(ss, &ast.Field{Name: "some_data_query", Alias: "some_data_query"})

	rr, qt := QueryRequestInfo(ss)

	if qt&QueryTypeMeta == 0 {
		t.Fatalf("QueryRequestInfo() type = %v, want QueryTypeMeta bit set", qt)
	}
	if len(rr) != len(metaNames) {
		t.Fatalf("QueryRequestInfo() returned %d requests, want %d", len(rr), len(metaNames))
	}
	for i, r := range rr {
		if r.QueryType != QueryTypeMeta {
			t.Errorf("request %q classified as %v, want QueryTypeMeta", r.Name, r.QueryType)
		}
		if r.Name != metaNames[i] {
			t.Errorf("request[%d] name = %q, want %q", i, r.Name, metaNames[i])
		}
		if r.Field == nil || r.Field.Name != metaNames[i] {
			t.Errorf("request %q lost its field reference", r.Name)
		}
	}
}
