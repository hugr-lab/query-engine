package metadata

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// hugr_type is read by two surfaces — __Field.hugr_type and
// _SearchHit.hugrType — so one field must never come back as one thing from
// introspection and another from search.
//
// The data-object cases below used to answer "" and were the reason a consumer
// guessed instead, from the GraphQL type string and the argument count. Both
// signals are wrong here: hugr generates arguments on ORDINARY columns (every
// Timestamp gets truncate, every Geometry gets transform, a @measurement
// column on a @cube gets measurement_func), and array columns are ordinary
// too.
func TestHugrFieldType_DataObjectMembers(t *testing.T) {
	ss := newCatalogTestService(t)
	provider := ss.Provider()
	ctx := t.Context()

	owner := &ast.Definition{
		Kind:       ast.Object,
		Name:       "orders",
		Directives: ast.DirectiveList{{Name: base.ObjectTableDirectiveName}},
	}
	if !sdl.IsDataObject(owner) {
		t.Fatal("test fixture is not recognised as a data object")
	}
	notAnObject := &ast.Definition{Kind: ast.Object, Name: "some_struct"}

	// Every case names a type the fixture's provider can resolve: hugrFieldType
	// bails out to "" when the field's return type is unknown to it (a @join
	// into a suspended source, say), so a made-up type name would make the
	// whole table pass for the wrong reason.

	field := func(typ string, args int, directives ...string) *ast.FieldDefinition {
		fd := &ast.FieldDefinition{Name: "f", Type: ast.NamedType(typ, nil)}
		for range args {
			fd.Arguments = append(fd.Arguments, &ast.ArgumentDefinition{Name: "a"})
		}
		for _, d := range directives {
			fd.Directives = append(fd.Directives, &ast.Directive{Name: d})
		}
		return fd
	}
	list := func(inner string, directives ...string) *ast.FieldDefinition {
		fd := &ast.FieldDefinition{Name: "f", Type: ast.ListType(ast.NamedType(inner, nil), nil)}
		for _, d := range directives {
			fd.Directives = append(fd.Directives, &ast.Directive{Name: d})
		}
		return fd
	}

	for _, tc := range []struct {
		name  string
		owner *ast.Definition
		fd    *ast.FieldDefinition
		want  base.HugrTypeField
	}{
		// --- stored values, including the ones a shape-based guess loses ---
		{"plain scalar column", owner, field("String", 0), base.HugrTypeFieldColumn},
		{"timestamp column takes truncate arguments", owner, field("Timestamp", 2), base.HugrTypeFieldColumn},
		{"geometry column takes transform arguments", owner, field("Geometry", 4), base.HugrTypeFieldColumn},
		{"measurement column on a cube gains an argument", owner,
			field("Float", 1, base.FieldMeasurementDirectiveName), base.HugrTypeFieldColumn},
		{"array column", owner, list("String"), base.HugrTypeFieldColumn},

		// --- derived ---
		{"computed sql expression", owner,
			field("String", 0, base.FieldSqlDirectiveName), base.HugrTypeFieldCalculated},
		{"extra-field companion carries @sql too, and is the more specific fact", owner,
			field("BigInt", 2, base.FieldExtraFieldDirectiveName, base.FieldSqlDirectiveName),
			base.HugrTypeFieldExtraField},
		{"scalar function call", owner,
			field("String", 0, base.FunctionCallDirectiveName), base.HugrTypeFieldFunction},
		{"table function join", owner,
			list("customers", base.FunctionCallTableJoinDirectiveName), base.HugrTypeFieldFunction},

		// --- paths ---
		{"declared cross-source join", owner,
			list("customers", base.JoinDirectiveName), base.HugrTypeFieldSelect},
		{"relation navigation field", owner,
			field("customers", 1, base.ReferencesQueryDirectiveName), base.HugrTypeFieldSelect},

		// --- the owner is what makes "column" claimable ---
		{"same field on a plain struct is not a column", notAnObject, field("String", 0), ""},
		{"no owner at all", nil, field("String", 0), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := hugrFieldType(ctx, provider, tc.owner, tc.fd)
			if err != nil {
				t.Fatalf("hugrFieldType: %v", err)
			}
			if got != tc.want && got != string(tc.want) {
				t.Errorf("hugrFieldType() = %v, want %q", got, tc.want)
			}
		})
	}
}
