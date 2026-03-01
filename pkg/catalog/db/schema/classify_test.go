package schema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vektah/gqlparser/v2/ast"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
)

// --- ClassifyType tests ---

func TestClassifyType_ModuleRoot(t *testing.T) {
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "myapp_query",
		Directives: ast.DirectiveList{
			{Name: base.ModuleRootDirectiveName, Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "myapp", Kind: ast.StringValue}},
				{Name: "type", Value: &ast.Value{Raw: "QUERY", Kind: ast.EnumValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeModule, ClassifyType(def))
}

func TestClassifyType_WellKnownModuleRoot(t *testing.T) {
	for _, name := range []string{"Query", "Mutation", "Function", "MutationFunction"} {
		t.Run(name, func(t *testing.T) {
			def := &ast.Definition{Kind: ast.Object, Name: name}
			assert.Equal(t, base.HugrTypeModule, ClassifyType(def))
		})
	}
}

func TestClassifyType_Table(t *testing.T) {
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "users",
		Directives: ast.DirectiveList{{Name: base.ObjectTableDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeTable, ClassifyType(def))
}

func TestClassifyType_View(t *testing.T) {
	def := &ast.Definition{
		Kind:       ast.Object,
		Name:       "user_stats",
		Directives: ast.DirectiveList{{Name: base.ObjectViewDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeView, ClassifyType(def))
}

func TestClassifyType_Filter(t *testing.T) {
	def := &ast.Definition{
		Kind:       ast.InputObject,
		Name:       "_users_filter",
		Directives: ast.DirectiveList{{Name: base.FilterInputDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFilter, ClassifyType(def))
}

func TestClassifyType_FilterList(t *testing.T) {
	def := &ast.Definition{
		Kind:       ast.InputObject,
		Name:       "_users_list_filter",
		Directives: ast.DirectiveList{{Name: base.FilterListInputDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFilterList, ClassifyType(def))
}

func TestClassifyType_DataInput(t *testing.T) {
	def := &ast.Definition{
		Kind:       ast.InputObject,
		Name:       "_users_input",
		Directives: ast.DirectiveList{{Name: base.DataInputDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeDataInput, ClassifyType(def))
}

func TestClassifyType_JoinQueries(t *testing.T) {
	def := &ast.Definition{Kind: ast.Object, Name: base.QueryTimeJoinsTypeName}
	assert.Equal(t, base.HugrTypeJoin, ClassifyType(def))
}

func TestClassifyType_SpatialQueries(t *testing.T) {
	def := &ast.Definition{Kind: ast.Object, Name: base.QueryTimeSpatialTypeName}
	assert.Equal(t, base.HugrTypeSpatial, ClassifyType(def))
}

func TestClassifyType_H3Aggregate(t *testing.T) {
	def := &ast.Definition{Kind: ast.Object, Name: base.H3QueryTypeName}
	assert.Equal(t, base.HugrTypeH3Agg, ClassifyType(def))
}

func TestClassifyType_H3Data(t *testing.T) {
	def := &ast.Definition{Kind: ast.Object, Name: base.H3DataQueryTypeName}
	assert.Equal(t, base.HugrTypeH3Data, ClassifyType(def))
}

func TestClassifyType_Unclassified(t *testing.T) {
	// Regular object without known directives
	def := &ast.Definition{Kind: ast.Object, Name: "SomeRandomType"}
	assert.Equal(t, base.HugrType(""), ClassifyType(def))
}

func TestClassifyType_Nil(t *testing.T) {
	assert.Equal(t, base.HugrType(""), ClassifyType(nil))
}

// --- ClassifyField tests ---

func TestClassifyField_Submodule(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "myapp",
		Type: &ast.Type{NamedType: "myapp_query"},
	}
	lookup := func(name string) *ast.Definition {
		if name == "myapp_query" {
			return &ast.Definition{
				Kind: ast.Object,
				Name: "myapp_query",
				Directives: ast.DirectiveList{
					{Name: base.ModuleRootDirectiveName, Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "myapp", Kind: ast.StringValue}},
						{Name: "type", Value: &ast.Value{Raw: "QUERY", Kind: ast.EnumValue}},
					}},
				},
			}
		}
		return nil
	}
	assert.Equal(t, base.HugrTypeFieldSubmodule, ClassifyField(field, nil, lookup))
}

func TestClassifyField_Aggregate(t *testing.T) {
	field := &ast.FieldDefinition{
		Name:       "users_aggregate",
		Directives: ast.DirectiveList{{Name: base.FieldAggregationQueryDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFieldAgg, ClassifyField(field, nil, nil))
}

func TestClassifyField_BucketAggregate(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "users_bucket_agg",
		Directives: ast.DirectiveList{
			{Name: base.FieldAggregationQueryDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgIsBucket, Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeFieldBucketAgg, ClassifyField(field, nil, nil))
}

func TestClassifyField_SelectOne(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "users_by_pk",
		Directives: ast.DirectiveList{
			{Name: base.QueryDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgType, Value: &ast.Value{Raw: base.QueryTypeTextSelectOne, Kind: ast.EnumValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeFieldSelectOne, ClassifyField(field, nil, nil))
}

func TestClassifyField_Select(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "users",
		Directives: ast.DirectiveList{
			{Name: base.QueryDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgType, Value: &ast.Value{Raw: base.QueryTypeTextSelect, Kind: ast.EnumValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeFieldSelect, ClassifyField(field, nil, nil))
}

func TestClassifyField_FunctionCall(t *testing.T) {
	field := &ast.FieldDefinition{
		Name:       "my_func",
		Directives: ast.DirectiveList{{Name: base.FunctionCallDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFieldFunction, ClassifyField(field, nil, nil))
}

func TestClassifyField_TableFunctionCallJoin(t *testing.T) {
	field := &ast.FieldDefinition{
		Name:       "nearby_places",
		Directives: ast.DirectiveList{{Name: base.FunctionCallTableJoinDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFieldFunction, ClassifyField(field, nil, nil))
}

func TestClassifyField_Function(t *testing.T) {
	field := &ast.FieldDefinition{
		Name:       "compute",
		Directives: ast.DirectiveList{{Name: base.FunctionDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFieldFunction, ClassifyField(field, nil, nil))
}

func TestClassifyField_Join(t *testing.T) {
	field := &ast.FieldDefinition{
		Name:       "departments",
		Directives: ast.DirectiveList{{Name: base.JoinDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFieldSelect, ClassifyField(field, nil, nil))
}

func TestClassifyField_ReferencesQuery(t *testing.T) {
	field := &ast.FieldDefinition{
		Name:       "users_in_dept",
		Directives: ast.DirectiveList{{Name: base.FieldReferencesQueryDirectiveName}},
	}
	assert.Equal(t, base.HugrTypeFieldSelect, ClassifyField(field, nil, nil))
}

func TestClassifyField_MutationInsert(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "insert_users",
		Directives: ast.DirectiveList{
			{Name: base.MutationDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgType, Value: &ast.Value{Raw: base.MutationTypeTextInsert, Kind: ast.EnumValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeFieldMutationInsert, ClassifyField(field, nil, nil))
}

func TestClassifyField_MutationUpdate(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "update_users",
		Directives: ast.DirectiveList{
			{Name: base.MutationDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgType, Value: &ast.Value{Raw: base.MutationTypeTextUpdate, Kind: ast.EnumValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeFieldMutationUpdate, ClassifyField(field, nil, nil))
}

func TestClassifyField_MutationDelete(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "delete_users",
		Directives: ast.DirectiveList{
			{Name: base.MutationDirectiveName, Arguments: ast.ArgumentList{
				{Name: base.ArgType, Value: &ast.Value{Raw: base.MutationTypeTextDelete, Kind: ast.EnumValue}},
			}},
		},
	}
	assert.Equal(t, base.HugrTypeFieldMutationDelete, ClassifyField(field, nil, nil))
}

func TestClassifyField_JoinFieldName(t *testing.T) {
	// In the real schema, the _join field on a data object returns the _join type,
	// which is NOT a module root. The classification checks td.Name == "Query"
	// where td is the field's return type definition. In practice, this pattern
	// fires when the field's return type resolves to a definition named "Query"
	// that is NOT a module root (the submodule check would catch it first otherwise).
	//
	// Since "Query" is always a module root (well-known name), this field-name
	// pattern can only fire if the lookup returns a definition that won't trigger
	// ModuleRootInfo. We test with a non-module-root definition to verify the
	// classification logic works correctly.
	field := &ast.FieldDefinition{
		Name: base.QueryTimeJoinsFieldName,
		Type: &ast.Type{NamedType: "SomeQueryType"},
	}
	lookup := func(name string) *ast.Definition {
		if name == "SomeQueryType" {
			// Return a definition with Name == QueryBaseName but no @module_root.
			// This won't trigger ModuleRootInfo because well-known names (Query)
			// DO trigger it. So we use a custom name that happens to match.
			return &ast.Definition{Kind: ast.Object, Name: base.QueryBaseName}
		}
		return nil
	}
	// "Query" as Name triggers ModuleRootInfo (well-known root), so submodule fires first.
	// This is consistent with the original code. Let's verify the _join field on a
	// table where the return type is _join (not Query):
	field.Type = &ast.Type{NamedType: base.QueryTimeJoinsTypeName}
	lookup = func(name string) *ast.Definition {
		if name == base.QueryTimeJoinsTypeName {
			return &ast.Definition{Kind: ast.Object, Name: base.QueryTimeJoinsTypeName}
		}
		return nil
	}
	// _join type is not a module root, and td.Name != "Query", so neither
	// submodule nor join classification fires → unclassified.
	assert.Equal(t, base.HugrTypeField(""), ClassifyField(field, nil, lookup))
}

func TestClassifyField_SpatialFieldName(t *testing.T) {
	// _spatial field returning _spatial type — not a module root, td.Name != "Query"
	field := &ast.FieldDefinition{
		Name: base.QueryTimeSpatialFieldName,
		Type: &ast.Type{NamedType: base.QueryTimeSpatialTypeName},
	}
	lookup := func(name string) *ast.Definition {
		if name == base.QueryTimeSpatialTypeName {
			return &ast.Definition{Kind: ast.Object, Name: base.QueryTimeSpatialTypeName}
		}
		return nil
	}
	assert.Equal(t, base.HugrTypeField(""), ClassifyField(field, nil, lookup))
}

func TestClassifyField_JQFieldName(t *testing.T) {
	// jq field returning JSON type — not a module root, td.Name != "Query"
	field := &ast.FieldDefinition{
		Name: base.JQTransformQueryName,
		Type: &ast.Type{NamedType: "JSON"},
	}
	lookup := func(name string) *ast.Definition {
		if name == "JSON" {
			return &ast.Definition{Kind: ast.Scalar, Name: "JSON"}
		}
		return nil
	}
	assert.Equal(t, base.HugrTypeField(""), ClassifyField(field, nil, lookup))
}

func TestClassifyField_H3AggFieldName(t *testing.T) {
	// h3 field returning a type defined as _h3_query
	field := &ast.FieldDefinition{
		Name: base.H3QueryFieldName,
		Type: &ast.Type{NamedType: base.H3QueryTypeName},
	}
	lookup := func(name string) *ast.Definition {
		if name == base.H3QueryTypeName {
			return &ast.Definition{Kind: ast.Object, Name: base.H3QueryTypeName}
		}
		return nil
	}
	assert.Equal(t, base.HugrTypeFieldH3Agg, ClassifyField(field, nil, lookup))
}

func TestClassifyField_SubmodulePrecedence(t *testing.T) {
	// If a field returns a module root type, submodule takes precedence over
	// field name patterns — even if the field is named "_join".
	field := &ast.FieldDefinition{
		Name: base.QueryTimeJoinsFieldName,
		Type: &ast.Type{NamedType: "Query"},
	}
	lookup := func(name string) *ast.Definition {
		if name == "Query" {
			// Query is a well-known module root
			return &ast.Definition{Kind: ast.Object, Name: base.QueryBaseName}
		}
		return nil
	}
	assert.Equal(t, base.HugrTypeFieldSubmodule, ClassifyField(field, nil, lookup))
}

func TestClassifyField_Unclassified(t *testing.T) {
	field := &ast.FieldDefinition{
		Name: "some_field",
		Type: &ast.Type{NamedType: "String"},
	}
	assert.Equal(t, base.HugrTypeField(""), ClassifyField(field, nil, nil))
}

func TestClassifyField_Nil(t *testing.T) {
	assert.Equal(t, base.HugrTypeField(""), ClassifyField(nil, nil, nil))
}
