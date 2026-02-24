package compare

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func pos() *ast.Position {
	return &ast.Position{Src: &ast.Source{Name: "test"}}
}

func makeSchema(types map[string]*ast.Definition) *ast.Schema {
	return &ast.Schema{Types: types}
}

func TestCompare_IdenticalSchemas(t *testing.T) {
	types := map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "id", Type: ast.NonNullNamedType("Int", pos())},
				{Name: "name", Type: ast.NamedType("String", pos())},
			},
		},
	}
	old := makeSchema(types)
	new := makeSchema(types)

	result := Compare(old, new)
	if !result.Equal() {
		t.Errorf("expected Equal() == true for identical schemas, got %d diffs", len(result.Diffs))
		for _, d := range result.Diffs {
			t.Logf("  diff: %s %s: %s", d.Kind, d.Path, d.Message)
		}
	}
}

func TestCompare_MissingType(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User"},
		"Post": {Kind: ast.Object, Name: "Post"},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User"},
	})

	result := Compare(old, new)
	if result.Equal() {
		t.Fatal("expected diffs for missing type")
	}

	found := false
	for _, d := range result.Diffs {
		if d.Path == "types.Post" && d.Kind == DiffMissing {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected DiffMissing for types.Post")
	}
}

func TestCompare_ExtraType(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User"},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User":    {Kind: ast.Object, Name: "User"},
		"Comment": {Kind: ast.Object, Name: "Comment"},
	})

	result := Compare(old, new)
	if result.Equal() {
		t.Fatal("expected diffs for extra type")
	}

	found := false
	for _, d := range result.Diffs {
		if d.Path == "types.Comment" && d.Kind == DiffExtra {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected DiffExtra for types.Comment")
	}
}

func TestCompare_ChangedField(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "name", Type: ast.NamedType("String", pos())},
			},
		},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "name", Type: ast.NonNullNamedType("String", pos())},
			},
		},
	})

	result := Compare(old, new)
	if result.Equal() {
		t.Fatal("expected diffs for changed field type")
	}

	found := false
	for _, d := range result.Diffs {
		if d.Path == "types.User.fields.name.type" && d.Kind == DiffChanged {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected DiffChanged for types.User.fields.name.type")
		for _, d := range result.Diffs {
			t.Logf("  diff: %s %s: %s", d.Kind, d.Path, d.Message)
		}
	}
}

func TestCompare_MissingField(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "id", Type: ast.NamedType("Int", pos())},
				{Name: "email", Type: ast.NamedType("String", pos())},
			},
		},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "id", Type: ast.NamedType("Int", pos())},
			},
		},
	})

	result := Compare(old, new)
	found := false
	for _, d := range result.Diffs {
		if d.Path == "types.User.fields.email" && d.Kind == DiffMissing {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected DiffMissing for types.User.fields.email")
	}
}

func TestCompare_KnownIssues(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "id", Type: ast.NamedType("Int", pos())},
				{Name: "email", Type: ast.NamedType("String", pos())},
			},
		},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Fields: ast.FieldList{
				{Name: "id", Type: ast.NamedType("Int", pos())},
			},
		},
	})

	result := Compare(old, new, KnownIssues("types.User.fields.email"))

	// Should be equal since the only diff is a known issue
	if !result.Equal() {
		t.Error("expected Equal() == true when only diff is a known issue")
		for _, d := range result.Diffs {
			t.Logf("  diff: %s %s: %s", d.Kind, d.Path, d.Message)
		}
	}
	if len(result.KnownIssues) != 1 {
		t.Errorf("expected 1 known issue, got %d", len(result.KnownIssues))
	}
}

func TestCompare_SkipSystemTypes(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User":   {Kind: ast.Object, Name: "User"},
		"__Type": {Kind: ast.Object, Name: "__Type"},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User"},
	})

	result := Compare(old, new, SkipSystemTypes())
	if !result.Equal() {
		t.Error("expected Equal() when only difference is system type")
		for _, d := range result.Diffs {
			t.Logf("  diff: %s %s: %s", d.Kind, d.Path, d.Message)
		}
	}
}

func TestCompare_AllowExtraTypes(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User"},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User":    {Kind: ast.Object, Name: "User"},
		"Comment": {Kind: ast.Object, Name: "Comment"},
	})

	result := Compare(old, new, AllowExtraTypes())
	if !result.Equal() {
		t.Error("expected Equal() with AllowExtraTypes")
		for _, d := range result.Diffs {
			t.Logf("  diff: %s %s: %s", d.Kind, d.Path, d.Message)
		}
	}
}

func TestCompare_IgnoreDescriptions(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User", Description: "old desc"},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {Kind: ast.Object, Name: "User", Description: "new desc"},
	})

	result := Compare(old, new, IgnoreDescriptions())
	if !result.Equal() {
		t.Error("expected Equal() with IgnoreDescriptions")
	}
}

func TestCompare_NilSchemas(t *testing.T) {
	result := Compare(nil, nil)
	if !result.Equal() {
		t.Error("expected Equal() for two nil schemas")
	}
}

func TestCompare_DirectiveDiff(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Directives: ast.DirectiveList{
				{Name: "table", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "users"}},
				}},
			},
		},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Directives: ast.DirectiveList{
				{Name: "table", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "user_table"}},
				}},
			},
		},
	})

	result := Compare(old, new)
	if result.Equal() {
		t.Fatal("expected diffs for changed directive arg")
	}

	found := false
	for _, d := range result.Diffs {
		if d.Kind == DiffChanged {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected DiffChanged for directive arg value")
	}
}

func TestCompare_IgnoreDirectiveArgs(t *testing.T) {
	old := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Directives: ast.DirectiveList{
				{Name: "table", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "users"}},
				}},
			},
		},
	})
	new := makeSchema(map[string]*ast.Definition{
		"User": {
			Kind: ast.Object,
			Name: "User",
			Directives: ast.DirectiveList{
				{Name: "table", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: "user_table"}},
				}},
			},
		},
	})

	result := Compare(old, new, IgnoreDirectiveArgs("table"))
	if !result.Equal() {
		t.Error("expected Equal() with IgnoreDirectiveArgs(table)")
		for _, d := range result.Diffs {
			t.Logf("  diff: %s %s: %s", d.Kind, d.Path, d.Message)
		}
	}
}
