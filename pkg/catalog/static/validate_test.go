package static

import (
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestValidateSchema_ValidProvider(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	errs := provider.ValidateSchema()
	if len(errs) > 0 {
		for _, e := range errs {
			t.Errorf("validation error: %v", e)
		}
	}
}

func TestValidateSchema_UnknownFieldType(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Add a type with a field referencing an unknown type
	provider.schema.Types["TestBadType"] = &ast.Definition{
		Kind: ast.Object,
		Name: "TestBadType",
		Fields: ast.FieldList{
			{
				Name: "bad_field",
				Type: ast.NamedType("NonExistentType", nil),
			},
		},
	}

	errs := provider.ValidateSchema()
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "NonExistentType") &&
			strings.Contains(e.Error(), "TestBadType") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected validation error for unknown type reference NonExistentType")
	}

	// Cleanup
	delete(provider.schema.Types, "TestBadType")
}

func TestValidateSchema_UnknownArgumentType(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider.schema.Types["TestArgType"] = &ast.Definition{
		Kind: ast.Object,
		Name: "TestArgType",
		Fields: ast.FieldList{
			{
				Name: "some_field",
				Type: ast.NamedType("String", nil),
				Arguments: ast.ArgumentDefinitionList{
					{
						Name: "bad_arg",
						Type: ast.NamedType("MissingInputType", nil),
					},
				},
			},
		},
	}

	errs := provider.ValidateSchema()
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "MissingInputType") &&
			strings.Contains(e.Error(), "bad_arg") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected validation error for unknown argument type MissingInputType")
	}

	delete(provider.schema.Types, "TestArgType")
}

func TestValidateSchema_UnknownInterface(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider.schema.Types["TestIfaceType"] = &ast.Definition{
		Kind:       ast.Object,
		Name:       "TestIfaceType",
		Interfaces: []string{"NonExistentInterface"},
	}

	errs := provider.ValidateSchema()
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "NonExistentInterface") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected validation error for unknown interface NonExistentInterface")
	}

	delete(provider.schema.Types, "TestIfaceType")
}

func TestValidateSchema_UnknownUnionMember(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	provider.schema.Types["TestUnion"] = &ast.Definition{
		Kind:  ast.Union,
		Name:  "TestUnion",
		Types: []string{"MissingMember"},
	}

	errs := provider.ValidateSchema()
	found := false
	for _, e := range errs {
		if strings.Contains(e.Error(), "MissingMember") {
			found = true
			break
		}
	}
	if !found {
		t.Error("expected validation error for unknown union member MissingMember")
	}

	delete(provider.schema.Types, "TestUnion")
}
