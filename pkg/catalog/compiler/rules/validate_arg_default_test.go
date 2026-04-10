package rules

import (
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

func argDefaultPos() *ast.Position {
	return &ast.Position{Src: &ast.Source{Name: "test"}}
}

func argDefaultDirective(value string) *ast.Directive {
	return &ast.Directive{
		Name:     base.ArgDefaultDirectiveName,
		Position: argDefaultPos(),
		Arguments: ast.ArgumentList{
			{
				Name: base.ArgValue,
				Value: &ast.Value{
					Raw:      value,
					Kind:     ast.StringValue,
					Position: argDefaultPos(),
				},
				Position: argDefaultPos(),
			},
		},
	}
}

func TestValidateArgDefaults_OnFunctionField(t *testing.T) {
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "Function",
		Fields: ast.FieldList{
			{
				Name:     "list_my_orders",
				Position: argDefaultPos(),
				Directives: ast.DirectiveList{
					{Name: base.FunctionDirectiveName, Position: argDefaultPos()},
				},
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:     "user_id",
						Type:     ast.NamedType("String", argDefaultPos()),
						Position: argDefaultPos(),
						Directives: ast.DirectiveList{
							argDefaultDirective("[$auth.user_id]"),
						},
					},
				},
			},
		},
	}
	if err := validateArgDefaults(def); err != nil {
		t.Errorf("expected no error, got: %v", err)
	}
}

func TestValidateArgDefaults_UnknownPlaceholder(t *testing.T) {
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "Function",
		Fields: ast.FieldList{
			{
				Name:     "bad",
				Position: argDefaultPos(),
				Directives: ast.DirectiveList{
					{Name: base.FunctionDirectiveName, Position: argDefaultPos()},
				},
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:     "x",
						Type:     ast.NamedType("String", argDefaultPos()),
						Position: argDefaultPos(),
						Directives: ast.DirectiveList{
							argDefaultDirective("[$typo.user_id]"),
						},
					},
				},
			},
		},
	}
	err := validateArgDefaults(def)
	if err == nil {
		t.Fatal("expected error for unknown placeholder")
	}
	if !strings.Contains(err.Error(), "not a known context variable") {
		t.Errorf("error should mention unknown placeholder, got: %v", err)
	}
}

func TestValidateArgDefaults_DefaultValueCollision(t *testing.T) {
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "Function",
		Fields: ast.FieldList{
			{
				Name:     "bad",
				Position: argDefaultPos(),
				Directives: ast.DirectiveList{
					{Name: base.FunctionDirectiveName, Position: argDefaultPos()},
				},
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:         "x",
						Type:         ast.NamedType("String", argDefaultPos()),
						Position:     argDefaultPos(),
						DefaultValue: &ast.Value{Raw: "static", Kind: ast.StringValue, Position: argDefaultPos()},
						Directives: ast.DirectiveList{
							argDefaultDirective("[$auth.user_id]"),
						},
					},
				},
			},
		},
	}
	err := validateArgDefaults(def)
	if err == nil {
		t.Fatal("expected error for default+arg_default collision")
	}
	if !strings.Contains(err.Error(), "default value") {
		t.Errorf("error should mention default value collision, got: %v", err)
	}
}

func TestValidateArgDefaults_OnNonFunctionField(t *testing.T) {
	// @arg_default on a regular table field's argument — should be rejected.
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "users",
		Directives: ast.DirectiveList{
			{Name: base.ObjectTableDirectiveName, Position: argDefaultPos()},
		},
		Fields: ast.FieldList{
			{
				Name:     "name",
				Position: argDefaultPos(),
				Type:     ast.NamedType("String", argDefaultPos()),
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:     "filter",
						Type:     ast.NamedType("String", argDefaultPos()),
						Position: argDefaultPos(),
						Directives: ast.DirectiveList{
							argDefaultDirective("[$auth.user_id]"),
						},
					},
				},
			},
		},
	}
	err := validateArgDefaults(def)
	if err == nil {
		t.Fatal("expected error for @arg_default on non-function field")
	}
	if !strings.Contains(err.Error(), "only valid on arguments of fields with @function") {
		t.Errorf("error should mention function-only restriction, got: %v", err)
	}
}

func TestValidateArgDefaults_OnFunctionCallField(t *testing.T) {
	// @function_call field IS allowed.
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "users",
		Fields: ast.FieldList{
			{
				Name:     "orders",
				Position: argDefaultPos(),
				Type:     ast.NamedType("Order", argDefaultPos()),
				Directives: ast.DirectiveList{
					{Name: base.FunctionCallDirectiveName, Position: argDefaultPos()},
				},
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:     "user_id",
						Type:     ast.NamedType("String", argDefaultPos()),
						Position: argDefaultPos(),
						Directives: ast.DirectiveList{
							argDefaultDirective("[$auth.user_id]"),
						},
					},
				},
			},
		},
	}
	if err := validateArgDefaults(def); err != nil {
		t.Errorf("expected no error for @arg_default on @function_call, got: %v", err)
	}
}

func TestValidateArgDefaults_OnInputObjectField(t *testing.T) {
	// @arg_default on input field of an args input type — allowed.
	def := &ast.Definition{
		Kind: ast.InputObject,
		Name: "my_view_args",
		Fields: ast.FieldList{
			{
				Name:     "user_id",
				Type:     ast.NamedType("String", argDefaultPos()),
				Position: argDefaultPos(),
				Directives: ast.DirectiveList{
					argDefaultDirective("[$auth.user_id]"),
				},
			},
		},
	}
	if err := validateArgDefaults(def); err != nil {
		t.Errorf("expected no error for @arg_default on input field, got: %v", err)
	}
}

func TestValidateArgDefaults_CatalogPlaceholderRejected(t *testing.T) {
	// [$catalog] is intentionally NOT in the @arg_default whitelist.
	def := &ast.Definition{
		Kind: ast.Object,
		Name: "Function",
		Fields: ast.FieldList{
			{
				Name:     "bad",
				Position: argDefaultPos(),
				Directives: ast.DirectiveList{
					{Name: base.FunctionDirectiveName, Position: argDefaultPos()},
				},
				Arguments: ast.ArgumentDefinitionList{
					{
						Name:     "cat",
						Type:     ast.NamedType("String", argDefaultPos()),
						Position: argDefaultPos(),
						Directives: ast.DirectiveList{
							argDefaultDirective("[$catalog]"),
						},
					},
				},
			},
		},
	}
	err := validateArgDefaults(def)
	if err == nil {
		t.Fatal("expected error: [$catalog] not allowed in @arg_default")
	}
	if !strings.Contains(err.Error(), "not a known context variable") {
		t.Errorf("error should mention unknown placeholder, got: %v", err)
	}
}
