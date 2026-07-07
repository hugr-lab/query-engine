package rules

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

func viewDef(name, sql string) *ast.Definition {
	pos := argDefaultPos()
	return &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Position: pos,
		Directives: ast.DirectiveList{
			{
				Name:     base.ObjectViewDirectiveName,
				Position: pos,
				Arguments: ast.ArgumentList{
					{Name: base.ArgName, Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
					{Name: base.ArgSQL, Value: &ast.Value{Raw: sql, Kind: ast.StringValue, Position: pos}, Position: pos},
				},
			},
		},
	}
}

func TestValidateArglessViewSQL(t *testing.T) {
	valid := []struct {
		name, sql string
	}{
		{"whitelisted auth placeholder", "SELECT id FROM t WHERE user_id = [$auth.user_id]"},
		{"user_id_int", "SELECT id FROM t WHERE category_id = [$auth.user_id_int]"},
		{"catalog system variable", "SELECT id FROM [$catalog].t"},
		{"plain column reference", "SELECT [id], [name] FROM t"},
		{"no placeholders", "SELECT id FROM t WHERE is_active = true"},
	}
	for _, tt := range valid {
		t.Run("ok/"+tt.name, func(t *testing.T) {
			if err := validateArglessViewSQL(viewDef("v", tt.sql)); err != nil {
				t.Errorf("expected valid, got: %v", err)
			}
		})
	}

	invalid := []struct {
		name, sql string
	}{
		{"custom claim not whitelisted", "SELECT id FROM t WHERE tenant = [$auth.tenant_id]"},
		{"typo in placeholder", "SELECT id FROM t WHERE user_id = [$auth.user_ids]"},
		{"unknown context var", "SELECT id FROM t WHERE x = [$some_arg]"},
	}
	for _, tt := range invalid {
		t.Run("reject/"+tt.name, func(t *testing.T) {
			if err := validateArglessViewSQL(viewDef("v", tt.sql)); err == nil {
				t.Errorf("expected a compile error for %q, got nil", tt.sql)
			}
		})
	}
}
