package perm

import (
	"context"
	"reflect"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

func authCtx() context.Context {
	return auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{
		Role:     "agent",
		UserId:   "42",
		UserName: "Agent A",
		AuthType: "jwt",
	})
}

// The substitution must pass a filter through unchanged except for `[$auth.*]`
// leaves. It used to drop arrays (the `_or` key vanished => allow-all), literal
// strings, booleans, numbers and nulls, and it mutated array elements in place.
func TestApplyContextVariable(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]any
		want map[string]any
	}{
		{
			name: "placeholder leaf substituted",
			in:   map[string]any{"agent_id": map[string]any{"eq": "[$auth.user_id]"}},
			want: map[string]any{"agent_id": map[string]any{"eq": "42"}},
		},
		{
			name: "int placeholder",
			in:   map[string]any{"owner": map[string]any{"eq": "[$auth.user_id_int]"}},
			want: map[string]any{"owner": map[string]any{"eq": 42}},
		},
		{
			name: "literal string preserved",
			in:   map[string]any{"status": map[string]any{"eq": "pending_review"}},
			want: map[string]any{"status": map[string]any{"eq": "pending_review"}},
		},
		{
			name: "bool, number and null preserved",
			in:   map[string]any{"shared": map[string]any{"eq": true}, "rank": map[string]any{"gt": 5.0}, "deleted_at": map[string]any{"is_null": nil}},
			want: map[string]any{"shared": map[string]any{"eq": true}, "rank": map[string]any{"gt": 5.0}, "deleted_at": map[string]any{"is_null": nil}},
		},
		{
			name: "scalar array preserved",
			in:   map[string]any{"id": map[string]any{"in": []any{1, 2, 3}}},
			want: map[string]any{"id": map[string]any{"in": []any{1, 2, 3}}},
		},
		{
			name: "_or array survives with substitution in elements",
			in: map[string]any{"_or": []any{
				map[string]any{"agent_id": map[string]any{"eq": "[$auth.user_id]"}},
				map[string]any{"shared": map[string]any{"eq": true}},
			}},
			want: map[string]any{"_or": []any{
				map[string]any{"agent_id": map[string]any{"eq": "42"}},
				map[string]any{"shared": map[string]any{"eq": true}},
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orig := deepCopyMap(tt.in)
			got := applyContextVariable(authCtx(), tt.in, nil)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("applyContextVariable() = %#v, want %#v", got, tt.want)
			}
			if !reflect.DeepEqual(tt.in, orig) {
				t.Errorf("input was mutated: %#v, want %#v", tt.in, orig)
			}
		})
	}
}

func TestApplyContextVariable_NoAuthContext(t *testing.T) {
	in := map[string]any{"agent_id": map[string]any{"eq": "[$auth.user_id]"}}
	got := applyContextVariable(context.Background(), in, nil)
	if !reflect.DeepEqual(got, in) {
		t.Errorf("without auth vars the filter must pass through unchanged, got %#v", got)
	}
}

func deepCopyMap(m map[string]any) map[string]any {
	res := make(map[string]any, len(m))
	for k, v := range m {
		res[k] = deepCopyValue(v)
	}
	return res
}

func deepCopyValue(v any) any {
	switch v := v.(type) {
	case map[string]any:
		return deepCopyMap(v)
	case []any:
		res := make([]any, len(v))
		for i, vv := range v {
			res[i] = deepCopyValue(vv)
		}
		return res
	default:
		return v
	}
}

// The shipped readonly role relies on ('Mutation', '*', disabled) blocking all
// mutation fields — the (object, *) case used to be unimplemented, so readonly
// could mutate.
func TestCheckObjectField_ReadonlyRole(t *testing.T) {
	r := RolePermissions{Name: "readonly", Permissions: []Permission{
		{Object: "Mutation", Field: "*", Disabled: true},
	}}
	if _, ok := r.Enabled("Mutation", "core"); ok {
		t.Error("(Mutation, *) disabled must block every Mutation field")
	}
	if _, ok := r.Enabled("Query", "core"); !ok {
		t.Error("queries must stay open by default")
	}
}

// The layered-permissions contract published in the access-control docs:
// exact (object, field) > (object, *) > (*, field) > (*, *), unmatched = allowed.
func TestCheckObjectField_Precedence(t *testing.T) {
	r := RolePermissions{Name: "limited_editor", Permissions: []Permission{
		{Object: "*", Field: "*", Disabled: false},
		{Object: "*", Field: "email", Hidden: true},
		{Object: "users", Field: "ssn", Disabled: true},
		{Object: "Mutation", Field: "*", Disabled: true},
		{Object: "Mutation", Field: "update_users", Disabled: false},
	}}

	tests := []struct {
		object, field string
		wantEnabled   bool
		wantVisible   bool
	}{
		{"users", "name", true, true},
		{"users", "email", true, false},  // (*, email) hidden but not disabled
		{"users", "ssn", false, true},    // exact disabled
		{"Mutation", "insert_users", false, true},
		{"Mutation", "update_users", true, true}, // exact allow beats (Mutation, *)
	}
	for _, tt := range tests {
		if _, ok := r.Enabled(tt.object, tt.field); ok != tt.wantEnabled {
			t.Errorf("Enabled(%s, %s) = %v, want %v", tt.object, tt.field, ok, tt.wantEnabled)
		}
		if _, ok := r.Visible(tt.object, tt.field); ok != tt.wantVisible {
			t.Errorf("Visible(%s, %s) = %v, want %v", tt.object, tt.field, ok, tt.wantVisible)
		}
	}
}

func TestCheckObjectField_ObjectWildcardBeatsFieldWildcard(t *testing.T) {
	r := RolePermissions{Permissions: []Permission{
		{Object: "*", Field: "name", Disabled: false},
		{Object: "users", Field: "*", Disabled: true},
	}}
	if _, ok := r.Enabled("users", "name"); ok {
		t.Error("(users, *) must beat (*, name)")
	}
	if _, ok := r.Enabled("orders", "name"); !ok {
		t.Error("(*, name) allow must apply to other objects")
	}
}

// mcp passes a literal "*" as the field; a seeded (type, "*") row must match it
// as an exact row.
func TestCheckObjectField_LiteralStarField(t *testing.T) {
	r := RolePermissions{Permissions: []Permission{
		{Object: "mcp:tables:query", Field: "*", Hidden: true},
	}}
	if _, ok := r.Visible("mcp:tables:query", "*"); ok {
		t.Error("literal '*' field must match the (type, '*') row")
	}
	if _, ok := r.Visible("mcp:function", "*"); !ok {
		t.Error("other types stay visible by default")
	}
}

// Filter and data lookups must follow the same precedence as enable/visible
// checks, deterministically — not first-slice-match, exact rows only.
func TestFilterArgument_Precedence(t *testing.T) {
	ctx := authCtx()
	exact := map[string]any{"owner": map[string]any{"eq": "[$auth.user_id]"}}
	broad := map[string]any{"tenant": map[string]any{"eq": "t1"}}
	r := RolePermissions{Permissions: []Permission{
		{Object: "users", Field: "*", Filter: broad},
		{Object: "users", Field: "orders", Filter: exact},
	}}

	got := r.FilterArgument(ctx, "users", "orders")
	want := map[string]any{"owner": map[string]any{"eq": "42"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("exact row must win: got %#v, want %#v", got, want)
	}

	got = r.FilterArgument(ctx, "users", "profile")
	if !reflect.DeepEqual(got, broad) {
		t.Errorf("(users, *) filter must apply to unmatched fields: got %#v", got)
	}

	if got := r.FilterArgument(ctx, "orders", "items"); got != nil {
		t.Errorf("no matching row must yield no filter, got %#v", got)
	}

	disabled := RolePermissions{Disabled: true, Permissions: r.Permissions}
	if got := disabled.FilterArgument(ctx, "users", "orders"); got != nil {
		t.Errorf("disabled role must yield no filter, got %#v", got)
	}
}

func TestDataArgument_Wildcard(t *testing.T) {
	ctx := authCtx()
	r := RolePermissions{Permissions: []Permission{
		{Object: "Mutation", Field: "*", Data: map[string]any{"created_by": "[$auth.user_id]"}},
		{Object: "Mutation", Field: "insert_articles", Data: map[string]any{"author_id": "[$auth.user_id]"}},
	}}

	got := r.DataArgument(ctx, "Mutation", "insert_articles")
	want := map[string]any{"author_id": "42"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("exact data row must win: got %#v, want %#v", got, want)
	}

	got = r.DataArgument(ctx, "Mutation", "insert_comments")
	want = map[string]any{"created_by": "42"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("(Mutation, *) data must apply to unmatched fields: got %#v, want %#v", got, want)
	}
}

func TestBestMatch_FirstRowWinsOnTie(t *testing.T) {
	r := RolePermissions{Permissions: []Permission{
		{Object: "users", Field: "orders", Filter: map[string]any{"a": map[string]any{"eq": 1}}},
		{Object: "users", Field: "orders", Filter: map[string]any{"b": map[string]any{"eq": 2}}},
	}}
	p := r.bestMatch("users", "orders")
	if p == nil || !reflect.DeepEqual(p.Filter, map[string]any{"a": map[string]any{"eq": 1}}) {
		t.Errorf("ties must keep the first row, got %#v", p)
	}
}
