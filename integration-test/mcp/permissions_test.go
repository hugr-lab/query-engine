//go:build duckdb_arrow

package mcp_test

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/perm"
)

// The permission matrix. Every catalog tool reads through the meta-query
// family in the CALLER's context, so the engine's resolvers are the only
// filter — pkg/mcp holds no policy of its own. That is a design decision
// (design-035 D2) worth pinning: the tools must agree with each other, and
// none of them may name something the engine would refuse to describe.
//
// The role is built once and every rule shape is a different lever:
//   - a table-level rule hides a whole data object,
//   - a field rule hides one field of a visible object,
//   - a rule on a module root type hides ONE generated query of an object
//     that stays visible otherwise,
//   - the same shape on the function root type hides a callable.

const matrixRole = "mcp_matrix"

const (
	hiddenObject   = "core_api_keys"
	visibleObject  = "core_data_sources"
	hiddenField    = "path" // the DSN of core_data_sources
	hiddenQuery    = "data_sources_aggregation"
	hiddenFunction = "describe_data_source_schema"
)

var (
	matrixOnce  sync.Once
	matrixPerms *perm.RolePermissions
	matrixErr   error
)

// restricted returns a handler that runs every request under matrixRole,
// installing permissions exactly the way the HTTP middleware does in
// production. The MCP handler is mounted bare in this suite, so without this
// wrapper the context carries no permissions at all and everything is visible.
func restricted(t *testing.T) http.Handler {
	t.Helper()
	matrixOnce.Do(func() { matrixPerms, matrixErr = buildMatrixRole() })
	require.NoError(t, matrixErr)
	require.NotNil(t, matrixPerms)
	return roleHandler(t, matrixPerms)
}

// roleHandler wraps the bare MCP handler with the identity and permissions the
// endpoint middleware would have installed.
func roleHandler(t *testing.T, p *perm.RolePermissions) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := auth.ContextWithAuthInfo(r.Context(), &auth.AuthInfo{
			Role: p.Name, UserId: p.Name, UserName: p.Name,
			AuthType: "test", AuthProvider: "test",
		})
		testHandler.ServeHTTP(w, r.WithContext(perm.CtxWithPerm(ctx, p)))
	})
}

func buildMatrixRole() (*perm.RolePermissions, error) {
	ctx := auth.ContextWithFullAccess(context.Background())
	res, err := testService.Query(ctx, `mutation ($name: String!) {
		core {
			insert_roles(data: {
				name: $name
				description: "MCP permission matrix"
				permissions: [
					{type_name: "data-object:query", field_name: "`+hiddenObject+`", hidden: true}
					{type_name: "`+visibleObject+`", field_name: "`+hiddenField+`", hidden: true}
					{type_name: "_module_core_query", field_name: "`+hiddenQuery+`", hidden: true}
					{type_name: "_module_core_function", field_name: "`+hiddenFunction+`", hidden: true}
				]
			}) { name }
		}
	}`, map[string]any{"name": matrixRole})
	if err != nil {
		return nil, err
	}
	res.Close()

	p, err := perm.New(testService).RolePermissions(
		auth.ContextWithAuthInfo(context.Background(), &auth.AuthInfo{Role: matrixRole}),
	)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// TestPermissionMatrix_ListHidesObject — the map an agent starts from must not
// name a table it may not read. The admin side of the comparison is what keeps
// this from passing vacuously: the object has to be there to be missing.
func TestPermissionMatrix_ListHidesObject(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	all := catalogList(t, admin, map[string]any{"kind": "data_object", "limit": 200})
	mine := catalogList(t, role, map[string]any{"kind": "data_object", "limit": 200})

	require.Contains(t, all.names(), hiddenObject, "the object exists unrestricted")
	assert.NotContains(t, mine.names(), hiddenObject, "a hidden object is not in the map")
	assert.Contains(t, mine.names(), visibleObject, "hiding one object hides only that one")
	assert.Equal(t, all.Total-1, mine.Total, "total counts what the caller may see, after filtering")

	// The module counters are part of the same map and must agree with it.
	mods := catalogList(t, role, map[string]any{"kind": "module"})
	adminMods := catalogList(t, admin, map[string]any{"kind": "module"})
	countOf := func(p catalogPage, name string) int {
		for _, it := range p.Items {
			if it.Name == name {
				return *it.DataObjects
			}
		}
		t.Fatalf("module %q missing from %v", name, p.names())
		return 0
	}
	assert.Equal(t, countOf(adminMods, "core")-1, countOf(mods, "core"),
		"the module's data-object count drops with the object it lost")
}

// TestPermissionMatrix_DescribeRefusesHidden — describe is the rung that turns
// a name into a callable query, so it is where a leak would be worth the most.
// A hidden object is indistinguishable from a missing one, by design.
func TestPermissionMatrix_DescribeRefusesHidden(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	args := map[string]any{"kind": "data_object", "names": []string{hiddenObject, visibleObject}}

	got := catalogDescribe(t, admin, args)
	require.Len(t, got.Items, 2, "both objects describe unrestricted")

	got = catalogDescribe(t, role, args)
	require.Len(t, got.Items, 1)
	assert.Equal(t, visibleObject, got.Items[0].Name)
	assert.Equal(t, []string{hiddenObject}, got.NotFound,
		"a hidden name comes back as not found — the tool cannot tell the agent which")
}

// TestPermissionMatrix_QueryGate — hiding one generated query removes exactly
// that entry. The object stays visible and the rest of its queries stay
// callable: an agent that may read rows but not aggregate them must not be
// told to write the aggregation.
func TestPermissionMatrix_QueryGate(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	queryNames := func(h http.Handler) []string {
		got := catalogDescribe(t, h, map[string]any{"kind": "data_object", "names": []string{visibleObject}})
		require.Len(t, got.Items, 1)
		out := make([]string, 0, len(got.Items[0].Queries))
		for _, q := range got.Items[0].Queries {
			out = append(out, q.Name)
		}
		return out
	}

	full, mine := queryNames(admin), queryNames(role)
	require.Contains(t, full, hiddenQuery, "the query exists unrestricted")
	assert.NotContains(t, mine, hiddenQuery, "the hidden query is not offered")
	assert.Contains(t, mine, "data_sources", "the select query survives")
	assert.Contains(t, mine, "data_sources_by_pk", "so does the single-row one")
	assert.Len(t, mine, len(full)-1, "exactly one query was removed")
}

// TestPermissionMatrix_HiddenField — the field rules bite on both field
// surfaces, the logical one and the introspection one, because both read the
// same permission-filtered engine path.
func TestPermissionMatrix_HiddenField(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	for _, tc := range []struct {
		tool string
		args map[string]any
	}{
		{"catalog-object_fields", map[string]any{"object": visibleObject, "limit": 200}},
		{"schema-type_fields", map[string]any{"type_name": visibleObject, "limit": 200}},
	} {
		t.Run(tc.tool, func(t *testing.T) {
			full := callFields(t, admin, tc.tool, tc.args)
			mine := callFields(t, role, tc.tool, tc.args)

			require.Contains(t, full.names(), hiddenField, "the field exists unrestricted")
			assert.NotContains(t, mine.names(), hiddenField, "a hidden field is not listed")
			assert.Contains(t, mine.names(), "name", "the rest of the object is intact")
			assert.Equal(t, full.Total-1, mine.Total, "the count follows the listing")
		})
	}
}

// TestPermissionMatrix_RelationToHidden — a relation is a path to another
// object, so an edge pointing at something the caller may not read is not a
// usable path. It disappears from both ways of walking it.
func TestPermissionMatrix_RelationToHidden(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	targets := func(h http.Handler) []string {
		got := catalogDescribe(t, h, map[string]any{
			"kind": "data_object", "names": []string{"core_roles"}, "relations_limit": 200,
		})
		require.Len(t, got.Items, 1)
		out := make([]string, 0, len(got.Items[0].Relations))
		for _, r := range got.Items[0].Relations {
			out = append(out, r.DataObject)
		}
		return out
	}

	require.Contains(t, targets(admin), hiddenObject, "core_roles is referenced by the hidden object")
	assert.NotContains(t, targets(role), hiddenObject, "an edge into a hidden object is not a path")

	// The same edge as a navigation FIELD.
	fullFields := callFields(t, admin, "catalog-object_fields", map[string]any{"object": "core_roles", "limit": 200})
	myFields := callFields(t, role, "catalog-object_fields", map[string]any{"object": "core_roles", "limit": 200})
	refs := func(p fieldsPage) []string {
		out := []string{}
		for _, f := range p.Items {
			if f.RefObject != "" {
				out = append(out, f.RefObject)
			}
		}
		return out
	}
	require.Contains(t, refs(fullFields), hiddenObject)
	assert.NotContains(t, refs(myFields), hiddenObject,
		"the navigation field to a hidden object is gone from the field surface too")
}

// TestPermissionMatrix_HiddenFunction — the callable half of the map obeys the
// same rules, keyed on the module's function root type.
func TestPermissionMatrix_HiddenFunction(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	args := map[string]any{"kind": "function", "limit": 200, "module": "core"}
	full := catalogList(t, admin, args)
	mine := catalogList(t, role, args)
	require.Contains(t, full.names(), hiddenFunction, "the function exists unrestricted")
	assert.NotContains(t, mine.names(), hiddenFunction)

	got := catalogDescribe(t, role, map[string]any{
		"kind": "function", "names": []string{hiddenFunction}, "module": "core",
	})
	assert.Empty(t, got.Items)
	assert.Equal(t, []string{hiddenFunction}, got.NotFound)
}

// TestPermissionMatrix_DataFieldValuesRefusesHidden — data-field_values runs a
// real aggregation, and it resolves the object through the same meta query, so
// an object the caller may not see is simply not found. It never reaches the
// planner with a query it would refuse anyway.
func TestPermissionMatrix_DataFieldValuesRefusesHidden(t *testing.T) {
	role := restricted(t)
	mcpInit(t, role)

	resp := jsonRPC(t, role, "tools/call", map[string]any{
		"name": "data-field_values",
		"arguments": map[string]any{
			"object_name": hiddenObject, "field_name": "name",
		},
	})
	result := resp["result"].(map[string]any)
	require.Equal(t, true, result["isError"], "a hidden object must not be summarised")
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "not visible to you")

	// A hidden FIELD of a visible object is refused the same way — the field
	// list the tool checks against is the permission-filtered one.
	resp = jsonRPC(t, role, "tools/call", map[string]any{
		"name": "data-field_values",
		"arguments": map[string]any{
			"object_name": visibleObject, "field_name": hiddenField,
		},
	})
	result = resp["result"].(map[string]any)
	require.Equal(t, true, result["isError"])
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "has no field")
}

// TestPermissionMatrix_SearchAgreesWithDescribe is the invariant the whole
// design rests on: search must never surface something describe would refuse
// to show. Search is the one tool that may rank under full access, so this is
// the crossing where a leak would appear — every hit is fed back through the
// rung it points at, under the same role.
func TestPermissionMatrix_SearchAgreesWithDescribe(t *testing.T) {
	admin, role := handler(t), restricted(t)
	mcpInit(t, admin)
	mcpInit(t, role)

	// Queries chosen to pull at exactly what the role may not have: the hidden
	// object, the hidden field, the hidden function, and a broad one. The
	// 'wanted' name is what the query reaches unrestricted — without it the
	// whole test could pass on an empty result set.
	// 'stillAnswers' marks the queries whose match is not the hidden thing
	// itself: for the other two the honest answer under this role IS empty.
	for _, tc := range []struct {
		query, wanted string
		stillAnswers  bool
	}{
		{"api keys", hiddenObject, false},
		{"path", "", true},
		{"describe data source schema", hiddenFunction, false},
		{"data source", visibleObject, true},
	} {
		t.Run(tc.query, func(t *testing.T) {
			unrestricted := catalogSearch(t, admin, map[string]any{"query": tc.query, "limit": 200})
			require.NotEmpty(t, unrestricted.Items, "the query finds nothing at all — it proves nothing")
			if tc.wanted != "" {
				var reached bool
				for _, hit := range unrestricted.Items {
					if hit.Name == tc.wanted {
						reached = true
					}
				}
				require.True(t, reached, "%q must reach %q unrestricted", tc.query, tc.wanted)
			}

			page := catalogSearch(t, role, map[string]any{"query": tc.query, "limit": 200})
			if tc.stillAnswers {
				require.NotEmpty(t, page.Items, "the role still gets answers")
			}
			for _, hit := range page.Items {
				assert.NotEqual(t, hiddenFunction, hit.Name, "search surfaced a hidden function")
			}

			byKind := map[string][]string{}
			for _, hit := range page.Items {
				assert.NotEqual(t, hiddenObject, hit.Name, "search surfaced a hidden object")
				if hit.Kind == "field" {
					assert.False(t, hit.Object == visibleObject && hit.Name == hiddenField,
						"search surfaced a hidden field")
					// A field hit is only usable if the object still lists it.
					fields := callFields(t, role, "catalog-object_fields",
						map[string]any{"object": hit.Object, "limit": 200})
					assert.Contains(t, fields.names(), hit.Name,
						"field hit %s.%s is not in the object's own field list", hit.Object, hit.Name)
					continue
				}
				key := hit.Kind
				if hit.Kind == "function" {
					key += "\x00" + hit.Module
				}
				byKind[key] = append(byKind[key], hit.Name)
			}

			// Every structural hit must describe, in this same role's context.
			for key, names := range byKind {
				kind, module, _ := strings.Cut(key, "\x00")
				args := map[string]any{"kind": kind, "names": names}
				if module != "" {
					args["module"] = module
				}
				got := catalogDescribe(t, role, args)
				assert.Empty(t, got.NotFound,
					"search named %v of kind %s that describe refuses to show", got.NotFound, kind)
				assert.Len(t, got.Items, len(names))
			}
		})
	}
}

// TestPermissionMatrix_NoDataAccessStillExplores is the reason the tools read
// the meta-query family rather than the catalog views: a role denied on a
// module's DATA must still be able to explore its schema. Permissions are
// keyed on the (type, field) pair, so denying Query.core says nothing about
// Query._catalog — MCP keeps working, and only the data query is refused.
func TestPermissionMatrix_NoDataAccessStillExplores(t *testing.T) {
	h := roleHandler(t, &perm.RolePermissions{
		Name: "mcp_no_data",
		Permissions: []perm.Permission{
			{Object: "Query", Field: "core", Disabled: true},
		},
	})
	mcpInit(t, h)

	page := catalogList(t, h, map[string]any{"kind": "data_object", "limit": 200})
	assert.Contains(t, page.names(), visibleObject, "the map is still readable")

	got := catalogDescribe(t, h, map[string]any{"kind": "data_object", "names": []string{visibleObject}})
	require.Len(t, got.Items, 1, "so is the description")
	assert.NotEmpty(t, got.Items[0].Queries)

	fields := callFields(t, h, "catalog-object_fields", map[string]any{"object": visibleObject, "limit": 200})
	assert.Contains(t, fields.names(), "name", "and the field list")

	// The data itself is another matter: data-field_values runs a real query
	// under the caller's rights, and that is where the denial lands.
	resp := jsonRPC(t, h, "tools/call", map[string]any{
		"name": "data-field_values",
		"arguments": map[string]any{
			"object_name": visibleObject, "field_name": "type",
		},
	})
	result := resp["result"].(map[string]any)
	require.Equal(t, true, result["isError"], "reading rows must still be refused")
	assert.Contains(t, result["content"].([]any)[0].(map[string]any)["text"].(string), "forbidden")
}
