package static

import (
	"slices"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/base"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/ast"
)

// base.MetaTypes is a hand-written list, and a hand-written list of "types
// reachable only from a meta-field" is exactly the kind of thing that rots:
// add a field to _DataObject that returns a new object and the permission
// exemption stops composing one hop early, silently, for wildcard-deny
// deployments only.
//
// This walks the real system schema from the meta-fields and holds the list to
// what it claims to be. It also fails the other way — a type listed but no
// longer reachable is an exemption nothing needs, and stale exemptions are how
// a list stops being auditable.
func TestMetaTypesCoverTheMetaSurface(t *testing.T) {
	schema, err := initSystemSchema()
	require.NoError(t, err)

	query := schema.Types[base.QueryBaseName]
	require.NotNil(t, query, "the system schema must define Query")

	reachable := map[string]struct{}{}
	var walk func(name string)
	walk = func(name string) {
		if _, seen := reachable[name]; seen {
			return
		}
		def := schema.Types[name]
		// Scalars, enums and GraphQL's own introspection types need no entry:
		// a scalar has no fields to gate, an enum has no subtree, and "__" is
		// recognised by prefix.
		if def == nil || def.Kind != ast.Object || strings.HasPrefix(name, "__") {
			return
		}
		reachable[name] = struct{}{}
		for _, f := range def.Fields {
			walk(f.Type.Name())
		}
	}
	for _, name := range base.MetaQueryFields {
		fd := query.Fields.ForName(name)
		require.NotNilf(t, fd, "meta-field %s is listed in base.MetaQueryFields but not declared on Query", name)
		walk(fd.Type.Name())
	}

	for name := range reachable {
		require.Containsf(t, base.MetaTypes, name,
			"%s is reachable from a meta-field but missing from base.MetaTypes — "+
				"the permission exemption stops composing at it", name)
	}
	for _, name := range base.MetaTypes {
		_, ok := reachable[name]
		require.Truef(t, ok,
			"%s is in base.MetaTypes but no meta-field reaches it — a stale exemption", name)
	}
}

// The meta surface must not reach the DATA surface. This is the hazard that
// rules out inferring the list from a naming convention: _h3_query and
// _h3_data_query are @system, start with "_", and lead straight into data
// objects. If a meta type ever gains a path to one of them, every field below
// that path silently stops being permission-checked.
func TestMetaTypesDoNotReachTheDataSurface(t *testing.T) {
	schema, err := initSystemSchema()
	require.NoError(t, err)

	forbidden := []string{"_h3_query", "_h3_data_query", base.QueryBaseName, base.MutationBaseName}
	for _, name := range base.MetaTypes {
		def := schema.Types[name]
		require.NotNilf(t, def, "base.MetaTypes names %s, which the system schema does not define", name)
		for _, f := range def.Fields {
			require.Falsef(t, slices.Contains(forbidden, f.Type.Name()),
				"meta type %s.%s reaches %s — the exemption would cover the data surface",
				name, f.Name, f.Type.Name())
		}
	}
}
