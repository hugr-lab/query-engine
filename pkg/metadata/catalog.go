package metadata

import (
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

// Logical-model introspection (_catalog / _module / _dataObject /
// _function). Resolution consumes the narrow catalog.LogicalModel seam for
// structure and reuses the standard __Type/__Field/__InputValue resolvers for
// leaves. Permission semantics mirror __schema exactly: hidden elements are
// absent, disabled elements stay visible, full-access contexts see everything.

func processCatalogQuery(ctx context.Context, provider catalog.Provider, field *ast.Field, maxDepth int) (any, error) {
	lm := catalog.LogicalModelFromProvider(provider)
	info := lm.Module(ctx, "")
	if info == nil {
		return nil, nil
	}
	return moduleResolver(ctx, lm, provider, info, field.SelectionSet, maxDepth)
}

func processModuleQuery(ctx context.Context, provider catalog.Provider, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
	name, ok := metaStringArg(field, vars, "name")
	if !ok {
		return nil, ErrInvalidTypeQuery
	}
	lm := catalog.LogicalModelFromProvider(provider)
	info := lm.Module(ctx, name)
	if info == nil {
		return nil, nil
	}
	return moduleResolver(ctx, lm, provider, info, field.SelectionSet, maxDepth)
}

func processDataObjectQuery(ctx context.Context, provider catalog.Provider, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
	name, ok := metaStringArg(field, vars, "name")
	if !ok {
		return nil, ErrInvalidTypeQuery
	}
	if name == "" {
		return nil, nil
	}
	lm := catalog.LogicalModelFromProvider(provider)
	obj := lm.DataObject(ctx, name)
	if obj == nil || !dataObjectVisible(ctx, obj) {
		return nil, nil
	}
	return dataObjectResolver(ctx, lm, provider, obj, field.SelectionSet, maxDepth)
}

func processFunctionQuery(ctx context.Context, provider catalog.Provider, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
	module, ok := metaStringArg(field, vars, "module")
	if !ok {
		return nil, ErrInvalidTypeQuery
	}
	name, ok := metaStringArg(field, vars, "name")
	if !ok {
		return nil, ErrInvalidTypeQuery
	}
	if name == "" {
		return nil, nil
	}
	lm := catalog.LogicalModelFromProvider(provider)
	entry := lm.Function(ctx, module, name)
	if entry == nil || !functionVisible(ctx, entry) {
		return nil, nil
	}
	return functionResolver(ctx, lm, provider, entry, field.SelectionSet, maxDepth)
}

// processTypesQuery resolves _types — the GraphQL type definitions of the
// logical model. scope: SOURCE (default) = residual base types defined by
// data sources (the future entity-storage types table); SYSTEM = the
// engine-defined types. Type-list semantics mirror __schema.types (no
// permission filtering of the list itself; per-type fields are filtered).
func processTypesQuery(ctx context.Context, provider catalog.Provider, field *ast.Field, maxDepth int, vars map[string]any) (any, error) {
	scope := "SOURCE"
	if field.Arguments.ForName("scope") != nil {
		if args := field.ArgumentMap(vars); args != nil {
			if s, ok := args["scope"].(string); ok && s != "" {
				scope = s
			}
		}
	}
	lm := catalog.LogicalModelFromProvider(provider)
	typesIter := lm.SourceTypes(ctx)
	if scope == "SYSTEM" {
		typesIter = lm.SystemTypes(ctx)
	}
	var res []map[string]any
	for _, def := range typesIter {
		data, err := typeResolver(ctx, provider, ast.NamedType(def.Name, &ast.Position{}), field.SelectionSet, maxDepth)
		if err != nil {
			return nil, err
		}
		res = append(res, data)
	}
	return res, nil
}

// metaStringArg extracts a required string argument of a meta query. An empty
// string is a VALID value (the root module) — unlike __type(name:), absence or
// a non-string value is the only failure.
func metaStringArg(field *ast.Field, vars map[string]any, name string) (string, bool) {
	if field.Arguments.ForName(name) == nil {
		return "", false
	}
	args := field.ArgumentMap(vars)
	if args == nil {
		return "", false
	}
	v, ok := args[name]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func moduleResolver(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider, info *catalog.ModuleInfo, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	if maxDepth <= 0 {
		return nil, nil
	}
	moduleRootType := func(kind sdl.ModuleObjectType) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			// RootTypes carries NAMES; the definition is resolved only here —
			// when the root-type field is actually selected.
			rt := info.RootTypes[kind]
			if rt == "" {
				return nil, nil
			}
			data, err := typeResolver(ctx, provider, ast.NamedType(rt, &ast.Position{}), field.SelectionSet, maxDepth)
			if errors.Is(err, ErrTypeNotFound) {
				return nil, nil
			}
			return data, err
		}
	}
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return info.Name, nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return info.Description, nil
		},
		"longDescription": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return nil, nil
		},
		"dataSources": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if info.DataSources == nil {
				return []string{}, nil
			}
			return info.DataSources, nil
		},
		"modules": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for child := range lm.Modules(ctx, info.Name) {
				if !submoduleVisible(ctx, lm, info, child) {
					continue
				}
				if !moduleHasVisibleContent(ctx, lm, provider, child) {
					continue
				}
				data, err := moduleResolver(ctx, lm, provider, child, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				if data != nil {
					res = append(res, data)
				}
			}
			return res, nil
		},
		"dataObjects": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for obj := range lm.DataObjects(ctx, info.Name) {
				if !dataObjectVisible(ctx, obj) {
					continue
				}
				data, err := dataObjectResolver(ctx, lm, provider, obj, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				if data != nil {
					res = append(res, data)
				}
			}
			return res, nil
		},
		"functions": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for entry := range lm.Functions(ctx, info.Name) {
				if !functionVisible(ctx, entry) {
					continue
				}
				data, err := functionResolver(ctx, lm, provider, entry, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				if data != nil {
					res = append(res, data)
				}
			}
			return res, nil
		},
		"queryType":            moduleRootType(sdl.ModuleQuery),
		"mutationType":         moduleRootType(sdl.ModuleMutation),
		"subscriptionType":     moduleRootType(sdl.ModuleSubscription),
		"functionType":         moduleRootType(sdl.ModuleFunction),
		"mutationFunctionType": moduleRootType(sdl.ModuleMutationFunction),
	}, "_Module")
}

func dataObjectResolver(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider, obj *sdl.Object, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	if maxDepth <= 0 {
		return nil, nil
	}
	def := obj.Definition()
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Name, nil
		},
		"type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return strings.ToUpper(obj.Type), nil
		},
		"properties": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return propertiesResolver(ctx, obj, field.SelectionSet)
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return def.Description, nil
		},
		"longDescription": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return nil, nil
		},
		"moduleName": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return sdl.ObjectModule(def), nil
		},
		"module": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			info := lm.Module(ctx, sdl.ObjectModule(def))
			if info == nil {
				return nil, nil
			}
			return moduleResolver(ctx, lm, provider, info, field.SelectionSet, maxDepth-1)
		},
		"primaryKey": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			pk := sdl.ObjectPrimaryKeys(def)
			if pk == nil {
				return []string{}, nil
			}
			return pk, nil
		},
		"args": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if obj.InputArgsName == "" {
				return nil, nil
			}
			it := provider.ForName(ctx, obj.InputArgsName)
			if it == nil {
				return nil, nil
			}
			res := []map[string]any{}
			for _, f := range it.Fields {
				if isArgServerInjected(f.Directives) {
					continue
				}
				data, err := inputValueResolver(ctx, provider, f, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"fields": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return objectFieldsResolver(ctx, provider, def, field, maxDepth)
		},
		"relations": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for rel := range lm.Relations(ctx, def.Name) {
				if !relationVisible(ctx, lm, rel) {
					continue
				}
				data, err := relationResolver(ctx, lm, provider, rel, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				if data != nil {
					res = append(res, data)
				}
			}
			return res, nil
		},
		"dataSourceName": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return obj.Catalog, nil
		},
		"dataSources": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return dataObjectSources(obj), nil
		},
	}, "_DataObject")
}

func propertiesResolver(ctx context.Context, obj *sdl.Object, ss ast.SelectionSet) (map[string]any, error) {
	boolField := func(v bool) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return v, nil
		}
	}
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename":   typeNameResolver,
		"isCube":       boolField(obj.IsCube),
		"isM2M":        boolField(obj.IsM2M),
		"isHypertable": boolField(obj.IsHypertable),
		"softDelete":   boolField(obj.SoftDelete),
		"hasVectors":   boolField(obj.HasVectors),
	}, "_DataObjectProperties")
}

func relationResolver(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider, rel *catalog.RelationInfo, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	if maxDepth <= 0 {
		return nil, nil
	}
	farObject := func(name string) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if name == "" {
				return nil, nil
			}
			obj := lm.DataObject(ctx, name)
			if obj == nil {
				return nil, nil
			}
			return dataObjectResolver(ctx, lm, provider, obj, field.SelectionSet, maxDepth-1)
		}
	}
	stringOrNil := func(v string) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if v == "" {
				return nil, nil
			}
			return v, nil
		}
	}
	keys := func(v []string) fieldResolverFunc {
		return func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if v == nil {
				return []string{}, nil
			}
			return v, nil
		}
	}
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return rel.Name, nil
		},
		"direction": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return string(rel.Direction), nil
		},
		"kind": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return string(rel.Kind), nil
		},
		"fieldName":       stringOrNil(rel.FieldName),
		"description":     stringOrNil(rel.Description),
		"dataObject":      farObject(rel.DataObject),
		"through":         farObject(rel.Through),
		"sourceKeys":      keys(rel.SourceKeys),
		"destinationKeys": keys(rel.DestinationKeys),
		"dataSource":      stringOrNil(rel.DataSource),
	}, "_Relation")
}

func functionResolver(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider, entry *catalog.FunctionEntry, ss ast.SelectionSet, maxDepth int) (map[string]any, error) {
	if maxDepth <= 0 {
		return nil, nil
	}
	return processSelectionSet(ctx, ss, map[string]fieldResolverFunc{
		"__typename": typeNameResolver,
		"name": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return entry.Field.Name, nil
		},
		"type": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return functionTypeText(entry.Kind), nil
		},
		"description": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return entry.Field.Description, nil
		},
		"longDescription": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return nil, nil
		},
		"moduleName": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return entry.Module, nil
		},
		"module": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			info := lm.Module(ctx, entry.Module)
			if info == nil {
				return nil, nil
			}
			return moduleResolver(ctx, lm, provider, info, field.SelectionSet, maxDepth-1)
		},
		"args": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			res := []map[string]any{}
			for _, a := range entry.Field.Arguments {
				if isArgServerInjected(a.Directives) {
					continue
				}
				data, err := argumentResolver(ctx, provider, a, field.SelectionSet, maxDepth-1)
				if err != nil {
					return nil, err
				}
				res = append(res, data)
			}
			return res, nil
		},
		"returns": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			data, err := typeResolver(ctx, provider, entry.Field.Type, field.SelectionSet, maxDepth-1)
			if errors.Is(err, ErrTypeNotFound) {
				return nil, nil
			}
			return data, err
		},
		"isTable": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			return entry.IsTable, nil
		},
		"dataSourceName": func(ctx context.Context, field *ast.Field, onType string) (any, error) {
			if entry.DataSource == "" {
				return nil, nil
			}
			return entry.DataSource, nil
		},
	}, "_Function")
}

func functionTypeText(kind sdl.ModuleObjectType) string {
	switch kind {
	case sdl.ModuleFunction:
		return "FUNCTION"
	case sdl.ModuleMutationFunction:
		return "MUTATION"
	case sdl.ModuleSubscription:
		return "SUBSCRIPTION"
	}
	return ""
}

// dataObjectSources returns the owning data source plus every source that
// contributed extension fields (@join/@function_call added cross-source).
func dataObjectSources(obj *sdl.Object) []string {
	owner := obj.Catalog
	seen := map[string]struct{}{}
	if owner != "" {
		seen[owner] = struct{}{}
	}
	for _, f := range obj.Definition().Fields {
		if c := base.FieldDefCatalog(f); c != "" {
			seen[c] = struct{}{}
		}
	}
	res := make([]string, 0, len(seen))
	for s := range seen {
		res = append(res, s)
	}
	slices.Sort(res)
	return res
}

// --- permission filters (same semantics as __schema introspection) ---

// dataObjectVisible reports whether a data object is visible to the request:
// not hidden by a table-level (data-object) rule and with at least one visible
// select query field on its module's query root type. Disabled objects stay
// visible by design.
func dataObjectVisible(ctx context.Context, obj *sdl.Object) bool {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return true
	}
	if p.DataObjectHidden(obj.TypeName()) {
		return false
	}
	rootName := sdl.ModuleTypeName(sdl.ObjectModule(obj.Definition()), sdl.ModuleQuery)
	selects := 0
	for _, q := range obj.Queries() {
		if q.Type != sdl.QueryTypeSelect {
			continue
		}
		selects++
		if _, ok := p.Visible(rootName, q.Name); ok {
			return true
		}
	}
	// No select queries declared — nothing to gate on; keep visible.
	return selects == 0
}

// functionVisible reports whether a callable member is visible to the request
// (role field visibility on its module root type).
func functionVisible(ctx context.Context, entry *catalog.FunctionEntry) bool {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return true
	}
	rootName := sdl.ModuleTypeName(entry.Module, entry.Kind)
	_, ok := p.Visible(rootName, entry.Field.Name)
	return ok
}

// submoduleVisible reports whether a child module's navigation field is
// visible on at least one of the parent's root types that carry it. The root
// definitions are resolved lazily — only on the permission-filtered path.
func submoduleVisible(ctx context.Context, lm catalog.LogicalModel, parent *catalog.ModuleInfo, child *catalog.ModuleInfo) bool {
	p := perm.PermissionsFromCtx(ctx)
	if p == nil {
		return true
	}
	seg := child.Name
	if i := strings.LastIndex(seg, "."); i >= 0 {
		seg = seg[i+1:]
	}
	for _, rootName := range parent.RootTypes {
		rootDef := lm.Type(ctx, rootName)
		if rootDef == nil || rootDef.Fields.ForName(seg) == nil {
			continue
		}
		if _, ok := p.Visible(rootDef.Name, seg); ok {
			return true
		}
	}
	return false
}

// relationVisible drops relation entries whose far object or M2M junction is
// hidden — a hidden endpoint removes the whole edge (fail-closed: an
// unresolvable endpoint hides too).
func relationVisible(ctx context.Context, lm catalog.LogicalModel, rel *catalog.RelationInfo) bool {
	far := lm.DataObject(ctx, rel.DataObject)
	if far == nil || !dataObjectVisible(ctx, far) {
		return false
	}
	if rel.Through != "" {
		junction := lm.DataObject(ctx, rel.Through)
		if junction == nil || !dataObjectVisible(ctx, junction) {
			return false
		}
	}
	return true
}

// moduleHasVisibleContent reports whether a module still has any visible data
// object, function, or non-empty submodule after permission filtering — empty
// modules are omitted from listings (direct _module lookups still resolve).
func moduleHasVisibleContent(ctx context.Context, lm catalog.LogicalModel, provider catalog.Provider, info *catalog.ModuleInfo) bool {
	for obj := range lm.DataObjects(ctx, info.Name) {
		if dataObjectVisible(ctx, obj) {
			return true
		}
	}
	for entry := range lm.Functions(ctx, info.Name) {
		if functionVisible(ctx, entry) {
			return true
		}
	}
	for child := range lm.Modules(ctx, info.Name) {
		if !submoduleVisible(ctx, lm, info, child) {
			continue
		}
		if moduleHasVisibleContent(ctx, lm, provider, child) {
			return true
		}
	}
	return false
}
