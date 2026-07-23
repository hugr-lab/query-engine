package store

import (
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// Module-root synthesis (Ш4.7): the rootRules registry assembles
// Query/Mutation/Function/MutationFunction/Subscription and the _module_*
// roots from their members. A top-level root starts from a deep copy of the
// static prelude stub (introspection/h3/jq/function/query members live
// there); a module root starts from an empty @module_root definition. The
// ordered rules then contribute: data-object query shapes, module functions,
// child-module gateways, and the function-gateway attribution.

// rootScope is the resolved target of one root build.
type rootScope struct {
	module string // dotted module path; "" = top-level root
	kind   sdl.ModuleObjectType
	name   string
	info   *moduleInfoRow // aggregated subtree kind flags
}

func (sc *rootScope) top() bool { return sc.module == "" }

// resolveModuleRoot (4) serves the synthesized roots. Every syntactic parse
// of the name is tried against the modules table; a module without the kind
// (or an empty store for the top-level roots) yields nil and the static stub
// stays authoritative.
func resolveModuleRoot(ctx context.Context, s *Store, name string) *ast.Definition {
	g := s.genCtx()
	for _, ref := range classifyModuleRootName(name) {
		for _, module := range g.resolveModulePath(ctx, ref.Module) {
			if def := buildModuleRoot(ctx, g, rootScope{module: module, kind: ref.Kind, name: name}); def != nil {
				return def
			}
		}
	}
	return nil
}

// resolveModulePath maps the underscored path from the type name back to the
// dotted module names that produce it ("" maps to the root).
func (g *genContext) resolveModulePath(ctx context.Context, underscored string) []string {
	if underscored == "" {
		return []string{""}
	}
	return g.s.queryNames(ctx, `SELECT mm.name FROM core.catalog.modules mm
		WHERE replace(mm.name, '.', '_') = `+lit(underscored)+` ORDER BY mm.name`)
}

func buildModuleRoot(ctx context.Context, g *genContext, sc rootScope) *ast.Definition {
	rows := g.s.readModuleInfo(ctx, sc.module)
	if len(rows) == 0 {
		return nil // inactive / unknown module — static stubs stay authoritative
	}
	sc.info = rows[0]
	if !rootKindAvailable(sc.info, sc.kind, sc.top()) {
		return nil
	}

	var def *ast.Definition
	if sc.top() {
		def = copyRootStub(g.s.static.ForName(ctx, sc.name))
		if def == nil {
			return nil
		}
	} else {
		def = &ast.Definition{
			Kind:     ast.Object,
			Name:     sc.name,
			Position: reconPos,
			Directives: ast.DirectiveList{
				directive("module_root",
					strArg(base.ArgName, sc.module),
					enumArg("type", rootKindEnum(sc.kind))),
			},
		}
		for _, src := range g.s.moduleKindSources(ctx, sc.module, sc.kind) {
			def.Directives = append(def.Directives, moduleCatalogDirective(src))
		}
	}

	for i := range rootRules {
		rootRules[i].apply(ctx, g, &sc, def)
	}
	return def
}

// rootKindAvailable gates a root on the aggregated subtree flags; the
// top-level Query always exists.
func rootKindAvailable(info *moduleInfoRow, kind sdl.ModuleObjectType, top bool) bool {
	switch kind {
	case sdl.ModuleQuery:
		return top || info.HasDataObjects
	case sdl.ModuleMutation:
		return info.HasMutations || (top && info.HasMutFunctions)
	case sdl.ModuleFunction:
		return info.HasFunctions
	case sdl.ModuleMutationFunction:
		return info.HasMutFunctions
	case sdl.ModuleSubscription:
		return info.HasSubscriptions
	}
	return false
}

func rootKindEnum(kind sdl.ModuleObjectType) string {
	switch kind {
	case sdl.ModuleQuery:
		return "QUERY"
	case sdl.ModuleMutation:
		return "MUTATION"
	case sdl.ModuleFunction:
		return "FUNCTION"
	case sdl.ModuleMutationFunction:
		return "MUT_FUNCTION"
	case sdl.ModuleSubscription:
		return "SUBSCRIPTION"
	}
	return ""
}

// rootGatewayWord is the kind word in child-gateway descriptions.
func rootGatewayWord(kind sdl.ModuleObjectType) string {
	switch kind {
	case sdl.ModuleQuery:
		return "query"
	case sdl.ModuleMutation:
		return "mutation"
	case sdl.ModuleFunction:
		return "function"
	case sdl.ModuleMutationFunction:
		return "mutation function"
	case sdl.ModuleSubscription:
		return "subscription"
	}
	return ""
}

// functionKindOf maps a root kind to the catalog.functions.kind value it
// hosts. Top-level roots host the module-"" functions on Query/Mutation.
func functionKindOf(kind sdl.ModuleObjectType) string {
	switch kind {
	case sdl.ModuleFunction, sdl.ModuleQuery:
		return "function"
	case sdl.ModuleMutationFunction, sdl.ModuleMutation:
		return "mutation"
	case sdl.ModuleSubscription:
		return "subscription"
	}
	return ""
}

// moduleCatalogDirective is the per-source @module_catalog attribution.
func moduleCatalogDirective(dataSource string) *ast.Directive {
	return directive(base.ModuleCatalogDirectiveName, strArg(base.ArgName, dataSource))
}

// moduleKindSources lists the ACTIVE sources contributing the kind to the
// module subtree — the @module_catalog attribution set.
func (s *Store) moduleKindSources(ctx context.Context, module string, kind sdl.ModuleObjectType) []string {
	flag := map[sdl.ModuleObjectType]string{
		sdl.ModuleQuery:            "md.has_data_objects",
		sdl.ModuleMutation:         "md.has_tables AND NOT ms.read_only",
		sdl.ModuleFunction:         "md.has_functions",
		sdl.ModuleMutationFunction: "md.has_mut_functions AND NOT ms.read_only",
		sdl.ModuleSubscription:     "md.has_subscriptions",
	}[kind]
	return s.queryNames(ctx, `SELECT DISTINCT md.data_source FROM core.catalog.module_data_sources md`+
		activeMeta("ms", "md.data_source")+`
		WHERE md.module = `+lit(module)+` AND (`+flag+`) ORDER BY md.data_source`)
}

// copyRootStub deep-copies a static root stub far enough that appended
// members and per-field directive attributions never touch the shared AST.
func copyRootStub(stub *ast.Definition) *ast.Definition {
	if stub == nil {
		return nil
	}
	def := *stub
	def.Directives = append(ast.DirectiveList{}, stub.Directives...)
	def.Fields = make(ast.FieldList, len(stub.Fields))
	for i, f := range stub.Fields {
		nf := *f
		nf.Directives = append(ast.DirectiveList{}, f.Directives...)
		def.Fields[i] = &nf
	}
	return &def
}

// The ordered root contributors.
var rootRules = []rootRule{
	rootObjectShapesRule,
	rootFunctionsRule,
	rootChildGatewaysRule,
	rootFunctionGatewayRule,
}

// rootObjectShapesRule instantiates the queryShapes root fields for the
// module's DIRECT data objects (query shapes on query roots, mutation shapes
// on mutation roots).
var rootObjectShapesRule = rootRule{
	name: "object_shapes",
	apply: func(ctx context.Context, g *genContext, sc *rootScope, def *ast.Definition) {
		if sc.kind != sdl.ModuleQuery && sc.kind != sdl.ModuleMutation {
			return
		}
		for _, name := range g.s.dataObjectNames(ctx, sc.module) {
			row, ok := g.s.readDataObject(ctx, name)
			if !ok {
				continue
			}
			t := g.objectTraits(ctx, row)
			for i := range queryShapes {
				shape := &queryShapes[i]
				if shape.rootKind != sc.kind || !shape.matches(t) {
					continue
				}
				def.Fields = append(def.Fields, shape.root(t)...)
			}
		}
	},
}

// rootFunctionsRule adds the module's functions of the root's kind. On the
// top-level roots the module-"" functions attach directly to Query/Mutation
// (RootTypeAssembler behavior). On FUNCTION module roots, list-of-data-object
// functions additionally get their aggregation twin pair
// (addModuleFuncAggregations).
var rootFunctionsRule = rootRule{
	name: "functions",
	apply: func(ctx context.Context, g *genContext, sc *rootScope, def *ast.Definition) {
		kind := functionKindOf(sc.kind)
		if kind == "" {
			return
		}
		if sc.top() && (sc.kind == sdl.ModuleQuery || sc.kind == sdl.ModuleMutation) {
			// module-"" functions only; modules' functions live under Function/MutationFunction.
		} else if sc.kind == sdl.ModuleQuery || sc.kind == sdl.ModuleMutation {
			return
		}
		srcs := g.s.activeSources(ctx)
		for _, fn := range g.s.readFunctions(ctx, sc.module) {
			if fn.Kind != kind {
				continue
			}
			fd := functionField(fn)
			// Subscription decoration mirrors the assembler: module-routed
			// fields gain @catalog + bare @subscription, root-level ones stay
			// exactly as declared.
			if sc.kind == sdl.ModuleSubscription {
				if !sc.top() {
					fd.Directives = append(fd.Directives,
						catalogDirective(fn.DataSource, srcs[fn.DataSource].Engine),
						directive(base.SubscriptionDirectiveName))
				}
			} else {
				fd.Directives = append(fd.Directives, catalogDirective(fn.DataSource, srcs[fn.DataSource].Engine))
			}
			def.Fields = append(def.Fields, fd)
			if sc.kind != sdl.ModuleFunction {
				continue
			}
			target, isList := listReturnTarget(fn)
			if !isList || !g.s.dataObjectExists(ctx, target) {
				continue
			}
			catalog := func() *ast.Directive {
				return catalogDirective(fn.DataSource, srcs[fn.DataSource].Engine)
			}
			funcAgg := func(isBucket bool) *ast.Directive {
				return directive("aggregation_query",
					strArg(base.ArgName, fn.Name), boolArg(base.ArgIsBucket, isBucket))
			}
			def.Fields = append(def.Fields,
				&ast.FieldDefinition{
					Name:       fn.Name + "_aggregation",
					Type:       ast.NamedType("_"+target+"_aggregation", reconPos),
					Arguments:  emitArgumentDefs(fn.Args),
					Directives: ast.DirectiveList{funcAgg(false), catalog()},
					Position:   reconPos,
				},
				&ast.FieldDefinition{
					Name:       fn.Name + "_bucket_aggregation",
					Type:       ast.ListType(ast.NamedType("_"+target+"_aggregation_bucket", reconPos), reconPos),
					Arguments:  emitArgumentDefs(fn.Args),
					Directives: ast.DirectiveList{funcAgg(true), catalog()},
					Position:   reconPos,
				},
			)
		}
	},
}

// listReturnTarget parses a function's stored return type; isList reports a
// list of the named type.
func listReturnTarget(fn *function) (string, bool) {
	typ := parseFieldType(fn.Returns)
	return typ.Name(), typ.NamedType == ""
}

// rootChildGatewaysRule wires the child modules that carry the kind. The
// compiler's description convention: intermediate gateways always carry it;
// top-level gateways only on the Function/MutationFunction roots
// (Query/Mutation/Subscription top gateways stay bare).
var rootChildGatewaysRule = rootRule{
	name: "child_gateways",
	apply: func(ctx context.Context, g *genContext, sc *rootScope, def *ast.Definition) {
		describeTop := sc.kind == sdl.ModuleFunction || sc.kind == sdl.ModuleMutationFunction
		for _, child := range g.s.readChildModuleInfos(ctx, sc.module) {
			if !rootKindAvailable(child, sc.kind, false) {
				continue
			}
			parts := strings.Split(child.Name, ".")
			fd := &ast.FieldDefinition{
				Name:     parts[len(parts)-1],
				Type:     ast.NamedType(sdl.ModuleTypeName(child.Name, sc.kind), reconPos),
				Position: reconPos,
			}
			if !sc.top() || describeTop {
				fd.Description = "The root " + rootGatewayWord(sc.kind) + " object of the module " + child.Name
			}
			for _, src := range g.s.moduleKindSources(ctx, child.Name, sc.kind) {
				fd.Directives = append(fd.Directives, moduleCatalogDirective(src))
			}
			def.Fields = append(def.Fields, fd)
		}
	},
}

// rootFunctionGatewayRule attributes the prelude `function` gateway on the
// top-level Query/Mutation with @module_catalog per contributing source.
var rootFunctionGatewayRule = rootRule{
	name: "function_gateway",
	apply: func(ctx context.Context, g *genContext, sc *rootScope, def *ast.Definition) {
		if !sc.top() {
			return
		}
		var gatewayKind sdl.ModuleObjectType
		switch sc.kind {
		case sdl.ModuleQuery:
			gatewayKind = sdl.ModuleFunction
		case sdl.ModuleMutation:
			gatewayKind = sdl.ModuleMutationFunction
		default:
			return
		}
		fd := def.Fields.ForName("function")
		if fd == nil {
			return
		}
		sources := g.s.moduleKindSources(ctx, "", gatewayKind)
		if len(sources) == 0 {
			return // no functions anywhere — the bare prelude field stays
		}
		fd.Description = "Functions"
		for _, src := range sources {
			fd.Directives = append(fd.Directives, moduleCatalogDirective(src))
		}
	},
}
