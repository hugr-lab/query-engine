package store

import (
	"bytes"
	"context"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

// collect walks the base definitions produced by the PARTIAL compiler
// (VALIDATE + PREPARE — prefixed, extension-merged, validated, NOT generated)
// and builds the desired row state of the catalog namespace. It writes the
// PHYSICAL model verbatim: references go in as declared (object @references and
// field @field_references, both as plain fk rows whose source IS the declaring
// table) with NO generation. The derived surface — m2m navigation, aggregation
// / filter types, module roots — is regenerated on READ, not stored.
//
// Directives land in the rows via the directive↔bag pair tables
// (pairs_object.go / pairs_field.go / pairs_function.go) — the collect half of
// each pair; the emit half rebuilds them in reconstruct*.
//
// dataSource is the owner attributed to entities the compiler leaves untagged
// (functions and source types carry no @catalog after PREPARE — only data
// objects do). Data objects / fields keep their own @catalog when present.
func collect(ctx context.Context, defs base.ExtensionsSource, dataSource string) *desired {
	d := newDesired()
	// The AsModule option routes the source's members into a module named
	// after the source (PrefixPreparer keeps it in ObjectInfo only — the
	// directive is not always materialized, so normalize here).
	if co, ok := defs.(interface{ CompileOptions() base.Options }); ok {
		d.asModule = co.CompileOptions().AsModule
	}
	// Data objects and residual source types live in the (in-place mutated)
	// definitions after VALIDATE+PREPARE.
	for def := range defs.Definitions(ctx) {
		switch {
		case sdl.IsSystemType(def):
			// system layer lives in the static provider — never stored
		case sdl.IsDataObject(def):
			collectDataObject(ctx, defs, d, def, dataSource)
		case isSourceDefinedType(def):
			module := sdl.ObjectModule(def)
			ensureModule(d, module)
			d.types[def.Name] = &sourceType{
				Name:        def.Name,
				Kind:        typeKindText(def.Kind),
				DataSource:  orDefault(sdl.CatalogName(def), dataSource),
				Module:      module,
				Definition:  formatDefinitionSDL(def),
				Description: def.Description,
			}
		}
	}
	// Extensions() holds only what could not be merged into this source's own
	// definitions: module roots (Function/MutationFunction/Subscription — the
	// callable members) and `extend type <object>` targeting a data object
	// owned by ANOTHER source (an extension source adding fields). Same-source
	// extends were already merged into the definitions by PREPARE.
	for ext := range defs.Extensions(ctx) {
		if sdl.ModuleRootInfo(ext) != nil {
			collectModuleRoot(ctx, defs, d, ext, dataSource)
			continue
		}
		collectExtensionFields(ctx, defs, d, ext, resolveExtensionTarget(ctx, defs, ext.Name), dataSource)
	}
	buildModuleClosure(d)
	return d
}

func collectDataObject(ctx context.Context, defs base.DefinitionsSource, d *desired, def *ast.Definition, dataSource string) {
	if _, ok := d.dataObjects[def.Name]; ok {
		return
	}
	owner := orDefault(sdl.CatalogName(def), dataSource)
	obj := &dataObject{
		Name:        def.Name,
		DataSource:  owner,
		Description: def.Description,
	}
	collectObjectDirectives(def, obj) // fills OriginalName / Module / Kind + bag
	// AsModule routing: the module is the source name, an inline @module
	// nests under it (PrefixPreparer semantics: "shop" + "air" → "shop.air").
	if d.asModule {
		obj.Module = nestModule(dataSource, obj.Module)
	}
	// @args(required:) is auto-computed by the compiler from the input type's
	// NonNull fields — normalize it into the bag the same way (gen_view.go).
	if obj.Properties.ArgsTypeName != "" && !obj.Properties.RequiredArgs {
		if inputDef := defs.ForName(ctx, obj.Properties.ArgsTypeName); inputDef != nil {
			for _, f := range inputDef.Fields {
				if f.Type.NonNull {
					obj.Properties.RequiredArgs = true
					break
				}
			}
		}
	}
	ensureModule(d, obj.Module)
	d.dataObjects[def.Name] = obj

	ordinal := 0
	for _, f := range def.Fields {
		if skipField(f) {
			continue
		}
		row := &field{
			TypeName:             def.Name,
			Name:                 f.Name,
			FieldType:            f.Type.String(),
			Args:                 mapFunctionArgs(f),
			DataSource:           fieldOwner(f, owner),
			DependencyDataSource: base.FieldDefDependency(f),
			Ordinal:              ordinal,
			Description:          f.Description,
		}
		collectFieldDirectives(f, row) // fills IsPK / DeprecationReason + bag
		normalizeFuncCallModules(d, row, dataSource)
		d.fields[pkKey(def.Name, f.Name)] = row
		ordinal++
		collectFieldReferences(ctx, defs, d, def.Name, owner, f)
	}

	collectObjectReferences(d, def.Name, owner, def)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// originalName returns a data object's unprefixed source name from
// @original_name (added by the prefixer), or its own name when unprefixed.
func originalName(def *ast.Definition) string {
	if n := base.DirectiveArgString(def.Directives.ForName(base.OriginalNameDirectiveName), base.ArgName); n != "" {
		return n
	}
	return def.Name
}

// nestModule applies the AsModule nesting rule: empty → the source name, an
// inline module nests under it (unless already nested).
func nestModule(dataSource, module string) string {
	if module == "" {
		return dataSource
	}
	if module == dataSource || strings.HasPrefix(module, dataSource+".") {
		return module
	}
	return dataSource + "." + module
}

// normalizeFuncCallModules rewrites the @function_call / @tfcj binding module
// to the FUNCTION's compiled module under AsModule (the compiled path resolves
// the target function and stores its full module path).
func normalizeFuncCallModules(d *desired, row *field, dataSource string) {
	if !d.asModule || row.Properties == nil {
		return
	}
	if fc := row.Properties.FunctionCall; fc != nil {
		fc.Function.Module = nestModule(dataSource, fc.Function.Module)
	}
	if fc := row.Properties.TableFunctionCallJoin; fc != nil {
		fc.Function.Module = nestModule(dataSource, fc.Function.Module)
	}
}

// resolveExtensionTarget maps a leftover extension's target to the COMPILED
// type name. A same-source extension survives the merger unprefixed when
// PrefixPreparer (a per-definition rule) renamed the base first — the full
// compiler matches it back through @original_name, mirrored here. Cross-source
// extensions already reference the compiled name.
func resolveExtensionTarget(ctx context.Context, defs base.DefinitionsSource, name string) string {
	if defs.ForName(ctx, name) != nil {
		return name
	}
	for def := range defs.Definitions(ctx) {
		if originalName(def) == name {
			return def.Name
		}
	}
	return name
}

// collectExtensionFields adds the fields of a cross-source `extend type <object>`
// to the fields table, attributed to THIS (extending) source. The object row
// itself is owned by the source that declares it and is never created here; the
// extending source does not back the object's module either (no closure row) —
// so unloading it drops only its added fields. Ordinal is local to the
// extension (the read side orders base fields before extension fields).
//
// An extension may also declare an object-level @references — an edge between
// two OTHER sources' objects, which is a thing only an extension can express
// and which has no field of its own to hang on (the body is a `_stub`). It is
// attributed to the extending source like the fields are, so unloading the
// extension takes the edge with it, and a reload of either endpoint leaves it
// alone.
func collectExtensionFields(ctx context.Context, defs base.DefinitionsSource, d *desired, ext *ast.Definition, typeName, dataSource string) {
	ordinal := 0
	for _, f := range ext.Fields {
		if skipField(f) {
			continue
		}
		owner := orDefault(base.FieldDefCatalog(f), dataSource)
		row := &field{
			TypeName:             typeName,
			Name:                 f.Name,
			FieldType:            f.Type.String(),
			Args:                 mapFunctionArgs(f),
			DataSource:           owner,
			DependencyDataSource: base.FieldDefDependency(f),
			Ordinal:              ordinal,
			Description:          f.Description,
		}
		collectFieldDirectives(f, row)
		normalizeFuncCallModules(d, row, dataSource)
		d.fields[pkKey(typeName, f.Name)] = row
		ordinal++
		collectFieldReferences(ctx, defs, d, typeName, owner, f)
	}
	collectObjectReferences(d, typeName, dataSource, ext)
}

// collectObjectReferences stores each object-level @references as a plain fk
// relation row whose source is the declaring object. is_m2m stays a property of
// the junction OBJECT (@table(is_m2m:true)); the m2m navigation is generated on
// read from the junction's two fk legs — not stored here.
//
// Omitted args are stored NORMALIZED with the compiler's defaults
// (gen_references.go): name → <target>[_<field> when single-key], query →
// target name, references_query → declaring object name.
func collectObjectReferences(d *desired, source, owner string, def *ast.Definition) {
	for _, dir := range def.Directives.ForNames(base.ReferencesDirectiveName) {
		ref := sdl.ReferencesInfo(dir)
		if ref == nil || ref.ReferencesName == "" {
			continue
		}
		// Compiler mirror (gen_references.go): keyless references emit nothing.
		if len(ref.SourceFields()) == 0 || len(ref.ReferencesFields()) == 0 {
			continue
		}
		name := ref.Name
		if name == "" {
			name = ref.ReferencesName
			if sf := ref.SourceFields(); len(sf) == 1 {
				name += "_" + sf[0]
			}
		}
		// An explicit references_query: "" is stored as declared (verbatim
		// @field_references re-emission fidelity); generation collapses "" to
		// the default. Only an ABSENT argument is normalized here.
		refQuery := ref.ReferencesQuery
		if dir.Arguments.ForName(base.ArgReferencesQuery) == nil {
			refQuery = source
		}
		putRelation(d, &relation{
			Source:                      source,
			Name:                        name,
			Kind:                        "fk",
			Destination:                 ref.ReferencesName,
			SourceKeys:                  ref.SourceFields(),
			DestinationKeys:             ref.ReferencesFields(),
			SourceField:                 orDefault(ref.Query, ref.ReferencesName),
			SourceFieldDescription:      ref.Description,
			DestinationField:            refQuery,
			DestinationFieldDescription: ref.ReferencesDescription,
			DataSource:                  owner,
		})
	}
}

// collectFieldReferences stores each field-level @field_references as a plain fk
// relation row (single-field legs). Mirrors the compiler's
// FieldReferencesToReferences normalization: the target field defaults to the
// referenced object's PK, the relation name to "<target>_<field>".
func collectFieldReferences(ctx context.Context, defs base.DefinitionsSource, d *desired, source, owner string, f *ast.FieldDefinition) {
	for _, dir := range f.Directives.ForNames(base.FieldReferencesDirectiveName) {
		refName := base.DirectiveArgString(dir, base.ArgReferencesName)
		if refName == "" {
			continue
		}
		targetField := base.DirectiveArgString(dir, base.ArgField)
		if targetField == "" {
			targetField = targetPrimaryKey(ctx, defs, refName)
		}
		if targetField == "" {
			continue
		}
		name := base.DirectiveArgString(dir, base.ArgName)
		if name == "" {
			name = refName + "_" + f.Name
		}
		query := base.DirectiveArgString(dir, base.ArgQuery)
		if query == "" {
			query = refName
		}
		// An EXPLICIT references_query: "" survives as declared (verbatim
		// re-emission fidelity; generation collapses "" to the default) —
		// only an ABSENT argument is normalized here.
		refQuery := base.DirectiveArgString(dir, base.ArgReferencesQuery)
		if dir.Arguments.ForName(base.ArgReferencesQuery) == nil {
			refQuery = source
		}
		putRelation(d, &relation{
			Source:                      source,
			Name:                        name,
			Kind:                        "fk",
			Destination:                 refName,
			SourceKeys:                  []string{f.Name},
			DestinationKeys:             []string{targetField},
			SourceField:                 query,
			DestinationField:            refQuery,
			SourceFieldDescription:      base.DirectiveArgString(dir, base.ArgDescription),
			DestinationFieldDescription: base.DirectiveArgString(dir, base.ArgReferencesDescription),
			FieldDeclared:               true,
			DataSource:                  owner,
		})
	}
}

// putRelation stores a relation row, guarding the (source, name) primary key
// (relation names are unique among the declaring object's references).
func putRelation(d *desired, row *relation) {
	key := pkKey(row.Source, row.Name)
	if _, ok := d.relations[key]; !ok {
		d.relations[key] = row
	}
}

// targetPrimaryKey returns the single @pk field of a referenced object, or ""
// — the compiler defaults @field_references(field:) to the target PK only when
// the key is a SINGLE column (gen_references.go); a composite PK yields no
// default and the relation is dropped.
func targetPrimaryKey(ctx context.Context, defs base.DefinitionsSource, typeName string) string {
	def := defs.ForName(ctx, typeName)
	if def == nil {
		return ""
	}
	pk := ""
	for _, f := range def.Fields {
		if f.Directives.ForName(base.FieldPrimaryKeyDirectiveName) == nil {
			continue
		}
		if pk != "" {
			return ""
		}
		pk = f.Name
	}
	return pk
}

// collectModuleRoot extracts the callable members of a module root type
// (Function / MutationFunction / Subscription) — the root itself is never
// stored (it is synthesized on read).
func collectModuleRoot(ctx context.Context, defs base.DefinitionsSource, d *desired, def *ast.Definition, dataSource string) {
	kind := functionKindText(sdl.ModuleRootInfo(def).Type)
	for _, f := range def.Fields {
		if strings.HasPrefix(f.Name, "__") {
			continue
		}
		module := sdl.FunctionModule(f)
		// AsModule routing (FunctionRule mirror): no module → the source
		// name, an inline @module nests under it. EXCEPT subscriptions: the
		// assembler only defaults the empty module and keeps an inline
		// @module as written (no source-name nesting).
		if d.asModule {
			if kind == "subscription" {
				module = orDefault(module, dataSource)
			} else {
				module = nestModule(dataSource, module)
			}
		}
		ensureModule(d, module)
		// The KIND is part of the identity: function, mutation function and
		// subscription are independent GraphQL root namespaces, and one name
		// legally appears in several of them (an HTTP source exposes every
		// operation as both a function and a mutation function; core.models
		// exposes `completion` as a function AND as a subscription). Keying
		// without the kind silently dropped every duplicate — the dedup exists
		// only to keep a repeated conflict target out of the multi-row write.
		key := pkKey(module, f.Name, kind)
		if _, ok := d.functions[key]; ok {
			continue
		}
		row := &function{
			Module:      module,
			Name:        f.Name,
			Kind:        kind,
			DataSource:  orDefault(base.FieldDefCatalog(f), dataSource),
			Returns:     f.Type.String(),
			IsTable:     isDataObjectType(ctx, defs, f.Type.Name()),
			Description: f.Description,
		}
		collectFunctionDirectives(f, row) // fills DeprecationReason + bag + args
		d.functions[key] = row
	}
}

func isDataObjectType(ctx context.Context, defs base.DefinitionsSource, typeName string) bool {
	def := defs.ForName(ctx, typeName)
	return def != nil && sdl.IsDataObject(def)
}

// fieldOwner attributes a field to its declaring data source, falling back to
// the object owner for fields without an explicit @catalog — a field row must
// never carry an empty owner, or unload could not sweep it.
func fieldOwner(f *ast.FieldDefinition, owner string) string {
	if c := base.FieldDefCatalog(f); c != "" {
		return c
	}
	return owner
}

// skipField excludes non-base fields. Pre-generate the object carries only
// declared fields, so this only trims introspection helpers and the (absent)
// generated projections — DECLARED @join / @function_call / @field_references
// fields STAY (they are real columns / bindings).
func skipField(f *ast.FieldDefinition) bool {
	if strings.HasPrefix(f.Name, "__") {
		return true
	}
	switch f.Name {
	case "_stub", "_placeholder", // declaration placeholders, never columns
		base.QueryTimeJoinsFieldName, base.QueryTimeSpatialFieldName,
		base.H3QueryFieldName, base.QueryEmbeddingsDistanceFieldName:
		return true
	}
	if f.Directives.ForName(base.FieldExtraFieldDirectiveName) != nil {
		return true
	}
	if f.Directives.ForName(base.FieldAggregationQueryDirectiveName) != nil {
		return true
	}
	return sdl.IsReferencesSubquery(f)
}

// isSourceDefinedType reports whether a definition is a residual source base
// type (structural object / input / enum / interface / union) — the content of
// catalog.types. Replicated from catalog.isSourceDefinedType (unexported).
func isSourceDefinedType(def *ast.Definition) bool {
	if sdl.IsSystemType(def) || sdl.IsDataObject(def) || sdl.ModuleRootInfo(def) != nil {
		return false
	}
	return def.Directives.ForName(base.FilterInputDirectiveName) == nil &&
		def.Directives.ForName(base.FilterListInputDirectiveName) == nil &&
		def.Directives.ForName(base.DataInputDirectiveName) == nil &&
		def.Directives.ForName(base.ObjectAggregationDirectiveName) == nil
}

// --- module tree ---

// ensureModule records a module and all its dotted-name ancestors; the implicit
// root "" is never stored.
func ensureModule(d *desired, name string) {
	for name != "" {
		if _, ok := d.modules[name]; !ok {
			d.modules[name] = &module{Name: name, Parent: moduleParent(name)}
		}
		name = moduleParent(name)
	}
}

// buildModuleClosure fills catalog.module_data_sources: each collected entity's
// owner source is recorded on its module AND every ancestor INCLUDING the root
// "", with the root-kind flags of the entity — the hierarchy is walked ONCE
// here at save time, so the read side never recurses (a module's kinds are a
// flat bool_or aggregate over its closure rows).
func buildModuleClosure(d *desired) {
	for _, obj := range d.dataObjects {
		for _, anc := range moduleChain(obj.Module) {
			ms := touchModuleSource(d, anc, obj.DataSource)
			if ms == nil {
				continue
			}
			ms.HasDataObjects = true
			if obj.Kind == "table" {
				ms.HasTables = true
			}
		}
	}
	for _, fn := range d.functions {
		for _, anc := range moduleChain(fn.Module) {
			ms := touchModuleSource(d, anc, fn.DataSource)
			if ms == nil {
				continue
			}
			switch fn.Kind {
			case "mutation":
				ms.HasMutFunctions = true
			case "subscription":
				ms.HasSubscriptions = true
			default:
				ms.HasFunctions = true
			}
		}
	}
}

func touchModuleSource(d *desired, module, source string) *moduleSource {
	if source == "" {
		return nil
	}
	key := pkKey(module, source)
	if _, ok := d.moduleSources[key]; !ok {
		d.moduleSources[key] = &moduleSource{Module: module, DataSource: source}
	}
	return d.moduleSources[key]
}

func moduleParent(name string) string {
	if i := strings.LastIndex(name, "."); i >= 0 {
		return name[:i]
	}
	return ""
}

// moduleChain returns the module, its dotted-name ancestors and the root
// (a.b.c → [a.b.c, a.b, a, ""]) — the closure rows of one entity.
func moduleChain(name string) []string {
	var out []string
	for name != "" {
		out = append(out, name)
		name = moduleParent(name)
	}
	return append(out, "")
}

// --- text helpers ---

func functionKindText(kind sdl.ModuleObjectType) string {
	switch kind {
	case sdl.ModuleMutationFunction:
		return "mutation"
	case sdl.ModuleSubscription:
		return "subscription"
	default:
		return "function"
	}
}

func typeKindText(kind ast.DefinitionKind) string {
	switch kind {
	case ast.Object:
		return "object"
	case ast.InputObject:
		return "input"
	case ast.Enum:
		return "enum"
	case ast.Scalar:
		return "scalar"
	case ast.Interface:
		return "interface"
	case ast.Union:
		return "union"
	}
	return strings.ToLower(string(kind))
}

// formatDefinitionSDL renders one definition as GraphQL SDL text.
func formatDefinitionSDL(def *ast.Definition) string {
	var buf bytes.Buffer
	formatter.NewFormatter(&buf).FormatSchemaDocument(&ast.SchemaDocument{Definitions: ast.DefinitionList{def}})
	return strings.TrimSpace(buf.String())
}
