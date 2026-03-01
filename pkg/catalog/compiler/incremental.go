package compiler

import (
	"context"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	"github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// CompileChanges compiles incremental DDL SDL changes against an existing
// provider schema. It produces a CompiledCatalog compatible with Provider.Update().
//
// Parameters:
//   - provider: the current compiled schema
//   - baseCatalog: the full original (uncompiled) source catalog — needed for
//     prefix renaming of type references across the whole catalog namespace
//   - changes: incremental DDL with @drop directives on types to remove,
//     new definitions for types to add, and extensions for field-level changes
//   - opts: compile options (same as for full compilation)
//
// The pipeline:
//  1. CLASSIFY — definitions → toAdd/toDrop; check for extensions
//  2. BUILD SOURCE NAMES (from baseCatalog + changes)
//  3. TYPE DROPS — emit @drop DDL for types + derived types + shared fields
//  4. PREPARE — prefix rename on definitions AND extensions
//  5. COMPILE NEW TYPES — GENERATE→ASSEMBLE→FINALIZE on toAdd definitions
//  6. COMPILE FIELD CHANGES — process extensions for field add/drop/replace
//  7. RETURN CompiledCatalog
func CompileChanges(
	ctx context.Context,
	provider base.Provider,
	baseCatalog base.DefinitionsSource,
	changes base.DefinitionsSource,
	opts Options,
) (base.CompiledCatalog, error) {
	c := New(GlobalRules()...)
	return c.CompileChanges(ctx, provider, baseCatalog, changes, opts)
}

// CompileChanges compiles incremental DDL changes using the compiler's rules.
func (c *Compiler) CompileChanges(
	ctx context.Context,
	provider base.Provider,
	baseCatalog base.DefinitionsSource,
	changes base.DefinitionsSource,
	opts base.Options,
) (base.CompiledCatalog, error) {
	// 1. CLASSIFY — separate definitions into add/drop buckets
	classified := classifyChanges(ctx, changes)

	// Check if changes source also provides extensions (field-level changes)
	var allExtensions []*ast.Definition // all extensions (for field add/drop/replace on base + derived types)
	var refExtensions []*ast.Definition // subset with @field_references (also need pipeline for reference generation)
	if extSource, ok := changes.(base.ExtensionsSource); ok {
		for ext := range extSource.Extensions(ctx) {
			allExtensions = append(allExtensions, ext)
			if hasFieldReferences(ext) {
				refExtensions = append(refExtensions, ext)
			}
		}
	}

	totalDefs := len(classified.toAdd) + len(classified.toDrop)
	output := newIndexedOutput((totalDefs + len(allExtensions)) * 8)

	// 2. Build source names from baseCatalog for prefix renaming.
	// This includes ALL names from the catalog namespace, not just the changes.
	// baseCatalog definitions may already be prefixed (modified in-place by initial
	// compilation), so we recover original names via @original_name directive.
	sourceNames := make(map[string]bool)
	for def := range baseCatalog.Definitions(ctx) {
		name := def.Name
		if on := def.Directives.ForName(base.OriginalNameDirectiveName); on != nil {
			if arg := on.Arguments.ForName("name"); arg != nil {
				name = arg.Value.Raw
			}
		}
		sourceNames[name] = true
	}
	for _, def := range classified.toAdd {
		sourceNames[def.Name] = true
	}

	// 3. Process DROPS — emit @drop DDL for types and all derived types
	for _, def := range classified.toDrop {
		emitDropDDL(output, def, opts, sourceNames)
	}

	// 4. Apply prefix to extensions (target type name + field type refs + directive args)
	if opts.Prefix != "" {
		for _, ext := range allExtensions {
			prefixExtension(ext, opts.Prefix, sourceNames)
		}
	}

	// 5. Compile new type definitions (if any)
	addDefs := classified.toAdd

	if len(addDefs) > 0 {
		source := newIncrementalSourceWithExtensions(addDefs, baseCatalog, nil)
		cctx := newCompilationContext(ctx, source, provider, opts, output)

		// Recover ObjectInfo for existing provider types
		recoverProviderObjects(ctx, provider, cctx, opts)

		// PREPARE — CatalogTagger + PrefixPreparer on change definitions only
		if opts.Name != "" {
			catDir := catalogDirective(opts.Name, opts.EngineType)
			for _, def := range addDefs {
				if def.Kind == ast.Object {
					def.Directives = append(def.Directives, catDir)
				}
			}
		}
		if err := rules.PrefixAndRegister(cctx, sourceNames); err != nil {
			return nil, err
		}

		// Run GENERATE → ASSEMBLE → FINALIZE using existing rules
		for _, phase := range []base.Phase{base.PhaseGenerate, base.PhaseAssemble, base.PhaseFinalize} {
			phaseRules := c.rules[phase]
			if len(phaseRules) == 0 {
				continue
			}

			var defRules []base.DefinitionRule
			var batchRules []base.BatchRule
			for _, r := range phaseRules {
				switch rule := r.(type) {
				case base.DefinitionRule:
					defRules = append(defRules, rule)
				case base.BatchRule:
					batchRules = append(batchRules, rule)
				}
			}

			if len(defRules) > 0 {
				for def := range source.Definitions(ctx) {
					for _, rule := range defRules {
						if rule.Match(def) {
							if err := rule.Process(cctx, def); err != nil {
								return nil, wrapRuleError(phase, rule.Name(), def, err)
							}
						}
					}
				}
				for _, def := range cctx.promoted {
					for _, rule := range defRules {
						if rule.Match(def) {
							if err := rule.Process(cctx, def); err != nil {
								return nil, wrapRuleError(phase, rule.Name(), def, err)
							}
						}
					}
				}
			}

			for _, rule := range batchRules {
				if err := rule.ProcessAll(cctx); err != nil {
					return nil, wrapRuleError(phase, rule.Name(), nil, err)
				}
			}
		}
	}

	// 5b. Process reference extensions: convert @field_references to @references,
	// then generate all reference query/filter/aggregation fields directly.
	if len(refExtensions) > 0 {
		source := newIncrementalSourceWithExtensions(nil, baseCatalog, nil)
		cctx := newCompilationContext(ctx, source, provider, opts, output)
		recoverProviderObjects(ctx, provider, cctx, opts)

		for _, ext := range refExtensions {
			pos := base.CompiledPos(ext.Name)
			for _, f := range ext.Fields {
				for _, dir := range f.Directives.ForNames(base.FieldReferencesDirectiveName) {
					refDir := rules.FieldReferencesToReferences(f.Name, dir, ext, cctx, pos)
					if refDir != nil {
						ext.Directives = append(ext.Directives, refDir)
					}
				}
			}
			// Generate forward/back reference fields, filter refs, agg refs
			for _, dir := range ext.Directives.ForNames(base.ReferencesDirectiveName) {
				rules.ProcessExtensionReferences(cctx, ext, dir, pos)
			}
		}
	}

	// 6. Process ALL field-level extensions (add/drop/replace on base + derived types).
	// Reference extensions also go through here for their scalar field additions
	// (filter, mut, agg), while reference field generation is handled by step 5.
	if len(allExtensions) > 0 {
		typeExists := func(name string) bool {
			return provider.ForName(ctx, name) != nil
		}
		providerLookup := func(name string) *ast.Definition {
			return provider.ForName(ctx, name)
		}
		processFieldExtensions(output, allExtensions, opts, types.Lookup, typeExists, providerLookup)
	}

	return newCompiledCatalog(output), nil
}

// changeClassification separates incremental changes into add/drop buckets.
type changeClassification struct {
	toAdd []*ast.Definition
	toDrop []*ast.Definition
}

// classifyChanges iterates changes and separates them by DDL control directive.
func classifyChanges(ctx context.Context, changes base.DefinitionsSource) *changeClassification {
	cc := &changeClassification{}
	for def := range changes.Definitions(ctx) {
		switch {
		case base.IsDropDefinition(def):
			cc.toDrop = append(cc.toDrop, def)
		default:
			cc.toAdd = append(cc.toAdd, def)
		}
	}
	return cc
}

// processFieldExtensions processes extension definitions for field-level changes.
// Each extension targets an existing type and may contain:
//   - Fields without @drop/@replace → field add
//   - Fields with @drop → field drop (including reference-generated fields)
//   - Fields with @replace → field replace (drop old + add new)
//   - Directives (@drop_directive, new directives) → pass through as-is
//
// The providerLookup function resolves existing type definitions from the provider,
// used to check for @field_references when dropping fields.
func processFieldExtensions(
	output *indexedOutput,
	extensions []*ast.Definition,
	opts base.Options,
	scalarLookup func(string) types.ScalarType,
	typeExists func(string) bool,
	providerLookup func(string) *ast.Definition,
) {
	for _, ext := range extensions {
		typeName := ext.Name

		// Separate fields by operation
		for _, f := range ext.Fields {
			if f.Name == "_stub" || f.Name == "_placeholder" {
				continue
			}

			switch {
			case base.IsDropField(f):
				// Drop field from derived types (filter, mut, agg)
				emitFieldDropExtensions(output, typeName, f.Name, opts, typeExists)
				// Also drop from the base type itself
				pos := base.CompiledPos("incremental-field-drop")
				emitDropFieldOnType(output, typeName, f.Name, pos)

				// Check existing field in provider for special handling
				if providerLookup != nil {
					if typeDef := providerLookup(typeName); typeDef != nil {
						existingField := typeDef.Fields.ForName(f.Name)
						if existingField != nil {
							// If field had @field_references, drop reference-generated fields
							if existingField.Directives.ForName(base.FieldReferencesDirectiveName) != nil {
								emitReferenceDropExtensions(output, typeDef, f.Name, typeExists)
							}
							// If field was virtual (@join/@function_call/@table_function_call_join),
							// drop aggregation fields it generated
							if isIncrementalVirtualField(existingField) {
								emitVirtualFieldDropExtensions(output, typeName, f.Name, existingField, typeExists)
							}
						}
					}
				}
			case base.IsReplaceField(f):
				// Strip the @replace directive from the field before emitting add
				stripped := stripControlDirectivesFromField(f)
				emitFieldReplaceExtensions(output, typeName, stripped, opts, scalarLookup, typeExists)
				// Also replace on the base type itself (drop old + add new)
				pos := base.CompiledPos("incremental-field-replace")
				emitDropFieldOnType(output, typeName, f.Name, pos)
				output.AddExtension(&ast.Definition{
					Kind: ext.Kind, Name: typeName, Position: pos,
					Fields: ast.FieldList{stripped},
				})
			default:
				if isIncrementalVirtualField(f) {
					// Virtual fields need special handling: subquery args, aggregation fields
					emitVirtualFieldAddExtensions(output, typeName, f, opts, scalarLookup, typeExists, providerLookup)
				} else {
					emitFieldAddExtensions(output, typeName, f, opts, scalarLookup)
				}
				// Also add to the base type itself
				output.AddExtension(&ast.Definition{
					Kind: ext.Kind, Name: typeName, Position: f.Position,
					Fields: ast.FieldList{f},
				})
			}
		}

		// Pass through extension directives (e.g., @drop_directive, new directives)
		if len(ext.Directives) > 0 {
			output.AddExtension(&ast.Definition{
				Kind:       ext.Kind,
				Name:       typeName,
				Position:   ext.Position,
				Directives: ext.Directives,
			})

			// Detect module change and emit module restructuring
			oldModule, newModule := detectModuleChange(ext.Directives)
			if newModule != "" && providerLookup != nil {
				if typeDef := providerLookup(typeName); typeDef != nil {
					if oldModule == "" {
						// Fall back to provider's @module directive
						if md := typeDef.Directives.ForName(base.ModuleDirectiveName); md != nil {
							oldModule = base.DirectiveArgString(md, base.ArgName)
						}
					}
					if oldModule != "" {
						emitModuleChangeExtensions(output, typeDef, oldModule, newModule, opts, typeExists, providerLookup)
					}
				}
			}
		}
	}
}

// prefixExtension applies prefix renaming to an extension's target type name,
// field type references, and directive arguments.
func prefixExtension(ext *ast.Definition, prefix string, sourceNames map[string]bool) {
	// Prefix the target type name
	if sourceNames[ext.Name] {
		ext.Name = prefix + "_" + ext.Name
	}

	// Prefix field type references
	for _, f := range ext.Fields {
		rules.RenameTypeRefs(f.Type, prefix, sourceNames)
		for _, d := range f.Directives.ForNames("field_references") {
			rules.RenameDirectiveArgIfSource(d, "references_name", prefix, sourceNames)
		}
	}

	// Prefix directive arguments on the extension itself
	for _, d := range ext.Directives.ForNames("references") {
		rules.RenameDirectiveArgIfSource(d, "references_name", prefix, sourceNames)
		rules.RenameDirectiveArgIfSource(d, "m2m_name", prefix, sourceNames)
	}
}

// stripControlDirectivesFromField returns a copy of the field with DDL control
// directives (@drop, @replace, @if_not_exists) removed.
func stripControlDirectivesFromField(f *ast.FieldDefinition) *ast.FieldDefinition {
	clone := *f
	clone.Directives = base.StripControlDirectives(f.Directives)
	return &clone
}

// emitDropDDL generates @drop(if_exists: true) definitions for a type and all
// its derived types (filter, aggregation, mutation inputs, etc.), plus
// @drop(if_exists: true) fields on shared types (Query, Mutation, _join, _join_aggregation).
func emitDropDDL(output *indexedOutput, def *ast.Definition, opts base.Options, sourceNames map[string]bool) {
	// Apply prefix to get the compiled type name
	compiledName := def.Name
	if opts.Prefix != "" && sourceNames[def.Name] {
		compiledName = opts.Prefix + "_" + def.Name
	}

	// fieldName is the query/mutation field name (original when AsModule)
	fieldName := compiledName
	if opts.AsModule {
		fieldName = def.Name
	}

	pos := base.CompiledPos("incremental-drop")

	// Drop the type itself
	output.AddDefinition(dropDefinition(compiledName, def.Kind, pos))

	// Drop all derived types
	for _, derived := range base.DerivedTypeNames(compiledName) {
		output.AddDefinition(dropDefinition(derived, ast.InputObject, pos))
	}

	// Drop fields on shared types (Query, Mutation, _join, _join_aggregation)
	for targetType, fieldNames := range base.DerivedExtensionFields(compiledName, fieldName) {
		emitDropFieldsExtension(output, targetType, fieldNames, pos)
	}
}

// emitDropFieldsExtension emits an extension for targetType with @drop(if_exists:true)
// fields for each field name. Used to clean up fields on shared types like Query,
// Mutation, _join, _join_aggregation when a data type is dropped or replaced.
func emitDropFieldsExtension(output *indexedOutput, targetType string, fieldNames []string, pos *ast.Position) {
	var fields ast.FieldList
	for _, name := range fieldNames {
		fields = append(fields, &ast.FieldDefinition{
			Name: name,
			Type: ast.NamedType("Boolean", pos), // stub type — field will be dropped
			Directives: ast.DirectiveList{
				{
					Name: base.DropDirectiveName,
					Arguments: ast.ArgumentList{
						{Name: "if_exists", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
					},
					Position: pos,
				},
			},
			Position: pos,
		})
	}
	output.AddExtension(&ast.Definition{
		Kind:     ast.Object,
		Name:     targetType,
		Position: pos,
		Fields:   fields,
	})
}

// dropDefinition creates a definition with @drop(if_exists: true).
func dropDefinition(name string, kind ast.DefinitionKind, pos *ast.Position) *ast.Definition {
	return &ast.Definition{
		Kind: kind,
		Name: name,
		Directives: ast.DirectiveList{
			{
				Name: base.DropDirectiveName,
				Arguments: ast.ArgumentList{
					{Name: "if_exists", Value: &ast.Value{Raw: "true", Kind: ast.BooleanValue, Position: pos}, Position: pos},
				},
				Position: pos,
			},
		},
		Position: pos,
		Fields: ast.FieldList{
			{Name: "_stub", Type: ast.NamedType("Boolean", pos), Position: pos},
		},
	}
}

// catalogDirective creates a @catalog(name, engine) directive.
func catalogDirective(name, engine string) *ast.Directive {
	pos := base.CompiledPos("incremental")
	return &ast.Directive{
		Name: base.CatalogDirectiveName,
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
			{Name: "engine", Value: &ast.Value{Raw: engine, Kind: ast.StringValue, Position: pos}, Position: pos},
		},
		Position: pos,
	}
}

// recoverProviderObjects registers ObjectInfo for all compiled types in the
// provider that belong to the same catalog. GENERATE rules need this info
// when processing references to existing types.
func recoverProviderObjects(
	ctx context.Context,
	provider base.Provider,
	cctx *compilationContext,
	opts base.Options,
) {
	catalogName := opts.Name
	for _, def := range providerDefsForCatalog(ctx, provider, catalogName) {
		if cctx.GetObject(def.Name) != nil {
			continue // already registered (from change defs)
		}
		info := base.RecoverObjectInfo(def)
		cctx.RegisterObject(def.Name, info)
	}
}

// providerDefsForCatalog returns all definitions in the provider that belong
// to the given catalog.
func providerDefsForCatalog(ctx context.Context, provider base.Provider, catalogName string) []*ast.Definition {
	var defs []*ast.Definition
	for _, def := range provider.Types(ctx) {
		if base.DefinitionCatalog(def) == catalogName {
			defs = append(defs, def)
		}
	}
	return defs
}

// incrementalSource wraps change definitions as a DefinitionsSource (and
// optionally ExtensionsSource). Definitions() yields only the change
// definitions (for rule processing), but ForName() can also resolve types
// from the baseCatalog. Extensions() yields reference extensions that need
// full pipeline processing.
type incrementalSource struct {
	defs        []*ast.Definition
	defMap      map[string]*ast.Definition
	exts        []*ast.Definition
	baseCatalog base.DefinitionsSource
}

func newIncrementalSourceWithExtensions(defs []*ast.Definition, baseCatalog base.DefinitionsSource, exts []*ast.Definition) *incrementalSource {
	defMap := make(map[string]*ast.Definition, len(defs))
	for _, d := range defs {
		defMap[d.Name] = d
	}
	return &incrementalSource{defs: defs, defMap: defMap, exts: exts, baseCatalog: baseCatalog}
}

func (s *incrementalSource) ForName(ctx context.Context, name string) *ast.Definition {
	if d, ok := s.defMap[name]; ok {
		return d
	}
	return s.baseCatalog.ForName(ctx, name)
}

func (s *incrementalSource) DirectiveForName(ctx context.Context, name string) *ast.DirectiveDefinition {
	return s.baseCatalog.DirectiveForName(ctx, name)
}

func (s *incrementalSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.defs {
			if !yield(d) {
				return
			}
		}
	}
}

func (s *incrementalSource) DirectiveDefinitions(ctx context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return s.baseCatalog.DirectiveDefinitions(ctx)
}

// Extensions implements base.ExtensionsSource for reference extensions.
func (s *incrementalSource) Extensions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.exts {
			if !yield(d) {
				return
			}
		}
	}
}

// DefinitionExtensions returns extensions for a specific type name.
func (s *incrementalSource) DefinitionExtensions(_ context.Context, name string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.exts {
			if d.Name == name {
				if !yield(d) {
					return
				}
			}
		}
	}
}
