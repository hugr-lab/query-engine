package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// reconstructDataObject rebuilds the COMPILED object definition from
// catalog.*. The directive↔bag pair tables (pairs_object.go / pairs_field.go)
// re-attach every stored directive; on top of them:
//   - @catalog(name, engine) — from the row's data_source + data_source_meta
//     (object level always; field level for virtual and foreign-owned fields);
//   - enriched @references — the object's physical legs plus the synthesized
//     m2m projections through is_m2m junctions;
//   - @field_references mirrored back onto field-declared leg columns;
//   - the def-level markers: @filter_input / @filter_list_input /
//     @data_input×2 and the queryShapes @query/@mutation set;
//   - the fieldRules contributions (nav fields, subquery args, aggregation
//     twins, extra fields, shared members — gen_fields.go).
//
// All reads are activity-gated.
func reconstructDataObject(ctx context.Context, g *genContext, name string) (*ast.Definition, error) {
	row, err := g.readDataObject(ctx, name)
	if err != nil || row == nil {
		return nil, err
	}
	t, err := g.objectTraits(ctx, row)
	if err != nil {
		return nil, err
	}
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        row.Name,
		Description: row.Description,
		Directives:  emitObjectDirectives(row),
		Position:    reconPos,
	}
	def.Directives = append(def.Directives, catalogDirective(row.DataSource, t.src.Engine))

	// Enriched @references: own physical legs, then the m2m projections.
	fwd, err := g.relationsBySource(ctx, name)
	if err != nil {
		return nil, err
	}
	for _, r := range fwd {
		targetDesc := ""
		if target, err := g.readDataObject(ctx, r.Destination); err != nil {
			return nil, err
		} else if target != nil {
			targetDesc = target.Description
		}
		def.Directives = append(def.Directives, referencesDirective(r, targetDesc, row.Description))
	}
	legs, err := g.relationsByDestination(ctx, name)
	if err != nil {
		return nil, err
	}
	for _, leg := range legs {
		if !leg.m2mJunction {
			continue
		}
		junctionDesc := ""
		if junction, err := g.readDataObject(ctx, leg.Source); err != nil {
			return nil, err
		} else if junction != nil {
			junctionDesc = junction.Description
		}
		cos, err := g.relationsBySource(ctx, leg.Source)
		if err != nil {
			return nil, err
		}
		for _, co := range cos {
			if co.Name == leg.Name {
				continue
			}
			def.Directives = append(def.Directives, m2mReferencesProjection(leg, co, junctionDesc))
		}
	}

	// Derived-type markers + the queryShapes @query/@mutation set.
	def.Directives = append(def.Directives,
		directive(base.FilterInputDirectiveName, strArg(base.ArgName, row.Name+filterSuffix)))
	m2mEndpoint, err := g.isM2MEndpoint(ctx, name)
	if err != nil {
		return nil, err
	}
	if t.needsListFilter(len(fwd) > 0, m2mEndpoint) {
		def.Directives = append(def.Directives,
			directive(base.FilterListInputDirectiveName, strArg(base.ArgName, row.Name+listFilterSuffix)))
	}
	if t.writable() {
		def.Directives = append(def.Directives,
			directive(base.DataInputDirectiveName, strArg(base.ArgName, row.Name+mutDataSuffix)),
			directive(base.DataInputDirectiveName, strArg(base.ArgName, row.Name+mutInputDataSuffix)))
	}
	def.Directives = append(def.Directives, shapeMarkers(t)...)
	// Module functions returning a LIST of this object rename its
	// AGGREGATE/AGGREGATE_BUCKET markers to the function-based names — the
	// old compiler's module-function precedence, replicated by the assembler
	// (replaceOrAddQueryDirective); the LAST function in (module, name) order
	// wins.
	fn, err := g.lastListReturningModuleFunction(ctx, row.Name)
	if err != nil {
		return nil, err
	}
	if fn != "" {
		replaceQueryMarkerName(def, "AGGREGATE", fn+"_aggregation")
		replaceQueryMarkerName(def, "AGGREGATE_BUCKET", fn+"_bucket_aggregation")
	}

	for _, f := range t.fields {
		fd := reconstructField(f)
		if f.DependencyDataSource != "" {
			// Extension-source fields carry @dependency; VIRTUAL ones
			// additionally carry @catalog of the source their data comes from
			// — resolved into the field's data_source at WRITE time
			// (resolveVirtualAttribution), read back verbatim here.
			if isVirtualStoreField(f) {
				fd.Directives = append(fd.Directives, catalogDirective(f.DataSource, t.srcs[f.DataSource].Engine))
			}
			fd.Directives = append(fd.Directives,
				directive(base.DependencyDirectiveName, strArg(base.ArgName, f.DependencyDataSource)))
		} else if f.DataSource != row.DataSource || isVirtualStoreField(f) {
			fd.Directives = append(fd.Directives, catalogDirective(f.DataSource, t.srcs[f.DataSource].Engine))
		}
		def.Fields = append(def.Fields, fd)
	}
	// Field-declared legs mirror @field_references onto their column.
	for _, r := range fwd {
		if !r.FieldDeclared || len(r.SourceKeys) != 1 {
			continue
		}
		if fd := def.Fields.ForName(r.SourceKeys[0]); fd != nil {
			fd.Directives = append(fd.Directives, fieldReferencesDirective(r))
		}
	}

	for i := range fieldRules {
		if err := fieldRules[i].apply(ctx, g, t, def); err != nil {
			return nil, err
		}
	}
	return def, nil
}

// lastListReturningModuleFunction returns the name of the LAST (module, name)
// ordered module function (kind=function, module != ”) returning a list of
// the object — the one whose aggregation names win the def markers.
func (g *genContext) lastListReturningModuleFunction(ctx context.Context, typeName string) (string, error) {
	variants := lit("["+typeName+"]") + `, ` + lit("["+typeName+"!]") + `, ` +
		lit("["+typeName+"]!") + `, ` + lit("["+typeName+"!]!")
	names, err := g.queryNames(ctx, `SELECT f.name FROM core.catalog.functions f`+
		activeMeta("m", "f.data_source")+`
		WHERE f.kind = 'function' AND f.module <> '' AND f.returns IN (`+variants+`)
		ORDER BY f.module, f.name`)
	if err != nil || len(names) == 0 {
		return "", err
	}
	return names[len(names)-1], nil
}

func replaceQueryMarkerName(def *ast.Definition, queryType, name string) {
	for _, d := range def.Directives {
		if d.Name != "query" || base.DirectiveArgString(d, "type") != queryType {
			continue
		}
		if a := d.Arguments.ForName(base.ArgName); a != nil {
			a.Value.Raw = name
		}
		return
	}
}

// needsListFilter: X_list_filter exists when some filter lists X — X is the
// many side of a back projection (it declares non-m2m legs with back
// navigation) or an m2m endpoint. An is_m2m junction is never listed, and a
// parameterized view is excluded from filter nesting entirely.
func (t *objectTraits) needsListFilter(hasBackProjections, isM2MEndpoint bool) bool {
	if t.row.Properties != nil && t.row.Properties.ArgsTypeName != "" {
		return false
	}
	return !t.isM2M() && (hasBackProjections || isM2MEndpoint)
}

// isM2MEndpoint reports whether some is_m2m junction leg points at the object.
func (g *genContext) isM2MEndpoint(ctx context.Context, name string) (bool, error) {
	legs, err := g.relationsByDestination(ctx, name)
	if err != nil {
		return false, err
	}
	for _, leg := range legs {
		if leg.m2mJunction {
			return true, nil
		}
	}
	return false, nil
}

// m2mReferencesProjection is the endpoint-oriented @references the compiler
// stamps on both m2m sides (m2mReferencesDirective): keys re-oriented
// endpoint→junction, query names from the two legs, m2m_name = junction.
func m2mReferencesProjection(leg *relationEdge, co *relation, junctionDesc string) *ast.Directive {
	return directive(base.ReferencesDirectiveName,
		strArg(base.ArgName, leg.Name),
		strArg(base.ArgReferencesName, co.Destination),
		strListArg(base.ArgSourceFields, leg.DestinationKeys),
		strListArg(base.ArgReferencesFields, leg.SourceKeys),
		strArg(base.ArgQuery, orDefault(leg.DestinationField, leg.Source)),
		strArg(base.ArgReferencesQuery, orDefault(co.DestinationField, co.Source)),
		boolArg(base.ArgIsM2M, true),
		strArg(base.ArgM2MName, leg.Source),
		strArg(base.ArgDescription, junctionDesc),
		strArg(base.ArgReferencesDescription, junctionDesc),
	)
}

// reconstructField rebuilds a base field definition via the field pair table.
func reconstructField(f *field) *ast.FieldDefinition {
	return &ast.FieldDefinition{
		Name:        f.Name,
		Type:        parseFieldType(f.FieldType),
		Description: f.Description,
		Arguments:   emitArgumentDefs(f.Args),
		Directives:  emitFieldDirectives(f),
		Position:    reconPos,
	}
}

// catalogDirective rebuilds @catalog(name, engine) — name IS the data source;
// the engine comes from data_source_meta (activeEngines).
func catalogDirective(dataSource, engine string) *ast.Directive {
	return directive(base.CatalogDirectiveName,
		strArg(base.ArgName, dataSource), strArg(argEngine, engine))
}

// referencesDirective rebuilds an object-level @references from a stored
// relation row with the compiler's enrichment: is_m2m/m2m_name always
// present, descriptions defaulting to the two objects' descriptions. A
// FIELD-declared leg always carries the OBJECT descriptions here — the
// compiler's @field_references→@references conversion drops the leg's own
// description args (they survive only on the filter-field copy).
func referencesDirective(r *relation, targetDesc, sourceDesc string) *ast.Directive {
	description := r.SourceFieldDescription
	if description == "" || r.FieldDeclared {
		description = targetDesc
	}
	referencesDescription := r.DestinationFieldDescription
	if referencesDescription == "" || r.FieldDeclared {
		referencesDescription = sourceDesc
	}
	return directive(base.ReferencesDirectiveName,
		strArg(base.ArgName, r.Name),
		strArg(base.ArgReferencesName, r.Destination),
		strListArg(base.ArgSourceFields, r.SourceKeys),
		strListArg(base.ArgReferencesFields, r.DestinationKeys),
		strArg(base.ArgQuery, r.SourceField),
		strArg(base.ArgReferencesQuery, orDefault(r.DestinationField, r.Source)),
		boolArg(base.ArgIsM2M, false),
		strArg(base.ArgM2MName, ""),
		strArg(base.ArgDescription, description),
		strArg(base.ArgReferencesDescription, referencesDescription),
	)
}

// dependencyDirective is the @dependency(name:) marker dependency tracking
// reads: stamped on extension-contributed fields and on every GENERATED
// member of an extension-owned object.
func dependencyDirective(name string) *ast.Directive {
	return directive(base.DependencyDirectiveName, strArg(base.ArgName, name))
}

// joinMachineryTarget resolves whether a declared @join field gets its
// generated surface (subquery args, aggregation twins, aggregation members).
// EXTENSION-contributed joins (the only legal cross-source form — the
// validator rejects @join to a foreign object in a regular source) get the
// machinery whenever the target is visible; a same-source declared join
// additionally requires the attribution match (a legacy cross-source row
// reads as a bare field). The lookup runs through the memoized session
// readers, so the target's source lands in the provenance set either way — a
// target flag flip evicts the cached definition.
//
// The target ROW comes back with it: whether the join field takes an `args`
// argument depends on the target being a parameterized view, which only the
// row knows.
func (g *genContext) joinMachineryTarget(ctx context.Context, f *field) (*dataObject, bool, error) {
	target := parseFieldType(f.FieldType).Name()
	row, err := g.readDataObject(ctx, target)
	if err != nil {
		return nil, false, err
	}
	if row == nil {
		return nil, false, nil
	}
	if f.DependencyDataSource == "" && row.DataSource != f.DataSource {
		return row, false, nil
	}
	return row, true, nil
}

// activeSource is the per-source meta the generation layer branches on
// (activeSources / activeEngines read it for ACTIVE sources only).
type activeSource struct {
	Engine      string
	ReadOnly    bool
	AsModule    bool
	IsExtension bool
}

// activeSources reads the active-source meta map, memoized per session (the
// single hottest lookup — the generation rules consult it repeatedly). The
// meta itself records no touched source: a definition's dependency on a
// source comes from the ROWS it reads, not from the meta consult.
func (g *genContext) activeSources(ctx context.Context) (map[string]activeSource, error) {
	if g.haveSources {
		return g.sources, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read source meta: %w", err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT data_source, engine, read_only, as_module, is_extension FROM core.catalog.data_source_meta
		WHERE loaded AND NOT disabled AND NOT suspended`)
	if err != nil {
		return nil, fmt.Errorf("read source meta: %w", err)
	}
	defer rows.Close()
	out := map[string]activeSource{}
	for rows.Next() {
		var source string
		var meta activeSource
		if err := rows.Scan(&source, &meta.Engine, &meta.ReadOnly, &meta.AsModule, &meta.IsExtension); err != nil {
			return nil, fmt.Errorf("read source meta: %w", err)
		}
		out[source] = meta
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read source meta: %w", err)
	}
	g.sources, g.haveSources = out, true
	return out, nil
}

func (g *genContext) activeEngines(ctx context.Context) (map[string]string, error) {
	srcs, err := g.activeSources(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(srcs))
	for name, meta := range srcs {
		out[name] = meta.Engine
	}
	return out, nil
}

// --- catalog.* readers (fill the shared entity models) ---

// readDataObject reads one data-object row; absent = (nil, nil). An object
// whose declared @dependency sources (cross-source views, requirements: the
// validator enforces the declaration) are not ALL active reads as absent —
// the dependency sources are still recorded as provenance, so a flag flip on
// a dependency evicts every cached definition that consulted this object.
func (g *genContext) readDataObject(ctx context.Context, name string) (*dataObject, error) {
	if row, ok := g.objects[name]; ok {
		return row, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read data object %s: %w", name, err)
	}
	defer conn.Close()
	r := dataObject{Properties: &dataObjectProperties{}}
	var props, desc sql.NullString
	err = conn.QueryRow(ctx, `SELECT o.name, o.original_name, o.data_source, o.module, o.kind,
		o.properties::JSON::VARCHAR, o.description
		FROM core.catalog.data_objects o`+activeMeta("m", "o.data_source")+`
		WHERE o.name = `+lit(name)).
		Scan(&r.Name, &r.OriginalName, &r.DataSource, &r.Module, &r.Kind, &props, &desc)
	if err != nil {
		if err != sql.ErrNoRows {
			return nil, fmt.Errorf("read data object %s: %w", name, err)
		}
		if g.objects == nil {
			g.objects = map[string]*dataObject{}
		}
		g.objects[name] = nil
		return nil, nil
	}
	r.Description = desc.String
	if props.Valid {
		_ = json.Unmarshal([]byte(props.String), r.Properties)
	}
	g.touch(r.DataSource)
	if g.objects == nil {
		g.objects = map[string]*dataObject{}
	}
	if len(r.Properties.Dependencies) > 0 {
		g.touch(r.Properties.Dependencies...)
		srcs, err := g.activeSources(ctx)
		if err != nil {
			return nil, err
		}
		for _, dep := range r.Properties.Dependencies {
			if _, ok := srcs[dep]; !ok {
				// An unregistered dependency counts as inactive too.
				g.objects[name] = nil
				return nil, nil
			}
		}
	}
	g.objects[name] = &r
	return &r, nil
}

func (g *genContext) readFields(ctx context.Context, typeName string) ([]*field, error) {
	if out, ok := g.fieldRows[typeName]; ok {
		return out, nil
	}
	conn, err := g.s.pool.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("read fields of %s: %w", typeName, err)
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT f.name, f.field_type, f.properties::JSON::VARCHAR,
		f.args::JSON::VARCHAR, f.data_source, f.dependency_data_source, f.is_pk, f.ordinal,
		f.deprecation_reason, f.description
		FROM core.catalog.fields f`+activeMeta("m", "f.data_source")+`
		WHERE f.type_name = `+lit(typeName)+` ORDER BY f.ordinal, f.name`)
	if err != nil {
		return nil, fmt.Errorf("read fields of %s: %w", typeName, err)
	}
	defer rows.Close()
	var out []*field
	hasDependencies := false
	for rows.Next() {
		f := field{TypeName: typeName, Properties: &fieldProperties{}}
		var props, args, dependency, deprecated, desc sql.NullString
		if err := rows.Scan(&f.Name, &f.FieldType, &props, &args, &f.DataSource, &dependency, &f.IsPK, &f.Ordinal, &deprecated, &desc); err != nil {
			return nil, fmt.Errorf("read fields of %s: %w", typeName, err)
		}
		f.DependencyDataSource = dependency.String
		f.DeprecationReason = deprecated.String
		f.Description = desc.String
		if props.Valid {
			_ = json.Unmarshal([]byte(props.String), f.Properties)
		}
		if args.Valid {
			_ = json.Unmarshal([]byte(args.String), &f.Args)
		}
		// Provenance is recorded for EVERY row — including fields hidden by
		// the dependency gate below, so a dependency flag flip evicts the
		// cached definitions built without them.
		g.touch(f.DataSource, f.DependencyDataSource)
		hasDependencies = hasDependencies || f.DependencyDataSource != ""
		out = append(out, &f)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read fields of %s: %w", typeName, err)
	}
	// A field whose dependency source (@dependency — the source its data
	// comes from) is inactive is hidden, mirroring the compiled provider's
	// read-time gate on dependency_catalog (db/read.go).
	if hasDependencies {
		srcs, err := g.activeSources(ctx)
		if err != nil {
			return nil, err
		}
		visible := out[:0]
		for _, f := range out {
			if f.DependencyDataSource != "" {
				if _, ok := srcs[f.DependencyDataSource]; !ok {
					continue
				}
			}
			visible = append(visible, f)
		}
		out = visible
	}
	if g.fieldRows == nil {
		g.fieldRows = map[string][]*field{}
	}
	g.fieldRows[typeName] = out
	return out, nil
}
