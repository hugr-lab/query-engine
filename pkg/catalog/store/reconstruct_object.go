package store

import (
	"context"
	"database/sql"
	"encoding/json"

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
func reconstructDataObject(ctx context.Context, s *Store, name string) *ast.Definition {
	row, ok := s.readDataObject(ctx, name)
	if !ok {
		return nil
	}
	g := s.genCtx()
	t := g.objectTraits(ctx, row)
	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        row.Name,
		Description: row.Description,
		Directives:  emitObjectDirectives(row),
		Position:    reconPos,
	}
	def.Directives = append(def.Directives, catalogDirective(row.DataSource, t.src.Engine))

	// Enriched @references: own physical legs, then the m2m projections.
	fwd := s.relationsBySource(ctx, name)
	for _, r := range fwd {
		targetDesc := ""
		if target, ok := s.readDataObject(ctx, r.Destination); ok {
			targetDesc = target.Description
		}
		def.Directives = append(def.Directives, referencesDirective(r, targetDesc, row.Description))
	}
	for _, leg := range s.relationsByDestination(ctx, name) {
		if !leg.m2mJunction {
			continue
		}
		junctionDesc := ""
		if junction, ok := s.readDataObject(ctx, leg.Source); ok {
			junctionDesc = junction.Description
		}
		for _, co := range s.relationsBySource(ctx, leg.Source) {
			if co.Name == leg.Name {
				continue
			}
			def.Directives = append(def.Directives, m2mReferencesProjection(leg, co, junctionDesc))
		}
	}

	// Derived-type markers + the queryShapes @query/@mutation set.
	def.Directives = append(def.Directives,
		directive(base.FilterInputDirectiveName, strArg(base.ArgName, row.Name+filterSuffix)))
	if t.needsListFilter(len(fwd) > 0, s.isM2MEndpoint(ctx, name)) {
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
	if fn := s.lastListReturningModuleFunction(ctx, row.Name); fn != "" {
		replaceQueryMarkerName(def, "AGGREGATE", fn+"_aggregation")
		replaceQueryMarkerName(def, "AGGREGATE_BUCKET", fn+"_bucket_aggregation")
	}

	for _, f := range t.fields {
		fd := reconstructField(f)
		if f.DependencyDataSource != "" {
			// Extension-source fields carry @dependency instead of @catalog.
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
		fieldRules[i].apply(ctx, g, t, def)
	}
	return def
}

// lastListReturningModuleFunction returns the name of the LAST (module, name)
// ordered module function (kind=function, module != '') returning a list of
// the object — the one whose aggregation names win the def markers.
func (s *Store) lastListReturningModuleFunction(ctx context.Context, typeName string) string {
	variants := `'[` + typeName + `]', '[` + typeName + `!]', '[` + typeName + `]!', '[` + typeName + `!]!'`
	names := s.queryNames(ctx, `SELECT f.name FROM core.catalog.functions f`+
		activeMeta("m", "f.data_source")+`
		WHERE f.kind = 'function' AND f.module <> '' AND f.returns IN (`+variants+`)
		ORDER BY f.module, f.name`)
	if len(names) == 0 {
		return ""
	}
	return names[len(names)-1]
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
func (s *Store) isM2MEndpoint(ctx context.Context, name string) bool {
	for _, leg := range s.relationsByDestination(ctx, name) {
		if leg.m2mJunction {
			return true
		}
	}
	return false
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

// activeSource is the per-source meta the generation layer branches on
// (activeSources / activeEngines read it for ACTIVE sources only).
type activeSource struct {
	Engine   string
	ReadOnly bool
	AsModule bool
}

func (s *Store) activeSources(ctx context.Context) map[string]activeSource {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT data_source, engine, read_only, as_module FROM core.catalog.data_source_meta
		WHERE loaded AND NOT disabled AND NOT suspended`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	out := map[string]activeSource{}
	for rows.Next() {
		var source string
		var meta activeSource
		if err := rows.Scan(&source, &meta.Engine, &meta.ReadOnly, &meta.AsModule); err != nil {
			return out
		}
		out[source] = meta
	}
	return out
}

func (s *Store) activeEngines(ctx context.Context) map[string]string {
	srcs := s.activeSources(ctx)
	out := make(map[string]string, len(srcs))
	for name, meta := range srcs {
		out[name] = meta.Engine
	}
	return out
}

// --- catalog.* readers (fill the shared entity models) ---

func (s *Store) readDataObject(ctx context.Context, name string) (*dataObject, bool) {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil, false
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
		return nil, false
	}
	r.Description = desc.String
	if props.Valid {
		_ = json.Unmarshal([]byte(props.String), r.Properties)
	}
	return &r, true
}

func (s *Store) readFields(ctx context.Context, typeName string) []*field {
	conn, err := s.pool.Conn(ctx)
	if err != nil {
		return nil
	}
	defer conn.Close()
	rows, err := conn.Query(ctx, `SELECT f.name, f.field_type, f.properties::JSON::VARCHAR,
		f.args::JSON::VARCHAR, f.data_source, f.dependency_data_source, f.is_pk, f.ordinal,
		f.deprecation_reason, f.description
		FROM core.catalog.fields f`+activeMeta("m", "f.data_source")+`
		WHERE f.type_name = `+lit(typeName)+` ORDER BY f.ordinal, f.name`)
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []*field
	for rows.Next() {
		f := field{TypeName: typeName, Properties: &fieldProperties{}}
		var props, args, dependency, deprecated, desc sql.NullString
		if err := rows.Scan(&f.Name, &f.FieldType, &props, &args, &f.DataSource, &dependency, &f.IsPK, &f.Ordinal, &deprecated, &desc); err != nil {
			return out
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
		out = append(out, &f)
	}
	return out
}
