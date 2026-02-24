package rules

import (
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var _ base.DefinitionRule = (*CubeHypertableRule)(nil)

// CubeHypertableRule adds measurement_func arguments to @measurement fields
// on @cube objects, and gapfill arguments to @timescale_key Timestamp fields
// on @hypertable objects. It also propagates these arguments to the
// corresponding aggregation type fields.
type CubeHypertableRule struct{}

func (r *CubeHypertableRule) Name() string     { return "CubeHypertableRule" }
func (r *CubeHypertableRule) Phase() base.Phase { return base.PhaseGenerate }

func (r *CubeHypertableRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("cube") != nil || def.Directives.ForName("hypertable") != nil
}

func (r *CubeHypertableRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	info := ctx.GetObject(def.Name)
	if info == nil {
		return nil
	}
	pos := compiledPos(def.Name)
	aggTypeName := "_" + def.Name + "_aggregation"
	aggDef := ctx.LookupType(aggTypeName)

	// @cube: add measurement_func argument to @measurement fields
	if info.IsCube {
		for _, f := range def.Fields {
			if f.Directives.ForName("measurement") == nil {
				continue
			}
			typeName := f.Type.Name()
			s := ctx.ScalarLookup(typeName)
			if s == nil {
				continue
			}
			ma, ok := s.(types.MeasurementAggregatable)
			if !ok {
				continue
			}
			measurementAggType := ma.MeasurementAggregationTypeName()
			mfArg := &ast.ArgumentDefinition{
				Name:        "measurement_func",
				Description: "Aggregation function for measurement field",
				Type:        ast.NamedType(measurementAggType, pos),
				Position:    pos,
			}
			f.Arguments = append(f.Arguments, mfArg)

			// Also add measurement_func to the corresponding aggregation field
			if aggDef != nil {
				if aggField := aggDef.Fields.ForName(f.Name); aggField != nil {
					aggField.Arguments = append(aggField.Arguments, &ast.ArgumentDefinition{
						Name:        "measurement_func",
						Description: "Aggregation function for measurement field",
						Type:        ast.NamedType(measurementAggType, pos),
						Position:    pos,
					})
				}
			}
		}
	}

	// @hypertable: add gapfill argument to @timescale_key Timestamp fields
	if info.IsHypertable {
		for _, f := range def.Fields {
			if f.Directives.ForName("timescale_key") == nil {
				continue
			}
			if f.Type.NamedType != "Timestamp" {
				continue
			}
			gapfillArg := &ast.ArgumentDefinition{
				Name:        "gapfill",
				Description: "Extracts the specified part of the timestamp",
				Type:        ast.NamedType("Boolean", pos),
				Position:    pos,
			}
			f.Arguments = append(f.Arguments, gapfillArg)

			// Also add gapfill to the corresponding aggregation field
			if aggDef != nil {
				if aggField := aggDef.Fields.ForName(f.Name); aggField != nil {
					aggField.Arguments = append(aggField.Arguments, &ast.ArgumentDefinition{
						Name:        "gapfill",
						Description: "Extracts the specified part of the timestamp",
						Type:        ast.NamedType("Boolean", pos),
						Position:    pos,
					})
				}
			}
		}
	}

	return nil
}
