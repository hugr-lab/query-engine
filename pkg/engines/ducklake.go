package engines

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	_ Engine                       = &DuckLake{}
	_ EngineAggregator             = &DuckLake{}
	_ EngineVectorDistanceCalculator = &DuckLake{}
)

// DuckLake engine delegates all SQL generation to DuckDB but reports
// SupportTimeTravel: true in its capabilities.
type DuckLake struct {
	duckdb *DuckDB
}

func NewDuckLake() *DuckLake {
	return &DuckLake{duckdb: NewDuckDB()}
}

func (e *DuckLake) Type() Type { return TypeDuckLake }

func (e *DuckLake) Capabilities() *compiler.EngineCapabilities {
	caps := e.duckdb.Capabilities()
	caps.General.SupportTimeTravel = true
	return caps
}

func (e *DuckLake) SQLValue(v any) (string, error) {
	return e.duckdb.SQLValue(v)
}

func (e *DuckLake) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
	return e.duckdb.FunctionCall(name, positional, named)
}

func (e *DuckLake) RepackObject(sqlName string, field *ast.Field) string {
	return e.duckdb.RepackObject(sqlName, field)
}

func (e *DuckLake) UnpackObjectToFieldList(sqlName string, field *ast.Field) string {
	return e.duckdb.UnpackObjectToFieldList(sqlName, field)
}

func (e *DuckLake) FilterOperationSQLValue(sqlName, path, op string, value any, params []any) (string, []any, error) {
	return e.duckdb.FilterOperationSQLValue(sqlName, path, op, value, params)
}

func (e *DuckLake) FieldValueByPath(sqlName, path string) string {
	return e.duckdb.FieldValueByPath(sqlName, path)
}

func (e *DuckLake) PackFieldsToObject(prefix string, field *ast.Field) string {
	return e.duckdb.PackFieldsToObject(prefix, field)
}

func (e *DuckLake) MakeObject(fields map[string]string) string {
	return e.duckdb.MakeObject(fields)
}

func (e *DuckLake) AddObjectFields(sqlName string, fields map[string]string) string {
	return e.duckdb.AddObjectFields(sqlName, fields)
}

func (e *DuckLake) ApplyFieldTransforms(ctx context.Context, qe types.Querier, sql string, field *ast.Field, args sdl.FieldQueryArguments, params []any) (string, []any, error) {
	return e.duckdb.ApplyFieldTransforms(ctx, qe, sql, field, args, params)
}

func (e *DuckLake) ExtractJSONStruct(sql string, jsonStruct map[string]any) string {
	return e.duckdb.ExtractJSONStruct(sql, jsonStruct)
}

func (e *DuckLake) TimestampTransform(sql string, field *ast.Field, args sdl.FieldQueryArguments) string {
	return e.duckdb.TimestampTransform(sql, field, args)
}

func (e *DuckLake) ExtractNestedTypedValue(sql, path, t string) string {
	return e.duckdb.ExtractNestedTypedValue(sql, path, t)
}

func (e *DuckLake) LateralJoin(sql, alias string) string {
	return e.duckdb.LateralJoin(sql, alias)
}

// EngineAggregator interface
func (e *DuckLake) AggregateFuncSQL(funcName, sql, path, factor string, originField *ast.FieldDefinition, isHyperTable bool, args map[string]any, params []any) (string, []any, error) {
	return e.duckdb.AggregateFuncSQL(funcName, sql, path, factor, originField, isHyperTable, args, params)
}

func (e *DuckLake) AggregateFuncAny(sql string) string {
	return e.duckdb.AggregateFuncAny(sql)
}

func (e *DuckLake) JSONTypeCast(sql string) string {
	return e.duckdb.JSONTypeCast(sql)
}

// EngineVectorDistanceCalculator interface
func (e *DuckLake) VectorDistanceSQL(sql, distMetric string, vector types.Vector, params []any) (string, []any, error) {
	return e.duckdb.VectorDistanceSQL(sql, distMetric, vector, params)
}
