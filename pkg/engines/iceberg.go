package engines

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

var (
	_ Engine                         = &Iceberg{}
	_ EngineAggregator               = &Iceberg{}
	_ EngineVectorDistanceCalculator = &Iceberg{}
)

// Iceberg engine delegates all SQL generation to DuckDB but reports
// SupportTimeTravel: true in its capabilities.
type Iceberg struct {
	duckdb *DuckDB
}

func NewIceberg() *Iceberg {
	return &Iceberg{duckdb: NewDuckDB()}
}

func (e *Iceberg) Type() Type { return TypeIceberg }

func (e *Iceberg) Capabilities() *compiler.EngineCapabilities {
	dbCaps := e.duckdb.Capabilities()
	caps := *dbCaps // defensive copy
	caps.General.SupportTimeTravel = true
	// DuckDB Iceberg extension doesn't support targeted inserts (INSERT INTO tbl(col1,col2) VALUES ...)
	caps.Insert.Insert = false
	caps.Insert.Returning = false
	caps.Insert.InsertReferences = false
	return &caps
}

func (e *Iceberg) SQLValue(v any) (string, error) {
	return e.duckdb.SQLValue(v)
}

func (e *Iceberg) FunctionCall(name string, positional []any, named map[string]any) (string, error) {
	return e.duckdb.FunctionCall(name, positional, named)
}

func (e *Iceberg) RepackObject(sqlName string, field *ast.Field) string {
	return e.duckdb.RepackObject(sqlName, field)
}

func (e *Iceberg) UnpackObjectToFieldList(sqlName string, field *ast.Field) string {
	return e.duckdb.UnpackObjectToFieldList(sqlName, field)
}

func (e *Iceberg) FilterOperationSQLValue(sqlName, path, op string, value any, params []any) (string, []any, error) {
	return e.duckdb.FilterOperationSQLValue(sqlName, path, op, value, params)
}

func (e *Iceberg) FieldValueByPath(sqlName, path string) string {
	return e.duckdb.FieldValueByPath(sqlName, path)
}

func (e *Iceberg) PackFieldsToObject(prefix string, field *ast.Field) string {
	return e.duckdb.PackFieldsToObject(prefix, field)
}

func (e *Iceberg) MakeObject(fields map[string]string) string {
	return e.duckdb.MakeObject(fields)
}

func (e *Iceberg) AddObjectFields(sqlName string, fields map[string]string) string {
	return e.duckdb.AddObjectFields(sqlName, fields)
}

func (e *Iceberg) ApplyFieldTransforms(ctx context.Context, qe types.Querier, sql string, field *ast.Field, args sdl.FieldQueryArguments, params []any) (string, []any, error) {
	return e.duckdb.ApplyFieldTransforms(ctx, qe, sql, field, args, params)
}

func (e *Iceberg) ExtractJSONStruct(sql string, jsonStruct map[string]any) string {
	return e.duckdb.ExtractJSONStruct(sql, jsonStruct)
}

func (e *Iceberg) TimestampTransform(sql string, field *ast.Field, args sdl.FieldQueryArguments) string {
	return e.duckdb.TimestampTransform(sql, field, args)
}

func (e *Iceberg) ExtractNestedTypedValue(sql, path, t string) string {
	return e.duckdb.ExtractNestedTypedValue(sql, path, t)
}

func (e *Iceberg) LateralJoin(sql, alias string) string {
	return e.duckdb.LateralJoin(sql, alias)
}

// EngineAggregator interface
func (e *Iceberg) AggregateFuncSQL(funcName, sql, path, factor string, originField *ast.FieldDefinition, isHyperTable bool, args map[string]any, params []any) (string, []any, error) {
	return e.duckdb.AggregateFuncSQL(funcName, sql, path, factor, originField, isHyperTable, args, params)
}

func (e *Iceberg) AggregateFuncAny(sql string) string {
	return e.duckdb.AggregateFuncAny(sql)
}

func (e *Iceberg) JSONTypeCast(sql string) string {
	return e.duckdb.JSONTypeCast(sql)
}

// EngineVectorDistanceCalculator interface
func (e *Iceberg) VectorDistanceSQL(sql, distMetric string, vector types.Vector, params []any) (string, []any, error) {
	return e.duckdb.VectorDistanceSQL(sql, distMetric, vector, params)
}
