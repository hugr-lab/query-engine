package engines

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

type Type string

const (
	TypePostgres Type = "postgres"
	TypeDuckDB   Type = "duckdb"
	TypeHttp     Type = "http"
)

type Engine interface {
	Type() Type
	SQLValue(any) (string, error)
	FunctionCall(name string, positional []any, named map[string]any) (string, error)
	RepackObject(sqlName string, field *ast.Field) string
	UnpackObjectToFieldList(sqlName string, field *ast.Field) string
	FilterOperationSQLValue(sqlName, path, op string, value any, params []any) (string, []any, error)
	FieldValueByPath(sqlName, path string) string
	PackFieldsToObject(prefix string, field *ast.Field) string
	MakeObject(fields map[string]string) string
	AddObjectFields(sqlName string, fields map[string]string) string
	ApplyFieldTransforms(ctx context.Context, qe types.Querier, sql string, field *ast.Field, args compiler.FieldQueryArguments, params []any) (string, []any, error)
	ExtractJSONStruct(sql string, jsonStruct map[string]any) string
	TimestampTransform(sql string, field *ast.Field, args compiler.FieldQueryArguments) string
	// ExtractNestedTypedValue extracts value from nested field by path and cast it to type
	// type can be one of: number, string, bool, "" (empty string) - for extract json as is
	ExtractNestedTypedValue(sql, path, t string) string
	LateralJoin(sql, alias string) string
}

type EngineAggregator interface {
	// AggregateFuncSQL returns SQL for aggregate function
	Engine
	AggregateFuncSQL(funcName, sql, path, factor string, originField *ast.FieldDefinition, isHyperTable bool, args map[string]any, params []any) (string, []any, error)
	AggregateFuncAny(sql string) string
	JSONTypeCast(sql string) string
}

type EngineQueryScanner interface {
	Engine
	WarpScann(db, query string) string
	WrapExec(db, query string) string
}

type EngineTypeCaster interface {
	Engine
	ToIntermediateType(*ast.Field) (string, error)
	CastFromIntermediateType(field *ast.Field, toJSON bool) (string, error)
}

type EngineVectorDistanceCalculator interface {
	VectorDistanceSQL(sql, distMetric string, vector types.Vector, params []any) (string, []any, error)
}

type EngineKeyWordExtender interface {
	KeyWords() []string
}

func Ident(s string) string {
	if !base.IsValidIdentifier(s) {
		return fmt.Sprintf(`"%s"`, s)
	}
	return s
}

func SQLValueArrayFormatter[T types.ScalarTypes](e Engine, values []T) (string, error) {
	var valueStrings []string
	for _, v := range values {
		s, err := e.SQLValue(v)
		if err != nil {
			return "", err
		}
		valueStrings = append(valueStrings, s)
	}
	return fmt.Sprintf("ARRAY[%s]", strings.Join(valueStrings, ",")), nil
}

var (
	ErrNotEnoughParamsToApply   = fmt.Errorf("not enough params to apply to query")
	ErrNotLastNParamsWasApplied = fmt.Errorf("not all last N params was applied")
)

func ApplyQueryParams(e Engine, query string, params []any) (string, int, error) {
	if len(params) == 0 {
		return query, 0, nil
	}
	out := make([]rune, 0, len(query))

	deep := 0 // deep of string (if deep > 0 we are inside string)
	qr := []rune(query)
	applied := map[int]string{}
	var err error
	sum := 0 // sum of applied params to check if all last N params was applied (compare to of arithmetic progression)
	for i := 0; i < len(qr); i++ {
		r := qr[i]
		if deep != 0 && r == '\'' {
			// reduce deep if we are left from string
			out = append(out, r)
			// escape quote
			deep--
			continue
		}
		if r == '\'' {
			// increase deep if we are out of string
			out = append(out, r)
			deep++
			continue
		}
		if r != '$' || deep != 0 {
			out = append(out, r)
			continue
		}
		// can be a param, check next 3 runes is num
		currentParamNum := 0
		j := 1
		for ; i+j < len(qr) && j < 5; j++ {
			rn := qr[i+j]
			if !isNumRune(rn) {
				break
			}
			if j == 4 {
				currentParamNum = 0
				break
			}
			currentParamNum = 10*currentParamNum + numFromRune(rn)
		}
		if currentParamNum == 0 {
			out = append(out, r)
			continue
		}
		if currentParamNum > len(params) {
			return "", 0, fmt.Errorf("not enough params to apply to query")
		}
		v, ok := applied[currentParamNum]
		if !ok {
			v, err = e.SQLValue(params[currentParamNum-1])
			if err != nil {
				return "", 0, err
			}
			applied[currentParamNum] = v
			sum += currentParamNum
		}
		out = append(out, []rune(v)...)
		i += j - 1
	}
	if float64(sum) != float64(len(applied)*(2*len(params)-len(applied)+1))/2 {
		return "", 0, fmt.Errorf("not all last N params was applied")
	}

	return string(out), len(params) - len(applied), nil
}

func isNumRune(r rune) bool {
	return r >= '0' && r <= '9'
}

func numFromRune(r rune) int {
	return int(r - '0')
}

type SelectedField struct {
	Field  *ast.Field
	OnType string // works for in inline fragments to select only some fields
}

type SelectionSet []SelectedField

func (ss SelectionSet) ForName(name string) *SelectedField {
	for _, s := range ss {
		if s.Field.Name == name {
			return &s
		}
	}
	return nil
}

func (ss SelectionSet) ForAlias(alias string) *SelectedField {
	for _, s := range ss {
		if s.Field.Alias == alias {
			return &s
		}
	}
	return nil
}

func (ss SelectionSet) ForPath(path string) *SelectedField {
	if len(ss) == 0 {
		return nil
	}
	pp := strings.SplitN(path, ".", 2)
	for _, s := range ss {
		if s.Field.Alias == pp[0] {
			if len(pp) == 1 {
				return &s
			}
			return SelectedFields(s.Field.SelectionSet).ForPath(pp[1])
		}
	}
	return nil
}

func (ss SelectionSet) ScalarForPath(path string) *SelectedField {
	if len(ss) == 0 {
		return nil
	}
	pp := strings.SplitN(path, ".", 2)
	for _, s := range ss {
		if s.Field.Alias == pp[0] {
			if len(pp) == 1 {
				if s.Field.Definition.Type.NamedType == "" ||
					!compiler.IsScalarType(s.Field.Definition.Type.NamedType) {
					return nil
				}
				return &s
			}
			if s.Field.Definition.Type.NamedType == "" &&
				s.Field.Directives.ForName(base.UnnestDirectiveName) == nil {
				return nil
			}
			return SelectedFields(s.Field.SelectionSet).ScalarForPath(pp[1])
		}
	}
	return nil
}

func (ss SelectionSet) AsSelectionSet() ast.SelectionSet {
	var selections ast.SelectionSet
	for _, s := range ss {
		selections = append(selections, s.Field)
	}
	return selections
}

func SelectedFields(ss ast.SelectionSet) SelectionSet {
	var fields SelectionSet
	for _, s := range ss {
		switch s := s.(type) {
		case *ast.FragmentSpread:
			fields = append(fields, SelectedFields(s.Definition.SelectionSet)...)
		case *ast.InlineFragment:
			ff := SelectedFields(s.SelectionSet)
			for _, f := range ff {
				f.OnType = s.TypeCondition
				fields = append(fields, f)
			}
		case *ast.Field:
			fields = append(fields, SelectedField{Field: s})
		}
	}
	return fields
}
