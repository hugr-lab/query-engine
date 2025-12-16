package runtime

import (
	"errors"
	"fmt"

	"github.com/duckdb/duckdb-go/v2"
)

func DuckDBStructTypeFromSchemaMust(schema map[string]any) duckdb.TypeInfo {
	ti, err := DuckDBStructTypeFromSchema(schema)
	if err != nil {
		panic(err)
	}
	return ti
}

func DuckDBStructTypeFromSchema(schema map[string]any) (duckdb.TypeInfo, error) {
	var see []duckdb.StructEntry
	for k, v := range schema {
		switch v := v.(type) {
		case string:
			ti, err := DuckDBTypeByName(v)
			if err != nil {
				return nil, err
			}
			se, err := DuckDBStructEntryInfo(k, ti)
			if err != nil {
				return nil, err
			}
			see = append(see, se)
		case duckdb.Type:
			se, err := DuckDBStructEntryInfo(k, v)
			if err != nil {
				return nil, err
			}
			see = append(see, se)
		case map[string]any:
			ti, err := DuckDBStructTypeFromSchema(v)
			if err != nil {
				return nil, err
			}
			se, err := duckdb.NewStructEntry(ti, k)
			if err != nil {
				return nil, err
			}
			see = append(see, se)
		case []string:
			if len(v) != 1 {
				return nil, errors.New("duckdb struct type must be a single typed")
			}
			t, err := DuckDBTypeByName(v[0])
			if err != nil {
				return nil, err
			}
			ti, err := duckdb.NewTypeInfo(t)
			if err != nil {
				return nil, err
			}
			ti, err = duckdb.NewListInfo(ti)
			if err != nil {
				return nil, err
			}
			se, err := duckdb.NewStructEntry(ti, k)
			if err != nil {
				return nil, err
			}
			see = append(see, se)
		case []duckdb.Type:
			if len(v) != 1 {
				return nil, errors.New("duckdb struct type must be a single typed")
			}
			ti, err := duckdb.NewTypeInfo(v[0])
			if err != nil {
				return nil, err
			}
			ti, err = duckdb.NewListInfo(ti)
			if err != nil {
				return nil, err
			}
			se, err := duckdb.NewStructEntry(ti, k)
			if err != nil {
				return nil, err
			}
			see = append(see, se)
		case []map[string]any:
			if len(v) != 1 {
				return nil, errors.New("duckdb struct type must be a single typed")
			}
			ti, err := DuckDBStructTypeFromSchema(v[0])
			if err != nil {
				return nil, err
			}
			ti, err = duckdb.NewListInfo(ti)
			if err != nil {
				return nil, err
			}
			se, err := duckdb.NewStructEntry(ti, k)
			if err != nil {
				return nil, err
			}
			see = append(see, se)
		default:
			return nil, fmt.Errorf("unsupported duckdb struct type: %T", v)
		}

	}
	if len(see) == 0 {
		return nil, errors.New("empty duckdb struct type")
	}
	if len(see) == 1 {
		return duckdb.NewStructInfo(see[0])
	}
	return duckdb.NewStructInfo(see[0], see[1:]...)
}

func DuckDBStructEntryInfo(name string, t duckdb.Type) (duckdb.StructEntry, error) {
	ti, err := duckdb.NewTypeInfo(t)
	if err != nil {
		return nil, err
	}
	se, err := duckdb.NewStructEntry(ti, name)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func DuckDBTypeInfoByNameMust(name string) duckdb.TypeInfo {
	ti, err := duckdb.NewTypeInfo(DuckDBTypeByNameMust(name))
	if err != nil {
		panic(err)
	}
	return ti
}

func DuckDBTypeByName(name string) (duckdb.Type, error) {
	if t, ok := duckdbTypeNameMap[name]; ok {
		return t, nil
	}
	return duckdb.TYPE_ANY, fmt.Errorf("unknown type: %s", name)
}

func DuckDBTypeByNameMust(name string) duckdb.Type {
	t, err := DuckDBTypeByName(name)
	if err != nil {
		panic(err)
	}
	return t
}

func DuckDBListInfoByName(name string) (duckdb.TypeInfo, error) {
	ti, err := duckdb.NewTypeInfo(DuckDBTypeByNameMust(name))
	if err != nil {
		return nil, err
	}
	return duckdb.NewListInfo(ti)
}

func DuckDBListInfoByNameMust(name string) duckdb.TypeInfo {
	ti, err := DuckDBListInfoByName(name)
	if err != nil {
		panic(err)
	}
	return ti
}

var duckdbTypeNameMap = map[string]duckdb.Type{
	"INTEGER":   duckdb.TYPE_INTEGER,
	"BIGINT":    duckdb.TYPE_BIGINT,
	"FLOAT":     duckdb.TYPE_FLOAT,
	"DOUBLE":    duckdb.TYPE_DOUBLE,
	"VARCHAR":   duckdb.TYPE_VARCHAR,
	"BOOLEAN":   duckdb.TYPE_BOOLEAN,
	"BLOB":      duckdb.TYPE_BLOB,
	"DATE":      duckdb.TYPE_DATE,
	"TIME":      duckdb.TYPE_TIME,
	"TIMESTAMP": duckdb.TYPE_TIMESTAMP,
	"JSON":      duckdb.TYPE_VARCHAR,
}
