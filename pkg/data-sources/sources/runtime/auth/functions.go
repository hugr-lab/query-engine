package auth

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"strings"

	"github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/perm"
)

func registerFunctions(ctx context.Context, pool *db.Pool) error {
	// core_auth_me — scalar, no args
	err := pool.RegisterScalarFunction(ctx, &db.ScalarFunctionNoArgs[map[string]any]{
		Name: "core_auth_me",
		Execute: func(ctx context.Context) (map[string]any, error) {
			info := auth.AuthInfoFromContext(ctx)
			if info == nil {
				return nil, nil
			}
			result := map[string]any{
				"user_id":                   info.UserId,
				"user_name":                 info.UserName,
				"role":                      info.Role,
				"auth_type":                 info.AuthType,
				"auth_provider":             info.AuthProvider,
				"impersonated_by_user_id":   "",
				"impersonated_by_user_name": "",
				"impersonated_by_user_role": "",
			}
			if impBy := auth.ImpersonatedByFromContext(ctx); impBy != nil {
				result["impersonated_by_user_id"] = impBy.UserId
				result["impersonated_by_user_name"] = impBy.UserName
				result["impersonated_by_user_role"] = impBy.Role
			}
			return result, nil
		},
		ConvertOutput: func(out map[string]any) (any, error) {
			if out == nil {
				return nil, nil
			}
			return out, nil
		},
		OutputType: meOutputType(),
		IsVolatile: true,
	})
	if err != nil {
		return err
	}

	// core_auth_my_permissions — scalar, no args
	err = pool.RegisterScalarFunction(ctx, &db.ScalarFunctionNoArgs[map[string]any]{
		Name: "core_auth_my_permissions",
		Execute: func(ctx context.Context) (map[string]any, error) {
			rp := perm.PermissionsFromCtx(ctx)
			if rp == nil {
				return nil, nil
			}
			return convertPermissions(rp)
		},
		ConvertOutput: func(out map[string]any) (any, error) {
			if out == nil {
				return nil, nil
			}
			return out, nil
		},
		OutputType: myPermissionsOutputType(),
		IsVolatile: true,
	})
	if err != nil {
		return err
	}

	type checkAccessInfoArgs struct {
		TypeName string
		Field    string
	}

	type checkAccessInfoResult struct {
		Enabled bool           `json:"enabled"`
		Visible bool           `json:"visible"`
		Filter  map[string]any `json:"filter"`
		Data    map[string]any `json:"data"`
	}

	// core_auth_check_access_info — scalar function with args
	err = pool.RegisterScalarFunction(ctx, &db.ScalarFunctionWithArgs[checkAccessInfoArgs, checkAccessInfoResult]{
		Name: "core_auth_check_access_info",
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		ConvertInput: func(args []driver.Value) (checkAccessInfoArgs, error) {
			typeName := args[0].(string)
			field := args[1].(string)
			return checkAccessInfoArgs{
				TypeName: typeName,
				Field:    field,
			}, nil
		},
		Execute: func(ctx context.Context, input checkAccessInfoArgs) (checkAccessInfoResult, error) {
			rp := perm.PermissionsFromCtx(ctx)
			if rp == nil {
				return checkAccessInfoResult{Enabled: true, Visible: true}, nil
			}
			p, enabled := rp.Enabled(input.TypeName, input.Field)
			_, visible := rp.Visible(input.TypeName, input.Field)
			res := checkAccessInfoResult{
				Enabled: enabled,
				Visible: visible,
			}
			if p != nil {
				res.Filter = p.Data
				res.Data = p.Filter
			}
			return res, nil
		},
		ConvertOutput: func(out checkAccessInfoResult) (any, error) {
			b, err := json.Marshal(out)
			if err != nil {
				return nil, err
			}
			return string(b), nil
		},
		OutputType:            runtime.DuckDBTypeInfoByNameMust("JSON"),
		IsVolatile:            true,
		IsSpecialNullHandling: true,
	})

	// core_auth_check_access — table function with args
	// fields is a comma-separated string because DuckDB table functions don't support LIST arguments
	err = pool.RegisterTableRowFunction(ctx, &db.TableRowFunctionWithArgs[checkAccessInput, checkAccessEntry]{
		Name: "core_auth_check_access",
		Arguments: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		ConvertArgs: func(named map[string]any, args ...any) (checkAccessInput, error) {
			typeName := args[0].(string)
			fieldsStr := args[1].(string)
			fields := splitFields(fieldsStr)
			return checkAccessInput{
				TypeName: typeName,
				Fields:   fields,
			}, nil
		},
		ColumnInfos: []duckdb.ColumnInfo{
			{Name: "field", T: runtime.DuckDBTypeInfoByNameMust("VARCHAR")},
			{Name: "enabled", T: runtime.DuckDBTypeInfoByNameMust("BOOLEAN")},
			{Name: "visible", T: runtime.DuckDBTypeInfoByNameMust("BOOLEAN")},
		},
		Execute: func(ctx context.Context, input checkAccessInput) ([]checkAccessEntry, error) {
			rp := perm.PermissionsFromCtx(ctx)
			entries := make([]checkAccessEntry, len(input.Fields))
			for i, field := range input.Fields {
				entry := checkAccessEntry{
					Field:   field,
					Enabled: true,
					Visible: true,
				}
				if rp != nil {
					if rp.Disabled {
						entry.Enabled = false
						entry.Visible = false
					} else {
						_, entry.Enabled = rp.Enabled(input.TypeName, field)
						_, entry.Visible = rp.Visible(input.TypeName, field)
					}
				}
				entries[i] = entry
			}
			return entries, nil
		},
		FillRow: func(out checkAccessEntry, row duckdb.Row) error {
			if err := duckdb.SetRowValue(row, 0, out.Field); err != nil {
				return err
			}
			if err := duckdb.SetRowValue(row, 1, out.Enabled); err != nil {
				return err
			}
			if err := duckdb.SetRowValue(row, 2, out.Visible); err != nil {
				return err
			}
			return nil
		},
	})
	return err
}

type checkAccessInput struct {
	TypeName string
	Fields   []string
}

func splitFields(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	fields := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			fields = append(fields, p)
		}
	}
	return fields
}

type checkAccessEntry struct {
	Field   string
	Enabled bool
	Visible bool
	Filter  map[string]any
	Data    map[string]any
}

func convertPermissions(rp *perm.RolePermissions) (map[string]any, error) {
	perms := make([]map[string]any, len(rp.Permissions))
	for i, p := range rp.Permissions {
		filterStr, err := marshalJSONField(p.Filter)
		if err != nil {
			return nil, err
		}
		dataStr, err := marshalJSONField(p.Data)
		if err != nil {
			return nil, err
		}
		perms[i] = map[string]any{
			"object":   p.Object,
			"field":    p.Field,
			"hidden":   p.Hidden,
			"disabled": p.Disabled,
			"filter":   filterStr,
			"data":     dataStr,
		}
	}
	return map[string]any{
		"role_name":   rp.Name,
		"disabled":    rp.Disabled,
		"permissions": perms,
	}, nil
}

func marshalJSONField(v map[string]any) (string, error) {
	if len(v) == 0 {
		return "", nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func meOutputType() duckdb.TypeInfo {
	return runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
		"user_id":                   "VARCHAR",
		"user_name":                 "VARCHAR",
		"role":                      "VARCHAR",
		"auth_type":                 "VARCHAR",
		"auth_provider":             "VARCHAR",
		"impersonated_by_user_id":   "VARCHAR",
		"impersonated_by_user_name": "VARCHAR",
		"impersonated_by_user_role": "VARCHAR",
	})
}

func myPermissionsOutputType() duckdb.TypeInfo {
	return runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
		"role_name": "VARCHAR",
		"disabled":  "BOOLEAN",
		"permissions": []map[string]any{{
			"object":   "VARCHAR",
			"field":    "VARCHAR",
			"hidden":   "BOOLEAN",
			"disabled": "BOOLEAN",
			"filter":   "VARCHAR",
			"data":     "VARCHAR",
		}},
	})
}
