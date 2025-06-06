package db

import (
	"bytes"
	"fmt"
	"text/template"
)

type ScriptDBType string

const (
	SDBPostgres         ScriptDBType = "postgres"
	SDBDuckDB           ScriptDBType = "duckdb"
	SDBAttachedDuckDB   ScriptDBType = "attached_duckdb"
	SDBAttachedPostgres ScriptDBType = "attached_postgres"
)

func ParseSQLScriptTemplate(dbType ScriptDBType, script string) (string, error) {
	t, err := template.New("script").Funcs(
		template.FuncMap{
			"isPostgres": func() bool {
				return dbType == SDBPostgres || dbType == SDBAttachedPostgres
			},
			"isDuckDB": func() bool {
				return dbType == SDBDuckDB || dbType == SDBAttachedDuckDB
			},
			"isAttachedDuckdb": func() bool {
				return dbType == SDBAttachedDuckDB
			},
			"isAttachedPostgres": func() bool {
				return dbType == SDBAttachedPostgres
			},
		},
	).Parse(script)
	if err != nil {
		return "", fmt.Errorf("failed to parse template: %w", err)
	}
	var fw bytes.Buffer
	err = t.Execute(&fw, nil)
	if err != nil {
		return "", fmt.Errorf("failed to execute template: %w", err)
	}

	return fw.String(), nil
}
