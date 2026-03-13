package db

import (
	"github.com/duckdb/duckdb-go/v2"
)

func DuckDBOperationResult() duckdb.TypeInfo {
	t, _ := duckdb.NewTypeInfo(duckdb.TYPE_BOOLEAN)
	success, _ := duckdb.NewStructEntry(t, "success")
	t, _ = duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)
	affected, _ := duckdb.NewStructEntry(t, "affected_rows")
	t, _ = duckdb.NewTypeInfo(duckdb.TYPE_BIGINT)
	lastId, _ := duckdb.NewStructEntry(t, "last_id")
	t, _ = duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	message, _ := duckdb.NewStructEntry(t, "message")

	s, _ := duckdb.NewStructInfo(success, affected, lastId, message)
	return s
}
