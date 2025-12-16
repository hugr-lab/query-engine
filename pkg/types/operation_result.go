package types

import (
	"database/sql"

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

type OperationResult struct {
	Succeed bool   `json:"success"`
	Msg     string `json:"message"`
	Rows    int    `json:"affected_rows"`
	LastId  int    `json:"last_id"`
}

func Result(msg string, rows, lastId int) *OperationResult {
	return &OperationResult{
		Succeed: true,
		Msg:     msg,
		Rows:    rows,
		LastId:  lastId,
	}
}

func ErrResult(err error) *OperationResult {
	return &OperationResult{
		Msg: err.Error(),
	}
}

func SQLResult(msg string, res sql.Result) *OperationResult {
	rows, _ := res.RowsAffected()
	lastId, _ := res.LastInsertId()
	return Result(msg, int(rows), int(lastId))
}

func SQLError(msg string, err error) *OperationResult {
	return &OperationResult{
		Msg: msg + ": " + err.Error(),
	}
}

func (r *OperationResult) CollectSQL(res sql.Result) {
	rows, _ := res.RowsAffected()
	lastId, _ := res.LastInsertId()
	r.Rows += int(rows)
	r.LastId = int(lastId)
}

func (r *OperationResult) ToDuckdb() map[string]interface{} {
	if r == nil {
		return nil
	}
	return map[string]interface{}{
		"success":       r.Succeed,
		"affected_rows": r.Rows,
		"last_id":       r.LastId,
		"message":       r.Msg,
	}
}
