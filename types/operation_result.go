package types

import (
	"database/sql"
)

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

func (r *OperationResult) ToDuckdb() map[string]any {
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
