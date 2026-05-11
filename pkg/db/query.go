package db

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"strings"

	"github.com/hugr-lab/query-engine/types"
)

func (db *Pool) QueryTableToSlice(ctx context.Context, data any, q string, params ...any) error {
	res, err := db.QueryArrowTable(ctx, q, true, params...)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	err = json.NewEncoder(buf).Encode(res)
	if err != nil {
		return err
	}
	return json.NewDecoder(buf).Decode(data)
}

func (db *Pool) QueryArrowTable(ctx context.Context, q string, wrap bool, params ...any) (any, error) {
	if db.IsTxContext(ctx) {
		return db.queryJsonTableTx(ctx, q, wrap, params...)
	}
	return db.QueryJsonTableArrow(ctx, q, wrap, params...)
}

func (db *Pool) QueryJsonTableArrow(ctx context.Context, q string, wrap bool, params ...any) (types.ArrowTable, error) {
	ar, err := db.Arrow(ctx)
	if err != nil {
		return nil, err
	}
	defer ar.Close()

	if wrap {
		q = wrapJSON(q)
	}
	if strings.Contains(q, "json_field_demo") {
		log.Printf("DEBUG Arrow.QueryContext args=%v query=%s", params, q)
		// Diagnostic pre-query: introspect each piece of the typed JSON-field
		// comparison so we can see whether json_value / try_cast / CAST each
		// produce the expected typed value for row 11.
		diag := `SELECT
			json_value(data::JSON, '$.signup.day') AS jv_raw,
			json_type(data, '$.signup.day') AS jv_type,
			try_cast(json_value(data::JSON, '$.signup.day') AS DATE) AS jv_as_date,
			CAST('2024-01-15' AS DATE) AS rhs_date,
			try_cast(json_value(data::JSON, '$.signup.day') AS DATE) = CAST('2024-01-15' AS DATE) AS cmp
			FROM local_db.json_field_demo WHERE id = 11`
		drow, derr := ar.QueryContext(ctx, diag)
		if derr != nil {
			log.Printf("DEBUG diag err: %v", derr)
		} else {
			dt, _ := types.NewArrowTableFromReader(drow)
			b, _ := json.Marshal(dt)
			log.Printf("DEBUG diag row11: %s", string(b))
			drow.Release()
		}
	}
	reader, err := ar.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer reader.Release()
	table, err := types.NewArrowTableFromReader(reader)
	if err != nil {
		return nil, err
	}
	if wrap {
		table.SetInfo("wrapped")
	}

	return table, nil
}

func (db *Pool) queryJsonTableTx(ctx context.Context, q string, wrap bool, params ...any) (any, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	if wrap {
		q = wrapJSON(q)
	}
	rows, err := conn.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var res []types.JsonValue
	for rows.Next() {
		var val types.JsonValue
		err := rows.Scan(&val)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}

	return res, nil
}

func (db *Pool) QueryJsonScalarArray(ctx context.Context, q string, params ...any) (any, error) {
	if db.IsTxContext(ctx) {
		return db.queryJsonScalarArrayTx(ctx, q, params...)
	}
	return db.QueryJsonScalarArrayArrow(ctx, q, params...)
}

func (db *Pool) QueryJsonScalarArrayArrow(ctx context.Context, q string, params ...any) (types.ArrowTable, error) {
	ar, err := db.Arrow(ctx)
	if err != nil {
		return nil, err
	}
	defer ar.Close()

	if strings.Contains(q, "json_field_demo") {
		log.Printf("DEBUG Arrow.QueryContext args=%v query=%s", params, q)
	}
	reader, err := ar.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, err
	}

	table, err := types.NewArrowTableFromReader(reader)
	if err != nil {
		return nil, err
	}
	table.SetInfo("asArray")
	return table, nil
}

func (db *Pool) queryJsonScalarArrayTx(ctx context.Context, q string, params ...any) (any, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, q, params...)
	if err != nil {
		return nil, err
	}

	var res []any
	for rows.Next() {
		var val types.JsonValue
		err := rows.Scan(&val)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

func (db *Pool) QueryJsonRow(ctx context.Context, q string, params ...any) (*types.JsonValue, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var val types.JsonValue
	err = conn.QueryRow(ctx, wrapJSON(q), params...).Scan(&val)
	if err != nil {
		return nil, err
	}

	return &val, nil
}

func (db *Pool) QueryScalarValue(ctx context.Context, q string, params ...any) (any, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var val any
	err = conn.QueryRow(ctx, q, params...).Scan(&val)
	if err != nil {
		return nil, err
	}

	return val, nil
}

func (db *Pool) QueryRowToData(ctx context.Context, data any, q string, params ...any) error {
	val, err := db.QueryJsonRow(ctx, q, params...)
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	err = json.NewEncoder(buf).Encode(val)
	if err != nil {
		return err
	}
	return json.NewDecoder(buf).Decode(data)
}

// Runs a query write results to a stream
// This method does not support transactions
func (db *Pool) QueryTableStream(ctx context.Context, q string, params ...any) (types.ArrowTable, func(), error) {
	ar, err := db.Arrow(ctx)
	if err != nil {
		return nil, nil, err
	}
	if strings.Contains(q, "json_field_demo") {
		log.Printf("DEBUG Arrow.QueryContext args=%v query=%s", params, q)
	}
	reader, err := ar.QueryContext(ctx, q, params...)
	if err != nil {
		_ = ar.Close()
		return nil, nil, err
	}
	finalize := func() {
		_ = ar.Close()
	}
	return types.NewArrowTableStream(reader), finalize, nil
}

func wrapJSON(query string) string {
	return "SELECT (_data::JSON)::TEXT FROM (" + query + ") AS _data"
}
