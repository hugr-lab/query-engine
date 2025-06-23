package db

import (
	"bytes"
	"context"
	"encoding/json"
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

func (db *Pool) QueryJsonTableArrow(ctx context.Context, q string, wrap bool, params ...any) (ArrowTable, error) {
	ar, err := db.Arrow(ctx)
	if err != nil {
		return nil, err
	}
	defer ar.Close()

	if wrap {
		q = wrapJSON(q)
	}
	reader, err := ar.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, err
	}
	table := NewArrowTableStream(reader)
	table.wrapped = wrap

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

	var res []JsonValue
	for rows.Next() {
		var val JsonValue
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

func (db *Pool) QueryJsonScalarArrayArrow(ctx context.Context, q string, params ...any) (ArrowTable, error) {
	ar, err := db.Arrow(ctx)
	if err != nil {
		return nil, err
	}
	defer ar.Close()

	reader, err := ar.QueryContext(ctx, q, params...)
	if err != nil {
		return nil, err
	}

	table := NewArrowTableStream(reader)
	table.asArray = true
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
		var val JsonValue
		err := rows.Scan(&val)
		if err != nil {
			return nil, err
		}
		res = append(res, val)
	}
	return res, nil
}

func (db *Pool) QueryJsonRow(ctx context.Context, q string, params ...any) (*JsonValue, error) {
	conn, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var val JsonValue
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

	return &val, nil
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

func wrapJSON(query string) string {
	return "SELECT (_data::JSON)::TEXT FROM (" + query + ") AS _data"
}
