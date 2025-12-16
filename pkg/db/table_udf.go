package db

import (
	"context"

	"github.com/duckdb/duckdb-go/v2"
)

func RegisterTableRowFunction(ctx context.Context, db *Pool, function TableRowFunction) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	return duckdb.RegisterTableUDF(c.DBConn(), function.FuncName(), function.Bind(ctx))
}

type TableRowFunction interface {
	FuncName() string
	Bind(ctx context.Context) duckdb.RowTableFunction
}

type TableRowFunctionWithArgs[I any, O any] struct {
	Name           string
	Execute        func(ctx context.Context, input I) ([]O, error)
	Arguments      []duckdb.TypeInfo
	NamedArguments map[string]duckdb.TypeInfo
	ConvertArgs    func(named map[string]any, args ...any) (I, error)
	ColumnInfos    []duckdb.ColumnInfo
	FillRow        func(out O, row duckdb.Row) error
}

func (f *TableRowFunctionWithArgs[I, O]) FuncName() string {
	return f.Name
}

func (f *TableRowFunctionWithArgs[I, O]) Bind(ctx context.Context) duckdb.RowTableFunction {
	return duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{
			Arguments:      f.Arguments,
			NamedArguments: f.NamedArguments,
		},
		BindArguments: func(named map[string]any, args ...any) (duckdb.RowTableSource, error) {
			input, err := f.ConvertArgs(named, args...)
			if err != nil {
				return nil, err
			}
			return &tableRowUDFSource[I, O]{
				ctx:      ctx,
				args:     input,
				colInfos: f.ColumnInfos,
				execute:  f.Execute,
				fillRow:  f.FillRow,
			}, nil
		},
	}
}

type tableRowUDFSource[I any, O any] struct {
	ctx  context.Context
	args I

	colInfos []duckdb.ColumnInfo
	execute  func(ctx context.Context, input I) ([]O, error)
	fillRow  func(out O, row duckdb.Row) error

	data []O
	err  error
	curr int
}

func (s *tableRowUDFSource[I, O]) Init() {
	s.data, s.err = s.execute(s.ctx, s.args)
}

func (s *tableRowUDFSource[I, O]) Cardinality() *duckdb.CardinalityInfo {
	return &duckdb.CardinalityInfo{
		Cardinality: uint(len(s.data)),
		Exact:       true,
	}
}

func (s *tableRowUDFSource[I, O]) ColumnInfos() []duckdb.ColumnInfo {
	return s.colInfos
}

func (s *tableRowUDFSource[I, O]) FillRow(row duckdb.Row) (bool, error) {
	if s.err != nil {
		return s.curr < len(s.data), &duckdb.Error{
			Type: duckdb.ErrorTypeBinder,
			Msg:  s.err.Error(),
		}
	}

	if s.curr >= len(s.data) {
		return false, nil
	}

	err := s.fillRow(s.data[s.curr], row)
	if err != nil {
		return true, &duckdb.Error{
			Type: duckdb.ErrorTypeExecutor,
			Msg:  err.Error(),
		}
	}
	s.curr++
	return true, nil
}

type TableRowFunctionNoArgs[O any] struct {
	Name        string
	Execute     func(ctx context.Context) ([]O, error)
	ColumnInfos []duckdb.ColumnInfo
	FillRow     func(out O, row duckdb.Row) error
}

func (f *TableRowFunctionNoArgs[O]) FuncName() string {
	return f.Name
}

func (f *TableRowFunctionNoArgs[O]) Bind(ctx context.Context) duckdb.RowTableFunction {
	return duckdb.RowTableFunction{
		Config: duckdb.TableFunctionConfig{},
		BindArguments: func(named map[string]any, args ...any) (duckdb.RowTableSource, error) {

			return &tableRowUDFSource[any, O]{
				ctx:      ctx,
				colInfos: f.ColumnInfos,
				execute: func(ctx context.Context, input any) ([]O, error) {
					return f.Execute(ctx)
				},
				fillRow: f.FillRow,
			}, nil
		},
	}
}
