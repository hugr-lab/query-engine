package db

import (
	"context"
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb/v2"
)

type ScalarFunction interface {
	FuncName() string
	Executor() func(ctx context.Context, args []driver.Value) (any, error)
	Config() duckdb.ScalarFuncConfig
}

type ScalarFunctionSet interface {
	FuncName() string
	Functions(ctx context.Context, db *Pool) []duckdb.ScalarFunc
}

func RegisterScalarFunction(ctx context.Context, db *Pool, function ScalarFunction) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	return duckdb.RegisterScalarUDF(c.DBConn(), function.FuncName(), &scalarUDF{
		function: function,
	})
}

func RegisterScalarFunctionSet(ctx context.Context, db *Pool, set ScalarFunctionSet) error {
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	return duckdb.RegisterScalarUDFSet(c.DBConn(), set.FuncName(), set.Functions(ctx, db)...)
}

var _ ScalarFunction = (*ScalarFunctionWithArgs[any, any])(nil)

type ScalarFunctionWithArgs[I any, O any] struct {
	Name                  string
	Execute               func(ctx context.Context, input I) (O, error)
	ConvertInput          func(args []driver.Value) (I, error)
	ConvertOutput         func(out O) (any, error)
	InputTypes            []duckdb.TypeInfo
	OutputType            duckdb.TypeInfo
	IsVolatile            bool
	IsSpecialNullHandling bool
}

func (f *ScalarFunctionWithArgs[I, O]) FuncName() string {
	return f.Name
}

func (f *ScalarFunctionWithArgs[I, O]) Executor() func(ctx context.Context, args []driver.Value) (any, error) {
	return func(ctx context.Context, args []driver.Value) (any, error) {
		if len(args) != len(f.InputTypes) {
			return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid number of arguments"}
		}
		// Convert args to the expected types
		// and call the function
		ca, err := f.ConvertInput(args)
		if err != nil {
			return nil, err
		}
		out, err := f.Execute(ctx, ca)
		if err != nil {
			return nil, &duckdb.Error{
				Type: duckdb.ErrorTypeInternal,
				Msg:  err.Error(),
			}
		}
		if f.ConvertOutput == nil {
			return out, nil
		}
		co, err := f.ConvertOutput(out)
		if err != nil {
			return nil, &duckdb.Error{
				Type: duckdb.ErrorTypeInternal,
				Msg:  err.Error(),
			}
		}
		return co, nil
	}
}

func (f *ScalarFunctionWithArgs[I, O]) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{
		InputTypeInfos:      f.InputTypes,
		ResultTypeInfo:      f.OutputType,
		Volatile:            f.IsVolatile,
		SpecialNullHandling: f.IsSpecialNullHandling,
	}
}

var _ ScalarFunction = (*ScalarFunctionNoArgs[any])(nil)

type ScalarFunctionNoArgs[O any] struct {
	Name          string
	Execute       func(ctx context.Context) (O, error)
	ConvertOutput func(out O) (any, error)
	OutputType    duckdb.TypeInfo
	IsVolatile    bool
}

func (f *ScalarFunctionNoArgs[O]) FuncName() string {
	return f.Name
}

func (f *ScalarFunctionNoArgs[O]) Executor() func(ctx context.Context, args []driver.Value) (any, error) {
	return func(ctx context.Context, args []driver.Value) (any, error) {
		if len(args) != 0 {
			return nil, &duckdb.Error{Type: duckdb.ErrorTypeParameterNotResolved, Msg: "invalid number of arguments"}
		}
		// call the function
		out, err := f.Execute(ctx)
		if err != nil {
			return nil, &duckdb.Error{
				Type: duckdb.ErrorTypeInternal,
				Msg:  err.Error(),
			}
		}
		if f.ConvertOutput == nil {
			return out, nil
		}
		co, err := f.ConvertOutput(out)
		if err != nil {
			return nil, &duckdb.Error{
				Type: duckdb.ErrorTypeInternal,
				Msg:  err.Error(),
			}
		}
		return co, nil
	}
}

func (f *ScalarFunctionNoArgs[O]) Config() duckdb.ScalarFuncConfig {
	return duckdb.ScalarFuncConfig{
		InputTypeInfos: []duckdb.TypeInfo{},
		ResultTypeInfo: f.OutputType,
		Volatile:       f.IsVolatile,
	}
}

type ScalarFunctionCaster[O any] interface {
	ScalarUDF(set *ScalarFunctionTypedSet[O]) ScalarFunction
}

type ScalarFunctionTypedSet[O any] struct {
	Name                  string
	Funcs                 []ScalarFunctionCaster[O]
	ConvertOutput         func(out O) (any, error)
	OutputType            duckdb.TypeInfo
	IsVolatile            bool
	IsSpecialNullHandling bool
}

func (s *ScalarFunctionTypedSet[O]) FuncName() string {
	return s.Name
}

func (s *ScalarFunctionTypedSet[O]) Functions(ctx context.Context, db *Pool) []duckdb.ScalarFunc {
	var funcs []duckdb.ScalarFunc
	for _, f := range s.Funcs {
		funcs = append(funcs, &scalarUDF{
			db:       db,
			function: f.ScalarUDF(s),
		})
	}
	return funcs
}

type ScalarFunctionSetItem[I any, O any] struct {
	Execute      func(ctx context.Context, input I) (O, error)
	ConvertInput func(args []driver.Value) (I, error)
	InputTypes   []duckdb.TypeInfo
}

func (f *ScalarFunctionSetItem[I, O]) ScalarUDF(set *ScalarFunctionTypedSet[O]) ScalarFunction {
	return &ScalarFunctionWithArgs[I, O]{
		Name:                  set.Name,
		Execute:               f.Execute,
		ConvertInput:          f.ConvertInput,
		ConvertOutput:         set.ConvertOutput,
		InputTypes:            f.InputTypes,
		OutputType:            set.OutputType,
		IsVolatile:            set.IsVolatile,
		IsSpecialNullHandling: set.IsSpecialNullHandling,
	}
}

type ScalarFunctionSetItemNoArgs[O any] struct {
	Execute func(ctx context.Context) (O, error)
}

func (f *ScalarFunctionSetItemNoArgs[O]) ScalarUDF(set *ScalarFunctionTypedSet[O]) ScalarFunction {
	return &ScalarFunctionNoArgs[O]{
		Name:          set.Name,
		Execute:       func(ctx context.Context) (O, error) { return f.Execute(ctx) },
		ConvertOutput: set.ConvertOutput,
		OutputType:    set.OutputType,
		IsVolatile:    set.IsVolatile,
	}
}

type scalarUDF struct {
	db       *Pool
	function ScalarFunction
}

func (f *scalarUDF) Config() duckdb.ScalarFuncConfig {
	return f.function.Config()
}

func (f *scalarUDF) Executor() duckdb.ScalarFuncExecutor {
	return duckdb.ScalarFuncExecutor{
		RowContextExecutor: func(ctx context.Context, values []driver.Value) (any, error) {
			if f.db.IsTxContext(ctx) {
				ctx = ClearTxContext(ctx)
			}
			return f.function.Executor()(ctx, values)
		},
	}
}
