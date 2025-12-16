package types

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type validateOnlyKeyType struct{}

type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*Response, error)
	RegisterDataSource(ctx context.Context, ds DataSource) error
	LoadDataSource(ctx context.Context, name string) error
	UnloadDataSource(ctx context.Context, name string) error
	DataSourceStatus(ctx context.Context, name string) (string, error)
	DescribeDataSource(ctx context.Context, name string, self bool) (string, error)
}

type Request struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
	OperationName string         `json:"operationName,omitempty"`
	ValidateOnly  bool           `json:"validateOnly,omitempty"`
}

type Response struct {
	Data       map[string]any `json:"data,omitempty"`
	Extensions map[string]any `json:"extensions,omitempty"`
	Errors     gqlerror.List  `json:"errors,omitempty"`
}

func ContextWithValidateOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, validateOnlyKeyType{}, true)
}

func IsValidateOnlyContext(ctx context.Context) bool {
	v := ctx.Value(validateOnlyKeyType{})
	if v == nil {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

func (r *Response) Close() {
	if r == nil {
		return
	}
	if r.Data != nil {
		DataClose(r.Data)
	}
	if r.Extensions != nil {
		DataClose(r.Extensions)
	}
	r.Data = nil
	r.Extensions = nil
	r.Errors = nil
}

func DataClose(data any) {
	if data == nil {
		return
	}
	switch v := data.(type) {
	case *db.JsonValue:
	case db.ArrowTable:
		v.Release()
	case map[string]any:
		for _, val := range v {
			DataClose(val)
		}
	case []any:
		for _, val := range v {
			DataClose(val)
		}
	case *Response:
		v.Close()
	default:
		return
	}
}

func ErrResponse(err error) Response {
	return Response{
		Errors: WarpGraphQLError(err),
	}
}

func WarpGraphQLError(err error) gqlerror.List {
	var l gqlerror.List
	if errors.As(err, &l) {
		return l
	}
	return gqlerror.List{gqlerror.WrapIfUnwrapped(err)}
}

func (r *Response) Err() error {
	if len(r.Errors) == 0 {
		return nil
	}
	return r.Errors
}

func (r *Response) ScanData(path string, dest interface{}) error {
	if r.Data == nil {
		return ErrNoData
	}
	return scanRecursive(path, r.Data, dest)
}

var (
	ErrNoData        = errors.New("no data")
	ErrWrongDataPath = errors.New("wrong data path")
)

func scanRecursive(path string, data any, dest interface{}) error {
	if data == nil {
		return ErrNoData
	}
	if path == "" {
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		return json.Unmarshal(b, dest)
	}
	pp := strings.SplitN(path, ".", 2)
	switch v := data.(type) {
	case map[string]any:
		val, ok := v[pp[0]]
		if !ok {
			return ErrWrongDataPath
		}
		p := ""
		if len(pp) > 1 {
			p = pp[1]
		}
		return scanRecursive(p, val, dest)
	default:
		return ErrWrongDataPath
	}
}

func (r *Response) DataPart(path string) any {
	if r.Data == nil {
		return nil
	}
	return ExtractResponseData(path, r.Data)
}

func ExtractResponseData(path string, data map[string]any) any {
	if path == "" {
		return data
	}
	pp := strings.SplitN(path, ".", 2)
	if len(pp) == 1 {
		return data[pp[0]]
	}
	d, ok := data[pp[0]]
	if !ok || d == nil {
		return nil
	}
	dm, ok := d.(map[string]any)
	if !ok {
		return nil
	}
	return ExtractResponseData(pp[1], dm)
}
