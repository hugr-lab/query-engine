package types

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/vektah/gqlparser/v2/gqlerror"
)

type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*Response, error)
}

type Response struct {
	Data       map[string]any `json:"data,omitempty"`
	Extensions map[string]any `json:"extensions,omitempty"`
	Errors     gqlerror.List  `json:"errors,omitempty"`
}

func ErrResponse(err error) Response {
	var l gqlerror.List
	if errors.As(err, &l) {
		return Response{Errors: l}
	}
	return Response{
		Errors: gqlerror.List{gqlerror.WrapIfUnwrapped(err)},
	}
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
			return ErrNoData
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
