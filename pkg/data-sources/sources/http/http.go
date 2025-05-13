package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

type Source struct {
	engine     *engines.HttpEngine
	ds         types.DataSource
	isAttached bool
	client     *http.Client

	mu       sync.RWMutex
	document *ast.SchemaDocument
	spec     *openapi3.T
	params   httpSourceParams
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:         ds,
		isAttached: attached,
		engine:     engines.NewHttp(),
	}, nil
}

func (s *Source) Name() string {
	return s.ds.Name
}

func (s *Source) Definition() types.DataSource {
	return s.ds
}

func (s *Source) ReadOnly() bool {
	return s.ds.ReadOnly
}

func (s *Source) Engine() engines.Engine {
	return s.engine
}

func (s *Source) IsAttached() bool {
	return s.isAttached
}

func (s *Source) Attach(ctx context.Context, db *db.Pool) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isAttached {
		return sources.ErrDataSourceAttached
	}

	s.ds.Path, err = sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return err
	}

	params, err := sourceParamsFromPath(s.ds.Path)
	if err != nil {
		return err
	}
	s.params = params
	if params.hasSpec {
		err := s.loadSpecs(ctx)
		if err != nil {
			return err
		}
	}
	s.client, err = newHttpClient(s.params, http.DefaultTransport)
	if err != nil {
		return err
	}
	s.isAttached = true
	return nil
}

func (s *Source) Detach(ctx context.Context, db *db.Pool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isAttached {
		return nil
	}
	s.isAttached = false
	return nil
}

func (s *Source) Request(ctx context.Context, path, method, headers, params, body string) (*http.Response, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.isAttached {
		return nil, sources.ErrDataSourceNotAttached
	}
	// 1.path
	su, err := url.Parse(s.params.serverURL)
	if err != nil {
		return nil, err
	}
	if su.Path != "" {
		su.JoinPath(path)
	}
	if su.Path == "" {
		su.Path = path
	}
	// 2. params
	rp := url.Values{}
	if params != "" {
		var pp map[string]any
		err = json.Unmarshal([]byte(params), &pp)
		if err != nil {
			return nil, err
		}
		for k, v := range pp {
			if v == nil {
				continue
			}
			vv, err := valueToRequestParamValue(v)
			if err != nil {
				return nil, err
			}
			for _, v := range vv {
				rp.Add(k, v)
			}
		}
	}

	su.RawQuery = rp.Encode()
	// 3. headers to map
	rh := map[string]string{}
	if headers != "" {
		var hh map[string]any
		err = json.Unmarshal([]byte(headers), &hh)
		if err != nil {
			return nil, err
		}
		for k, v := range hh {
			if v == nil {
				continue
			}
			vv, err := valueToRequestParamValue(v)
			if err != nil {
				return nil, err
			}
			if len(vv) == 0 {
				continue
			}
			if len(vv) > 1 {
				return nil, fmt.Errorf("header %s has more than one value", k)
			}
			rh[k] = vv[0]
		}
	}
	if _, ok := rh["Content-Type"]; !ok {
		rh["Content-Type"] = "application/json"
	}

	// 4. body
	var br io.Reader
	if method != http.MethodGet && body != "" {
		// delete null body values
		if strings.HasPrefix(body, "{") && strings.HasSuffix(body, "}") {
			var data map[string]any
			err = json.Unmarshal([]byte(body), &data)
			if err != nil {
				return nil, err
			}
			out := map[string]any{}
			for k, v := range data {
				if v == nil {
					continue
				}
				out[k] = v
			}
			b, err := json.Marshal(out)
			if err != nil {
				return nil, err
			}
			body = string(b)
		}

		br = bytes.NewReader([]byte(body))
	}

	req, err := http.NewRequestWithContext(ctx, method, su.String(), br)
	if err != nil {
		return nil, err
	}

	for k, v := range rh {
		req.Header.Add(k, v)
	}

	return s.client.Do(req)
}

func valueToRequestParamValue(v any) (res []string, err error) {
	switch v := v.(type) {
	case string:
		return []string{v}, nil
	case int, int32, int64, float32, float64, bool:
		return []string{fmt.Sprintf("%v", v)}, nil
	case map[string]any:
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return []string{string(b)}, nil
	case time.Time:
		return []string{v.Format(time.RFC3339)}, nil
	case []any:
		return arrayValueToRequestParamValue(v)
	case []string:
		return arrayValueToRequestParamValue(v)
	case []int:
		return arrayValueToRequestParamValue(v)
	case []float64:
		return arrayValueToRequestParamValue(v)
	case []bool:
		return arrayValueToRequestParamValue(v)
	case []map[string]any:
		return arrayValueToRequestParamValue(v)
	case []int32:
		return arrayValueToRequestParamValue(v)
	case []int64:
		return arrayValueToRequestParamValue(v)
	case []float32:
		return arrayValueToRequestParamValue(v)
	default:
		return nil, fmt.Errorf("unsupported value type %T", v)
	}
}

func arrayValueToRequestParamValue[T any](v []T) (res []string, err error) {
	for _, v := range v {
		s, err := valueToRequestParamValue(v)
		if err != nil {
			return nil, err
		}
		res = append(res, s...)
	}
	return res, nil
}
