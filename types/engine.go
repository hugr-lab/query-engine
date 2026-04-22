package types

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

type validateOnlyKeyType struct{}

type Querier interface {
	Query(ctx context.Context, query string, vars map[string]any) (*Response, error)
	Subscribe(ctx context.Context, query string, vars map[string]any) (*Subscription, error)
	RegisterDataSource(ctx context.Context, ds DataSource) error
	LoadDataSource(ctx context.Context, name string) error
	UnloadDataSource(ctx context.Context, name string, opts ...UnloadOpt) error
	DataSourceStatus(ctx context.Context, name string) (string, error)
	DescribeDataSource(ctx context.Context, name string, self bool) (string, error)
}

// SubscriptionEvent is a data event in the subscription stream.
// One event per data path (query) or per native source event batch.
// Metadata (geometry, table info) is in Reader's Arrow schema metadata.
type SubscriptionEvent struct {
	Path   string             // Data object path (e.g. "core.data_sources"). Empty for native subscriptions.
	Reader array.RecordReader // Data reader. Transport decides how to consume: graphql-ws reads all → JSON next; IPC streams batches.
}

// Subscription is the result of Querier.Subscribe.
// Events channel produces SubscriptionEvents until closed.
// Errors are reported via Reader.Err() after Reader.Next() returns false,
// or via Err() after Events is closed (for subscription-level errors).
type Subscription struct {
	Events <-chan SubscriptionEvent
	Cancel func()
	err    error
}

// Err returns the subscription-level error (e.g. from subscription_error frame).
// Call after Events channel is closed.
func (s *Subscription) Err() error { return s.err }

// SetErr sets the subscription-level error. Called by the transport layer.
func (s *Subscription) SetErr(err error) { s.err = err }

type UnloadOpt func(*UnloadOpts)

type UnloadOpts struct {
	Hard bool
}

func WithHardUnload() UnloadOpt {
	return func(opts *UnloadOpts) {
		opts.Hard = true
	}
}

type Request struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables"`
	OperationName string         `json:"operationName,omitempty"`
	ValidateOnly  bool           `json:"validateOnly,omitempty"`
}

// JQRequest is a GraphQL query with an optional JQ transformation.
type JQRequest struct {
	JQ    string  `json:"jq"`
	Query Request `json:"query"`
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
	case *JsonValue:
	case ArrowTable:
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

// ScanData navigates Response.Data down a dotted path and deserialises
// the leaf into dest. It is the generic scan API sitting above the typed
// Scan[T] / ScanObject / ScanTable helpers.
//
// Leaf dispatch:
//
//   - Table leaves (types.ArrowTable) go through the native Arrow row
//     scanner (same path as ScanTable) — no JSON round-trip. dest must
//     be a pointer to a slice; geometry / time / decimal are decoded
//     column-wise via the reflect-plan-backed row scanner.
//   - Object leaves (*types.JsonValue) pass their raw JSON bytes to the
//     geometry-aware scanObject decoder.
//   - Scalar / map / slice leaves marshal via stdlib json and go through
//     the same scanObject decoder.
//
// Callers that need stdlib-json behaviour on table leaves (no-tag field
// matching by Go name, json.RawMessage, custom UnmarshalJSON,
// encoding.TextUnmarshaler) should use ScanDataJSON, which forces the
// JSON path on every leaf type.
//
// Error semantics: missing path segment → ErrWrongDataPath,
// present-but-nil leaf → ErrNoData.
func (r *Response) ScanData(path string, dest any) error {
	if r.Data == nil {
		return ErrNoData
	}
	return scanRecursive(path, r.Data, dest, false)
}

// ScanDataJSON is ScanData's sibling that forces the stdlib-JSON path
// even for ArrowTable leaves. Use when the destination type depends on
// encoding/json behaviour the native row scanner does not cover:
//
//   - fields without a json tag matched by Go field name,
//   - json.RawMessage fields,
//   - types with custom UnmarshalJSON / TextUnmarshaler / BinaryUnmarshaler.
//
// Tables are rendered via ArrowTable.MarshalJSON (extension-aware
// RecordToJSON — geometry as GeoJSON, timestamps as RFC3339Nano) and
// then unmarshalled through the geometry-aware scanObject decoder, so
// orb.Geometry / time.Time still work. *JsonValue leaves bypass the
// remarshal exactly as in ScanData.
//
// Prefer ScanData for the fast path. ScanDataJSON is the escape hatch.
func (r *Response) ScanDataJSON(path string, dest any) error {
	if r.Data == nil {
		return ErrNoData
	}
	return scanRecursive(path, r.Data, dest, true)
}

var (
	ErrNoData         = errors.New("no data")
	ErrWrongDataPath  = errors.New("wrong data path")
	ErrGeometryDecode = errors.New("geometry decode")
)

func scanRecursive(path string, data any, dest any, forceJSON bool) error {
	if data == nil {
		return ErrNoData
	}
	if path == "" {
		// ArrowTable leaves go through the native Arrow row scanner by
		// default; forceJSON routes them through MarshalJSON → scanObject
		// so stdlib-json features (no-tag matching, RawMessage,
		// UnmarshalJSON, TextUnmarshaler) apply.
		if tbl, ok := data.(ArrowTable); ok && !forceJSON {
			return scanTableInto(tbl, dest)
		}
		// *JsonValue carries raw JSON bytes already — skip the remarshal.
		if jv, ok := data.(*JsonValue); ok {
			return scanObject([]byte(*jv), dest)
		}
		b, err := json.Marshal(data)
		if err != nil {
			return err
		}
		return scanObject(b, dest)
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
		return scanRecursive(p, val, dest, forceJSON)
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
