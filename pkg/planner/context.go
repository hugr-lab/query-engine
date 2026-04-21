package planner

import (
	"context"

	"github.com/vektah/gqlparser/v2/ast"
)

// Raw-results mode is now the default for list-type fields — they return
// native Arrow tables with extension-typed geometry columns, decoded to
// GeoJSON on the Go side by types.RecordToJSON. Scalar / named-type
// fields continue to roundtrip through (_data::JSON)::TEXT and require
// ST_AsGeoJSON at SQL level, so IsRawResultsQuery returns false for
// them.
//
// The context-carried opt-in flag (ContextWithRawResultsFlag) and the
// @raw directive are retained as no-ops for binary compatibility — they
// used to let IPC-style handlers switch an individual request into raw
// mode. Now that raw is the default for every list field, the helpers
// have no effect; they're kept only so existing call sites compile
// unchanged and can be deleted in a follow-up polish pass.

type ctxKey string

var rawResultsKey = ctxKey("rawResults")

// ContextWithRawResultsFlag is kept as a no-op for compatibility with
// existing call sites. Raw mode is now the unconditional default for
// list fields — setting the flag changes nothing.
//
// Deprecated: will be removed in the polish phase of the unified-scan
// refactor.
func ContextWithRawResultsFlag(ctx context.Context) context.Context {
	return context.WithValue(ctx, rawResultsKey, true)
}

// IsRawResultsQuery returns true iff the field's GraphQL type is a list
// (i.e. the planner should run the list/table codepath against
// QueryArrowTable and let RecordToJSON handle geometry/temporal/decimal
// rendering). Scalar / named-type fields always return false because
// QueryJsonRow unconditionally wraps them in (_data::JSON)::TEXT, which
// requires ST_AsGeoJSON at SQL level to produce valid GeoJSON text.
func IsRawResultsQuery(_ context.Context, field *ast.Field) bool {
	return field.Definition.Type.NamedType == ""
}
