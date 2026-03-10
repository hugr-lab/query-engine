package planner

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// AtInfo holds time-travel parameters for DuckLake queries.
type AtInfo struct {
	Version   int
	Timestamp string
}

// resolveAtInfo extracts @at directive info from a query field.
// It first checks the query-level directive on the field, then falls back
// to the SDL-level directive on the field definition's parent type.
func resolveAtInfo(field *ast.Field, vars map[string]any) *AtInfo {
	// Check query-level @at directive first
	dir := field.Directives.ForName(base.AtDirectiveName)
	if dir == nil && field.Definition != nil {
		// Fall back to SDL-level @at on the object definition
		dir = field.ObjectDefinition.Directives.ForName(base.AtDirectiveName)
		if dir == nil && field.Definition.Type != nil {
			// Check on the type definition itself (for table/view types)
			// This is handled via the object definition above
		}
	}
	if dir == nil {
		return nil
	}

	info := &AtInfo{}

	versionStr := sdl.DirectiveArgValue(dir, "version", vars)
	if versionStr != "" {
		v, err := strconv.Atoi(versionStr)
		if err == nil {
			info.Version = v
			return info
		}
	}

	timestampStr := sdl.DirectiveArgValue(dir, "timestamp", vars)
	if timestampStr != "" {
		info.Timestamp = timestampStr
		return info
	}

	return nil
}

// atClause generates the SQL AT clause for time-travel queries.
func atClause(info *AtInfo) (string, error) {
	if info == nil {
		return "", nil
	}
	if info.Version != 0 {
		return fmt.Sprintf(" AT (VERSION => %d)", info.Version), nil
	}
	if info.Timestamp != "" {
		ts, err := sanitizeTimestamp(info.Timestamp)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf(" AT (TIMESTAMP => '%s')", ts), nil
	}
	return "", nil
}

// sanitizeTimestamp validates that a timestamp string is in RFC3339 format.
func sanitizeTimestamp(ts string) (string, error) {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return "", fmt.Errorf("invalid timestamp %q: must be in RFC3339 format (e.g. 2024-01-15T10:30:00Z): %w", ts, err)
	}
	return t.Format(time.RFC3339), nil
}
