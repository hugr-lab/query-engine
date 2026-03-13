package db

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/types"
)

// table returns the fully qualified table name with the provider's prefix.
// For attached DuckDB databases, prefix is "core." so table("_schema_types") returns "core._schema_types".
// For native (non-attached) databases, prefix is empty and the table name is returned as-is.
func (p *Provider) table(name string) string {
	return p.prefix + name
}

// pgDatabaseName extracts the attached database name from the prefix.
// e.g., "core." → "core".
func (p *Provider) pgDatabaseName() string {
	return strings.TrimSuffix(p.prefix, ".")
}

// execWrite executes a write operation (INSERT/UPDATE/DELETE).
// Parameters are always inlined into the SQL for uniform behavior.
// For attached PostgreSQL, additionally wraps with postgres_execute().
func (p *Provider) execWrite(ctx context.Context, conn *Connection, query string, args ...any) (sql.Result, error) {
	if !p.isPostgres {
		// DuckDB: inline parameters and execute directly.
		inlined, err := inlineParams(query, args)
		if err != nil {
			return nil, fmt.Errorf("inline params: %w", err)
		}
		return conn.Exec(ctx, inlined)
	}

	// PostgreSQL: strip table prefix from query template BEFORE inlining
	// parameters. This avoids corrupting literal values that contain the
	// prefix string (e.g., catalog name "core.meta" → "meta").
	stripped := query
	if p.prefix != "" {
		stripped = strings.ReplaceAll(stripped, p.prefix, "")
	}

	inlined, err := inlineParams(stripped, args)
	if err != nil {
		return nil, fmt.Errorf("inline params: %w", err)
	}

	// Escape single quotes for the postgres_execute string wrapper
	inlined = strings.ReplaceAll(inlined, "'", "''")

	dbName := p.pgDatabaseName()
	wrapped := fmt.Sprintf(`CALL postgres_execute('%s', ' %s ')`, dbName, inlined)
	return conn.Exec(ctx, wrapped)
}

// inlineParams replaces $1, $2, ... placeholders with SQL literal values.
// Replaces from highest index to lowest to avoid $1 matching inside $10, $11, etc.
func inlineParams(query string, args []any) (string, error) {
	result := query
	for i := len(args); i >= 1; i-- {
		lit, err := sqlLiteral(args[i-1])
		if err != nil {
			return "", fmt.Errorf("param $%d: %w", i, err)
		}
		result = strings.ReplaceAll(result, fmt.Sprintf("$%d", i), lit)
	}
	return result, nil
}

// sqlLiteral converts a Go value to a SQL literal string suitable for PostgreSQL.
func sqlLiteral(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'", nil
	case *string:
		if val == nil {
			return "NULL", nil
		}
		return "'" + strings.ReplaceAll(*val, "'", "''") + "'", nil
	case bool:
		if val {
			return "true", nil
		}
		return "false", nil
	case int:
		return strconv.Itoa(val), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case types.Vector: // []float64 — convert to float32 precision for DB storage
		if val == nil {
			return "NULL", nil
		}
		parts := make([]string, len(val))
		for i, f := range val {
			parts[i] = strconv.FormatFloat(float64(float32(f)), 'f', -1, 32)
		}
		return "'[" + strings.Join(parts, ",") + "]'", nil
	case []float32:
		// pgvector format: '[0.1,0.2,0.3]'
		parts := make([]string, len(val))
		for i, f := range val {
			parts[i] = strconv.FormatFloat(float64(f), 'f', -1, 32)
		}
		return "'[" + strings.Join(parts, ",") + "]'", nil
	default:
		return "", fmt.Errorf("unsupported SQL literal type: %T", v)
	}
}
