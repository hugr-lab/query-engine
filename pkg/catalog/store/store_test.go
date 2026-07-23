//go:build duckdb_arrow

package store

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewSystemLayer covers M1: the store assembles the binary-owned system
// layer in memory (no DB I/O) and qualifies catalog tables correctly.
func TestNewSystemLayer(t *testing.T) {
	ctx := t.Context()
	pool, err := db.NewPool("")
	require.NoError(t, err)
	t.Cleanup(func() { pool.Close() })

	s, err := New(ctx, pool, Config{VecSize: 8}, nil)
	require.NoError(t, err)

	// The system layer resolves base scalars and classifies them as system —
	// exactly the definitions the writer must NEVER persist to catalog.*.
	for _, name := range []string{"Int", "Float", "String", "Boolean"} {
		def := s.System().ForName(ctx, name)
		require.NotNilf(t, def, "system layer resolves %s", name)
		assert.Truef(t, sdl.IsSystemType(def), "%s classified as system type", name)
	}

	// Base directives are present in the assembled layer.
	require.NotNil(t, s.System().DirectiveForName(ctx, "module"),
		"base @module directive assembled")
}
