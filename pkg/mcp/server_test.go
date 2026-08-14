package mcp

import (
	"testing"

	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every tool except data-execute_mutation only reads. mcp-go's NewTool
// defaults every tool to readOnly=false / destructive=true, so an
// unannotated tool silently advertises itself as destructive — which is
// exactly what hosts use to decide how scary the approval dialog looks.
func TestToolAnnotationsMarkEverythingButTheMutationReadOnly(t *testing.T) {
	s := New(nil, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})

	tools := s.mcp.ListTools()
	require.NotEmpty(t, tools)

	for name, tool := range tools {
		ro, de := tool.Tool.Annotations.ReadOnlyHint, tool.Tool.Annotations.DestructiveHint
		require.NotNil(t, ro, "%s: readOnlyHint must be set", name)
		require.NotNil(t, de, "%s: destructiveHint must be set", name)
		if name == "data-execute_mutation" {
			assert.False(t, *ro, "the mutation tool writes")
			assert.True(t, *de, "and must say so")
			continue
		}
		assert.Truef(t, *ro, "%s only reads", name)
		assert.Falsef(t, *de, "%s must not advertise itself as destructive", name)
	}
}
