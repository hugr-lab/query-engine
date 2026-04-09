package client

import (
	"context"

	"github.com/hugr-lab/query-engine/types"
)

// AsUser returns a context that causes the client to execute
// queries and subscriptions as the specified user with the specified role.
// Only works when the client is authenticated via WithSecretKeyAuth.
func AsUser(ctx context.Context, userId, userName, role string) context.Context {
	return types.AsUser(ctx, userId, userName, role)
}
