package client

import (
	"context"
	"net/http"

	"github.com/hugr-lab/query-engine/types"
)

// AsUser returns a context that causes the client to execute
// queries and subscriptions as the specified user with the specified role.
// Only works when the client is authenticated via WithSecretKeyAuth.
func AsUser(ctx context.Context, userId, userName, role string) context.Context {
	return types.AsUser(ctx, userId, userName, role)
}

// setAsUserHeaders adds impersonation headers to an HTTP request
// if AsUser is set in the context. Uses dedicated x-hugr-impersonated-* headers
// separate from identity headers.
func setAsUserHeaders(ctx context.Context, req *http.Request) {
	id := types.AsUserFromContext(ctx)
	if id == nil {
		return
	}
	req.Header.Set("x-hugr-impersonated-user-id", id.UserId)
	req.Header.Set("x-hugr-impersonated-user-name", id.UserName)
	req.Header.Set("x-hugr-impersonated-role", id.Role)
}
