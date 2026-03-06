package cluster

import "context"

type clusterBroadcastKey struct{}

// ContextWithClusterBroadcast marks the call as coming from a management broadcast.
// LoadDataSource will skip compilation when this is set (schema already in CoreDB).
func ContextWithClusterBroadcast(ctx context.Context) context.Context {
	return context.WithValue(ctx, clusterBroadcastKey{}, true)
}

// IsClusterBroadcast returns true if this call originates from a cluster broadcast.
func IsClusterBroadcast(ctx context.Context) bool {
	v, _ := ctx.Value(clusterBroadcastKey{}).(bool)
	return v
}
