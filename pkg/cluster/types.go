package cluster

import "time"

// ClusterConfig holds configuration for cluster mode.
// All fields are populated from CLUSTER_* environment variables.
type ClusterConfig struct {
	Enabled      bool          // CLUSTER_ENABLED
	Role         string        // CLUSTER_ROLE: "management" | "worker"
	NodeName     string        // CLUSTER_NODE_NAME
	NodeURL      string        // CLUSTER_NODE_URL (this node's IPC endpoint)
	Secret       string        // CLUSTER_SECRET (inter-node auth)
	Heartbeat    time.Duration // CLUSTER_HEARTBEAT (default: 30s)
	GhostTTL     time.Duration // CLUSTER_GHOST_TTL (default: 2m)
	PollInterval time.Duration // CLUSTER_POLL_INTERVAL (default: 30s)
}

// Cluster roles.
const (
	RoleManagement = "management"
	RoleWorker     = "worker"
)

// IsManagement returns true if this node is the management node.
func (c ClusterConfig) IsManagement() bool {
	return c.Enabled && c.Role == RoleManagement
}

// IsWorker returns true if this node is a worker node.
func (c ClusterConfig) IsWorker() bool {
	return c.Enabled && c.Role == RoleWorker
}

// NodeInfo represents a registered cluster node.
type NodeInfo struct {
	Name          string    `json:"name"`
	URL           string    `json:"url"`
	Role          string    `json:"role"`
	Version       string    `json:"version"`
	StartedAt     time.Time `json:"started_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	Error         string    `json:"error"`
}

// ClusterResult is the aggregated result of a broadcast operation.
type ClusterResult struct {
	Success bool         `json:"success"`
	Results []NodeResult `json:"results"`
}

// NodeResult is the per-node result of a broadcast operation.
type NodeResult struct {
	Node    string `json:"node"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// StorageParams holds parameters for registering an object storage secret.
type StorageParams struct {
	Type     string
	Name     string
	Scope    string
	Key      string
	Secret   string
	Region   string
	Endpoint string
	UseSSL   bool
	URLStyle string
}
