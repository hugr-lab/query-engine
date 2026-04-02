package hugrapp

import (
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

// HeartbeatConfig is an alias for sources.HeartbeatConfig.
type HeartbeatConfig = sources.HeartbeatConfig

// DefaultHeartbeatConfig returns the default heartbeat configuration.
func DefaultHeartbeatConfig() HeartbeatConfig {
	return HeartbeatConfig{
		Interval:   30 * time.Second,
		Timeout:    10 * time.Second,
		MaxRetries: 3,
	}
}
