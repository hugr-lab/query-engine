package llm

import (
	"sync"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/ratelimit"
)

// rateLimitMixin provides rate limiting fields and lazy initialization
// shared by all LLM source types.
type rateLimitMixin struct {
	resolver    sources.DataSourceResolver
	limiter     *ratelimit.Limiter
	limiterOnce sync.Once
}

func (m *rateLimitMixin) SetDataSourceResolver(resolver sources.DataSourceResolver) {
	m.resolver = resolver
}

// ensureLimiter lazily initializes the rate limiter on first use.
// Deferred because the Redis store may not be attached when the LLM source attaches.
func (m *rateLimitMixin) ensureLimiter(name string, cfg openAIConfig) {
	m.limiterOnce.Do(func() {
		if cfg.RPM == 0 && cfg.TPM == 0 {
			return
		}
		var store sources.StoreSource
		if cfg.RateStore != "" && m.resolver != nil {
			if ds, err := m.resolver.Resolve(cfg.RateStore); err == nil {
				store, _ = ds.(sources.StoreSource)
			}
		}
		m.limiter = ratelimit.New(name, cfg.RPM, cfg.TPM, store)
	})
}
