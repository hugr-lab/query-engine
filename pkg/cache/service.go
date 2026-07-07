package cache

import (
	"context"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/eko/gocache/lib/v4/marshaler"
	"github.com/eko/gocache/lib/v4/store"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"golang.org/x/sync/singleflight"
)

// Provides a cache for the query engine
// The cache is used to store the results of queries and to speed up the execution of queries

var ErrMissCache = errors.New("key not found in cache")

func QueryKey(query string, vars map[string]any) (string, error) {
	// Generate a unique key for the query based on the query string and variables
	b, err := json.Marshal(vars)
	if err != nil {
		return "", err
	}
	key := query + string(b)
	// Generate a hash of the key to use as the cache key
	return fmt.Sprintf("%x", md5.Sum([]byte(key))), nil
}

type Service struct {
	config  Config
	enabled bool

	group singleflight.Group
	cache *marshaler.Marshaler

	engine *engines.DuckDB
}

func New(config Config) *Service {
	return &Service{
		config: config,
		engine: engines.NewDuckDB(),
	}
}

func (s *Service) Init(ctx context.Context) error {
	// Initialize the cache
	cm, err := s.config.Init(ctx)
	if errors.Is(err, ErrNoCacheConfigured) {
		s.enabled = false
		return nil
	}
	if err != nil {
		return err
	}

	s.cache = marshaler.New(cm)
	s.enabled = true

	return nil
}

// formatKey namespaces the cache key by the caller's identity, so a cached
// result is never served to a different identity whose result could differ:
//
//   - Full-access requests bypass row-level security (query.go), so their
//     result is identity-independent; they share a single "fa" namespace. (An
//     internal full-access query with an explicit cache key — e.g. the
//     per-role RolePermissions query — thus stays keyed once per that key, not
//     per user.)
//   - Otherwise the namespace is a hash of (role, user id). Hashing rather than
//     joining with a separator keeps the namespace collision-free even when a
//     role or user id is attacker-controlled or contains the separator.
//
// Known limitation: the namespace covers role + user id only. A cached query
// whose result depends on another auth dimension not reflected in the user id
// — impersonated_by_*, or a custom token claim used in an RLS filter such as
// [$auth.org_id] — is NOT distinguished, and such a query must not rely on the
// cache for isolation on that dimension.
func (s *Service) formatKey(ctx context.Context, key string) string {
	if auth.IsFullAccess(ctx) {
		return "fa:" + key
	}
	ai := auth.AuthInfoFromContext(ctx)
	if ai == nil {
		return key
	}
	return identityNamespace(ai.Role, ai.UserId) + ":" + key
}

// identityNamespace returns a collision-free hash of the identity components.
// Length-prefixing before hashing guarantees distinct (role, user id) pairs
// never map to the same namespace regardless of their contents.
func identityNamespace(role, userID string) string {
	return fmt.Sprintf("%x", md5.Sum(fmt.Appendf(nil, "%d:%s%d:%s", len(role), role, len(userID), userID)))
}

func (s *Service) Get(ctx context.Context, key string) (any, error) {
	if !s.enabled {
		return nil, ErrMissCache
	}
	return s.getFormatted(ctx, s.formatKey(ctx, key))
}

func (s *Service) getFormatted(ctx context.Context, formattedKey string) (any, error) {
	var item CacheItem
	v, err := s.cache.Get(ctx, formattedKey, &item)
	if err != nil || v == nil {
		return nil, ErrMissCache
	}
	return item.Data, nil
}

func (s *Service) Set(ctx context.Context, key string, data any, options ...Option) error {
	if !s.enabled {
		return nil
	}
	return s.setFormatted(ctx, s.formatKey(ctx, key), data, options...)
}

func (s *Service) setFormatted(ctx context.Context, formattedKey string, data any, options ...Option) error {
	item, err := NewCacheItem(data)
	if err != nil {
		return err
	}
	o := s.defaultOptions()
	for _, opt := range options {
		opt(o)
	}

	return s.cache.Set(ctx, formattedKey, item, o.toStoreOptions()...)
}

func (s *Service) Load(ctx context.Context, key string, fn func() (any, error), options ...Option) (any, error) {
	if !s.enabled {
		return fn()
	}
	// Namespace the key by identity ONCE and use it for the singleflight group
	// too — deduping on the bare key would coalesce concurrent identical queries
	// from DIFFERENT identities and hand one caller another's result.
	fk := s.formatKey(ctx, key)
	v, err := s.getFormatted(ctx, fk)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, ErrMissCache) {
		return nil, err
	}
	v, err, _ = s.group.Do(fk, func() (any, error) {
		v, err := s.getFormatted(ctx, fk)
		if err == nil {
			return v, nil
		}
		if !errors.Is(err, ErrMissCache) {
			return nil, err
		}
		v, err = fn()
		if err != nil {
			return nil, err
		}
		if err = s.setFormatted(ctx, fk, v, options...); err != nil {
			return nil, err
		}
		return v, nil
	})

	return v, err
}

// Delete removes the CALLER's cache entry for key. With per-identity
// namespacing this clears only the acting identity's copy; cross-identity
// invalidation (e.g. after a mutation) must use tag-based Invalidate, which is
// identity-independent.
func (s *Service) Delete(ctx context.Context, key string) error {
	if !s.enabled {
		return nil
	}
	return s.cache.Delete(ctx, s.formatKey(ctx, key))
}

func (s *Service) Invalidate(ctx context.Context, tags ...string) error {
	if !s.enabled {
		return nil
	}
	if len(tags) == 0 {
		return s.cache.Clear(ctx)
	}
	return s.cache.Invalidate(ctx, store.WithInvalidateTags(tags))
}

func (s *Service) defaultOptions() *Options {
	return &Options{
		ttl: time.Duration(s.config.TTL),
	}
}

type Option func(o *Options)

type Options struct {
	ttl  time.Duration
	tags []string
}

func ApplyOptions(opts ...Option) *Options {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func (o *Options) toStoreOptions() []store.Option {
	var oo []store.Option
	oo = append(oo, store.WithExpiration(o.ttl))
	if len(o.tags) > 0 {
		oo = append(oo, store.WithTags(o.tags))
	}
	return oo
}

func WithTTL(ttl time.Duration) Option {
	return func(o *Options) {
		o.ttl = ttl
	}
}
func WithTags(tags ...string) Option {
	return func(o *Options) {
		o.tags = tags
	}
}
