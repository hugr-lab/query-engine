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

var ErrMissCache = errors.New("Key not found in cache")

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

func (s *Service) formatKey(ctx context.Context, key string) string {
	ai := auth.AuthInfoFromContext(ctx)
	if ai != nil {
		key = fmt.Sprintf("%s:%s", ai.Role, key)
	}

	return key
}

func (s *Service) Get(ctx context.Context, key string) (any, error) {
	if !s.enabled {
		return nil, ErrMissCache
	}
	var item CacheItem
	v, err := s.cache.Get(ctx, s.formatKey(ctx, key), &item)
	if err != nil || v == nil {
		return nil, ErrMissCache
	}
	return item.Data, nil
}

func (s *Service) Set(ctx context.Context, key string, data any, options ...Option) error {
	if !s.enabled {
		return nil
	}
	item, err := NewCacheItem(data)
	if err != nil {
		return err
	}
	o := s.defaultOptions()
	for _, opt := range options {
		opt(o)
	}

	return s.cache.Set(ctx, s.formatKey(ctx, key), item, o.toStoreOptions()...)
}

func (s *Service) Load(ctx context.Context, key string, fn func() (any, error), options ...Option) (any, error) {
	if !s.enabled {
		return fn()
	}
	v, err := s.Get(ctx, key)
	if err == nil {
		return v, nil
	}
	if !errors.Is(err, ErrMissCache) {
		return nil, err
	}
	v, err, _ = s.group.Do(key, func() (any, error) {
		v, err := s.Get(ctx, key)
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
		err = s.Set(ctx, key, v, options...)
		if err != nil {
			return nil, err
		}
		return v, nil
	})

	return v, err
}

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
