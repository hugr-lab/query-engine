package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

type Source struct {
	ds         types.DataSource
	engine     engines.Engine
	isAttached bool
	client     *goredis.Client
	timeout    time.Duration
}

func New(ds types.DataSource, attached bool) (*Source, error) {
	return &Source{
		ds:     ds,
		engine: engines.NewDuckDB(),
	}, nil
}

func (s *Source) Name() string             { return s.ds.Name }
func (s *Source) Definition() types.DataSource { return s.ds }
func (s *Source) Engine() engines.Engine   { return s.engine }
func (s *Source) IsAttached() bool         { return s.isAttached }
func (s *Source) ReadOnly() bool           { return false }

func (s *Source) Attach(ctx context.Context, _ *db.Pool) error {
	if s.isAttached {
		return sources.ErrDataSourceAttached
	}

	path, err := sources.ApplyEnvVars(s.ds.Path)
	if err != nil {
		return err
	}

	opts, err := goredis.ParseURL(path)
	if err != nil {
		return fmt.Errorf("parse redis URL: %w", err)
	}

	s.timeout = 5 * time.Second
	if opts.ReadTimeout > 0 {
		s.timeout = opts.ReadTimeout
	}

	s.client = goredis.NewClient(opts)

	pingCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	if err := s.client.Ping(pingCtx).Err(); err != nil {
		s.client.Close()
		return fmt.Errorf("redis ping failed: %w", err)
	}

	s.isAttached = true
	return nil
}

func (s *Source) Detach(_ context.Context, _ *db.Pool) error {
	if s.client != nil {
		s.client.Close()
	}
	s.isAttached = false
	return nil
}

// --- StoreSource interface ---

func (s *Source) Get(ctx context.Context, key string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	val, err := s.client.Get(ctx, key).Result()
	if err == goredis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return val, true, nil
}

func (s *Source) Set(ctx context.Context, key string, value string, ttlSeconds int) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var ttl time.Duration
	if ttlSeconds > 0 {
		ttl = time.Duration(ttlSeconds) * time.Second
	}
	return s.client.Set(ctx, key, value, ttl).Err()
}

func (s *Source) Del(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Del(ctx, key).Err()
}

func (s *Source) Incr(ctx context.Context, key string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Incr(ctx, key).Result()
}

func (s *Source) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.IncrBy(ctx, key, value).Result()
}

func (s *Source) Expire(ctx context.Context, key string, ttlSeconds int) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Expire(ctx, key, time.Duration(ttlSeconds)*time.Second).Err()
}

func (s *Source) Keys(ctx context.Context, pattern string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Keys(ctx, pattern).Result()
}

var (
	_ sources.Source      = (*Source)(nil)
	_ sources.StoreSource = (*Source)(nil)
)
