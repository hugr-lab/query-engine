package cache

import (
	"context"
	"errors"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/eko/gocache/lib/v4/cache"
	"github.com/eko/gocache/lib/v4/store"
	bs "github.com/eko/gocache/store/bigcache/v4"
	mc "github.com/eko/gocache/store/memcache/v4"
	ps "github.com/eko/gocache/store/pegasus/v4"
	rs "github.com/eko/gocache/store/redis/v4"
	rrs "github.com/eko/gocache/store/rediscluster/v4"
	"github.com/redis/go-redis/v9"
)

type BackendType string

const (
	L2RedisBackend     BackendType = "redis"
	L2MemcachedBackend BackendType = "memcached"
	L2PegasusBackend   BackendType = "pegasus"
)

var (
	ErrNoCacheConfigured = errors.New("no cache configured")
)

type Config struct {
	TTL time.Duration // default time to live for the cache
	L1  L1Config
	L2  L2Config
}

func (c Config) Init(ctx context.Context) (cache.CacheInterface[any], error) {
	switch {
	case c.L1.Enabled && !c.L2.Enabled:
		store, err := c.L1.Init(ctx)
		if err != nil {
			return nil, err
		}
		return cache.New[any](store), nil
	case !c.L1.Enabled && c.L2.Enabled:
		store, err := c.L2.Init(ctx)
		if err != nil {
			return nil, err
		}
		return cache.New[any](store), nil
	case c.L1.Enabled && c.L2.Enabled:
		l1, err := c.L1.Init(ctx)
		if err != nil {
			return nil, err
		}
		l2, err := c.L2.Init(ctx)
		if err != nil {
			return nil, err
		}
		return cache.NewChain(cache.New[any](l1), cache.New[any](l2)), nil
	}
	return nil, ErrNoCacheConfigured
}

type L1Config struct {
	Enabled      bool
	MaxSize      int // Maximum size of the cache Megabytes
	MaxItemSize  int // Maximum size of an item in the cache
	Shards       int // Number of shards in the cache
	CleanTime    time.Duration
	EvictionTime time.Duration
}
type L2Config struct {
	Enabled bool
	Backend BackendType

	Addresses []string // Addresses for the L2 cache
	Database  int      // Database for the L2 cache (for Redis)
	Username  string   // Username for the L2 cache (for Redis)
	Password  string   // Password for the L2 cache (for Redis)
}

func (c L1Config) Init(ctx context.Context) (store.StoreInterface, error) {
	conf := bigcache.DefaultConfig(c.EvictionTime)
	conf.HardMaxCacheSize = c.MaxSize
	conf.CleanWindow = c.CleanTime
	conf.MaxEntrySize = c.MaxItemSize
	if c.Shards == 0 {
		conf.Shards = 64
	}
	if conf.MaxEntrySize == 0 {
		conf.MaxEntrySize = 100 << 20 // 100MB
	}
	client, err := bigcache.New(ctx, conf)
	if err != nil {
		return nil, err
	}
	return bs.NewBigcache(client), nil
}

func (c L2Config) Init(ctx context.Context) (store.StoreInterface, error) {
	switch c.Backend {
	case L2RedisBackend:
		if len(c.Addresses) == 0 {
			return nil, errors.New("redis addresses are required")
		}
		if len(c.Addresses) == 1 {
			client := redis.NewClient(&redis.Options{
				Addr:     c.Addresses[0],
				Username: c.Username,
				Password: c.Password,
				DB:       c.Database,
			})
			s := client.Ping(ctx)
			if err := s.Err(); err != nil {
				return nil, err
			}
			return rs.NewRedis(client), nil
		}
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    c.Addresses,
			Username: c.Username,
			Password: c.Password,
		})
		s := client.Ping(ctx)
		if err := s.Err(); err != nil {
			return nil, err
		}
		return rrs.NewRedisCluster(client), nil
	case L2MemcachedBackend:
		client := memcache.New(c.Addresses...)
		if err := client.Ping(); err != nil {
			return nil, err
		}
		return mc.NewMemcache(client), nil
	case L2PegasusBackend:
		return ps.NewPegasus(ctx, &ps.OptionsPegasus{
			MetaServers: c.Addresses,
		})
	default:
		return nil, errors.New("unsupported l2 backend type")
	}
}
