package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

// --- PubSubStoreSource interface ---

var pubsubSchema = arrow.NewSchema([]arrow.Field{
	{Name: "channel", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "message", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

func (s *Source) Subscribe(ctx context.Context, channel string) (*sources.SubscriptionResult, error) {
	ps := s.client.Subscribe(ctx, channel)
	return s.newPubSubResult(ctx, ps), nil
}

func (s *Source) PSubscribe(ctx context.Context, pattern string) (*sources.SubscriptionResult, error) {
	ps := s.client.PSubscribe(ctx, pattern)
	return s.newPubSubResult(ctx, ps), nil
}

func (s *Source) newPubSubResult(ctx context.Context, ps *goredis.PubSub) *sources.SubscriptionResult {
	ch := ps.Channel()
	reader := &channelRecordReader{
		schema: pubsubSchema,
		ch:     ch,
		ctx:    ctx,
	}
	return &sources.SubscriptionResult{
		Reader: reader,
		Cancel: func() { ps.Close() },
	}
}

func (s *Source) Publish(ctx context.Context, channel string, message string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.Publish(ctx, channel, message).Err()
}

func (s *Source) ConfigGet(ctx context.Context, key string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	res, err := s.client.ConfigGet(ctx, key).Result()
	if err != nil {
		return "", err
	}
	if v, ok := res[key]; ok {
		return v, nil
	}
	return "", nil
}

func (s *Source) ConfigSet(ctx context.Context, key string, value string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	return s.client.ConfigSet(ctx, key, value).Err()
}

// channelRecordReader implements array.RecordReader over a go-redis PubSub channel.
type channelRecordReader struct {
	schema *arrow.Schema
	ch     <-chan *goredis.Message
	ctx    context.Context

	current arrow.RecordBatch
	err     error
	refs    int64
	once    sync.Once
}

func (r *channelRecordReader) Retain()              { r.refs++ }
func (r *channelRecordReader) Schema() *arrow.Schema { return r.schema }
func (r *channelRecordReader) Err() error            { return r.err }

func (r *channelRecordReader) Release() {
	r.once.Do(func() {
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
	})
}

func (r *channelRecordReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}
	select {
	case <-r.ctx.Done():
		r.err = r.ctx.Err()
		return false
	case msg, ok := <-r.ch:
		if !ok {
			return false
		}
		mem := memory.NewGoAllocator()
		bldr := array.NewRecordBuilder(mem, r.schema)
		defer bldr.Release()
		bldr.Field(0).(*array.StringBuilder).Append(msg.Channel)
		bldr.Field(1).(*array.StringBuilder).Append(msg.Payload)
		r.current = bldr.NewRecordBatch()
		return true
	}
}

func (r *channelRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *channelRecordReader) RecordBatch() arrow.RecordBatch  { return r.current }

var (
	_ sources.Source           = (*Source)(nil)
	_ sources.StoreSource      = (*Source)(nil)
	_ sources.PubSubStoreSource = (*Source)(nil)
)
