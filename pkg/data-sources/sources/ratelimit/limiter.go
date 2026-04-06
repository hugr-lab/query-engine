// Package ratelimit provides request and token rate limiting for LLM sources.
// Supports both Redis-backed shared counters and in-memory per-node fallback.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
)

var ErrRateLimitExceeded = errors.New("rate limit exceeded")

// Limiter enforces per-minute request and token limits for an LLM source.
// When a StoreSource is provided, counters are shared via Redis keys.
// Otherwise, counters are tracked in-memory (per-node only).
type Limiter struct {
	sourceName string
	rpm        int // max requests per minute (0 = unlimited)
	tpm        int // max tokens per minute (0 = unlimited)

	store sources.StoreSource // nil → in-memory fallback

	mu       sync.Mutex
	inMemory map[string]int64
}

// New creates a rate limiter for the given source.
// If store is nil, uses in-memory counters.
func New(sourceName string, rpm, tpm int, store sources.StoreSource) *Limiter {
	return &Limiter{
		sourceName: sourceName,
		rpm:        rpm,
		tpm:        tpm,
		store:      store,
		inMemory:   make(map[string]int64),
	}
}

// Check verifies that the current request is within rate limits.
// Call before making an API request.
func (l *Limiter) Check(ctx context.Context) error {
	if l.rpm <= 0 {
		return nil
	}

	key := l.rpmKey()
	count, err := l.incr(ctx, key)
	if err != nil {
		return fmt.Errorf("rate limit check: %w", err)
	}

	if count > int64(l.rpm) {
		return fmt.Errorf("%w: %d requests per minute exceeded for %s", ErrRateLimitExceeded, l.rpm, l.sourceName)
	}
	return nil
}

// Record updates token counters after a successful API call.
// Returns an error if the token limit has been exceeded (informational — request already completed).
func (l *Limiter) Record(ctx context.Context, totalTokens int) error {
	if l.tpm <= 0 || totalTokens <= 0 {
		return nil
	}

	key := l.tpmKey()
	count, err := l.incrBy(ctx, key, int64(totalTokens))
	if err != nil {
		return fmt.Errorf("rate limit record: %w", err)
	}

	if count > int64(l.tpm) {
		return fmt.Errorf("%w: %d tokens per minute exceeded for %s", ErrRateLimitExceeded, l.tpm, l.sourceName)
	}
	return nil
}

// rpmKey returns the Redis/memory key for request count in the current minute window.
func (l *Limiter) rpmKey() string {
	window := time.Now().UTC().Format("2006-01-02T15:04")
	return fmt.Sprintf("ratelimit:%s:rpm:%s", l.sourceName, window)
}

// tpmKey returns the Redis/memory key for token count in the current minute window.
func (l *Limiter) tpmKey() string {
	window := time.Now().UTC().Format("2006-01-02T15:04")
	return fmt.Sprintf("ratelimit:%s:tpm:%s", l.sourceName, window)
}

func (l *Limiter) incr(ctx context.Context, key string) (int64, error) {
	if l.store != nil {
		val, err := l.store.Incr(ctx, key)
		if err != nil {
			return 0, err
		}
		// Set TTL on first increment (val == 1) to auto-expire after 2 minutes.
		if val == 1 {
			_ = l.store.Expire(ctx, key, 120)
		}
		return val, nil
	}

	// In-memory fallback
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cleanExpired()
	l.inMemory[key]++
	return l.inMemory[key], nil
}

func (l *Limiter) incrBy(ctx context.Context, key string, value int64) (int64, error) {
	if l.store != nil {
		val, err := l.store.IncrBy(ctx, key, value)
		if err != nil {
			return 0, err
		}
		if val == value { // first write to this key
			_ = l.store.Expire(ctx, key, 120)
		}
		return val, nil
	}

	// In-memory fallback
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cleanExpired()
	l.inMemory[key] += value
	return l.inMemory[key], nil
}

// cleanExpired removes in-memory keys from previous minute windows.
// Must be called with l.mu held.
func (l *Limiter) cleanExpired() {
	currentWindow := time.Now().UTC().Format("2006-01-02T15:04")
	for key := range l.inMemory {
		// Keys are "ratelimit:<source>:<type>:<window>" — keep only current window.
		if len(key) < 16 {
			delete(l.inMemory, key)
			continue
		}
		// Extract window from key suffix (last 16 chars: "2006-01-02T15:04")
		keyWindow := key[len(key)-16:]
		if keyWindow != currentWindow {
			delete(l.inMemory, key)
		}
	}
}
