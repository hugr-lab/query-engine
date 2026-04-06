package ratelimit

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLimiter_RPM_InMemory(t *testing.T) {
	l := New("test-source", 3, 0, nil)

	ctx := context.Background()

	// First 3 requests should succeed
	for i := 0; i < 3; i++ {
		err := l.Check(ctx)
		require.NoError(t, err, "request %d should succeed", i+1)
	}

	// 4th request should fail
	err := l.Check(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRateLimitExceeded), "expected ErrRateLimitExceeded, got: %v", err)
}

func TestLimiter_TPM_InMemory(t *testing.T) {
	l := New("test-source", 0, 1000, nil)

	ctx := context.Background()

	// Record 500 tokens — should succeed
	err := l.Record(ctx, 500)
	require.NoError(t, err)

	// Record 400 more — still under limit
	err = l.Record(ctx, 400)
	require.NoError(t, err)

	// Record 200 more — exceeds 1000
	err = l.Record(ctx, 200)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRateLimitExceeded))
}

func TestLimiter_NoLimits(t *testing.T) {
	l := New("test-source", 0, 0, nil)

	ctx := context.Background()

	// Should always succeed with no limits
	for i := 0; i < 100; i++ {
		err := l.Check(ctx)
		require.NoError(t, err)
	}

	err := l.Record(ctx, 1000000)
	require.NoError(t, err)
}

func TestLimiter_RPM_WithRedis(t *testing.T) {
	// This test uses the StoreSource interface via a mock.
	store := &mockStore{data: make(map[string]int64)}
	l := New("test-source", 2, 0, store)

	ctx := context.Background()

	// First 2 succeed
	require.NoError(t, l.Check(ctx))
	require.NoError(t, l.Check(ctx))

	// 3rd fails
	err := l.Check(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrRateLimitExceeded))

	// Verify Redis keys were used
	assert.Len(t, store.data, 1, "should have one RPM key")
}

// mockStore implements sources.StoreSource for testing.
type mockStore struct {
	data map[string]int64
}

func (m *mockStore) Get(_ context.Context, key string) (string, bool, error) {
	return "", false, nil
}

func (m *mockStore) Set(_ context.Context, key string, value string, ttlSeconds int) error {
	return nil
}

func (m *mockStore) Del(_ context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func (m *mockStore) Incr(_ context.Context, key string) (int64, error) {
	m.data[key]++
	return m.data[key], nil
}

func (m *mockStore) IncrBy(_ context.Context, key string, value int64) (int64, error) {
	m.data[key] += value
	return m.data[key], nil
}

func (m *mockStore) Expire(_ context.Context, key string, ttlSeconds int) error {
	return nil
}

func (m *mockStore) Keys(_ context.Context, pattern string) ([]string, error) {
	return nil, nil
}
