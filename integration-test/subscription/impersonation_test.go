//go:build duckdb_arrow

package subscription_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/types"
)

const meQuery = `{ function { core { auth { me { user_id user_name role auth_type auth_provider impersonated_by_user_id impersonated_by_user_name } } } } }`

type meResult struct {
	UserId       string `json:"user_id"`
	UserName     string `json:"user_name"`
	Role         string `json:"role"`
	AuthType     string `json:"auth_type"`
	AuthProvider string `json:"auth_provider"`
	ImpByUID     string `json:"impersonated_by_user_id"`
	ImpByName    string `json:"impersonated_by_user_name"`
}

func newAnonymousClient(t *testing.T) *client.Client {
	t.Helper()
	url := strings.TrimSuffix(testServer.URL, "/") + "/ipc"
	return client.NewClient(url)
}

func newAdminClient(t *testing.T) *client.Client {
	t.Helper()
	url := strings.TrimSuffix(testServer.URL, "/") + "/ipc"
	return client.NewClient(url, client.WithSecretKeyAuth("test-secret-key"))
}

// --- Admin impersonation (should work) ---

func TestImpersonation_Admin_Query_AsUser(t *testing.T) {
	c := newAdminClient(t)
	ctx := context.Background()

	// Query as impersonated user
	impCtx := types.AsUser(ctx, "user-123", "John Doe", "admin")
	resp, err := c.Query(impCtx, meQuery, nil)
	require.NoError(t, err)
	defer resp.Close()
	require.NoError(t, resp.Err())

	var me meResult
	err = resp.ScanData("function.core.auth.me", &me)
	require.NoError(t, err)

	assert.Equal(t, "user-123", me.UserId, "should be impersonated user")
	assert.Equal(t, "John Doe", me.UserName)
	assert.Equal(t, "admin", me.Role)
	assert.Equal(t, "impersonation", me.AuthType, "auth_type must be 'impersonation'")
	assert.Equal(t, "x-hugr-secret", me.AuthProvider)
	assert.Equal(t, "api", me.ImpByUID, "impersonated_by should be the original admin")
	assert.Equal(t, "api", me.ImpByName)
}

func TestImpersonation_Admin_Query_NoOverride(t *testing.T) {
	c := newAdminClient(t)
	ctx := context.Background()

	// Query without impersonation — should be default admin
	resp, err := c.Query(ctx, meQuery, nil)
	require.NoError(t, err)
	defer resp.Close()
	require.NoError(t, resp.Err())

	var me meResult
	err = resp.ScanData("function.core.auth.me", &me)
	require.NoError(t, err)

	assert.Equal(t, "api", me.UserId)
	assert.Equal(t, "admin", me.Role)
	assert.Equal(t, "apiKey", me.AuthType, "should be regular apiKey auth")
	assert.Empty(t, me.ImpByUID, "should not be impersonated")
}

func TestImpersonation_Admin_VerifyAdmin(t *testing.T) {
	c := newAdminClient(t)
	ctx := context.Background()

	err := c.VerifyAdmin(ctx)
	require.NoError(t, err)
}

func TestImpersonation_Admin_Subscribe_AsUser(t *testing.T) {
	c := newAdminClient(t)
	defer c.CloseSubscriptions()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe as impersonated user
	impCtx := types.AsUser(ctx, "user-456", "Jane Smith", "admin")
	sub, err := c.Subscribe(impCtx, `subscription { query(interval: "1s") { function { core { auth { me { user_id user_name role auth_type impersonated_by_user_id } } } } } }`, nil)
	require.NoError(t, err)
	defer sub.Cancel()

	// Read first event — should contain impersonated identity
	select {
	case event := <-sub.Events:
		assert.Equal(t, "function.core.auth.me", event.Path)
		require.NotNil(t, event.Reader)
		require.True(t, event.Reader.Next())

		batch := event.Reader.RecordBatch()
		require.Greater(t, int(batch.NumRows()), 0)

		// Check schema has expected columns
		schema := batch.Schema()
		fieldNames := make([]string, schema.NumFields())
		for i := range schema.NumFields() {
			fieldNames[i] = schema.Field(i).Name
		}
		assert.Contains(t, fieldNames, "user_id")
		assert.Contains(t, fieldNames, "auth_type")
		assert.Contains(t, fieldNames, "impersonated_by_user_id")

		event.Reader.Release()
	case <-ctx.Done():
		t.Fatal("timeout waiting for subscription event")
	}
}

func TestImpersonation_Admin_Subscribe_TwoUsers_SameConnection(t *testing.T) {
	c := newAdminClient(t)
	defer c.CloseSubscriptions()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Subscribe as user-A
	ctxA := types.AsUser(ctx, "user-A", "Alice", "admin")
	subA, err := c.Subscribe(ctxA, `subscription { query(interval: "1s") { function { core { auth { me { user_id auth_type } } } } } }`, nil)
	require.NoError(t, err)
	defer subA.Cancel()

	// Subscribe as user-B on same pooled connection
	ctxB := types.AsUser(ctx, "user-B", "Bob", "admin")
	subB, err := c.Subscribe(ctxB, `subscription { query(interval: "1s") { function { core { auth { me { user_id auth_type } } } } } }`, nil)
	require.NoError(t, err)
	defer subB.Cancel()

	// Both subscriptions should get events
	gotA := false
	gotB := false
	timeout := time.After(10 * time.Second)
	for !gotA || !gotB {
		select {
		case event := <-subA.Events:
			if event.Reader != nil {
				event.Reader.Release()
			}
			gotA = true
		case event := <-subB.Events:
			if event.Reader != nil {
				event.Reader.Release()
			}
			gotB = true
		case <-timeout:
			t.Fatalf("timeout: gotA=%v gotB=%v", gotA, gotB)
		}
	}
}

// --- Anonymous impersonation (should be rejected/ignored) ---

func TestImpersonation_Anonymous_Query_HeadersIgnored(t *testing.T) {
	c := newAnonymousClient(t)
	ctx := context.Background()

	// AsUser adds override headers, but anonymous provider ignores them
	impCtx := types.AsUser(ctx, "attacker", "Evil", "admin")
	resp, err := c.Query(impCtx, meQuery, nil)
	require.NoError(t, err)
	defer resp.Close()
	require.NoError(t, resp.Err())

	var me meResult
	err = resp.ScanData("function.core.auth.me", &me)
	require.NoError(t, err)

	assert.Equal(t, "anonymous", me.UserId, "override headers should be ignored")
	assert.Equal(t, "anonymous", me.AuthType)
	assert.Empty(t, me.ImpByUID)
}

func TestImpersonation_Anonymous_Subscribe_Rejected(t *testing.T) {
	c := newAnonymousClient(t)
	defer c.CloseSubscriptions()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Anonymous IPC subscribe with identity fields — server rejects
	impCtx := types.AsUser(ctx, "attacker", "Evil", "admin")
	sub, err := c.Subscribe(impCtx, `subscription { query(interval: "1s") { function { core { auth { me { user_id } } } } } }`, nil)
	if err != nil {
		assert.Contains(t, err.Error(), "secret key")
		return
	}
	defer sub.Cancel()

	// Subscription_error closes channel
	select {
	case event, ok := <-sub.Events:
		if !ok {
			return // rejected
		}
		if event.Reader != nil {
			event.Reader.Release()
		}
		t.Fatal("expected subscription to be rejected")
	case <-ctx.Done():
		return
	}
}
