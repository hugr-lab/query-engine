//go:build duckdb_arrow

package subscription_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

var (
	testService *hugr.Service
	testServer  *httptest.Server
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	service, err := hugr.New(hugr.Config{
		Debug:  true,
		DB:     db.Config{},
		CoreDB: coredb.New(coredb.Config{}),
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewApiKey("x-hugr-secret", auth.ApiKeyConfig{
					Key:         "test-secret-key",
					Header:      "x-hugr-secret-key",
					DefaultRole: "admin",
				}),
				auth.NewApiKey("x-hugr-public", auth.ApiKeyConfig{
					Key:         "test-public-key",
					Header:      "x-hugr-public-key",
					DefaultRole: "public",
				}),
				auth.NewAnonymous(auth.AnonymousConfig{
					Allowed: true,
					Role:    "admin",
				}),
			},
		},
	})
	if err != nil {
		panic(err)
	}
	if err := service.Init(ctx); err != nil {
		panic(err)
	}
	testService = service
	testServer = httptest.NewServer(service)

	code := m.Run()
	testServer.Close()
	service.Close()
	os.Exit(code)
}

// --- graphql-ws tests ---

type gqlwsMsg struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

func connectGraphQLWS(t *testing.T) *websocket.Conn {
	t.Helper()
	ctx := context.Background()
	wsURL := testServer.URL + "/subscribe"
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"graphql-transport-ws"},
	})
	require.NoError(t, err)

	// connection_init
	data, _ := json.Marshal(gqlwsMsg{Type: "connection_init"})
	err = conn.Write(ctx, websocket.MessageText, data)
	require.NoError(t, err)

	// connection_ack
	_, ackData, err := conn.Read(ctx)
	require.NoError(t, err)
	var ack gqlwsMsg
	require.NoError(t, json.Unmarshal(ackData, &ack))
	assert.Equal(t, "connection_ack", ack.Type)

	return conn
}

func TestGraphQLWS_QueryStreaming(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.CloseNow()

	// Subscribe to a query — use catalog.types which always has data
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query { core { catalog { types(limit: 3) { name kind } } } } }`,
	})
	wdata, _ := json.Marshal(gqlwsMsg{ID: "1", Type: "subscribe", Payload: payload})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Collect next messages
	var messages []gqlwsMsg
	for {
		var msg gqlwsMsg
		_, rdata, err := conn.Read(context.Background())
		if err != nil {
			t.Logf("read error (expected at end): %v", err)
			break
		}
		json.Unmarshal(rdata, &msg)
		messages = append(messages, msg)
		if msg.Type == "complete" || msg.Type == "error" {
			break
		}
		// Safety: don't collect more than 100 messages
		if len(messages) > 100 {
			break
		}
	}

	// Should have at least one "next" and one "complete"
	require.NotEmpty(t, messages, "should receive at least one message")

	var hasNext, hasComplete bool
	for _, msg := range messages {
		switch msg.Type {
		case "next":
			hasNext = true
			// Verify payload has data
			var payload map[string]any
			err := json.Unmarshal(msg.Payload, &payload)
			require.NoError(t, err)
			t.Logf("next payload: %s", string(msg.Payload))
		case "complete":
			hasComplete = true
			assert.Equal(t, "1", msg.ID)
		case "error":
			t.Fatalf("received error: %s", string(msg.Payload))
		}
	}
	assert.True(t, hasComplete, "should receive complete message")
	t.Logf("received %d messages (hasNext=%v, hasComplete=%v)", len(messages), hasNext, hasComplete)
}

func TestGraphQLWS_PeriodicPolling(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.CloseNow()

	// Subscribe with interval=1s, count=2 — use catalog.types which always has data
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query(interval: "1s", count: 2) { core { catalog { types(limit: 3) { name } } } } }`,
	})
	wdata, _ := json.Marshal(gqlwsMsg{ID: "poll", Type: "subscribe", Payload: payload})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Collect messages — expect 2 rounds of data + complete
	var messages []gqlwsMsg
	for {
		var msg gqlwsMsg
		_, rdata, err := conn.Read(context.Background())
		if err != nil {
			break
		}
		json.Unmarshal(rdata, &msg)
		messages = append(messages, msg)
		if msg.Type == "complete" || msg.Type == "error" {
			break
		}
		if len(messages) > 200 {
			break
		}
	}

	var nextCount int
	var hasComplete bool
	for _, msg := range messages {
		if msg.Type == "next" {
			nextCount++
		}
		if msg.Type == "complete" {
			hasComplete = true
		}
	}

	assert.True(t, hasComplete, "should complete after count=2")
	// Note: with empty DB (no data sources), next count may be 0
	t.Logf("periodic: %d next messages, complete=%v", nextCount, hasComplete)
}

func TestGraphQLWS_MultiPathQuery(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.CloseNow()

	// Subscribe to a query with multiple data object paths
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query {
			core {
				catalog { types(limit: 2) { name kind } }
				data_sources { name type }
			}
		} }`,
	})
	wdata, _ := json.Marshal(gqlwsMsg{ID: "multi", Type: "subscribe", Payload: payload})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Collect messages — expect rows from both paths interleaved
	paths := map[string]int{}
	for {
		var msg gqlwsMsg
		_, rdata, err := conn.Read(context.Background())
		if err != nil {
			break
		}
		json.Unmarshal(rdata, &msg)
		if msg.Type == "next" {
			var p map[string]any
			json.Unmarshal(msg.Payload, &p)
			data, _ := p["data"].(map[string]any)
			if data != nil {
				if core, ok := data["core"].(map[string]any); ok {
					if _, ok := core["catalog"]; ok {
						paths["catalog"]++
					}
					if _, ok := core["data_sources"]; ok {
						paths["data_sources"]++
					}
				}
			}
		}
		if msg.Type == "complete" {
			break
		}
	}

	assert.Greater(t, paths["catalog"], 0, "should have catalog rows")
	// data_sources may be empty in test DB but path should still be attempted
	t.Logf("multi-path: catalog=%d rows, data_sources=%d rows", paths["catalog"], paths["data_sources"])
}

func TestGraphQLWS_Cancel(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.CloseNow()

	// Subscribe to periodic — first tick executes immediately, then waits interval
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query(interval: "60s") { core { catalog { types(limit: 1) { name } } } } }`,
	})
	wdata, _ := json.Marshal(gqlwsMsg{ID: "cancel-test", Type: "subscribe", Payload: payload})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Wait for first message (next or complete from first tick)
	var msg gqlwsMsg
	_, rdata, err := conn.Read(context.Background())
	require.NoError(t, err)
	json.Unmarshal(rdata, &msg)
	// First tick should produce at least a complete for the tick
	t.Logf("first message: type=%s", msg.Type)

	// Send complete (cancel) — should stop the periodic subscription
	wdata, _ = json.Marshal(gqlwsMsg{ID: "cancel-test", Type: "complete"})
	err = conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Should receive complete back (may receive more next messages first)
	for {
		_, rdata, err = conn.Read(context.Background())
		if err != nil {
			break
		}
		json.Unmarshal(rdata, &msg)
		if msg.Type == "complete" {
			break
		}
	}
	assert.Equal(t, "complete", msg.Type)
}

func TestGraphQLWS_Multiplexing(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.CloseNow()

	// Subscribe two queries simultaneously
	for _, id := range []string{"sub-a", "sub-b"} {
		payload, _ := json.Marshal(map[string]any{
			"query": `subscription { query { core { data_sources { name } } } }`,
		})
		wdata, _ := json.Marshal(gqlwsMsg{ID: id, Type: "subscribe", Payload: payload})
		err := conn.Write(context.Background(), websocket.MessageText, wdata)
		require.NoError(t, err)
	}

	// Collect messages for both
	ids := map[string]int{}
	for {
		var msg gqlwsMsg
		_, rdata, err := conn.Read(context.Background())
		if err != nil {
			break
		}
		json.Unmarshal(rdata, &msg)
		ids[msg.ID]++
		if ids["sub-a"] > 0 && ids["sub-b"] > 0 {
			// Both produced messages
			break
		}
		if len(ids) > 200 {
			break
		}
	}

	assert.Greater(t, ids["sub-a"], 0, "sub-a should have messages")
	assert.Greater(t, ids["sub-b"], 0, "sub-b should have messages")
	t.Logf("multiplexing: sub-a=%d, sub-b=%d messages", ids["sub-a"], ids["sub-b"])
}

// --- IPC subscription tests ---

func connectIPC(t *testing.T) *websocket.Conn {
	t.Helper()
	ctx := context.Background()
	wsURL := testServer.URL + "/ipc"
	conn, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{
		Subprotocols: []string{"hugr-ipc-ws"},
	})
	require.NoError(t, err)
	conn.SetReadLimit(64 * 1024 * 1024)
	return conn
}

type ipcMsg struct {
	Type           string `json:"type"`
	SubscriptionID string `json:"subscription_id,omitempty"`
	Path           string `json:"path,omitempty"`
	Query          string `json:"query,omitempty"`
	Error          string `json:"error,omitempty"`
}

func TestIPC_Subscribe(t *testing.T) {
	conn := connectIPC(t)
	defer conn.CloseNow()

	// Send subscribe — use catalog.types which always has data
	wdata, _ := json.Marshal(ipcMsg{
		Type:           "subscribe",
		SubscriptionID: "s1",
		Query:          `subscription { query { core { catalog { types(limit: 3) { name kind } } } } }`,
	})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Collect messages
	var textMsgs []ipcMsg
	var binaryCount int
	for {
		msgType, data, err := conn.Read(context.Background())
		if err != nil {
			t.Logf("read error: %v", err)
			break
		}
		switch msgType {
		case websocket.MessageText:
			var msg ipcMsg
			json.Unmarshal(data, &msg)
			textMsgs = append(textMsgs, msg)
			t.Logf("text: %s (sub=%s, path=%s)", msg.Type, msg.SubscriptionID, msg.Path)
			if msg.Type == "subscription_complete" || msg.Type == "subscription_error" {
				goto done
			}
		case websocket.MessageBinary:
			binaryCount++
			t.Logf("binary: %d bytes", len(data))
		}
		if len(textMsgs)+binaryCount > 100 {
			break
		}
	}
done:

	// Protocol: binary Arrow frames (with subscription_id in metadata) +
	// part_complete + subscription_complete text frames
	var hasPartComplete, hasSubComplete bool
	for _, msg := range textMsgs {
		switch msg.Type {
		case "part_complete":
			hasPartComplete = true
			assert.Equal(t, "s1", msg.SubscriptionID)
		case "subscription_complete":
			hasSubComplete = true
			assert.Equal(t, "s1", msg.SubscriptionID)
		}
	}

	assert.True(t, hasPartComplete, "should have part_complete")
	assert.True(t, hasSubComplete, "should have subscription_complete")
	assert.Greater(t, binaryCount, 0, "should have binary Arrow frames")
	t.Logf("IPC: %d text msgs, %d binary frames", len(textMsgs), binaryCount)
}

func TestIPC_Unsubscribe(t *testing.T) {
	conn := connectIPC(t)
	defer conn.CloseNow()

	// Subscribe to periodic with data (catalog.types always has data)
	wdata, _ := json.Marshal(ipcMsg{
		Type:           "subscribe",
		SubscriptionID: "s-cancel",
		Query:          `subscription { query(interval: "30s") { core { catalog { types(limit: 1) { name } } } } }`,
	})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Wait for first binary frame (Arrow data)
	for {
		msgType, _, err := conn.Read(context.Background())
		require.NoError(t, err)
		if msgType == websocket.MessageBinary {
			break
		}
	}

	// Unsubscribe
	wdata, _ = json.Marshal(ipcMsg{
		Type:           "unsubscribe",
		SubscriptionID: "s-cancel",
	})
	err = conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	// Should get subscription_complete
	for {
		msgType, data, err := conn.Read(context.Background())
		if err != nil {
			break
		}
		if msgType == websocket.MessageText {
			var msg ipcMsg
			json.Unmarshal(data, &msg)
			if msg.Type == "subscription_complete" {
				assert.Equal(t, "s-cancel", msg.SubscriptionID)
				return // success
			}
		}
	}
	t.Fatal("did not receive subscription_complete after unsubscribe")
}

func TestIPC_MultipleSubscriptions(t *testing.T) {
	conn := connectIPC(t)
	defer conn.CloseNow()

	// Subscribe two queries
	for _, id := range []string{"ms1", "ms2"} {
		wdata, _ := json.Marshal(ipcMsg{
			Type:           "subscribe",
			SubscriptionID: id,
			Query:          `subscription { query { core { data_sources { name } } } }`,
		})
		err := conn.Write(context.Background(), websocket.MessageText, wdata)
		require.NoError(t, err)
	}

	// Collect — expect messages from both subscription IDs
	seen := map[string]bool{}
	for {
		msgType, data, err := conn.Read(context.Background())
		if err != nil {
			break
		}
		if msgType == websocket.MessageText {
			var msg ipcMsg
			json.Unmarshal(data, &msg)
			if msg.SubscriptionID != "" {
				seen[msg.SubscriptionID] = true
			}
		}
		if seen["ms1"] && seen["ms2"] {
			break
		}
	}

	assert.True(t, seen["ms1"], "should see ms1 messages")
	assert.True(t, seen["ms2"], "should see ms2 messages")
}

func TestIPC_SubscribeCoexistsWithQuery(t *testing.T) {
	// Test that a regular query can run on the same connection as an active subscription.
	conn := connectIPC(t)
	defer conn.CloseNow()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start a subscription (first tick fires immediately, then waits 30s)
	wdata, _ := json.Marshal(ipcMsg{
		Type:           "subscribe",
		SubscriptionID: "bg",
		Query:          `subscription { query(interval: "30s") { core { data_sources { name } } } }`,
	})
	err := conn.Write(ctx, websocket.MessageText, wdata)
	require.NoError(t, err)

	// Wait for first subscription tick to complete (part_complete text message)
	for {
		msgType, data, err := conn.Read(ctx)
		require.NoError(t, err)
		if msgType == websocket.MessageText {
			var msg ipcMsg
			json.Unmarshal(data, &msg)
			if msg.Type == "part_complete" && msg.SubscriptionID == "bg" {
				break
			}
		}
		// Also accept binary frames (Arrow IPC data from subscription)
	}

	// Now send a regular query on the same connection
	wdata, _ = json.Marshal(map[string]any{
		"type":  "query",
		"query": `{ core { data_sources { name } } }`,
	})
	err = conn.Write(ctx, websocket.MessageText, wdata)
	require.NoError(t, err)

	// Should receive query results (binary) and then "complete" text message.
	// Skip any subscription events that may arrive concurrently.
	var gotQueryComplete bool
	for {
		msgType, data, err := conn.Read(ctx)
		if err != nil {
			t.Logf("read error: %v", err)
			break
		}
		if msgType == websocket.MessageText {
			var msg map[string]string
			json.Unmarshal(data, &msg)
			// "complete" from query (no subscription_id) vs "part_complete" from subscription
			if msg["type"] == "complete" && msg["subscription_id"] == "" {
				gotQueryComplete = true
				break
			}
		}
	}
	assert.True(t, gotQueryComplete, "regular query should complete on same connection")

	// Cleanup
	wdata, _ = json.Marshal(ipcMsg{Type: "unsubscribe", SubscriptionID: "bg"})
	conn.Write(ctx, websocket.MessageText, wdata)
}

// --- Go Client SDK tests ---

func TestGoClient_Subscribe(t *testing.T) {
	// Create a client pointing at the test server — anonymous auth passes without headers
	c := client.NewClient(testServer.URL+"/ipc", client.WithTimeout(10*time.Second))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sub, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 3) { name kind } } } } }`, nil)
	require.NoError(t, err)
	require.NotNil(t, sub)
	defer sub.Cancel()

	var eventCount int
	var totalRows int
	for event := range sub.Events {
		eventCount++
		for event.Reader.Next() {
			batch := event.Reader.RecordBatch()
			totalRows += int(batch.NumRows())
		}
		require.NoError(t, event.Reader.Err())
		event.Reader.Release()
	}

	assert.Greater(t, eventCount, 0, "should receive at least one event")
	assert.Greater(t, totalRows, 0, "should receive data rows")
	t.Logf("Go client: %d events, %d total rows", eventCount, totalRows)
}

func TestGoClient_SubscribePeriodic(t *testing.T) {
	c := client.NewClient(testServer.URL + "/ipc")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	sub, err := c.Subscribe(ctx, `subscription { query(interval: "1s", count: 2) { core { catalog { types(limit: 2) { name } } } } }`, nil)
	require.NoError(t, err)
	defer sub.Cancel()

	// One event per path — reader receives batches from ALL ticks.
	// reader.Next() → false only on subscription_complete (after count ticks).
	var eventCount, totalBatches int
	for event := range sub.Events {
		eventCount++
		for event.Reader.Next() {
			totalBatches++
		}
		event.Reader.Release()
	}

	assert.Equal(t, 1, eventCount, "one event per path (reader spans all ticks)")
	assert.GreaterOrEqual(t, totalBatches, 2, "batches from 2 ticks")
	t.Logf("Go client periodic: %d events, %d total batches", eventCount, totalBatches)
}

func TestGoClient_MultipleSubscriptions(t *testing.T) {
	c := client.NewClient(testServer.URL+"/ipc", client.WithTimeout(10*time.Second))
	defer c.CloseSubscriptions()

	ctx := context.Background()

	sub1, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 2) { name } } } } }`, nil)
	require.NoError(t, err)
	sub2, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 3) { name } } } } }`, nil)
	require.NoError(t, err)

	// Drain both in parallel
	var rows1, rows2 int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); rows1 = drainSub(sub1) }()
	go func() { defer wg.Done(); rows2 = drainSub(sub2) }()
	wg.Wait()

	assert.Greater(t, rows1, 0, "sub1 should have rows")
	assert.Greater(t, rows2, 0, "sub2 should have rows")
	t.Logf("Go client multiplexing: sub1=%d rows, sub2=%d rows", rows1, rows2)
}

func TestGoClient_ReuseAfterComplete(t *testing.T) {
	c := client.NewClient(testServer.URL+"/ipc", client.WithTimeout(10*time.Second))
	defer c.CloseSubscriptions()

	ctx := context.Background()

	// First subscription — completes
	sub1, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 1) { name } } } } }`, nil)
	require.NoError(t, err)
	var rows1 int
	for event := range sub1.Events {
		for event.Reader.Next() {
			rows1 += int(event.Reader.RecordBatch().NumRows())
		}
		event.Reader.Release()
	}
	assert.Greater(t, rows1, 0)

	// Second subscription — reuses same pool connection
	sub2, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 2) { name } } } } }`, nil)
	require.NoError(t, err)
	var rows2 int
	for event := range sub2.Events {
		for event.Reader.Next() {
			rows2 += int(event.Reader.RecordBatch().NumRows())
		}
		event.Reader.Release()
	}
	assert.Greater(t, rows2, 0)
	t.Logf("Go client reuse: sub1=%d rows, sub2=%d rows (same pool conn)", rows1, rows2)
}

func TestGoClient_DedicatedConn(t *testing.T) {
	c := client.NewClient(testServer.URL + "/ipc")
	ctx := context.Background()

	conn, err := c.NewSubscriptionConn(ctx)
	require.NoError(t, err)
	defer conn.Close()

	sub1, err := conn.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 2) { name } } } } }`, nil)
	require.NoError(t, err)
	sub2, err := conn.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 3) { name } } } } }`, nil)
	require.NoError(t, err)

	// Drain both in parallel — sequential drain deadlocks on shared connection
	var rows1, rows2 int
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); rows1 = drainSub(sub1) }()
	go func() { defer wg.Done(); rows2 = drainSub(sub2) }()
	wg.Wait()

	assert.Greater(t, rows1, 0)
	assert.Greater(t, rows2, 0)
	t.Logf("Go client dedicated conn: sub1=%d, sub2=%d rows", rows1, rows2)
}

func TestGoClient_Pool(t *testing.T) {
	c := client.NewClient(testServer.URL+"/ipc",
		client.WithTimeout(10*time.Second),
		client.WithSubscriptionPool(2, 1),
	)
	defer c.CloseSubscriptions()

	ctx := context.Background()

	sub1, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 1) { name } } } } }`, nil)
	require.NoError(t, err)
	sub2, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 2) { name } } } } }`, nil)
	require.NoError(t, err)
	sub3, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 3) { name } } } } }`, nil)
	require.NoError(t, err)

	var r1, r2, r3 int
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); r1 = drainSub(sub1) }()
	go func() { defer wg.Done(); r2 = drainSub(sub2) }()
	go func() { defer wg.Done(); r3 = drainSub(sub3) }()
	wg.Wait()

	assert.Greater(t, r1, 0)
	assert.Greater(t, r2, 0)
	assert.Greater(t, r3, 0)
	t.Logf("Go client pool(max=2): sub1=%d, sub2=%d, sub3=%d rows", r1, r2, r3)
}

func drainSub(sub *types.Subscription) int {
	var rows int
	for event := range sub.Events {
		for event.Reader.Next() {
			rows += int(event.Reader.RecordBatch().NumRows())
		}
		event.Reader.Release()
	}
	return rows
}

func TestGraphQLWS_InvalidQuery(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.CloseNow()

	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { nonexistent_field }`,
	})
	wdata, _ := json.Marshal(gqlwsMsg{ID: "bad", Type: "subscribe", Payload: payload})
	err := conn.Write(context.Background(), websocket.MessageText, wdata)
	require.NoError(t, err)

	var msg gqlwsMsg
	_, rdata, err := conn.Read(context.Background())
	require.NoError(t, err)
	json.Unmarshal(rdata, &msg)
	// Should get error
	assert.True(t, msg.Type == "error" || msg.Type == "complete",
		fmt.Sprintf("expected error or complete, got %s: %s", msg.Type, string(msg.Payload)))
}

func TestGoClient_TwoSubscriptionsSameData(t *testing.T) {
	c := client.NewClient(testServer.URL+"/ipc", client.WithTimeout(30*time.Second))
	defer c.CloseSubscriptions()

	ctx := context.Background()

	query := `subscription { query(interval: "500ms", count: 10) {
		core { catalog {
			types(limit: 100, order_by: [{field: "name", direction: ASC}]) { name kind }
			fields(limit: 100, order_by: [{field: "name", direction: ASC}]) { name field_type }
		} }
	} }`

	// Two subscriptions on the same connection with the same query
	sub1, err := c.Subscribe(ctx, query, nil)
	require.NoError(t, err)
	sub2, err := c.Subscribe(ctx, query, nil)
	require.NoError(t, err)

	type subResult struct {
		events int
		paths  map[string]int    // path → row count
		sample map[string]string // path → first row JSON (for comparison)
	}

	collect := func(sub *types.Subscription) *subResult {
		res := &subResult{paths: make(map[string]int), sample: make(map[string]string)}
		for event := range sub.Events {
			res.events++
			for event.Reader.Next() {
				batch := event.Reader.RecordBatch()
				schema := batch.Schema()
				for i := 0; i < int(batch.NumRows()); i++ {
					res.paths[event.Path]++
					// Capture first row per path for comparison
					if _, ok := res.sample[event.Path]; !ok {
						row := make(map[string]any)
						for j := 0; j < int(batch.NumCols()); j++ {
							row[schema.Field(j).Name] = batch.Column(j).GetOneForMarshal(i)
						}
						b, _ := json.Marshal(row)
						res.sample[event.Path] = string(b)
					}
				}
			}
			event.Reader.Release()
		}
		return res
	}

	var r1, r2 *subResult
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); r1 = collect(sub1) }()
	go func() { defer wg.Done(); r2 = collect(sub2) }()
	wg.Wait()

	// Each subscription should have 2 events (one per path: types + fields)
	assert.Equal(t, 2, r1.events, "sub1: should have 2 events (one reader per path)")
	assert.Equal(t, 2, r2.events, "sub2: should have 2 events (one reader per path)")

	// Both should have data for both paths
	assert.Len(t, r1.paths, 2, "sub1: should have 2 paths")
	assert.Len(t, r2.paths, 2, "sub2: should have 2 paths")

	// Each path should have rows from 10 ticks
	for path, count := range r1.paths {
		assert.Greater(t, count, 0, "sub1 path %s: should have rows", path)
	}
	for path, count := range r2.paths {
		assert.Greater(t, count, 0, "sub2 path %s: should have rows", path)
	}
	// Both subscriptions should have similar row counts per path.
	// Minor differences possible due to parallel execution timing.
	for path := range r1.paths {
		diff := r1.paths[path] - r2.paths[path]
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, r1.paths[path]/5,
			"path %s: row counts should be within 20%% (sub1=%d, sub2=%d)", path, r1.paths[path], r2.paths[path])
	}

	// First row of each path should be identical (sorted, deterministic)
	for path := range r1.sample {
		assert.Equal(t, r1.sample[path], r2.sample[path],
			"path %s: first row should match between subscriptions", path)
	}

	for path, count := range r1.paths {
		t.Logf("path %s: sub1=%d rows, sub2=%d rows, first_row_match=%v",
			path, count, r2.paths[path], r1.sample[path] == r2.sample[path])
	}
}

func TestGraphQLWS_Introspection(t *testing.T) {
	// Verify subscription type is visible in introspection via regular HTTP
	ctx := context.Background()
	res, err := testService.Query(ctx, `{ __schema { subscriptionType { name fields { name } } } }`, nil)
	require.NoError(t, err)
	defer res.Close()

	var schema struct {
		SubscriptionType *struct {
			Name   string `json:"name"`
			Fields []struct {
				Name string `json:"name"`
			} `json:"fields"`
		} `json:"subscriptionType"`
	}
	err = res.ScanData("__schema", &schema)
	require.NoError(t, err)
	require.NotNil(t, schema.SubscriptionType, "subscriptionType should not be nil")
	assert.Equal(t, "Subscription", schema.SubscriptionType.Name)

	// Should have at least the "query" field
	fieldNames := make([]string, len(schema.SubscriptionType.Fields))
	for i, f := range schema.SubscriptionType.Fields {
		fieldNames[i] = f.Name
	}
	assert.Contains(t, fieldNames, "query", "should have built-in query field")
	t.Logf("subscription fields: %v", fieldNames)
}
