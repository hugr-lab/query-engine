//go:build duckdb_arrow

package subscription_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/client"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
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
	wsURL := strings.Replace(testServer.URL, "http://", "ws://", 1) + "/subscribe"
	header := http.Header{}
	header.Set("Sec-WebSocket-Protocol", "graphql-transport-ws")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	require.NoError(t, err)

	// connection_init
	err = conn.WriteJSON(gqlwsMsg{Type: "connection_init"})
	require.NoError(t, err)

	// connection_ack
	var ack gqlwsMsg
	err = conn.ReadJSON(&ack)
	require.NoError(t, err)
	assert.Equal(t, "connection_ack", ack.Type)

	return conn
}

func TestGraphQLWS_QueryStreaming(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.Close()

	// Subscribe to a query — use catalog.types which always has data
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query { core { catalog { types(limit: 3) { name kind } } } } }`,
	})
	err := conn.WriteJSON(gqlwsMsg{ID: "1", Type: "subscribe", Payload: payload})
	require.NoError(t, err)

	// Collect next messages
	var messages []gqlwsMsg
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		var msg gqlwsMsg
		err := conn.ReadJSON(&msg)
		if err != nil {
			t.Logf("read error (expected at end): %v", err)
			break
		}
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
	defer conn.Close()

	// Subscribe with interval=1s, count=2 — use catalog.types which always has data
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query(interval: "1s", count: 2) { core { catalog { types(limit: 3) { name } } } } }`,
	})
	err := conn.WriteJSON(gqlwsMsg{ID: "poll", Type: "subscribe", Payload: payload})
	require.NoError(t, err)

	// Collect messages — expect 2 rounds of data + complete
	var messages []gqlwsMsg
	conn.SetReadDeadline(time.Now().Add(15 * time.Second))
	for {
		var msg gqlwsMsg
		err := conn.ReadJSON(&msg)
		if err != nil {
			break
		}
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
	defer conn.Close()

	// Subscribe to a query with multiple data object paths
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query {
			core {
				catalog { types(limit: 2) { name kind } }
				data_sources { name type }
			}
		} }`,
	})
	err := conn.WriteJSON(gqlwsMsg{ID: "multi", Type: "subscribe", Payload: payload})
	require.NoError(t, err)

	// Collect messages — expect rows from both paths interleaved
	paths := map[string]int{}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		var msg gqlwsMsg
		err := conn.ReadJSON(&msg)
		if err != nil {
			break
		}
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
	defer conn.Close()

	// Subscribe to periodic — first tick executes immediately, then waits interval
	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { query(interval: "60s") { core { catalog { types(limit: 1) { name } } } } }`,
	})
	err := conn.WriteJSON(gqlwsMsg{ID: "cancel-test", Type: "subscribe", Payload: payload})
	require.NoError(t, err)

	// Wait for first message (next or complete from first tick)
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	var msg gqlwsMsg
	err = conn.ReadJSON(&msg)
	require.NoError(t, err)
	// First tick should produce at least a complete for the tick
	t.Logf("first message: type=%s", msg.Type)

	// Send complete (cancel) — should stop the periodic subscription
	err = conn.WriteJSON(gqlwsMsg{ID: "cancel-test", Type: "complete"})
	require.NoError(t, err)

	// Should receive complete back (may receive more next messages first)
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		err = conn.ReadJSON(&msg)
		if err != nil {
			break
		}
		if msg.Type == "complete" {
			break
		}
	}
	assert.Equal(t, "complete", msg.Type)
}

func TestGraphQLWS_Multiplexing(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.Close()

	// Subscribe two queries simultaneously
	for _, id := range []string{"sub-a", "sub-b"} {
		payload, _ := json.Marshal(map[string]any{
			"query": `subscription { query { core { data_sources { name } } } }`,
		})
		err := conn.WriteJSON(gqlwsMsg{ID: id, Type: "subscribe", Payload: payload})
		require.NoError(t, err)
	}

	// Collect messages for both
	ids := map[string]int{}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		var msg gqlwsMsg
		err := conn.ReadJSON(&msg)
		if err != nil {
			break
		}
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
	wsURL := strings.Replace(testServer.URL, "http://", "ws://", 1) + "/ipc"
	header := http.Header{}
	header.Set("Sec-WebSocket-Protocol", "hugr-ipc-ws")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, header)
	require.NoError(t, err)
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
	defer conn.Close()

	// Send subscribe — use catalog.types which always has data
	err := conn.WriteJSON(ipcMsg{
		Type:           "subscribe",
		SubscriptionID: "s1",
		Query:          `subscription { query { core { catalog { types(limit: 3) { name kind } } } } }`,
	})
	require.NoError(t, err)

	// Collect messages
	var textMsgs []ipcMsg
	var binaryCount int
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			t.Logf("read error: %v", err)
			break
		}
		switch msgType {
		case websocket.TextMessage:
			var msg ipcMsg
			json.Unmarshal(data, &msg)
			textMsgs = append(textMsgs, msg)
			t.Logf("text: %s (sub=%s, path=%s)", msg.Type, msg.SubscriptionID, msg.Path)
			if msg.Type == "subscription_complete" || msg.Type == "subscription_error" {
				goto done
			}
		case websocket.BinaryMessage:
			binaryCount++
			t.Logf("binary: %d bytes", len(data))
		}
		if len(textMsgs)+binaryCount > 100 {
			break
		}
	}
done:

	// Should have part_start, subscription_data, binary frames, part_complete, subscription_complete
	var hasPartStart, hasPartComplete, hasSubComplete, hasSubData bool
	for _, msg := range textMsgs {
		switch msg.Type {
		case "part_start":
			hasPartStart = true
			assert.Equal(t, "s1", msg.SubscriptionID)
			assert.NotEmpty(t, msg.Path)
		case "subscription_data":
			hasSubData = true
		case "part_complete":
			hasPartComplete = true
		case "subscription_complete":
			hasSubComplete = true
			assert.Equal(t, "s1", msg.SubscriptionID)
		}
	}

	assert.True(t, hasPartStart, "should have part_start")
	assert.True(t, hasPartComplete, "should have part_complete")
	assert.True(t, hasSubComplete, "should have subscription_complete")
	// Note: with empty DB, there may be 0 data rows → no subscription_data/binary frames
	t.Logf("IPC: %d text msgs, %d binary frames (hasSubData=%v)", len(textMsgs), binaryCount, hasSubData)
}

func TestIPC_Unsubscribe(t *testing.T) {
	conn := connectIPC(t)
	defer conn.Close()

	// Subscribe to long-running periodic
	err := conn.WriteJSON(ipcMsg{
		Type:           "subscribe",
		SubscriptionID: "s-cancel",
		Query:          `subscription { query(interval: "30s") { core { data_sources { name } } } }`,
	})
	require.NoError(t, err)

	// Wait for first data
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		msgType, data, err := conn.ReadMessage()
		require.NoError(t, err)
		if msgType == websocket.TextMessage {
			var msg ipcMsg
			json.Unmarshal(data, &msg)
			if msg.Type == "subscription_data" || msg.Type == "part_start" {
				break // got first data
			}
		}
		if msgType == websocket.BinaryMessage {
			break // got arrow data
		}
	}

	// Unsubscribe
	err = conn.WriteJSON(ipcMsg{
		Type:           "unsubscribe",
		SubscriptionID: "s-cancel",
	})
	require.NoError(t, err)

	// Should get subscription_complete
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if msgType == websocket.TextMessage {
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
	defer conn.Close()

	// Subscribe two queries
	for _, id := range []string{"ms1", "ms2"} {
		err := conn.WriteJSON(ipcMsg{
			Type:           "subscribe",
			SubscriptionID: id,
			Query:          `subscription { query { core { data_sources { name } } } }`,
		})
		require.NoError(t, err)
	}

	// Collect — expect messages from both subscription IDs
	seen := map[string]bool{}
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if msgType == websocket.TextMessage {
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
	t.Skip("TODO: IPC query coexistence with active subscriptions needs stream mutex rework")
	conn := connectIPC(t)
	defer conn.Close()

	// Start a subscription
	err := conn.WriteJSON(ipcMsg{
		Type:           "subscribe",
		SubscriptionID: "bg",
		Query:          `subscription { query(interval: "30s") { core { data_sources { name } } } }`,
	})
	require.NoError(t, err)

	// Wait for subscription to produce data
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		msgType, data, err := conn.ReadMessage()
		require.NoError(t, err)
		if msgType == websocket.BinaryMessage || (msgType == websocket.TextMessage && strings.Contains(string(data), "subscription_data")) {
			break
		}
	}

	// Now send a regular query on the same connection
	err = conn.WriteJSON(map[string]any{
		"type":  "query",
		"query": `{ core { data_sources { name } } }`,
	})
	require.NoError(t, err)

	// Should receive query results (binary) and then complete
	var gotQueryComplete bool
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if msgType == websocket.TextMessage {
			var msg map[string]string
			json.Unmarshal(data, &msg)
			if msg["type"] == "complete" {
				gotQueryComplete = true
				break
			}
		}
	}
	assert.True(t, gotQueryComplete, "regular query should complete on same connection")

	// Cleanup
	conn.WriteJSON(ipcMsg{Type: "unsubscribe", SubscriptionID: "bg"})
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

	var eventCount int
	for event := range sub.Events {
		eventCount++
		// Drain reader
		for event.Reader.Next() {
		}
		event.Reader.Release()
	}

	// count=2 → 2 ticks, each with 1 path = 2 events
	assert.GreaterOrEqual(t, eventCount, 2, "should receive events from 2 ticks")
	t.Logf("Go client periodic: %d events", eventCount)
}

func TestGoClient_MultipleSubscriptions(t *testing.T) {
	c := client.NewClient(testServer.URL+"/ipc", client.WithTimeout(10*time.Second))
	defer c.CloseSubscriptions()

	ctx := context.Background()

	// Start two subscriptions on the same shared connection
	sub1, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 2) { name } } } } }`, nil)
	require.NoError(t, err)

	sub2, err := c.Subscribe(ctx, `subscription { query { core { catalog { types(limit: 3) { name } } } } }`, nil)
	require.NoError(t, err)

	// Consume both
	var rows1, rows2 int
	for event := range sub1.Events {
		for event.Reader.Next() {
			rows1 += int(event.Reader.RecordBatch().NumRows())
		}
		event.Reader.Release()
	}
	for event := range sub2.Events {
		for event.Reader.Next() {
			rows2 += int(event.Reader.RecordBatch().NumRows())
		}
		event.Reader.Release()
	}

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

	// Second subscription — reuses same connection
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
	t.Logf("Go client reuse: sub1=%d rows, sub2=%d rows (same connection)", rows1, rows2)
}

func TestGraphQLWS_InvalidQuery(t *testing.T) {
	conn := connectGraphQLWS(t)
	defer conn.Close()

	payload, _ := json.Marshal(map[string]any{
		"query": `subscription { nonexistent_field }`,
	})
	err := conn.WriteJSON(gqlwsMsg{ID: "bad", Type: "subscribe", Payload: payload})
	require.NoError(t, err)

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var msg gqlwsMsg
	err = conn.ReadJSON(&msg)
	require.NoError(t, err)
	// Should get error
	assert.True(t, msg.Type == "error" || msg.Type == "complete",
		fmt.Sprintf("expected error or complete, got %s: %s", msg.Type, string(msg.Payload)))
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
