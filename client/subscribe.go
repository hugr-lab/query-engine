package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/hugr-lab/query-engine/types"
)

type ipcSubMsg struct {
	Type           string         `json:"type"`
	SubscriptionID string         `json:"subscription_id,omitempty"`
	Path           string         `json:"path,omitempty"`
	Query          string         `json:"query,omitempty"`
	Variables      map[string]any `json:"variables,omitempty"`
	Error          string         `json:"error,omitempty"`
}

// subscriptionConn is a shared WebSocket connection that multiplexes
// multiple subscriptions. Created lazily on first Subscribe call,
// reused for subsequent calls. Thread-safe.
type subscriptionConn struct {
	conn    *websocket.Conn
	mu      sync.Mutex // protects conn writes
	subs    map[string]*activeSub
	subsMu  sync.Mutex
	ctx     context.Context
	cancel  context.CancelFunc
	started bool
}

type activeSub struct {
	eventCh     chan types.SubscriptionEvent
	currentPath string
}

func (c *Client) subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
	sc, err := c.ensureSubscriptionConn(ctx)
	if err != nil {
		return nil, err
	}

	subID := uuid.NewString()
	sub := &activeSub{
		eventCh: make(chan types.SubscriptionEvent, 16),
	}

	sc.subsMu.Lock()
	sc.subs[subID] = sub
	sc.subsMu.Unlock()

	// Send subscribe message
	sc.mu.Lock()
	err = sc.conn.WriteJSON(ipcSubMsg{
		Type:           "subscribe",
		SubscriptionID: subID,
		Query:          query,
		Variables:      vars,
	})
	sc.mu.Unlock()
	if err != nil {
		sc.subsMu.Lock()
		delete(sc.subs, subID)
		sc.subsMu.Unlock()
		return nil, fmt.Errorf("send subscribe: %w", err)
	}

	return &types.Subscription{
		Events: sub.eventCh,
		Cancel: func() {
			sc.mu.Lock()
			_ = sc.conn.WriteJSON(ipcSubMsg{
				Type:           "unsubscribe",
				SubscriptionID: subID,
			})
			sc.mu.Unlock()
			sc.removeSub(subID)
		},
	}, nil
}

func (c *Client) ensureSubscriptionConn(ctx context.Context) (*subscriptionConn, error) {
	c.subConnMu.Lock()
	defer c.subConnMu.Unlock()

	if c.subConn != nil && c.subConn.ctx.Err() == nil {
		return c.subConn, nil
	}

	wsURL := c.url
	wsURL = strings.TrimSuffix(wsURL, "/query")
	wsURL = strings.TrimSuffix(wsURL, "/ipc")
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/ipc"

	header := http.Header{}
	if rt, ok := c.config.Transport.(*apiKeyTransport); ok {
		h := rt.apiKeyHeader
		if h == "" {
			h = "x-hugr-api-key"
		}
		header.Set(h, rt.apiKey)
	}

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("websocket connect: %w", err)
	}

	connCtx, connCancel := context.WithCancel(ctx)
	sc := &subscriptionConn{
		conn:   conn,
		subs:   make(map[string]*activeSub),
		ctx:    connCtx,
		cancel: connCancel,
	}

	go sc.readLoop()

	c.subConn = sc
	return sc, nil
}

// readLoop is the single reader goroutine for the shared connection.
// Routes incoming messages to subscriptions by subscription_id.
func (sc *subscriptionConn) readLoop() {
	defer sc.cancel()
	defer sc.closeAllSubs()

	var pendingSubID string

	for {
		msgType, data, err := sc.conn.ReadMessage()
		if err != nil {
			return
		}

		switch msgType {
		case websocket.TextMessage:
			var msg ipcSubMsg
			if err := json.Unmarshal(data, &msg); err != nil {
				continue
			}

			sc.subsMu.Lock()
			sub := sc.subs[msg.SubscriptionID]
			sc.subsMu.Unlock()

			switch msg.Type {
			case "part_start":
				if sub != nil {
					sub.currentPath = msg.Path
				}
			case "subscription_data":
				pendingSubID = msg.SubscriptionID
			case "part_complete":
				if sub != nil {
					sub.currentPath = ""
				}
			case "subscription_complete":
				sc.removeSub(msg.SubscriptionID)
			case "subscription_error":
				log.Printf("subscription %s error: %s", msg.SubscriptionID, msg.Error)
				sc.removeSub(msg.SubscriptionID)
			}

		case websocket.BinaryMessage:
			if pendingSubID == "" {
				continue
			}
			sc.subsMu.Lock()
			sub := sc.subs[pendingSubID]
			sc.subsMu.Unlock()
			pendingSubID = ""

			if sub == nil {
				continue
			}

			reader, err := ipc.NewReader(
				bytes.NewReader(data),
				ipc.WithAllocator(memory.DefaultAllocator),
			)
			if err != nil {
				continue
			}

			event := types.SubscriptionEvent{
				Path:   sub.currentPath,
				Reader: &ipcRecordReader{reader: reader},
			}
			// Non-blocking send — drop event if consumer is too slow.
			// This prevents readLoop from blocking on one slow subscription
			// and starving others (including their completion messages).
			select {
			case sub.eventCh <- event:
			default:
				reader.Release()
			}
		}
	}
}

func (sc *subscriptionConn) removeSub(id string) {
	sc.subsMu.Lock()
	sub, ok := sc.subs[id]
	if ok {
		delete(sc.subs, id)
	}
	sc.subsMu.Unlock()
	if sub != nil {
		close(sub.eventCh)
	}
}

func (sc *subscriptionConn) closeAllSubs() {
	sc.subsMu.Lock()
	subs := sc.subs
	sc.subs = make(map[string]*activeSub)
	sc.subsMu.Unlock()
	for _, sub := range subs {
		close(sub.eventCh)
	}
}

// CloseSubscriptions closes the shared subscription connection.
func (c *Client) CloseSubscriptions() {
	c.subConnMu.Lock()
	defer c.subConnMu.Unlock()
	if c.subConn != nil {
		c.subConn.cancel()
		_ = c.subConn.conn.Close()
		c.subConn = nil
	}
}

// ipcRecordReader wraps an ipc.Reader as array.RecordReader.
type ipcRecordReader struct {
	reader  *ipc.Reader
	current arrow.RecordBatch
	done    bool
	once    sync.Once
}

func (r *ipcRecordReader) Retain()  {}
func (r *ipcRecordReader) Release() { r.once.Do(func() { r.reader.Release() }) }
func (r *ipcRecordReader) Schema() *arrow.Schema { return r.reader.Schema() }

func (r *ipcRecordReader) Next() bool {
	if r.done {
		return false
	}
	if r.reader.Next() {
		r.current = r.reader.Record()
		return true
	}
	r.done = true
	return false
}

func (r *ipcRecordReader) Record() arrow.RecordBatch     { return r.current }
func (r *ipcRecordReader) RecordBatch() arrow.RecordBatch { return r.current }
func (r *ipcRecordReader) Err() error                     { return r.reader.Err() }

var _ array.RecordReader = (*ipcRecordReader)(nil)
