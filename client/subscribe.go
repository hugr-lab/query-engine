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

// --- Options ---

// SubscriptionPoolConfig controls how the client manages WebSocket connections for subscriptions.
type SubscriptionPoolConfig struct {
	MaxConns int // max connections in pool (default 1)
	IdleConn int // idle connections to keep open (default 1)
}

// WithSubscriptionPool sets the subscription connection pool size.
func WithSubscriptionPool(max, idle int) Option {
	return func(c *ClientConfig) {
		if max < 1 {
			max = 1
		}
		if idle < 0 {
			idle = 0
		}
		if idle > max {
			idle = max
		}
		c.SubPool = SubscriptionPoolConfig{MaxConns: max, IdleConn: idle}
	}
}

// --- IPC protocol ---

type ipcSubMsg struct {
	Type           string         `json:"type"`
	SubscriptionID string         `json:"subscription_id,omitempty"`
	Path           string         `json:"path,omitempty"`
	Query          string         `json:"query,omitempty"`
	Variables      map[string]any `json:"variables,omitempty"`
	Error          string         `json:"error,omitempty"`
	// Identity override fields (optional, requires secret key auth on connection)
	UserId   string `json:"user_id,omitempty"`
	UserName string `json:"user_name,omitempty"`
	Role     string `json:"role,omitempty"`
}

// --- SubscriptionConn ---

// SubscriptionConn is a single WebSocket connection multiplexing subscriptions.
type SubscriptionConn struct {
	conn   *websocket.Conn
	mu     sync.Mutex // protects conn writes
	subs   map[string]*activeSub
	subsMu sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
}

// activeSub tracks one subscription on the connection.
// eventCh delivers one event per part (each with a pipe Reader).
// activePipe is the current part's pipe being filled with batches.
type activeSub struct {
	eventCh chan types.SubscriptionEvent
	pipes   map[string]*batchPipe // one pipe per path, persists across ticks
	done    bool
}

// Subscribe creates a subscription on this connection.
func (sc *SubscriptionConn) Subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
	if sc.ctx.Err() != nil {
		return nil, fmt.Errorf("connection closed")
	}

	subID := uuid.NewString()
	sub := &activeSub{eventCh: make(chan types.SubscriptionEvent, 16)}

	sc.subsMu.Lock()
	sc.subs[subID] = sub
	sc.subsMu.Unlock()

	msg := ipcSubMsg{
		Type: "subscribe", SubscriptionID: subID,
		Query: query, Variables: vars,
	}
	if id := types.AsUserFromContext(ctx); id != nil {
		msg.UserId = id.UserId
		msg.UserName = id.UserName
		msg.Role = id.Role
	}

	sc.mu.Lock()
	err := sc.conn.WriteJSON(msg)
	sc.mu.Unlock()
	if err != nil {
		sc.removeSub(subID)
		return nil, fmt.Errorf("send subscribe: %w", err)
	}

	return &types.Subscription{
		Events: sub.eventCh,
		Cancel: func() {
			sc.mu.Lock()
			_ = sc.conn.WriteJSON(ipcSubMsg{Type: "unsubscribe", SubscriptionID: subID})
			sc.mu.Unlock()
			sc.removeSub(subID)
		},
	}, nil
}

// Count returns the number of active subscriptions.
func (sc *SubscriptionConn) Count() int {
	sc.subsMu.Lock()
	defer sc.subsMu.Unlock()
	return len(sc.subs)
}

// Close closes the connection and all subscriptions.
func (sc *SubscriptionConn) Close() {
	sc.cancel()
	_ = sc.conn.Close()
}

func (sc *SubscriptionConn) readLoop() {
	defer sc.cancel()
	defer sc.closeAllSubs()

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
			switch msg.Type {
			case "part_complete":
				// Part complete = one tick for this path finished.
				// Pipe stays open — next tick pushes into the same pipe.
				// reader.Next() continues to yield batches across ticks.
				// Pipe closes only on subscription_complete.
			case "subscription_complete", "subscription_error":
				if msg.Type == "subscription_error" {
					log.Printf("subscription %s error: %s", msg.SubscriptionID, msg.Error)
				}
				sc.completeSub(msg.SubscriptionID)
			}

		case websocket.BinaryMessage:
			// Arrow IPC batch with subscription metadata in schema.
			// Read subscription_id and path from Arrow schema metadata.
			reader, err := ipc.NewReader(bytes.NewReader(data), ipc.WithAllocator(memory.DefaultAllocator))
			if err != nil {
				continue
			}
			subID, path := readSubMeta(reader.Schema())
			if subID == "" {
				reader.Release()
				continue
			}

			sc.subsMu.Lock()
			sub := sc.subs[subID]
			sc.subsMu.Unlock()
			if sub == nil || sub.done {
				reader.Release()
				continue
			}

			// Ensure pipe exists for this path.
			// One pipe per path per subscription — persists across ticks.
			pipe, ok := sub.pipes[path]
			if !ok {
				pipe = newBatchPipe(sc.ctx)
				if sub.pipes == nil {
					sub.pipes = make(map[string]*batchPipe)
				}
				sub.pipes[path] = pipe
				// Send event to consumer with the new pipe (once per path)
				select {
				case sub.eventCh <- types.SubscriptionEvent{Path: path, Reader: pipe}:
				case <-sc.ctx.Done():
					reader.Release()
					return
				}
			}

			// Push batches to the path's pipe
			for reader.Next() {
				batch := reader.RecordBatch()
				batch.Retain()
				pipe.Send(batch)
			}
			reader.Release()
		}
	}
}

// readSubMeta extracts subscription_id and path from Arrow schema metadata.
func readSubMeta(schema *arrow.Schema) (subID, path string) {
	if schema == nil || schema.Metadata().Len() == 0 {
		return "", ""
	}
	meta := schema.Metadata()
	idx := meta.FindKey("subscription_id")
	if idx >= 0 {
		subID = meta.Values()[idx]
	}
	idx = meta.FindKey("path")
	if idx >= 0 {
		path = meta.Values()[idx]
	}
	return
}

// completeSub closes pipe and event channel for a subscription.
func (sc *SubscriptionConn) completeSub(id string) {
	sc.subsMu.Lock()
	sub := sc.subs[id]
	if sub != nil {
		delete(sc.subs, id)
	}
	sc.subsMu.Unlock()
	if sub != nil && !sub.done {
		sub.done = true
		for _, pipe := range sub.pipes {
			pipe.Close()
		}
		close(sub.eventCh)
	}
}

func (sc *SubscriptionConn) removeSub(id string) {
	sc.subsMu.Lock()
	sub, ok := sc.subs[id]
	if ok {
		delete(sc.subs, id)
	}
	sc.subsMu.Unlock()
	if sub != nil && !sub.done {
		sub.done = true
		for _, pipe := range sub.pipes {
			pipe.Close()
		}
		close(sub.eventCh)
	}
}

func (sc *SubscriptionConn) closeAllSubs() {
	sc.subsMu.Lock()
	subs := sc.subs
	sc.subs = make(map[string]*activeSub)
	sc.subsMu.Unlock()
	for _, sub := range subs {
		if !sub.done {
			sub.done = true
			for _, pipe := range sub.pipes {
				pipe.Close()
			}
			close(sub.eventCh)
		}
	}
}

// --- batchPipe: channel-backed RecordReader ---

// batchPipe implements array.RecordReader backed by a channel.
// readLoop pushes batches via Send; consumer reads via Next/RecordBatch.
type batchPipe struct {
	ch      chan arrow.RecordBatch
	current arrow.RecordBatch
	ctx     context.Context
	closed  sync.Once
}

func newBatchPipe(ctx context.Context) *batchPipe {
	return &batchPipe{
		ch:  make(chan arrow.RecordBatch, 256),
		ctx: ctx,
	}
}

func (p *batchPipe) Send(batch arrow.RecordBatch) {
	defer func() {
		if r := recover(); r != nil {
			// Channel closed — pipe already completed.
			batch.Release()
		}
	}()
	select {
	case p.ch <- batch:
	default:
		batch.Release()
	}
}

func (p *batchPipe) Close()   { p.closed.Do(func() { close(p.ch) }) }
func (p *batchPipe) Retain()  {}
func (p *batchPipe) Release() { p.Close() }
func (p *batchPipe) Err() error { return nil }

func (p *batchPipe) Schema() *arrow.Schema {
	if p.current != nil {
		return p.current.Schema()
	}
	return nil
}

func (p *batchPipe) Next() bool {
	if p.current != nil {
		p.current.Release()
		p.current = nil
	}
	select {
	case <-p.ctx.Done():
		return false
	case batch, ok := <-p.ch:
		if !ok {
			return false
		}
		p.current = batch
		return true
	}
}

func (p *batchPipe) Record() arrow.RecordBatch     { return p.current }
func (p *batchPipe) RecordBatch() arrow.RecordBatch { return p.current }

var _ array.RecordReader = (*batchPipe)(nil)

// --- Pool ---

type subscriptionPool struct {
	mu    sync.Mutex
	conns []*SubscriptionConn
	cfg   SubscriptionPoolConfig
}

func (p *subscriptionPool) leastLoaded() *SubscriptionConn {
	var best *SubscriptionConn
	bestCount := int(^uint(0) >> 1)
	for _, sc := range p.conns {
		if sc.ctx.Err() != nil {
			continue
		}
		if n := sc.Count(); n < bestCount {
			bestCount = n
			best = sc
		}
	}
	return best
}

func (p *subscriptionPool) removeStale() {
	alive := p.conns[:0]
	for _, sc := range p.conns {
		if sc.ctx.Err() == nil {
			alive = append(alive, sc)
		}
	}
	p.conns = alive
}

func (p *subscriptionPool) add(sc *SubscriptionConn)  { p.conns = append(p.conns, sc) }
func (p *subscriptionPool) closeAll() {
	for _, sc := range p.conns {
		sc.Close()
	}
	p.conns = nil
}

// --- Client methods ---

// NewSubscriptionConn creates a dedicated WebSocket connection.
func (c *Client) NewSubscriptionConn(ctx context.Context) (*SubscriptionConn, error) {
	return c.dialSubscriptionConn(ctx)
}

func (c *Client) subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
	sc, err := c.acquirePoolConn(ctx)
	if err != nil {
		return nil, err
	}
	return sc.Subscribe(ctx, query, vars)
}

func (c *Client) acquirePoolConn(ctx context.Context) (*SubscriptionConn, error) {
	c.subPoolMu.Lock()
	defer c.subPoolMu.Unlock()

	if c.subPool == nil {
		cfg := c.config.SubPool
		if cfg.MaxConns == 0 {
			cfg.MaxConns = 1
		}
		if cfg.IdleConn == 0 {
			cfg.IdleConn = 1
		}
		c.subPool = &subscriptionPool{cfg: cfg}
	}

	c.subPool.removeStale()
	if sc := c.subPool.leastLoaded(); sc != nil {
		return sc, nil
	}
	if len(c.subPool.conns) >= c.subPool.cfg.MaxConns {
		if sc := c.subPool.leastLoaded(); sc != nil {
			return sc, nil
		}
		return nil, fmt.Errorf("subscription pool exhausted (max=%d)", c.subPool.cfg.MaxConns)
	}

	sc, err := c.dialSubscriptionConn(ctx)
	if err != nil {
		return nil, err
	}
	c.subPool.add(sc)
	return sc, nil
}

func (c *Client) dialSubscriptionConn(ctx context.Context) (*SubscriptionConn, error) {
	wsURL := c.url
	wsURL = strings.TrimSuffix(wsURL, "/query")
	wsURL = strings.TrimSuffix(wsURL, "/ipc")
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/ipc"

	header := http.Header{}
	collectAuthHeaders(c.config.Transport, header)

	conn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("websocket connect: %w", err)
	}

	connCtx, connCancel := context.WithCancel(ctx)
	sc := &SubscriptionConn{
		conn:   conn,
		subs:   make(map[string]*activeSub),
		ctx:    connCtx,
		cancel: connCancel,
	}
	go sc.readLoop()
	return sc, nil
}

// collectAuthHeaders walks the transport chain and collects auth headers for WebSocket dial.
func collectAuthHeaders(rt http.RoundTripper, header http.Header) {
	if rt == nil {
		return
	}
	switch t := rt.(type) {
	case *apiKeyTransport:
		h := t.apiKeyHeader
		if h == "" {
			h = "x-hugr-api-key"
		}
		header.Set(h, t.apiKey)
		collectAuthHeaders(t.transport, header)
	case *tokenTransport:
		header.Set("Authorization", "Bearer "+t.token)
		collectAuthHeaders(t.transport, header)
	case *timezoneTransport:
		collectAuthHeaders(t.transport, header)
	case *noTimezoneTransport:
		collectAuthHeaders(t.transport, header)
	case *withUserRoleTransport:
		collectAuthHeaders(t.transport, header)
	case *withUserInfoTransport:
		collectAuthHeaders(t.transport, header)
	}
}

// CloseSubscriptions closes all pool connections.
func (c *Client) CloseSubscriptions() {
	c.subPoolMu.Lock()
	defer c.subPoolMu.Unlock()
	if c.subPool != nil {
		c.subPool.closeAll()
		c.subPool = nil
	}
}
