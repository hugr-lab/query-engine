package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// IPC subscription protocol message types
type ipcSubMsg struct {
	Type           string         `json:"type"`
	SubscriptionID string         `json:"subscription_id,omitempty"`
	Path           string         `json:"path,omitempty"`
	Query          string         `json:"query,omitempty"`
	Variables      map[string]any `json:"variables,omitempty"`
	Error          string         `json:"error,omitempty"`
}

func (c *Client) subscribe(ctx context.Context, query string, vars map[string]any) (*types.Subscription, error) {
	// Derive WebSocket URL from the client URL.
	// Client URL is typically http://host:port/ipc or http://host:port/query
	wsURL := c.url
	wsURL = strings.TrimSuffix(wsURL, "/query")
	wsURL = strings.TrimSuffix(wsURL, "/ipc")
	wsURL = strings.Replace(wsURL, "http://", "ws://", 1)
	wsURL = strings.Replace(wsURL, "https://", "wss://", 1)
	wsURL += "/ipc"

	header := http.Header{}
	// Apply auth headers from transport chain
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

	subID := uuid.NewString()
	ctx, cancel := context.WithCancel(ctx)

	// Send subscribe message
	err = conn.WriteJSON(ipcSubMsg{
		Type:           "subscribe",
		SubscriptionID: subID,
		Query:          query,
		Variables:      vars,
	})
	if err != nil {
		cancel()
		conn.Close()
		return nil, fmt.Errorf("send subscribe: %w", err)
	}

	eventCh := make(chan types.SubscriptionEvent, 16)

	// Reader goroutine — reads from WebSocket, demuxes, sends events
	go func() {
		defer close(eventCh)
		defer conn.Close()

		var currentPath string

		for {
			if ctx.Err() != nil {
				return
			}

			msgType, data, err := conn.ReadMessage()
			if err != nil {
				return
			}

			switch msgType {
			case websocket.TextMessage:
				var msg ipcSubMsg
				if err := json.Unmarshal(data, &msg); err != nil {
					continue
				}
				// Only process messages for our subscription
				if msg.SubscriptionID != "" && msg.SubscriptionID != subID {
					continue
				}

				switch msg.Type {
				case "part_start":
					currentPath = msg.Path
				case "subscription_data":
					// Next binary frame will be Arrow data
				case "part_complete":
					currentPath = ""
				case "subscription_complete":
					return
				case "subscription_error":
					return
				}

			case websocket.BinaryMessage:
				// Arrow IPC binary frame
				reader, err := ipc.NewReader(
					bytes.NewReader(data),
					ipc.WithAllocator(memory.DefaultAllocator),
				)
				if err != nil {
					continue
				}
				event := types.SubscriptionEvent{
					Path:   currentPath,
					Reader: &ipcRecordReader{reader: reader},
				}
				select {
				case eventCh <- event:
				case <-ctx.Done():
					reader.Release()
					return
				}
			}
		}
	}()

	return &types.Subscription{
		Events: eventCh,
		Cancel: func() {
			// Send unsubscribe
			_ = conn.WriteJSON(ipcSubMsg{
				Type:           "unsubscribe",
				SubscriptionID: subID,
			})
			cancel()
		},
	}, nil
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

func (r *ipcRecordReader) Schema() *arrow.Schema {
	return r.reader.Schema()
}

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

func (r *ipcRecordReader) Record() arrow.RecordBatch      { return r.current }
func (r *ipcRecordReader) RecordBatch() arrow.RecordBatch  { return r.current }
func (r *ipcRecordReader) Err() error                      { return r.reader.Err() }

var _ array.RecordReader = (*ipcRecordReader)(nil)
