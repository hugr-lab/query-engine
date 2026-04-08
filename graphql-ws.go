package hugr

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/gorilla/websocket"
	"github.com/hugr-lab/query-engine/types"
	"golang.org/x/sync/errgroup"
)

// graphql-ws protocol message types (https://github.com/enisdenjo/graphql-ws/blob/master/PROTOCOL.md)
const (
	gqlwsConnectionInit = "connection_init"
	gqlwsConnectionAck  = "connection_ack"
	gqlwsSubscribe      = "subscribe"
	gqlwsNext           = "next"
	gqlwsError          = "error"
	gqlwsComplete       = "complete"
	gqlwsPing           = "ping"
	gqlwsPong           = "pong"

	maxConcurrentSubscriptions = 10
)

var gqlwsUpgrader = &websocket.Upgrader{
	Subprotocols:     []string{"graphql-transport-ws"},
	CheckOrigin:      func(r *http.Request) bool { return true },
	HandshakeTimeout: 10 * time.Second,
}

type gqlwsMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type gqlwsSubscribePayload struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables,omitempty"`
	OperationName string         `json:"operationName,omitempty"`
}

func (s *Service) subscribeHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := gqlwsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("websocket upgrade failed: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel() // Cancel first — stops all subscription goroutines before writeCh becomes unreachable

	// Single writer channel — all writes go through here.
	// Not closed explicitly — GC handles it after all senders exit via ctx cancellation.
	writeCh := make(chan gqlwsMessage, 256)

	// Writer goroutine — only goroutine that touches conn.Write*
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-writeCh:
				if !ok {
					return
				}
				if err := conn.WriteJSON(msg); err != nil {
					cancel()
					return
				}
			}
		}
	}()

	// Ping goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				select {
				case writeCh <- gqlwsMessage{Type: gqlwsPong}: // piggyback on writeCh for keepalive
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	conn.SetCloseHandler(func(code int, text string) error {
		cancel()
		return nil
	})
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	// Wait for connection_init
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	var initMsg gqlwsMessage
	if err := conn.ReadJSON(&initMsg); err != nil || initMsg.Type != gqlwsConnectionInit {
		if s.config.Debug {
			log.Printf("graphql-ws: init failed: %v (type=%s)", err, initMsg.Type)
		}
		return
	}
	_ = conn.SetReadDeadline(time.Time{})
	select {
	case writeCh <- gqlwsMessage{Type: gqlwsConnectionAck}:
	case <-ctx.Done():
		return
	}

	// Per-subscription cancellation — only accessed from this goroutine (no mutex needed)
	subs := make(map[string]context.CancelFunc)

	// errgroup limits concurrent subscriptions
	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(maxConcurrentSubscriptions)

	// Read loop — single goroutine, owns subs map
	for {
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		var msg gqlwsMessage
		err := conn.ReadJSON(&msg)
		if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
			goto done
		}
		if err != nil {
			if s.config.Debug {
				log.Printf("graphql-ws: read error: %v", err)
			}
			goto done
		}

		switch msg.Type {
		case gqlwsSubscribe:
			var payload gqlwsSubscribePayload
			if err := json.Unmarshal(msg.Payload, &payload); err != nil {
				sendErr(writeCh, msg.ID, fmt.Errorf("invalid subscribe payload: %w", err))
				continue
			}
			subCtx, subCancel := context.WithCancel(egCtx)
			subs[msg.ID] = subCancel
			subID := msg.ID
			eg.Go(func() error {
				defer subCancel()
				defer func() {
					select {
					case writeCh <- gqlwsMessage{ID: subID, Type: gqlwsComplete}:
					case <-ctx.Done():
					}
				}()
				s.runSubscription(subCtx, writeCh, subID, payload)
				return nil
			})

		case gqlwsComplete:
			if cancel, ok := subs[msg.ID]; ok {
				cancel()
				delete(subs, msg.ID)
			}

		case gqlwsPing:
			select {
			case writeCh <- gqlwsMessage{Type: gqlwsPong}:
			case <-ctx.Done():
			}

		case gqlwsPong:
			// ignore
		}
	}

done:
	// Cancel all active subscriptions
	for _, subCancel := range subs {
		subCancel()
	}
	_ = eg.Wait()
}

// runSubscription executes a single subscription and streams events as JSON next messages.
func (s *Service) runSubscription(ctx context.Context, writeCh chan<- gqlwsMessage, id string, payload gqlwsSubscribePayload) {
	sub, err := s.Subscribe(ctx, payload.Query, payload.Variables)
	if err != nil {
		sendErr(writeCh, id, err)
		return
	}
	defer sub.Cancel()

	for event := range sub.Events {
		if ctx.Err() != nil {
			event.Reader.Release()
			return
		}
		if err := streamEventAsJSON(ctx, writeCh, id, event); err != nil {
			if s.config.Debug {
				log.Printf("graphql-ws: stream error for sub %s: %v", id, err)
			}
			return
		}
	}
}

// streamEventAsJSON reads all records from the event's Reader and sends each row as a `next` message.
func streamEventAsJSON(ctx context.Context, writeCh chan<- gqlwsMessage, id string, event types.SubscriptionEvent) error {
	defer event.Reader.Release()

	for event.Reader.Next() {
		batch := event.Reader.RecordBatch()
		if batch == nil {
			continue
		}
		for i := 0; i < int(batch.NumRows()); i++ {
			row := recordBatchRowToMap(batch, i)
			payload := map[string]any{
				"data": wrapInPath(event.Path, row),
			}
			payloadBytes, err := json.Marshal(payload)
			if err != nil {
				return fmt.Errorf("marshal row: %w", err)
			}
			msg := gqlwsMessage{ID: id, Type: gqlwsNext, Payload: payloadBytes}
			select {
			case writeCh <- msg:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return event.Reader.Err()
}

func sendErr(writeCh chan<- gqlwsMessage, id string, err error) {
	payload, _ := json.Marshal([]map[string]string{{"message": err.Error()}})
	select {
	case writeCh <- gqlwsMessage{ID: id, Type: gqlwsError, Payload: payload}:
	default:
	}
}

// recordBatchRowToMap converts a single row from a RecordBatch to a map.
func recordBatchRowToMap(batch arrow.RecordBatch, row int) map[string]any {
	result := make(map[string]any, int(batch.NumCols()))
	schema := batch.Schema()
	for col := 0; col < int(batch.NumCols()); col++ {
		field := schema.Field(col)
		arr := batch.Column(col)
		result[field.Name] = arr.GetOneForMarshal(row)
	}
	return result
}

// wrapInPath wraps a value in nested maps following a dot-separated path.
func wrapInPath(path string, value any) any {
	if path == "" {
		return value
	}
	result := value
	parts := splitPath(path)
	for i := len(parts) - 1; i >= 0; i-- {
		result = map[string]any{parts[i]: result}
	}
	return result
}

func splitPath(path string) []string {
	if path == "" {
		return nil
	}
	var parts []string
	start := 0
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			parts = append(parts, path[start:i])
			start = i + 1
		}
	}
	parts = append(parts, path[start:])
	return parts
}
