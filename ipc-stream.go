package hugr

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

var upgrader = &websocket.Upgrader{
	Subprotocols: []string{"hugr-ipc-ws"},
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for IPC WebSocket connections
	},
	HandshakeTimeout: 10 * time.Second,
	ReadBufferSize:   128 * 1024 * 1024, // 128 MB
	WriteBufferSize:  128 * 1024 * 1024, // 128 MB
}

type StreamMessage struct {
	Type           string                 `json:"type"`
	DataObject     string                 `json:"data_object,omitempty"`     // Optional, used for streaming table data
	SelectedFields []string               `json:"selected_fields,omitempty"` // Optional, used for streaming table data
	Query          string                 `json:"query,omitempty"`
	Variables      map[string]interface{} `json:"variables,omitempty"`
	Error          string                 `json:"error,omitempty"`
	SubscriptionID string                 `json:"subscription_id,omitempty"` // For subscribe/unsubscribe
	Path           string                 `json:"path,omitempty"`           // Data object path for subscription events
}

func (s *Service) isStreamingRequest(r *http.Request) bool {
	// Check for WebSocket upgrade headers
	upgrade := strings.ToLower(r.Header.Get("Upgrade"))
	connection := strings.ToLower(r.Header.Get("Connection"))

	return upgrade == "websocket" && strings.Contains(connection, "upgrade")
}

// ipcStreamHandler handles WebSocket connections for hugr IPC arrow stream (Inter-Process Communication) streaming.
func (s *Service) ipcStreamHandler(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to upgrade connection: %v", err), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	stream := &stream{
		queryId:       uuid.NewString(),
		conn:          conn,
		subscriptions: make(map[string]context.CancelFunc),
		writeCh:       make(chan wsMsg, 256),
	}
	ctx, cancel := context.WithCancel(r.Context())
	stream.connCancel = cancel
	go stream.writer(ctx)
	conn.SetCloseHandler(func(code int, text string) error {
		if stream.cancel != nil {
			stream.cancel()
		}
		stream.activeQuery = nil
		for id, subCancel := range stream.subscriptions {
			subCancel()
			delete(stream.subscriptions, id)
		}
		stream.connCancel()
		return nil
	})
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	go stream.ping(ctx)
	if s.config.Debug {
		if auth.IsFullAccess(ctx) {
			log.Printf("stream %s: IPC stream connection established: full access", stream.queryId)
		} else {
			ai := auth.AuthInfoFromContext(ctx)
			log.Printf("stream %s: IPC stream connection established: user %s, role %s", stream.queryId, ai.UserId, ai.Role)
		}
	}

	for {
		select {
		case <-ctx.Done():
			if s.config.Debug {
				log.Printf("stream %s: IPC stream context cancelled, closing connection", stream.queryId)
			}
			return
		default:
			var req StreamMessage
			err = conn.ReadJSON(&req)
			if websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure, websocket.CloseNormalClosure) {
				break
			}
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("Unexpected WebSocket close: %v", err)
				break
			}
			if err != nil {
				_ = stream.sendStreamError(fmt.Errorf("failed to read IPC request: %w", err))
				continue
			}
			if s.config.Debug {
				log.Printf("stream %s: Received IPC request: %s", stream.queryId, req.Type)
			}
			switch req.Type {
			// case "mutation":
			// in the future we can support mutations across IPC to allow for streaming updates, and inserts
			case "query_object":
				// Handle data_object streaming
				req.Query, err = s.dataObjectStreamQuery(ctx, req.DataObject, req.SelectedFields, req.Variables)
				if err != nil {
					_ = stream.sendStreamError(fmt.Errorf("failed to process data object query: %w", err))
					continue
				}
				fallthrough
			case "query":
				ctx, err := stream.setActiveQuery(ctx, &req)
				if err != nil {
					_ = stream.sendStreamError(fmt.Errorf("failed to set active query: %w", err))
					continue
				}
				go s.handleIPCStream(ctx, stream)
			case "cancel":
				if stream.cancel != nil {
					stream.cancel()
				}
			case "subscribe":
				go s.handleIPCSubscription(ctx, stream, req)
			case "unsubscribe":
				stream.cancelSubscription(req.SubscriptionID)
			default:
				_ = stream.sendStreamError(fmt.Errorf("unknown IPC stream message type: %s", req.Type))
			}
		}
	}
}

func (s *Service) handleIPCStream(ctx context.Context, stream *stream) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic recovered in IPC stream: %v\n%s", r, debug.Stack())
			_ = stream.sendStreamError(fmt.Errorf("internal error: %v", r))
		}
	}()
	defer func() {
		if err := stream.sendStreamComplete(); err != nil {
			_ = stream.sendStreamError(fmt.Errorf("failed to send stream complete: %w", err))
		}
	}()
	bp := sync.Pool{
		New: func() any {
			return &bytes.Buffer{}
		},
	}
	table, finalize, err := s.ProcessStreamQuery(ctx, stream.activeQuery.Query, stream.activeQuery.Variables)
	if err != nil {
		_ = stream.sendStreamError(fmt.Errorf("failed to process IPC stream query: %w", err))
		return
	}
	defer finalize()
	defer table.Release()
	reader, err := table.Reader(false)
	if err != nil {
		_ = stream.sendStreamError(fmt.Errorf("failed to create IPC stream reader: %w", err))
		return
	}
	defer reader.Release()
	for reader.Next() {
		select {
		case <-ctx.Done():
			_ = stream.sendStreamError(fmt.Errorf("stream cancelled: %w", ctx.Err()))
			return
		default:
			chunk := reader.RecordBatch()
			if reader.Err() != nil {
				_ = stream.sendStreamError(fmt.Errorf("error reading IPC stream: %w", reader.Err()))
				return
			}
			if chunk == nil {
				_ = stream.sendStreamError(fmt.Errorf("received nil chunk from IPC stream"))
				return
			}
			defer chunk.Release()
			buf := bp.Get().(*bytes.Buffer)
			writer := ipc.NewWriter(buf, ipc.WithLZ4())
			err = writer.Write(chunk)
			_ = writer.Close()
			if err != nil {
				_ = stream.sendStreamError(fmt.Errorf("failed to write IPC stream chunk: %w", err))
				buf.Reset()
				bp.Put(buf)
				return
			}
			err = stream.writeMessage(websocket.BinaryMessage, buf.Bytes())
			buf.Reset()
			bp.Put(buf)
			if err != nil {
				_ = stream.sendStreamError(fmt.Errorf("failed to send IPC stream chunk: %w", err))
				return
			}
		}
	}
}

// handleIPCSubscription handles a subscribe message on the IPC WebSocket.
// Streams Arrow IPC binary frames with subscription_data text frame markers.
func (s *Service) handleIPCSubscription(ctx context.Context, stream *stream, req StreamMessage) {
	subID := req.SubscriptionID
	if subID == "" {
		_ = stream.sendStreamError(fmt.Errorf("subscribe requires subscription_id"))
		return
	}

	subCtx, subCancel := context.WithCancel(ctx)
	stream.subscriptions[subID] = subCancel

	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in IPC subscription %s: %v\n%s", subID, r, debug.Stack())
		}
	}()
	defer func() {
		delete(stream.subscriptions, subID)
		subCancel()
		_ = stream.writeJSON(StreamMessage{Type: "subscription_complete", SubscriptionID: subID})
	}()

	sub, err := s.Subscribe(subCtx, req.Query, req.Variables)
	if err != nil {
		_ = stream.writeJSON(StreamMessage{Type: "subscription_error", SubscriptionID: subID, Error: err.Error()})
		return
	}
	defer sub.Cancel()

	bp := sync.Pool{New: func() any { return &bytes.Buffer{} }}

	for event := range sub.Events {
		if subCtx.Err() != nil {
			event.Reader.Release()
			return
		}

		// Stream Arrow batches with subscription metadata in Arrow schema.
		// No text frame markers needed — client routes by metadata.
		for event.Reader.Next() {
			if subCtx.Err() != nil {
				break
			}
			chunk := event.Reader.RecordBatch()
			if chunk == nil {
				continue
			}

			// Embed subscription_id and path in Arrow schema metadata
			meta := map[string]string{
				"subscription_id": subID,
				"path":            event.Path,
			}
			tagged := array.NewRecordBatch(addMeta(chunk.Schema(), meta), chunk.Columns(), chunk.NumRows())

			buf := bp.Get().(*bytes.Buffer)
			w := ipc.NewWriter(buf, ipc.WithLZ4())
			_ = w.Write(tagged)
			_ = w.Close()
			err := stream.writeMessage(websocket.BinaryMessage, buf.Bytes())
			buf.Reset()
			bp.Put(buf)
			if err != nil {
				event.Reader.Release()
				return
			}
		}

		event.Reader.Release()

		// Signal part complete — client closes the pipe for this path
		_ = stream.writeJSON(StreamMessage{
			Type:           "part_complete",
			SubscriptionID: subID,
			Path:           event.Path,
		})
	}
}

func (s *Service) dataObjectStreamQuery(ctx context.Context, dataObject string, fields []string, variables map[string]interface{}) (string, error) {
	provider := s.schema.Provider()
	if !strings.Contains(dataObject, ".") {
		// If no dot notation, assume it's a data object name without module
		def := provider.ForName(ctx, dataObject)
		if def == nil {
			return "", fmt.Errorf("data object %s not found in schema", dataObject)
		}
		if !sdl.IsDataObject(def) {
			return "", fmt.Errorf("data object %s is not a valid data object", dataObject)
		}
		path, field := sdl.ObjectQueryDefinition(ctx, provider, def, sdl.QueryTypeSelect)
		if path != "" {
			dataObject = path + "." + field.Name
		}
	}
	var query *ast.FieldDefinition
	queryDef := provider.ForName(ctx, base.QueryBaseName)
	if queryDef == nil {
		return "", fmt.Errorf("query base type not found in schema")
	}
	def := queryDef
	path := strings.Split(dataObject, ".")
	for _, part := range path {
		field := def.Fields.ForName(part)
		if field == nil {
			return "", fmt.Errorf("data object %s not found in schema", part)
		}
		def = provider.ForName(ctx, field.Type.Name())
		query = field
	}
	if !sdl.IsDataObject(def) || query == nil {
		return "", fmt.Errorf("data object %s is not a valid data object", dataObject)
	}
	path = path[:len(path)-1] // Remove the last part which is the field name
	// build the query
	op := &ast.OperationDefinition{
		Operation: ast.Query,
		Name:      "DataObjectStreamQuery",
		Position:  base.CompiledPos("data_object_stream_query"),
	}
	// 1. define variables if needed
	for key := range variables {
		arg := query.Arguments.ForName(key)
		if arg == nil {
			continue // Skip variables not defined in the query
		}
		op.VariableDefinitions = append(op.VariableDefinitions, &ast.VariableDefinition{
			Variable: key,
			Type:     arg.Type,
			Position: base.CompiledPos("data_object_stream_query"),
		})
	}
	// 2. build the query
	queryField := &ast.Field{
		Name:         query.Name,
		Alias:        query.Name,
		Position:     base.CompiledPos("data_object_stream_query"),
		SelectionSet: ast.SelectionSet{},
		Definition:   query,
	}
	if len(fields) == 0 {
		// If no fields specified, select all fields of the data object
		return "", fmt.Errorf("no fields specified for data object %s", dataObject)
	}
	for _, field := range fields {
		if field == "" {
			continue // Skip empty fields
		}
		// Check if the field exists in the query definition
		subField := def.Fields.ForName(field)
		if subField == nil {
			return "", fmt.Errorf("field %s not found in data object %s", field, dataObject)
		}
		// Add the field to the selection set
		queryField.SelectionSet = append(queryField.SelectionSet, &ast.Field{
			Name:             subField.Name,
			Alias:            subField.Name,
			Position:         base.CompiledPos("data_object_stream_query"),
			Definition:       subField,
			ObjectDefinition: def,
		})
	}
	if len(path) == 0 {
		op.SelectionSet = ast.SelectionSet{queryField}
	}
	def = queryDef
	if len(path) > 0 {
		var qp *ast.Field
		for _, part := range path {
			qpd := def.Fields.ForName(part)
			if qpd == nil {
				return "", fmt.Errorf("data object %s not found in schema", part)
			}
			f := &ast.Field{
				Name:             part,
				Alias:            part,
				Position:         base.CompiledPos("data_object_stream_query"),
				Definition:       qpd,
				ObjectDefinition: def,
			}
			if qp == nil {
				op.SelectionSet = append(op.SelectionSet, f)
			} else {
				qp.SelectionSet = append(qp.SelectionSet, f)
			}
			qp = f
		}
		qp.SelectionSet = append(qp.SelectionSet, queryField)
	}

	var b strings.Builder
	formatter.NewFormatter(&b).FormatQueryDocument(&ast.QueryDocument{
		Operations: ast.OperationList{op},
	})

	return b.String(), nil
}

// wsMsg is a message to write to the WebSocket connection.
type wsMsg struct {
	msgType int    // websocket.TextMessage, BinaryMessage, or PingMessage
	data    []byte // raw bytes for binary/ping
	json    any    // JSON-serializable value (when data is nil)
}

type stream struct {
	queryId    string
	conn       *websocket.Conn
	connCancel context.CancelFunc
	writeCh    chan wsMsg // Single writer goroutine reads from here

	activeQuery   *StreamMessage
	cancel        context.CancelFunc
	subscriptions map[string]context.CancelFunc
}

func (s *stream) setActiveQuery(ctx context.Context, req *StreamMessage) (context.Context, error) {
	if s.activeQuery != nil {
		return nil, fmt.Errorf("stream %s: another query is already active on this connection", s.queryId)
	}
	s.activeQuery = req
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	return ctx, nil
}

func (s *stream) sendStreamComplete() error {
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil
	}
	if s.activeQuery == nil {
		return fmt.Errorf("stream %s: cannot complete stream: no active query found", s.queryId)
	}
	s.activeQuery = nil
	return s.writeJSON(StreamMessage{Type: "complete"})
}

func (s *stream) sendStreamError(err error) error {
	return s.writeJSON(StreamMessage{Type: "error", Error: err.Error()})
}

func (s *stream) ping(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.writeMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("stream %s: Failed to send ping: %v", s.queryId, err)
				if s.cancel != nil {
					s.cancel()
				}
				s.connCancel()
				return
			}
		}
	}
}

func (s *stream) cancelSubscription(id string) {
	if cancel, ok := s.subscriptions[id]; ok {
		cancel()
		delete(s.subscriptions, id)
	}
}

// writer is the single goroutine that writes to the WebSocket connection.
// All other goroutines send messages via writeCh.
func (s *stream) writer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-s.writeCh:
			if !ok {
				return
			}
			var err error
			if msg.json != nil {
				err = s.conn.WriteJSON(msg.json)
			} else {
				err = s.conn.WriteMessage(msg.msgType, msg.data)
			}
			if err != nil {
				log.Printf("stream %s: write error: %v", s.queryId, err)
				s.connCancel()
				return
			}
		}
	}
}

// writeJSON sends a JSON message through the write channel.
func (s *stream) writeJSON(msg any) error {
	select {
	case s.writeCh <- wsMsg{json: msg}:
		return nil
	default:
		return fmt.Errorf("stream %s: write channel full", s.queryId)
	}
}

// writeMessage sends a raw message through the write channel.
func (s *stream) writeMessage(msgType int, data []byte) error {
	select {
	case s.writeCh <- wsMsg{msgType: msgType, data: data}:
		return nil
	default:
		return fmt.Errorf("stream %s: write channel full", s.queryId)
	}
}
