package hugr

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/gorilla/websocket"
	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

var upgrader = &websocket.Upgrader{
	Subprotocols: []string{"hugr-ipc-ws"},
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all origins for IPC WebSocket connections
	},
	HandshakeTimeout: 10 * time.Second,
	ReadBufferSize:   8024,
	WriteBufferSize:  8024,
}

type StreamMessage struct {
	Type           string                 `json:"type"`
	DataObject     string                 `json:"data_object,omitempty"`     // Optional, used for streaming table data
	SelectedFields []string               `json:"selected_fields,omitempty"` // Optional, used for streaming table data
	Query          string                 `json:"query,omitempty"`
	Variables      map[string]interface{} `json:"variables,omitempty"`
	Error          string                 `json:"error,omitempty"`
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
		conn: conn,
	}
	ctx, cancel := context.WithCancel(r.Context())
	stream.connCancel = cancel
	conn.SetCloseHandler(func(code int, text string) error {
		stream.mu.Lock()
		defer stream.mu.Unlock()
		if stream.cancel != nil {
			stream.cancel()
		}
		stream.activeQuery = nil // Clear active query on close
		stream.connCancel()
		return nil
	})
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	go stream.ping(ctx)

	for {
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
			stream.sendStreamError(fmt.Errorf("failed to read IPC request: %w", err))
			continue
		}

		switch req.Type {
		// case "mutation":
		// in the future we can support mutations across IPC to allow for streaming updates, and inserts
		case "query_object":
			// Handle data_object streaming
			req.Query, err = s.dataObjectStreamQuery(req.DataObject, req.SelectedFields, req.Variables)
			if err != nil {
				stream.sendStreamError(fmt.Errorf("failed to process data object query: %w", err))
				continue
			}
			fallthrough
		case "query":
			ctx, err := stream.setActiveQuery(ctx, &req)
			if err != nil {
				stream.sendStreamError(fmt.Errorf("failed to set active query: %w", err))
				continue
			}
			go s.handleIPCStream(ctx, stream)
		case "cancel":
			// Handle cancel request
			stream.mu.Lock()
			if stream.cancel != nil {
				stream.cancel()
			}
			stream.mu.Unlock()
		default:
			stream.sendStreamError(fmt.Errorf("unknown IPC stream message type: %s", req.Type))
		}
	}
}

func (s *Service) handleIPCStream(ctx context.Context, stream *stream) {
	defer func() {
		if err := stream.sendStreamComplete(); err != nil {
			stream.sendStreamError(fmt.Errorf("failed to send stream complete: %w", err))
		}
	}()
	bp := sync.Pool{
		New: func() any {
			return &bytes.Buffer{}
		},
	}
	table, finalize, err := s.ProcessStreamQuery(ctx, stream.activeQuery.Query, stream.activeQuery.Variables)
	if err != nil {
		stream.sendStreamError(fmt.Errorf("failed to process IPC stream query: %w", err))
		return
	}
	defer finalize()
	defer table.Release()
	reader, err := table.Reader(false)
	if err != nil {
		stream.sendStreamError(fmt.Errorf("failed to create IPC stream reader: %w", err))
		return
	}
	defer reader.Release()
	for reader.Next() {
		chunk := reader.Record()
		if reader.Err() != nil {
			stream.sendStreamError(fmt.Errorf("error reading IPC stream: %w", reader.Err()))
			return
		}
		if chunk == nil {
			continue // skip empty chunks
		}
		buf := bp.Get().(*bytes.Buffer)
		writer := ipc.NewWriter(buf)
		err = writer.Write(chunk)
		writer.Close()
		if err != nil {
			stream.sendStreamError(fmt.Errorf("failed to write IPC stream chunk: %w", err))
			buf.Reset()
			bp.Put(buf)
			return
		}
		err = stream.writeMessage(websocket.BinaryMessage, buf.Bytes())
		buf.Reset()
		bp.Put(buf)
		if err != nil {
			stream.sendStreamError(fmt.Errorf("failed to send IPC stream chunk: %w", err))
			return
		}
	}
}

func (s *Service) dataObjectStreamQuery(dataObject string, fields []string, variables map[string]interface{}) (string, error) {
	schema := s.Schema()
	if !strings.Contains(dataObject, ".") {
		// If no dot notation, assume it's a data object name without module
		def := schema.Types[dataObject]
		if def == nil {
			return "", fmt.Errorf("data object %s not found in schema", dataObject)
		}
		if !compiler.IsDataObject(def) {
			return "", fmt.Errorf("data object %s is not a valid data object", dataObject)
		}
		path, field := compiler.ObjectQueryDefinition(compiler.SchemaDefs(schema), def, compiler.QueryTypeSelect)
		if path != "" {
			dataObject = path + "." + field.Name
		}
	}
	var query *ast.FieldDefinition
	queryDef := schema.Types[base.QueryBaseName]
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
		def = schema.Types[field.Type.Name()]
		query = field
	}
	if !compiler.IsDataObject(def) || query == nil {
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

type stream struct {
	conn       *websocket.Conn
	connCancel context.CancelFunc // Cancel function for connection context

	mu          sync.Mutex     // Protects the queries map
	activeQuery *StreamMessage // Maps query IDs to StreamMessage
	cancel      context.CancelFunc
}

func (s *stream) setActiveQuery(ctx context.Context, req *StreamMessage) (context.Context, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeQuery != nil {
		return nil, fmt.Errorf("another query is already active on this connection")
	}
	s.activeQuery = req
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel
	return ctx, nil
}

func (s *stream) sendStreamComplete() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
		s.cancel = nil // Clear the cancel function after completion
	}
	if s.activeQuery == nil {
		return fmt.Errorf("cannot complete stream: no active query found")
	}
	s.activeQuery = nil // Clear the active query after completion
	// Send a completion message
	msg := StreamMessage{
		Type: "complete",
	}
	return s.conn.WriteJSON(msg)
}

func (s *stream) sendStreamError(err error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg := StreamMessage{
		Type:  "error",
		Error: err.Error(),
	}
	return s.conn.WriteJSON(msg)
}

func (s *stream) ping(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.writeMessage(websocket.PingMessage, []byte{}); err != nil {
				log.Printf("Failed to send ping: %v", err)
				if s.cancel != nil {
					s.cancel()
				}
				s.connCancel()
				return
			}
		}
	}
}

func (s *stream) writeMessage(msgType int, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return fmt.Errorf("cannot write message: WebSocket connection is closed")
	}
	return s.conn.WriteMessage(msgType, data)
}
