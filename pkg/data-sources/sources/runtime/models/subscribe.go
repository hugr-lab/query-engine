package models

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/vektah/gqlparser/v2/ast"
)

// streamEventSchema is the Arrow schema for llm_stream_event records.
var streamEventSchema = arrow.NewSchema([]arrow.Field{
	{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "content", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "model", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "finish_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tool_calls", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "prompt_tokens", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "completion_tokens", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
}, nil)

// Subscribe implements sources.SubscriptionSource for streaming LLM completions.
func (s *Source) Subscribe(ctx context.Context, field *ast.Field, vars map[string]any) (*sources.SubscriptionResult, error) {
	args := field.ArgumentMap(vars)

	switch field.Name {
	case "completion":
		return s.subscribeCompletion(ctx, args)
	case "chat_completion":
		return s.subscribeChatCompletion(ctx, args)
	default:
		return nil, fmt.Errorf("unknown subscription field %q", field.Name)
	}
}

func (s *Source) subscribeCompletion(ctx context.Context, args map[string]any) (*sources.SubscriptionResult, error) {
	modelName, _ := args["model"].(string)
	if modelName == "" {
		return nil, fmt.Errorf("model argument is required")
	}
	prompt, _ := args["prompt"].(string)
	if prompt == "" {
		return nil, fmt.Errorf("prompt argument is required")
	}

	opts := extractLLMOpts(args)
	messages := []sources.LLMMessage{{Role: "user", Content: prompt}}

	return s.startStream(ctx, modelName, messages, opts)
}

func (s *Source) subscribeChatCompletion(ctx context.Context, args map[string]any) (*sources.SubscriptionResult, error) {
	modelName, _ := args["model"].(string)
	if modelName == "" {
		return nil, fmt.Errorf("model argument is required")
	}

	rawMsgs, _ := args["messages"].([]any)
	if len(rawMsgs) == 0 {
		return nil, fmt.Errorf("messages argument is required")
	}

	var messages []sources.LLMMessage
	for _, raw := range rawMsgs {
		msgStr, _ := raw.(string)
		if msgStr == "" {
			continue
		}
		var msg sources.LLMMessage
		if err := json.Unmarshal([]byte(msgStr), &msg); err != nil {
			return nil, fmt.Errorf("invalid message JSON: %w", err)
		}
		messages = append(messages, msg)
	}

	opts := extractLLMOpts(args)

	// Parse tools if provided
	if rawTools, ok := args["tools"].([]any); ok {
		for _, raw := range rawTools {
			toolStr, _ := raw.(string)
			if toolStr == "" {
				continue
			}
			var tool sources.LLMTool
			if err := json.Unmarshal([]byte(toolStr), &tool); err != nil {
				return nil, fmt.Errorf("invalid tool JSON: %w", err)
			}
			opts.Tools = append(opts.Tools, tool)
		}
	}
	if tc, ok := args["tool_choice"].(string); ok {
		opts.ToolChoice = tc
	}

	return s.startStream(ctx, modelName, messages, opts)
}

func (s *Source) startStream(ctx context.Context, modelName string, messages []sources.LLMMessage, opts sources.LLMOptions) (*sources.SubscriptionResult, error) {
	ds, err := s.resolver.Resolve(modelName)
	if err != nil {
		return nil, fmt.Errorf("model %q not found: %w", modelName, err)
	}

	streamSrc, ok := ds.(sources.LLMStreamingSource)
	if !ok {
		return nil, fmt.Errorf("data source %q does not support streaming", modelName)
	}

	ctx, cancel := context.WithCancel(ctx)
	reader := newChannelRecordReader(ctx, streamEventSchema)

	go func() {
		defer reader.close()

		err := streamSrc.CreateChatCompletionStream(ctx, messages, opts,
			func(event *sources.LLMStreamEvent) error {
				rec := streamEventToRecord(event)
				return reader.send(ctx, rec)
			})
		if err != nil {
			reader.setErr(err)
		}
	}()

	return &sources.SubscriptionResult{
		Reader: reader,
		Cancel: cancel,
	}, nil
}

func extractLLMOpts(args map[string]any) sources.LLMOptions {
	var opts sources.LLMOptions
	if mt, ok := args["max_tokens"]; ok && mt != nil {
		switch v := mt.(type) {
		case int:
			opts.MaxTokens = v
		case int64:
			opts.MaxTokens = int(v)
		case float64:
			opts.MaxTokens = int(v)
		}
	}
	if temp, ok := args["temperature"]; ok && temp != nil {
		switch v := temp.(type) {
		case float64:
			opts.Temperature = v
		case int:
			opts.Temperature = float64(v)
		}
	}
	return opts
}

func streamEventToRecord(event *sources.LLMStreamEvent) arrow.RecordBatch {
	alloc := memory.NewGoAllocator()
	b := array.NewRecordBuilder(alloc, streamEventSchema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).Append(event.Type)
	b.Field(1).(*array.StringBuilder).Append(event.Content)
	b.Field(2).(*array.StringBuilder).Append(event.Model)
	b.Field(3).(*array.StringBuilder).Append(event.FinishReason)
	b.Field(4).(*array.StringBuilder).Append(event.ToolCalls)
	b.Field(5).(*array.Int32Builder).Append(int32(event.PromptTokens))
	b.Field(6).(*array.Int32Builder).Append(int32(event.CompletionTokens))

	return b.NewRecord()
}

// channelRecordReader implements array.RecordReader backed by a channel.
type channelRecordReader struct {
	schema  *arrow.Schema
	ch      chan arrow.RecordBatch
	current arrow.RecordBatch
	err     error
	mu      sync.Mutex
	done    bool
	ctx     context.Context
	refs    int64
}

func newChannelRecordReader(ctx context.Context, schema *arrow.Schema) *channelRecordReader {
	return &channelRecordReader{
		schema: schema,
		ch:     make(chan arrow.RecordBatch, 16),
		ctx:    ctx,
		refs:   1,
	}
}

func (r *channelRecordReader) send(ctx context.Context, rec arrow.RecordBatch) error {
	select {
	case <-ctx.Done():
		rec.Release()
		return ctx.Err()
	case r.ch <- rec:
		return nil
	}
}

func (r *channelRecordReader) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.done {
		r.done = true
		close(r.ch)
	}
}

func (r *channelRecordReader) setErr(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func (r *channelRecordReader) Retain() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs++
}

func (r *channelRecordReader) Release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.refs--
	if r.refs <= 0 {
		r.close()
		if r.current != nil {
			r.current.Release()
			r.current = nil
		}
		// Drain remaining records
		for rec := range r.ch {
			rec.Release()
		}
	}
}

func (r *channelRecordReader) Schema() *arrow.Schema {
	return r.schema
}

func (r *channelRecordReader) Next() bool {
	if r.current != nil {
		r.current.Release()
		r.current = nil
	}

	select {
	case <-r.ctx.Done():
		return false
	case rec, ok := <-r.ch:
		if !ok {
			return false
		}
		r.current = rec
		return true
	}
}

func (r *channelRecordReader) Record() arrow.RecordBatch {
	return r.current
}

func (r *channelRecordReader) RecordBatch() arrow.RecordBatch {
	return r.current
}

func (r *channelRecordReader) Err() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.err
}

var _ sources.SubscriptionSource = (*Source)(nil)
