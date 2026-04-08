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

var streamEventSchema = arrow.NewSchema([]arrow.Field{
	{Name: "type", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "content", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "model", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "finish_reason", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "tool_calls", Type: arrow.BinaryTypes.String, Nullable: true},
	{Name: "prompt_tokens", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
	{Name: "completion_tokens", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
}, nil)

var _ sources.SubscriptionSource = (*Source)(nil)

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

	return s.startStream(ctx, modelName, []sources.LLMMessage{{Role: "user", Content: prompt}}, extractLLMOpts(args))
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
	pipe := newRecordPipe(ctx, streamEventSchema)

	go func() {
		defer pipe.Close()
		err := streamSrc.CreateChatCompletionStream(ctx, messages, opts,
			func(event *sources.LLMStreamEvent) error {
				return pipe.Send(buildEventRecord(event))
			})
		if err != nil {
			pipe.SetErr(err)
		}
	}()

	return &sources.SubscriptionResult{Reader: pipe, Cancel: cancel}, nil
}

func extractLLMOpts(args map[string]any) sources.LLMOptions {
	var opts sources.LLMOptions
	if v, ok := args["max_tokens"]; ok && v != nil {
		switch n := v.(type) {
		case int:
			opts.MaxTokens = n
		case int64:
			opts.MaxTokens = int(n)
		case float64:
			opts.MaxTokens = int(n)
		}
	}
	if v, ok := args["temperature"]; ok && v != nil {
		switch n := v.(type) {
		case float64:
			opts.Temperature = n
		case int:
			opts.Temperature = float64(n)
		}
	}
	return opts
}

func buildEventRecord(event *sources.LLMStreamEvent) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, streamEventSchema)
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

// recordPipe is a channel-backed array.RecordReader.
// Producer sends records via Send, consumer reads via Next/RecordBatch.
// Close signals end-of-stream; context cancellation also stops reads.
type recordPipe struct {
	schema  *arrow.Schema
	ch      chan arrow.RecordBatch
	current arrow.RecordBatch
	ctx     context.Context
	err     error
	closed  sync.Once
}

func newRecordPipe(ctx context.Context, schema *arrow.Schema) *recordPipe {
	return &recordPipe{
		schema: schema,
		ch:     make(chan arrow.RecordBatch, 4),
		ctx:    ctx,
	}
}

// Send a record to the consumer. Blocks if buffer is full. Returns error on cancellation.
func (p *recordPipe) Send(rec arrow.RecordBatch) error {
	select {
	case <-p.ctx.Done():
		rec.Release()
		return p.ctx.Err()
	case p.ch <- rec:
		return nil
	}
}

// Close signals end-of-stream. Safe to call multiple times.
func (p *recordPipe) Close() {
	p.closed.Do(func() { close(p.ch) })
}

// SetErr records a producer-side error, readable via Err() after Next returns false.
func (p *recordPipe) SetErr(err error) { p.err = err }

func (p *recordPipe) Schema() *arrow.Schema           { return p.schema }
func (p *recordPipe) Record() arrow.RecordBatch        { return p.current }
func (p *recordPipe) RecordBatch() arrow.RecordBatch   { return p.current }
func (p *recordPipe) Err() error                       { return p.err }
func (p *recordPipe) Retain()                          {}
func (p *recordPipe) Release()                         { p.Close() }

func (p *recordPipe) Next() bool {
	if p.current != nil {
		p.current.Release()
		p.current = nil
	}
	select {
	case <-p.ctx.Done():
		return false
	case rec, ok := <-p.ch:
		if !ok {
			return false
		}
		p.current = rec
		return true
	}
}
