package models

import (
	"context"
	"database/sql/driver"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	duckdb "github.com/duckdb/duckdb-go/v2"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources"
	"github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

//go:embed schema.graphql
var schema string

// Source is the core.models runtime module.
type Source struct {
	db       *db.Pool
	resolver sources.DataSourceResolver
}

func New() *Source {
	return &Source{}
}

func (s *Source) Name() string           { return "core.models" }
func (s *Source) Engine() engines.Engine { return engines.NewDuckDB() }
func (s *Source) IsReadonly() bool       { return true }
func (s *Source) AsModule() bool         { return true }

// DataSourceServiceSetup implements sources.RuntimeSourceDataSourceUser.
func (s *Source) DataSourceServiceSetup(resolver sources.DataSourceResolver) {
	s.resolver = resolver
}

func (s *Source) Attach(ctx context.Context, pool *db.Pool) error {
	s.db = pool
	return s.registerUDFs(ctx)
}

func (s *Source) Catalog(_ context.Context) (cs.Catalog, error) {
	e := engines.NewDuckDB()
	opts := compiler.Options{
		Name:         s.Name(),
		Prefix:       "core_models",
		ReadOnly:     s.IsReadonly(),
		AsModule:     s.AsModule(),
		EngineType:   string(e.Type()),
		Capabilities: e.Capabilities(),
	}
	return cs.NewStringSource(s.Name(), e, opts, schema)
}

func (s *Source) registerUDFs(ctx context.Context) error {
	// core_models_embedding(model, input) → struct{vector, token_count}
	type embeddingArgs struct {
		model string
		input string
	}
	err := db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[embeddingArgs, *types.EmbeddingResult]{
		Name: "core_models_embedding",
		Execute: func(ctx context.Context, args embeddingArgs) (*types.EmbeddingResult, error) {
			ds, err := s.resolver.Resolve(args.model)
			if err != nil {
				return nil, fmt.Errorf("model %q not found: %w", args.model, err)
			}
			emb, ok := ds.(sources.EmbeddingSource)
			if !ok {
				return nil, fmt.Errorf("data source %q is not an embedding source", args.model)
			}
			return emb.CreateEmbedding(ctx, args.input)
		},
		ConvertInput: func(args []driver.Value) (embeddingArgs, error) {
			return embeddingArgs{model: args[0].(string), input: args[1].(string)}, nil
		},
		ConvertOutput: func(out *types.EmbeddingResult) (any, error) {
			return map[string]any{
				"vector":      out.Vector,
				"token_count": int32(out.TokenCount),
			}, nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
			"vector":      []duckdb.Type{duckdb.TYPE_FLOAT},
			"token_count": duckdb.TYPE_INTEGER,
		}),
	})
	if err != nil {
		return fmt.Errorf("register core_models_embedding: %w", err)
	}

	// core_models_embeddings(model, input_json) → struct{vectors, token_count}
	type embeddingsArgs struct {
		model string
		input string // JSON array of strings
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[embeddingsArgs, *types.EmbeddingsResult]{
		Name: "core_models_embeddings",
		Execute: func(ctx context.Context, args embeddingsArgs) (*types.EmbeddingsResult, error) {
			ds, err := s.resolver.Resolve(args.model)
			if err != nil {
				return nil, fmt.Errorf("model %q not found: %w", args.model, err)
			}
			emb, ok := ds.(sources.EmbeddingSource)
			if !ok {
				return nil, fmt.Errorf("data source %q is not an embedding source", args.model)
			}
			var inputs []string
			if err := json.Unmarshal([]byte(args.input), &inputs); err != nil {
				return nil, fmt.Errorf("input must be a JSON array of strings: %w", err)
			}
			return emb.CreateEmbeddings(ctx, inputs)
		},
		ConvertInput: func(args []driver.Value) (embeddingsArgs, error) {
			return embeddingsArgs{model: args[0].(string), input: args[1].(string)}, nil
		},
		ConvertOutput: func(out *types.EmbeddingsResult) (any, error) {
			vectors := make([]any, len(out.Vectors))
			for i, v := range out.Vectors {
				vectors[i] = v
			}
			return map[string]any{
				"vectors":     vectors,
				"token_count": int32(out.TokenCount),
			}, nil
		},
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
		},
		OutputType: runtime.DuckDBStructTypeFromSchemaMust(map[string]any{
			"vectors":     []duckdb.TypeInfo{runtime.DuckDBListInfoByNameMust("FLOAT")},
			"token_count": duckdb.TYPE_INTEGER,
		}),
	})
	if err != nil {
		return fmt.Errorf("register core_models_embeddings: %w", err)
	}

	// core_models_completion(model, prompt, max_tokens, temperature) → llm_result struct
	type completionArgs struct {
		model       string
		prompt      string
		maxTokens   int32
		temperature float64
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[completionArgs, any]{
		Name: "core_models_completion",
		Execute: func(ctx context.Context, args completionArgs) (any, error) {
			ds, err := s.resolver.Resolve(args.model)
			if err != nil {
				return nil, fmt.Errorf("model %q not found: %w", args.model, err)
			}
			llm, ok := ds.(sources.LLMSource)
			if !ok {
				return nil, fmt.Errorf("data source %q is not an LLM source", args.model)
			}
			opts := sources.LLMOptions{
				MaxTokens:   int(args.maxTokens),
				Temperature: args.temperature,
			}
			start := time.Now()
			result, err := llm.CreateCompletion(ctx, args.prompt, opts)
			if err != nil {
				return nil, err
			}
			result.LatencyMs = int(time.Since(start).Milliseconds())
			return llmResultToMap(result), nil
		},
		ConvertInput: func(args []driver.Value) (completionArgs, error) {
			a := completionArgs{
				model:  args[0].(string),
				prompt: args[1].(string),
			}
			if args[2] != nil {
				a.maxTokens = args[2].(int32)
			}
			if args[3] != nil {
				a.temperature = args[3].(float64)
			}
			return a, nil
		},
		ConvertOutput: func(out any) (any, error) { return out, nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("INTEGER"),
			runtime.DuckDBTypeInfoByNameMust("DOUBLE"),
		},
		OutputType: llmResultType(),
	})
	if err != nil {
		return fmt.Errorf("register core_models_completion: %w", err)
	}

	// core_models_chat_completion(model, messages, tools, tool_choice, max_tokens, temperature)
	type chatArgs struct {
		model       string
		messages    string
		tools       string
		toolChoice  string
		maxTokens   int32
		temperature float64
	}
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionWithArgs[chatArgs, any]{
		Name: "core_models_chat_completion",
		Execute: func(ctx context.Context, args chatArgs) (any, error) {
			ds, err := s.resolver.Resolve(args.model)
			if err != nil {
				return nil, fmt.Errorf("model %q not found: %w", args.model, err)
			}
			llm, ok := ds.(sources.LLMSource)
			if !ok {
				return nil, fmt.Errorf("data source %q is not an LLM source", args.model)
			}
			var messages []sources.LLMMessage
			if err := json.Unmarshal([]byte(args.messages), &messages); err != nil {
				return nil, fmt.Errorf("messages must be valid JSON: %w", err)
			}
			opts := sources.LLMOptions{
				MaxTokens:   int(args.maxTokens),
				Temperature: args.temperature,
				ToolChoice:  args.toolChoice,
			}
			if args.tools != "" {
				if err := json.Unmarshal([]byte(args.tools), &opts.Tools); err != nil {
					return nil, fmt.Errorf("tools must be valid JSON: %w", err)
				}
			}
			start := time.Now()
			result, err := llm.CreateChatCompletion(ctx, messages, opts)
			if err != nil {
				return nil, err
			}
			result.LatencyMs = int(time.Since(start).Milliseconds())
			return llmResultToMap(result), nil
		},
		ConvertInput: func(args []driver.Value) (chatArgs, error) {
			a := chatArgs{
				model:    args[0].(string),
				messages: args[1].(string),
			}
			if args[2] != nil {
				a.tools = args[2].(string)
			}
			if args[3] != nil {
				a.toolChoice = args[3].(string)
			}
			if args[4] != nil {
				a.maxTokens = args[4].(int32)
			}
			if args[5] != nil {
				a.temperature = args[5].(float64)
			}
			return a, nil
		},
		ConvertOutput: func(out any) (any, error) { return out, nil },
		InputTypes: []duckdb.TypeInfo{
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
			runtime.DuckDBTypeInfoByNameMust("INTEGER"),
			runtime.DuckDBTypeInfoByNameMust("DOUBLE"),
		},
		OutputType: llmResultType(),
	})
	if err != nil {
		return fmt.Errorf("register core_models_chat_completion: %w", err)
	}

	// core_models_sources() → JSON string (list of model_source_info)
	err = db.RegisterScalarFunction(ctx, s.db, &db.ScalarFunctionNoArgs[string]{
		Name: "core_models_sources",
		Execute: func(ctx context.Context) (string, error) {
			if s.resolver == nil {
				return "[]", nil
			}
			var result []map[string]any
			for _, ds := range s.resolver.ResolveAll() {
				ms, ok := ds.(sources.ModelSource)
				if !ok {
					continue
				}
				info := ms.ModelInfo()
				result = append(result, map[string]any{
					"name":     info.Name,
					"type":     info.Type,
					"provider": info.Provider,
					"model":    info.Model,
				})
			}
			b, _ := json.Marshal(result)
			return string(b), nil
		},
		ConvertOutput: func(out string) (any, error) { return out, nil },
		OutputType:    runtime.DuckDBTypeInfoByNameMust("VARCHAR"),
	})
	if err != nil {
		return fmt.Errorf("register core_models_sources: %w", err)
	}

	return nil
}

// helpers

func llmResultToMap(r *sources.LLMResult) map[string]any {
	var toolCallsJSON string
	if len(r.ToolCalls) > 0 {
		b, _ := json.Marshal(r.ToolCalls)
		toolCallsJSON = string(b)
	}
	return map[string]any{
		"content":           r.Content,
		"model":             r.Model,
		"finish_reason":     r.FinishReason,
		"prompt_tokens":     int32(r.PromptTokens),
		"completion_tokens": int32(r.CompletionTokens),
		"total_tokens":      int32(r.TotalTokens),
		"provider":          r.Provider,
		"latency_ms":        int32(r.LatencyMs),
		"tool_calls":        toolCallsJSON,
	}
}

// DuckDB type constructors

func llmResultType() duckdb.TypeInfo {
	varchar, _ := duckdb.NewTypeInfo(duckdb.TYPE_VARCHAR)
	intType, _ := duckdb.NewTypeInfo(duckdb.TYPE_INTEGER)

	content, _ := duckdb.NewStructEntry(varchar, "content")
	model, _ := duckdb.NewStructEntry(varchar, "model")
	finishReason, _ := duckdb.NewStructEntry(varchar, "finish_reason")
	promptTokens, _ := duckdb.NewStructEntry(intType, "prompt_tokens")
	completionTokens, _ := duckdb.NewStructEntry(intType, "completion_tokens")
	totalTokens, _ := duckdb.NewStructEntry(intType, "total_tokens")
	provider, _ := duckdb.NewStructEntry(varchar, "provider")
	latencyMs, _ := duckdb.NewStructEntry(intType, "latency_ms")
	toolCalls, _ := duckdb.NewStructEntry(varchar, "tool_calls")

	t, _ := duckdb.NewStructInfo(content, model, finishReason, promptTokens, completionTokens, totalTokens, provider, latencyMs, toolCalls)
	return t
}


var _ sources.RuntimeSourceDataSourceUser = (*Source)(nil)

// Ensure unused imports are used
var _ = errors.New
