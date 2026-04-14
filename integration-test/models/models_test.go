//go:build duckdb_arrow

package models_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/types"
)

var testService *hugr.Service

func TestMain(m *testing.M) {
	ctx := context.Background()

	service, err := hugr.New(hugr.Config{
		Debug:  true,
		DB:     db.Config{},
		CoreDB: coredb.New(coredb.Config{VectorSize: 768}),
		Auth:   &auth.Config{},
	})
	if err != nil {
		panic(err)
	}
	if err := service.Init(ctx); err != nil {
		panic(err)
	}
	testService = service

	// Register all data sources upfront
	registerDS(ctx, service)

	code := m.Run()
	service.Close()
	os.Exit(code)
}

func registerDS(ctx context.Context, s *hugr.Service) {
	// Embedding DS
	if url := os.Getenv("EMBEDDER_URL"); url != "" {
		mustQuery(ctx, s, `mutation($data: core_data_sources_mut_input_data!) {
			core { insert_data_sources(data: $data) { name } }
		}`, map[string]any{
			"data": map[string]any{
				"name": "test_embedder", "type": "embedding",
				"prefix": "test_embedder", "as_module": false, "path": url,
			},
		})
		mustQuery(ctx, s, `mutation { function { core { load_data_source(name: "test_embedder") { success } } } }`, nil)
	}

	// LLM DS (OpenAI-compatible)
	if url := os.Getenv("LLM_URL"); url != "" {
		mustQuery(ctx, s, `mutation($data: core_data_sources_mut_input_data!) {
			core { insert_data_sources(data: $data) { name } }
		}`, map[string]any{
			"data": map[string]any{
				"name": "test_llm", "type": "llm-openai",
				"prefix": "test_llm", "as_module": false, "path": url,
			},
		})
		mustQuery(ctx, s, `mutation { function { core { load_data_source(name: "test_llm") { success } } } }`, nil)
	}

	// Anthropic DS
	if key := os.Getenv("ANTHROPIC_KEY"); key != "" {
		path := "https://api.anthropic.com/v1/messages?model=claude-sonnet-4-20250514&api_key=" + key + "&max_tokens=4096&thinking_budget=2048&timeout=60s"
		mustQuery(ctx, s, `mutation($data: core_data_sources_mut_input_data!) {
			core { insert_data_sources(data: $data) { name } }
		}`, map[string]any{
			"data": map[string]any{
				"name": "test_anthropic", "type": "llm-anthropic",
				"prefix": "test_anthropic", "as_module": false, "path": path,
			},
		})
		mustQuery(ctx, s, `mutation { function { core { load_data_source(name: "test_anthropic") { success } } } }`, nil)
	}

	// Gemini DS
	if key := os.Getenv("GEMINI_KEY"); key != "" {
		path := "https://generativelanguage.googleapis.com/v1beta?model=gemini-3.1-pro-preview&api_key=" + key + "&max_tokens=4096&thinking_budget=2048&timeout=120s"
		mustQuery(ctx, s, `mutation($data: core_data_sources_mut_input_data!) {
			core { insert_data_sources(data: $data) { name } }
		}`, map[string]any{
			"data": map[string]any{
				"name": "test_gemini", "type": "llm-gemini",
				"prefix": "test_gemini", "as_module": false, "path": path,
			},
		})
		mustQuery(ctx, s, `mutation { function { core { load_data_source(name: "test_gemini") { success } } } }`, nil)
	}

	// OpenAI Remote DS (official API, not LM Studio)
	if key := os.Getenv("OPENAI_KEY"); key != "" {
		path := "https://api.openai.com/v1/chat/completions?model=\"gpt-5.4-mini-2026-03-17\"&api_key=" + key + "&max_tokens=4096&timeout=120s"
		mustQuery(ctx, s, `mutation($data: core_data_sources_mut_input_data!) {
			core { insert_data_sources(data: $data) { name } }
		}`, map[string]any{
			"data": map[string]any{
				"name": "test_openai_remote", "type": "llm-openai",
				"prefix": "test_openai_remote", "as_module": false, "path": path,
			},
		})
		mustQuery(ctx, s, `mutation { function { core { load_data_source(name: "test_openai_remote") { success } } } }`, nil)
	}
}

func mustQuery(ctx context.Context, s *hugr.Service, q string, vars map[string]any) {
	res, err := s.Query(ctx, q, vars)
	if err != nil {
		panic(err)
	}
	res.Close()
}

func query(t *testing.T, q string, vars map[string]any) *types.Response {
	t.Helper()
	res, err := testService.Query(context.Background(), q, vars)
	require.NoError(t, err)
	return res
}

// --- US1: Embeddings ---

func TestModels_Embedding(t *testing.T) {
	if os.Getenv("EMBEDDER_URL") == "" {
		t.Skip("EMBEDDER_URL not set")
	}
	res := query(t, `{ function { core { models { embedding(model: "test_embedder", input: "hello world") {
		vector token_count
	} } } } }`, nil)
	defer res.Close()

	var result map[string]any
	err := res.ScanData("function.core.models.embedding", &result)
	require.NoError(t, err)
	assert.Contains(t, result, "vector")
	assert.Contains(t, result, "token_count")
	t.Logf("embedding: vector present, token_count=%v", result["token_count"])
}

func TestModels_Embedding_NotFound(t *testing.T) {
	res, err := testService.Query(context.Background(),
		`{ function { core { models { embedding(model: "nonexistent", input: "test") { vector } } } } }`, nil)
	if err != nil {
		t.Logf("expected error: %v", err)
		return
	}
	defer res.Close()
	assert.True(t, len(res.Errors) > 0 || err != nil, "should error for nonexistent model")
}

// --- US2: Completion ---

func TestModels_Completion(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	res := query(t, `{ function { core { models { completion(model: "test_llm", prompt: "Say hello in one word", max_tokens: 100) {
		content model finish_reason prompt_tokens completion_tokens total_tokens provider latency_ms
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		Model        string `json:"model"`
		FinishReason string `json:"finish_reason"`
		LatencyMs    int    `json:"latency_ms"`
	}
	err := res.ScanData("function.core.models.completion", &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Model, "model should not be empty")
	assert.Greater(t, result.LatencyMs, 0, "latency should be positive")
	t.Logf("completion: %q, model=%s, finish=%s, latency=%dms", result.Content, result.Model, result.FinishReason, result.LatencyMs)
}

func TestModels_Completion_NotLLM(t *testing.T) {
	res, err := testService.Query(context.Background(),
		`{ function { core { models { completion(model: "nonexistent", prompt: "test") { content } } } } }`, nil)
	if err != nil {
		t.Logf("expected error: %v", err)
		return
	}
	defer res.Close()
	if len(res.Errors) > 0 {
		t.Logf("expected GraphQL errors: %v", res.Errors)
		return
	}
	var result map[string]any
	_ = res.ScanData("function.core.models.completion", &result)
	t.Errorf("completion with nonexistent model returned: %v", result)
}

// --- US3: Chat Completion ---

func TestModels_ChatCompletion(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	res := query(t, `{ function { core { models { chat_completion(
		model: "test_llm",
		messages: ["{\"role\":\"user\",\"content\":\"What is 2+2? Answer with just the number.\"}"],
		max_tokens: 50
	) {
		content finish_reason tool_calls
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		FinishReason string `json:"finish_reason"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.FinishReason)
	t.Logf("chat: %q, finish=%s", result.Content, result.FinishReason)
}

func TestModels_ChatCompletionWithTools(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	res := query(t, `{ function { core { models { chat_completion(
		model: "test_llm",
		messages: ["{\"role\":\"user\",\"content\":\"What is the weather in London?\"}"],
		tools: ["{\"name\":\"get_weather\",\"description\":\"Get weather for a city\",\"parameters\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\"}},\"required\":[\"city\"]}}"],
		tool_choice: "auto",
		max_tokens: 200
	) {
		content finish_reason tool_calls
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		FinishReason string `json:"finish_reason"`
		ToolCalls    string `json:"tool_calls"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.FinishReason)
	t.Logf("chat with tools: content=%q, finish=%s, tool_calls=%s",
		result.Content, result.FinishReason, result.ToolCalls)
}

func TestModels_ChatCompletionMultiTurn(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	res := query(t, `{ function { core { models { chat_completion(
		model: "test_llm",
		messages: [
			"{\"role\":\"system\",\"content\":\"You are a math tutor. Be concise.\"}",
			"{\"role\":\"user\",\"content\":\"What is 2+2?\"}",
			"{\"role\":\"assistant\",\"content\":\"4\"}",
			"{\"role\":\"user\",\"content\":\"And 3+3?\"}"
		],
		max_tokens: 50,
		temperature: 0.1
	) {
		content finish_reason prompt_tokens completion_tokens
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		FinishReason string `json:"finish_reason"`
		PromptTokens int    `json:"prompt_tokens"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.FinishReason)
	t.Logf("multi-turn: %q, finish=%s, prompt_tokens=%d", result.Content, result.FinishReason, result.PromptTokens)
}

// --- US5: Discovery ---

func TestModels_Sources_Empty(t *testing.T) {
	// Without registered model sources this returns whatever is loaded
	res := query(t, `{ function { core { models { model_sources { name type provider model } } } } }`, nil)
	defer res.Close()
	require.Empty(t, res.Errors)
	var result []map[string]any
	err := res.ScanData("function.core.models.model_sources", &result)
	require.NoError(t, err)
	t.Logf("model_sources: %d items", len(result))
}

func TestModels_Sources_WithRegistered(t *testing.T) {
	if os.Getenv("EMBEDDER_URL") == "" {
		t.Skip("EMBEDDER_URL not set")
	}
	res := query(t, `{ function { core { models { model_sources { name type provider model } } } } }`, nil)
	defer res.Close()

	var srcs []map[string]any
	err := res.ScanData("function.core.models.model_sources", &srcs)
	require.NoError(t, err)

	found := false
	for _, s := range srcs {
		if s["name"] == "test_embedder" {
			found = true
			assert.Equal(t, "embedding", s["type"])
			assert.Equal(t, "openai", s["provider"])
			t.Logf("discovered: %+v", s)
		}
	}
	assert.True(t, found, "test_embedder should appear in sources")
}

// --- Anthropic Provider ---

func TestModels_Anthropic_Completion(t *testing.T) {
	if os.Getenv("ANTHROPIC_KEY") == "" {
		t.Skip("ANTHROPIC_KEY not set")
	}
	res := query(t, `{ function { core { models { completion(model: "test_anthropic", prompt: "What is 2+2? Answer with just the number.", max_tokens: 50) {
		content model finish_reason prompt_tokens completion_tokens total_tokens provider latency_ms
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		Model        string `json:"model"`
		FinishReason string `json:"finish_reason"`
		Provider     string `json:"provider"`
		LatencyMs    int    `json:"latency_ms"`
	}
	err := res.ScanData("function.core.models.completion", &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Content)
	assert.Equal(t, "anthropic", result.Provider)
	assert.NotEmpty(t, result.FinishReason)
	t.Logf("anthropic completion: %q, model=%s, finish=%s, provider=%s, latency=%dms",
		result.Content, result.Model, result.FinishReason, result.Provider, result.LatencyMs)
}

func TestModels_Anthropic_ChatWithTools(t *testing.T) {
	if os.Getenv("ANTHROPIC_KEY") == "" {
		t.Skip("ANTHROPIC_KEY not set")
	}
	res := query(t, `{ function { core { models { chat_completion(
		model: "test_anthropic",
		messages: ["{\"role\":\"user\",\"content\":\"What is the weather in Paris?\"}"],
		tools: ["{\"name\":\"get_weather\",\"description\":\"Get current weather for a city\",\"parameters\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\",\"description\":\"City name\"}},\"required\":[\"city\"]}}"],
		tool_choice: "auto",
		max_tokens: 200
	) {
		content finish_reason tool_calls provider
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		FinishReason string `json:"finish_reason"`
		ToolCalls    string `json:"tool_calls"`
		Provider     string `json:"provider"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.Equal(t, "anthropic", result.Provider)
	assert.NotEmpty(t, result.FinishReason)
	// Claude should call get_weather for "What is the weather in Paris?"
	if result.FinishReason == "tool_use" {
		assert.NotEmpty(t, result.ToolCalls, "tool_calls should not be empty when finish_reason is tool_use")
		assert.Contains(t, result.ToolCalls, "get_weather", "should call get_weather tool")
	}
	t.Logf("anthropic chat+tools: content=%q, finish=%s, tool_calls=%s",
		result.Content, result.FinishReason, result.ToolCalls)
}

// --- Gemini Provider ---

func TestModels_Gemini_Completion(t *testing.T) {
	if os.Getenv("GEMINI_KEY") == "" {
		t.Skip("GEMINI_KEY not set")
	}
	res := query(t, `{ function { core { models { completion(model: "test_gemini", prompt: "What is 2+2? Answer with just the number.", max_tokens: 200) {
		content model finish_reason prompt_tokens completion_tokens total_tokens provider latency_ms
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		Model        string `json:"model"`
		FinishReason string `json:"finish_reason"`
		Provider     string `json:"provider"`
		LatencyMs    int    `json:"latency_ms"`
	}
	err := res.ScanData("function.core.models.completion", &result)
	require.NoError(t, err)
	assert.Equal(t, "gemini", result.Provider)
	assert.NotEmpty(t, result.FinishReason)
	assert.Greater(t, result.LatencyMs, 0)
	t.Logf("gemini completion: %q, model=%s, finish=%s, provider=%s, latency=%dms",
		result.Content, result.Model, result.FinishReason, result.Provider, result.LatencyMs)
}

func TestModels_Gemini_ChatWithTools(t *testing.T) {
	if os.Getenv("GEMINI_KEY") == "" {
		t.Skip("GEMINI_KEY not set")
	}
	res := query(t, `{ function { core { models { chat_completion(
		model: "test_gemini",
		messages: ["{\"role\":\"user\",\"content\":\"What is the weather in Tokyo?\"}"],
		tools: ["{\"name\":\"get_weather\",\"description\":\"Get current weather for a city\",\"parameters\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\",\"description\":\"City name\"}},\"required\":[\"city\"]}}"],
		tool_choice: "auto",
		max_tokens: 200
	) {
		content finish_reason tool_calls provider thought_signature
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content          string `json:"content"`
		FinishReason     string `json:"finish_reason"`
		ToolCalls        string `json:"tool_calls"`
		Provider         string `json:"provider"`
		ThoughtSignature string `json:"thought_signature"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.Equal(t, "gemini", result.Provider)
	assert.NotEmpty(t, result.FinishReason)
	if result.FinishReason == "tool_use" || result.ToolCalls != "" {
		assert.Contains(t, result.ToolCalls, "get_weather", "should call get_weather tool")
		assert.NotEmpty(t, result.ThoughtSignature, "Gemini 2.5+ should return thought_signature with tool calls")
	}
	t.Logf("gemini chat+tools: content=%q, finish=%s, tool_calls=%s, thought_sig=%q",
		result.Content, result.FinishReason, result.ToolCalls, result.ThoughtSignature[:min(len(result.ThoughtSignature), 50)])
}

// --- OpenAI Remote Provider (official API) ---

func TestModels_OpenAIRemote_Completion(t *testing.T) {
	if os.Getenv("OPENAI_KEY") == "" {
		t.Skip("OPENAI_KEY not set")
	}
	res := query(t, `{ function { core { models { completion(model: "test_openai_remote", prompt: "What is 2+2? Answer with just the number.", max_tokens: 50) {
		content model finish_reason prompt_tokens completion_tokens total_tokens provider latency_ms
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		Model        string `json:"model"`
		FinishReason string `json:"finish_reason"`
		Provider     string `json:"provider"`
		LatencyMs    int    `json:"latency_ms"`
	}
	err := res.ScanData("function.core.models.completion", &result)
	require.NoError(t, err)
	assert.NotEmpty(t, result.Content)
	assert.Equal(t, "openai", result.Provider)
	assert.NotEmpty(t, result.FinishReason)
	t.Logf("openai remote completion: %q, model=%s, finish=%s, provider=%s, latency=%dms",
		result.Content, result.Model, result.FinishReason, result.Provider, result.LatencyMs)
}

func TestModels_OpenAIRemote_ChatWithTools(t *testing.T) {
	if os.Getenv("OPENAI_KEY") == "" {
		t.Skip("OPENAI_KEY not set")
	}
	res := query(t, `{ function { core { models { chat_completion(
		model: "test_openai_remote",
		messages: ["{\"role\":\"user\",\"content\":\"What is the weather in Paris?\"}"],
		tools: ["{\"name\":\"get_weather\",\"description\":\"Get current weather for a city\",\"parameters\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\",\"description\":\"City name\"}},\"required\":[\"city\"]}}"],
		tool_choice: "auto",
		max_tokens: 200
	) {
		content finish_reason tool_calls provider
	} } } } }`, nil)
	defer res.Close()

	var result struct {
		Content      string `json:"content"`
		FinishReason string `json:"finish_reason"`
		ToolCalls    string `json:"tool_calls"`
		Provider     string `json:"provider"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.Equal(t, "openai", result.Provider)
	assert.NotEmpty(t, result.FinishReason)
	if result.FinishReason == "tool_use" {
		assert.NotEmpty(t, result.ToolCalls, "tool_calls should not be empty when finish_reason is tool_use")
		assert.Contains(t, result.ToolCalls, "get_weather", "should call get_weather tool")
	}
	t.Logf("openai remote chat+tools: content=%q, finish=%s, tool_calls=%s",
		result.Content, result.FinishReason, result.ToolCalls)
}

func TestModels_StreamCompletion_OpenAIRemote(t *testing.T) {
	if os.Getenv("OPENAI_KEY") == "" {
		t.Skip("OPENAI_KEY not set")
	}
	events := collectStreamEventsWithTimeout(t,
		`subscription { core { models { completion(model: "test_openai_remote", prompt: "Say hello in one word.", max_tokens: 50) {
			type content model finish_reason tool_calls prompt_tokens completion_tokens
		} } } }`, 120*time.Second)

	require.NotEmpty(t, events, "should receive streaming events")
	assertStreamEvents(t, "OpenAI Remote", events)
}

func TestModels_StreamChatCompletionWithTools_OpenAIRemote(t *testing.T) {
	if os.Getenv("OPENAI_KEY") == "" {
		t.Skip("OPENAI_KEY not set")
	}
	events := collectStreamEventsWithTimeout(t, fmt.Sprintf(streamToolCallQuery, "test_openai_remote"), 120*time.Second)
	require.NotEmpty(t, events, "should receive streaming events")
	assertStreamToolCalls(t, "OpenAI Remote", events)
}

// --- US6: LLM Streaming Subscriptions ---

func TestModels_StreamCompletion_OpenAI(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	events := collectStreamEvents(t,
		`subscription { core { models { completion(model: "test_llm", prompt: "Explain step by step why the sky is blue. Think through the physics involved.", max_tokens: 500) {
			type content model finish_reason tool_calls prompt_tokens completion_tokens
		} } } }`)

	require.NotEmpty(t, events, "should receive streaming events")
	assertStreamEvents(t, "OpenAI", events)
}

func TestModels_StreamChatCompletion_OpenAI(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	sub, err := testService.Subscribe(ctx,
		`subscription { core { models { chat_completion(
			model: "test_llm",
			messages: ["{\"role\":\"user\",\"content\":\"Say hello in one word\"}"],
			max_tokens: 50
		) {
			type content finish_reason
		} } } }`, nil)
	require.NoError(t, err)
	defer sub.Cancel()

	var eventCount int
	var content string
	for event := range sub.Events {
		for event.Reader.Next() {
			batch := event.Reader.RecordBatch()
			schema := batch.Schema()
			for i := 0; i < int(batch.NumRows()); i++ {
				eventCount++
				typeIdx := schema.FieldIndices("type")[0]
				contentIdx := schema.FieldIndices("content")[0]
				eventType := batch.Column(typeIdx).GetOneForMarshal(i)
				eventContent := batch.Column(contentIdx).GetOneForMarshal(i)
				if eventType == "content_delta" && eventContent != nil {
					content += fmt.Sprintf("%v", eventContent)
				}
			}
		}
		event.Reader.Release()
	}

	assert.Greater(t, eventCount, 0, "should receive events")
	// Note: some OpenAI-compatible servers may return content in finish event only
	t.Logf("OpenAI chat stream: %d events, content=%q", eventCount, content)
}

func TestModels_StreamCompletion_Anthropic(t *testing.T) {
	if os.Getenv("ANTHROPIC_KEY") == "" {
		t.Skip("ANTHROPIC_KEY not set")
	}
	events := collectStreamEvents(t,
		`subscription { core { models { completion(model: "test_anthropic", prompt: "Explain why 2+2=4 in one sentence. Think step by step.", max_tokens: 4096) {
			type content model finish_reason tool_calls prompt_tokens completion_tokens
		} } } }`)

	require.NotEmpty(t, events, "should receive events")
	assertStreamEvents(t, "Anthropic", events)
}

func TestModels_StreamCompletion_Gemini(t *testing.T) {
	if os.Getenv("GEMINI_KEY") == "" {
		t.Skip("GEMINI_KEY not set")
	}
	events := collectStreamEvents(t,
		`subscription { core { models { completion(model: "test_gemini", prompt: "Explain why 2+2=4 in one sentence. Think step by step.", max_tokens: 4096) {
			type content model finish_reason tool_calls prompt_tokens completion_tokens
		} } } }`)

	require.NotEmpty(t, events, "should receive events")
	assertStreamEvents(t, "Gemini", events)
}

// collectStreamEvents subscribes and collects all streaming events as row maps.
func collectStreamEvents(t *testing.T, query string) []map[string]any {
	return collectStreamEventsWithTimeout(t, query, 60*time.Second)
}

func collectStreamEventsWithTimeout(t *testing.T, query string, timeout time.Duration) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	sub, err := testService.Subscribe(ctx, query, nil)
	require.NoError(t, err)
	defer sub.Cancel()

	var events []map[string]any
	for event := range sub.Events {
		for event.Reader.Next() {
			batch := event.Reader.RecordBatch()
			schema := batch.Schema()
			for i := 0; i < int(batch.NumRows()); i++ {
				row := make(map[string]any)
				for j := 0; j < int(batch.NumCols()); j++ {
					row[schema.Field(j).Name] = batch.Column(j).GetOneForMarshal(i)
				}
				events = append(events, row)
			}
		}
		require.NoError(t, event.Reader.Err())
		event.Reader.Release()
	}
	return events
}

// assertStreamEvents verifies that a stream has proper event structure:
// - At least one content event (content_delta or reasoning)
// - Exactly one finish event with finish_reason, prompt_tokens, completion_tokens
func assertStreamEvents(t *testing.T, provider string, events []map[string]any) {
	t.Helper()

	var contentEvents, reasoningEvents int
	var finishEvent map[string]any
	var allContent string

	for _, e := range events {
		eventType := fmt.Sprintf("%v", e["type"])
		switch eventType {
		case "content_delta":
			contentEvents++
			if c := e["content"]; c != nil {
				allContent += fmt.Sprintf("%v", c)
			}
		case "reasoning":
			reasoningEvents++
			if c := e["content"]; c != nil {
				allContent += fmt.Sprintf("%v", c)
			}
		case "finish":
			finishEvent = e
		}
	}

	assert.True(t, contentEvents > 0 || reasoningEvents > 0,
		"%s: should have content_delta or reasoning events", provider)
	require.NotNil(t, finishEvent, "%s: should have a finish event", provider)

	// finish event should have finish_reason
	assert.NotEmpty(t, finishEvent["finish_reason"],
		"%s: finish event should have finish_reason", provider)

	t.Logf("%s stream: %d events total (%d content, %d reasoning, finish_reason=%v)",
		provider, len(events), contentEvents, reasoningEvents, finishEvent["finish_reason"])
	t.Logf("%s stream: prompt_tokens=%v completion_tokens=%v",
		provider, finishEvent["prompt_tokens"], finishEvent["completion_tokens"])

	// Log first few content chars
	if len(allContent) > 100 {
		allContent = allContent[:100] + "..."
	}
	t.Logf("%s stream content: %q", provider, allContent)
}

// --- Multi-Provider Discovery ---

func TestModels_Sources_AllProviders(t *testing.T) {
	res := query(t, `{ function { core { models { model_sources { name type provider model } } } } }`, nil)
	defer res.Close()

	var srcs []map[string]any
	err := res.ScanData("function.core.models.model_sources", &srcs)
	require.NoError(t, err)

	providers := map[string]bool{}
	for _, s := range srcs {
		providers[s["provider"].(string)] = true
		t.Logf("source: name=%s type=%s provider=%s model=%s", s["name"], s["type"], s["provider"], s["model"])
	}
	t.Logf("total sources: %d, providers: %v", len(srcs), providers)
}

// --- US4: Streaming Tool Call Format Tests ---

const streamToolCallQuery = `subscription { core { models { chat_completion(
	model: "%s",
	messages: ["{\"role\":\"user\",\"content\":\"What is the weather in London?\"}"],
	tools: ["{\"name\":\"get_weather\",\"description\":\"Get weather for a city\",\"parameters\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\"}},\"required\":[\"city\"]}}"],
	tool_choice: "auto",
	max_tokens: 200
) {
	type content finish_reason tool_calls prompt_tokens completion_tokens thought_signature
} } } }`

// assertStreamToolCalls validates that the finish event tool_calls field
// contains a valid []LLMToolCall JSON array with id, name, and arguments.
func assertStreamToolCalls(t *testing.T, provider string, events []map[string]any) {
	t.Helper()

	var finishEvent map[string]any
	for _, e := range events {
		if fmt.Sprintf("%v", e["type"]) == "finish" {
			finishEvent = e
		}
	}
	require.NotNil(t, finishEvent, "%s: should have a finish event", provider)

	toolCallsRaw := finishEvent["tool_calls"]
	require.NotNil(t, toolCallsRaw, "%s: finish event should have tool_calls", provider)

	toolCallsStr := fmt.Sprintf("%v", toolCallsRaw)
	require.NotEmpty(t, toolCallsStr, "%s: tool_calls should not be empty", provider)

	var calls []types.LLMToolCall
	err := json.Unmarshal([]byte(toolCallsStr), &calls)
	require.NoError(t, err, "%s: tool_calls should parse as []LLMToolCall, got: %s", provider, toolCallsStr)
	require.NotEmpty(t, calls, "%s: should have at least one tool call", provider)

	for i, tc := range calls {
		assert.NotEmpty(t, tc.Name, "%s: tool call[%d] should have a name", provider, i)
		assert.NotNil(t, tc.Arguments, "%s: tool call[%d] should have arguments", provider, i)
		t.Logf("%s stream tool call[%d]: id=%s name=%s args=%v", provider, i, tc.ID, tc.Name, tc.Arguments)
	}
	if ts := finishEvent["thought_signature"]; ts != nil && fmt.Sprintf("%v", ts) != "" {
		t.Logf("%s stream thought_signature=%q", provider, ts)
	}

	t.Logf("%s stream finish: finish_reason=%v, tool_calls=%d items", provider, finishEvent["finish_reason"], len(calls))
}

func TestModels_StreamChatCompletionWithTools_OpenAI(t *testing.T) {
	if os.Getenv("LLM_URL") == "" {
		t.Skip("LLM_URL not set")
	}
	events := collectStreamEvents(t, fmt.Sprintf(streamToolCallQuery, "test_llm"))
	require.NotEmpty(t, events, "should receive streaming events")
	assertStreamToolCalls(t, "OpenAI", events)
}

func TestModels_StreamChatCompletionWithTools_Anthropic(t *testing.T) {
	if os.Getenv("ANTHROPIC_KEY") == "" {
		t.Skip("ANTHROPIC_KEY not set")
	}
	// Anthropic with thinking requires max_tokens > thinking_budget (configured at 2048).
	q := `subscription { core { models { chat_completion(
		model: "test_anthropic",
		messages: ["{\"role\":\"user\",\"content\":\"What is the weather in London?\"}"],
		tools: ["{\"name\":\"get_weather\",\"description\":\"Get weather for a city\",\"parameters\":{\"type\":\"object\",\"properties\":{\"city\":{\"type\":\"string\"}},\"required\":[\"city\"]}}"],
		tool_choice: "auto",
		max_tokens: 4096
	) {
		type content finish_reason tool_calls prompt_tokens completion_tokens
	} } } }`
	events := collectStreamEvents(t, q)
	require.NotEmpty(t, events, "should receive streaming events")
	assertStreamToolCalls(t, "Anthropic", events)
}

func TestModels_StreamChatCompletionWithTools_Gemini(t *testing.T) {
	if os.Getenv("GEMINI_KEY") == "" {
		t.Skip("GEMINI_KEY not set")
	}
	events := collectStreamEvents(t, fmt.Sprintf(streamToolCallQuery, "test_gemini"))
	require.NotEmpty(t, events, "should receive streaming events")
	assertStreamToolCalls(t, "Gemini", events)
}

// --- Multi-turn Tool Call Round-trip Tests ---
// These verify the full cycle: request with tools → tool_calls response →
// send tool results in history → model responds with content.
// Critical for Gemini thoughtSignature placement and correct message format.

const roundTripToolDef = `{"name":"get_weather","description":"Get current weather for a city","parameters":{"type":"object","properties":{"city":{"type":"string","description":"City name"}},"required":["city"]}}`

func truncateStr(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// testToolCallRoundTrip runs a two-step non-streaming tool call cycle.
func testToolCallRoundTrip(t *testing.T, modelName, provider string, maxTokens int) {
	t.Helper()

	userMsg := `{"role":"user","content":"What is the weather in Tokyo? You must use the get_weather tool."}`

	// Step 1: chat_completion with tools → model should call get_weather
	step1Q := fmt.Sprintf(`query($messages: [String!]!, $tools: [String!]) {
		function { core { models { chat_completion(
			model: "%s", messages: $messages, tools: $tools,
			tool_choice: "auto", max_tokens: %d
		) { content finish_reason tool_calls provider thought_signature } } } } }`, modelName, maxTokens)

	res := query(t, step1Q, map[string]any{
		"messages": []string{userMsg},
		"tools":    []string{roundTripToolDef},
	})
	defer res.Close()

	var result struct {
		Content          string `json:"content"`
		FinishReason     string `json:"finish_reason"`
		ToolCalls        string `json:"tool_calls"`
		Provider         string `json:"provider"`
		ThoughtSignature string `json:"thought_signature"`
	}
	err := res.ScanData("function.core.models.chat_completion", &result)
	require.NoError(t, err)
	assert.Equal(t, provider, result.Provider)
	require.NotEmpty(t, result.ToolCalls, "%s: model should call tools for weather question", provider)

	var toolCalls []types.LLMToolCall
	err = json.Unmarshal([]byte(result.ToolCalls), &toolCalls)
	require.NoError(t, err, "%s: tool_calls JSON parse failed: %s", provider, result.ToolCalls)
	require.NotEmpty(t, toolCalls, "%s: should have at least one tool call", provider)

	for i, tc := range toolCalls {
		t.Logf("Step 1 — %s tool_call[%d]: id=%s name=%s args=%v",
			provider, i, tc.ID, tc.Name, tc.Arguments)
	}
	if result.ThoughtSignature != "" {
		t.Logf("Step 1 — %s thought_signature=%q", provider, truncateStr(result.ThoughtSignature, 50))
	}

	// Step 2: Build multi-turn messages with tool results
	// thought_signature is on the assistant MESSAGE, not on individual tool calls
	assistantMsg := types.LLMMessage{
		Role:             "assistant",
		Content:          result.Content,
		ToolCalls:        toolCalls,
		ThoughtSignature: result.ThoughtSignature,
	}
	assistantJSON, err := json.Marshal(assistantMsg)
	require.NoError(t, err)

	messages := []string{userMsg, string(assistantJSON)}
	for _, tc := range toolCalls {
		toolID := tc.ID
		if toolID == "" {
			toolID = tc.Name
		}
		toolResult := types.LLMMessage{
			Role:       "tool",
			Content:    `{"temperature": 22, "condition": "sunny", "humidity": 65}`,
			ToolCallID: toolID,
		}
		toolJSON, err := json.Marshal(toolResult)
		require.NoError(t, err)
		messages = append(messages, string(toolJSON))
	}

	t.Logf("Step 2 — sending %d messages back to %s", len(messages), provider)

	// Step 3: Second request with tool results in history → should get content response
	step2Q := fmt.Sprintf(`query($messages: [String!]!, $tools: [String!]) {
		function { core { models { chat_completion(
			model: "%s", messages: $messages, tools: $tools, max_tokens: %d
		) { content finish_reason tool_calls provider } } } } }`, modelName, maxTokens)

	res2 := query(t, step2Q, map[string]any{
		"messages": messages,
		"tools":    []string{roundTripToolDef},
	})
	defer res2.Close()

	var result2 struct {
		Content      string `json:"content"`
		FinishReason string `json:"finish_reason"`
		Provider     string `json:"provider"`
	}
	err = res2.ScanData("function.core.models.chat_completion", &result2)
	require.NoError(t, err)
	assert.Equal(t, provider, result2.Provider)
	assert.NotEmpty(t, result2.Content, "%s: should respond with content after tool results", provider)

	t.Logf("Step 3 — %s response: %q (finish=%s)", provider, truncateStr(result2.Content, 200), result2.FinishReason)
}

func TestModels_Gemini_ToolCallRoundTrip(t *testing.T) {
	if os.Getenv("GEMINI_KEY") == "" {
		t.Skip("GEMINI_KEY not set")
	}
	testToolCallRoundTrip(t, "test_gemini", "gemini", 4096)
}

func TestModels_Anthropic_ToolCallRoundTrip(t *testing.T) {
	if os.Getenv("ANTHROPIC_KEY") == "" {
		t.Skip("ANTHROPIC_KEY not set")
	}
	testToolCallRoundTrip(t, "test_anthropic", "anthropic", 4096)
}

func TestModels_OpenAIRemote_ToolCallRoundTrip(t *testing.T) {
	if os.Getenv("OPENAI_KEY") == "" {
		t.Skip("OPENAI_KEY not set")
	}
	testToolCallRoundTrip(t, "test_openai_remote", "openai", 200)
}

// --- Streaming Multi-turn Tool Call Round-trip Tests ---

// gqlString escapes a raw string for use as an inline GraphQL string literal.
func gqlString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	return `"` + s + `"`
}

// buildInlineStreamQuery builds a subscription query with messages and tools inlined.
func buildInlineStreamQuery(modelName string, messages []string, tools []string, maxTokens int, toolChoice string) string {
	var msgLiterals []string
	for _, m := range messages {
		msgLiterals = append(msgLiterals, gqlString(m))
	}
	var toolLiterals []string
	for _, t := range tools {
		toolLiterals = append(toolLiterals, gqlString(t))
	}
	toolChoicePart := ""
	if toolChoice != "" {
		toolChoicePart = fmt.Sprintf(`, tool_choice: "%s"`, toolChoice)
	}
	return fmt.Sprintf(`subscription { core { models { chat_completion(
		model: "%s", messages: [%s], tools: [%s]%s, max_tokens: %d
	) { type content finish_reason tool_calls prompt_tokens completion_tokens thought_signature } } } }`,
		modelName,
		strings.Join(msgLiterals, ", "),
		strings.Join(toolLiterals, ", "),
		toolChoicePart,
		maxTokens)
}

// testStreamToolCallRoundTrip runs a two-step streaming tool call cycle.
func testStreamToolCallRoundTrip(t *testing.T, modelName, provider string, maxTokens int, timeout time.Duration) {
	t.Helper()

	userMsg := `{"role":"user","content":"What is the weather in Tokyo? You must use the get_weather tool."}`

	// Step 1: Stream chat with tools → collect tool calls from finish event
	step1Q := buildInlineStreamQuery(modelName, []string{userMsg}, []string{roundTripToolDef}, maxTokens, "auto")

	events := collectStreamEventsWithTimeout(t, step1Q, timeout)
	require.NotEmpty(t, events, "%s stream: should receive events in step 1", provider)

	var finishEvent map[string]any
	for _, e := range events {
		if fmt.Sprintf("%v", e["type"]) == "finish" {
			finishEvent = e
		}
	}
	require.NotNil(t, finishEvent, "%s stream: should have finish event", provider)

	toolCallsRaw := finishEvent["tool_calls"]
	require.NotNil(t, toolCallsRaw, "%s stream: finish event should have tool_calls", provider)
	toolCallsStr := fmt.Sprintf("%v", toolCallsRaw)

	var toolCalls []types.LLMToolCall
	err := json.Unmarshal([]byte(toolCallsStr), &toolCalls)
	require.NoError(t, err, "%s stream: tool_calls parse failed: %s", provider, toolCallsStr)
	require.NotEmpty(t, toolCalls)

	for i, tc := range toolCalls {
		t.Logf("Step 1 stream — %s tool_call[%d]: id=%s name=%s args=%v",
			provider, i, tc.ID, tc.Name, tc.Arguments)
	}
	// thought_signature at finish event level, not per tool call
	thoughtSig := ""
	if ts := finishEvent["thought_signature"]; ts != nil {
		thoughtSig = fmt.Sprintf("%v", ts)
	}
	if thoughtSig != "" {
		t.Logf("Step 1 stream — %s thought_signature=%q", provider, truncateStr(thoughtSig, 50))
	}

	// Step 2: Build messages with tool results
	// thought_signature goes on the assistant MESSAGE
	assistantMsg := types.LLMMessage{
		Role:             "assistant",
		ToolCalls:        toolCalls,
		ThoughtSignature: thoughtSig,
	}
	assistantJSON, _ := json.Marshal(assistantMsg)
	messages := []string{userMsg, string(assistantJSON)}

	for _, tc := range toolCalls {
		toolID := tc.ID
		if toolID == "" {
			toolID = tc.Name
		}
		toolResult := types.LLMMessage{
			Role:       "tool",
			Content:    `{"temperature": 22, "condition": "sunny", "humidity": 65}`,
			ToolCallID: toolID,
		}
		toolJSON, _ := json.Marshal(toolResult)
		messages = append(messages, string(toolJSON))
	}

	t.Logf("Step 2 stream — sending %d messages to %s", len(messages), provider)

	// Step 3: Stream second request with tool results → should get content
	step2Q := buildInlineStreamQuery(modelName, messages, []string{roundTripToolDef}, maxTokens, "")

	events2 := collectStreamEventsWithTimeout(t, step2Q, timeout)
	require.NotEmpty(t, events2, "%s stream: should receive events in step 2", provider)

	var contentEvents int
	var allContent string
	var finishEvent2 map[string]any
	for _, e := range events2 {
		switch fmt.Sprintf("%v", e["type"]) {
		case "content_delta", "reasoning":
			contentEvents++
			if c := e["content"]; c != nil {
				allContent += fmt.Sprintf("%v", c)
			}
		case "finish":
			finishEvent2 = e
		}
	}

	require.NotNil(t, finishEvent2, "%s stream: should have finish event in step 2", provider)
	// Some providers (Gemini) may return content only in finish event
	assert.True(t, contentEvents > 0 || allContent != "" || finishEvent2["finish_reason"] == "stop",
		"%s stream: should have content or successful finish after tool results", provider)

	t.Logf("Step 3 stream — %s: %d content events, content=%q, finish=%v",
		provider, contentEvents, truncateStr(allContent, 200), finishEvent2["finish_reason"])
}

func TestModels_StreamToolCallRoundTrip_Gemini(t *testing.T) {
	if os.Getenv("GEMINI_KEY") == "" {
		t.Skip("GEMINI_KEY not set")
	}
	testStreamToolCallRoundTrip(t, "test_gemini", "gemini", 4096, 120*time.Second)
}

func TestModels_StreamToolCallRoundTrip_Anthropic(t *testing.T) {
	if os.Getenv("ANTHROPIC_KEY") == "" {
		t.Skip("ANTHROPIC_KEY not set")
	}
	testStreamToolCallRoundTrip(t, "test_anthropic", "anthropic", 4096, 120*time.Second)
}

func TestModels_StreamToolCallRoundTrip_OpenAIRemote(t *testing.T) {
	if os.Getenv("OPENAI_KEY") == "" {
		t.Skip("OPENAI_KEY not set")
	}
	testStreamToolCallRoundTrip(t, "test_openai_remote", "openai", 200, 120*time.Second)
}
