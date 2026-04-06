//go:build duckdb_arrow

package models_test

import (
	"context"
	"os"
	"testing"

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

	// LLM DS
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
