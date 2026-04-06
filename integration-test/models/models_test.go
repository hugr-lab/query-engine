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
	code := m.Run()
	service.Close()
	os.Exit(code)
}

func query(t *testing.T, q string, vars map[string]any) *types.Response {
	t.Helper()
	ctx := context.Background()
	res, err := testService.Query(ctx, q, vars)
	require.NoError(t, err)
	return res
}

// --- US1: Embeddings via Named Model ---

func TestModels_Embedding(t *testing.T) {
	embedderURL := os.Getenv("EMBEDDER_URL")
	if embedderURL == "" {
		t.Skip("EMBEDDER_URL not set")
	}

	// Register embedding DS
	res := query(t, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      "test_embedder",
			"type":      "embedding",
			"prefix":    "test_embedder",
			"as_module": false,
			"path":      embedderURL,
		},
	})
	res.Close()

	// Load
	res = query(t, `mutation { function { core { load_data_source(name: "test_embedder") { success message } } } }`, nil)
	res.Close()

	// Call embedding
	res = query(t, `{ function { core { models { embedding(model: "test_embedder", input: "hello world") { vector token_count } } } } }`, nil)
	defer res.Close()

	var result map[string]any
	err := res.ScanData("function.core.models.embedding", &result)
	require.NoError(t, err)
	assert.Contains(t, result, "vector")
	assert.Contains(t, result, "token_count")
	t.Logf("embedding: vector present, token_count=%v", result["token_count"])

	// Cleanup
	res2 := query(t, `mutation { function { core { unload_data_source(name: "test_embedder") { success } } } }`, nil)
	res2.Close()
	res3 := query(t, `mutation { core { delete_data_sources(filter: { name: { eq: "test_embedder" } }) { success } } }`, nil)
	res3.Close()
}

func TestModels_Embedding_NotFound(t *testing.T) {
	res, err := testService.Query(context.Background(),
		`{ function { core { models { embedding(model: "nonexistent", input: "test") { vector } } } } }`, nil)
	if err != nil {
		t.Logf("expected error: %v", err)
		return
	}
	defer res.Close()
	assert.True(t, len(res.Errors) > 0 || err != nil, "should return error for nonexistent model")
}

func TestModels_Sources_Empty(t *testing.T) {
	res := query(t, `{ function { core { models { model_sources { name type provider model } } } } }`, nil)
	defer res.Close()
	require.Empty(t, res.Errors)
	var result []map[string]any
	err := res.ScanData("function.core.models.model_sources", &result)
	require.NoError(t, err)
	t.Logf("model_sources: %v (count=%d)", result, len(result))
}

// --- US2: LLM Completion ---

func TestModels_Completion(t *testing.T) {
	llmURL := os.Getenv("LLM_URL")
	if llmURL == "" {
		t.Skip("LLM_URL not set (e.g., http://localhost:1234/v1/chat/completions?model=gemma-4&timeout=120s)")
	}

	// Register LLM DS
	res := query(t, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      "test_llm",
			"type":      "llm-openai",
			"prefix":    "test_llm",
			"as_module": false,
			"path":      llmURL,
		},
	})
	res.Close()

	res = query(t, `mutation { function { core { load_data_source(name: "test_llm") { success message } } } }`, nil)
	res.Close()

	// Call completion
	res = query(t, `{ function { core { models { completion(model: "test_llm", prompt: "Say hello in one word", max_tokens: 100) {
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
	// Content may be empty with small models — check that we got a response
	assert.NotEmpty(t, result.Model, "model should not be empty")
	assert.Greater(t, result.LatencyMs, 0, "latency should be positive")
	t.Logf("completion: %q, model=%s, finish=%s, latency=%dms", result.Content, result.Model, result.FinishReason, result.LatencyMs)

	// Cleanup
	res2 := query(t, `mutation { function { core { unload_data_source(name: "test_llm") { success } } } }`, nil)
	res2.Close()
	res3 := query(t, `mutation { core { delete_data_sources(filter: { name: { eq: "test_llm" } }) { success } } }`, nil)
	res3.Close()
}

func TestModels_Completion_NotLLM(t *testing.T) {
	// Calling completion with nonexistent model should not panic,
	// and should return either an error or null content.
	res, err := testService.Query(context.Background(),
		`{ function { core { models { completion(model: "nonexistent", prompt: "test") { content } } } } }`, nil)
	if err != nil {
		t.Logf("query returned error (expected): %v", err)
		return
	}
	defer res.Close()
	if len(res.Errors) > 0 {
		t.Logf("query returned GraphQL errors (expected): %v", res.Errors)
		return
	}
	// If no error, content should be empty/null
	var result map[string]any
	_ = res.ScanData("function.core.models.completion", &result)
	t.Errorf("completion with nonexistent model returned: %v", result)
}

// --- US3: Chat Completion with Tools ---

func TestModels_ChatCompletion(t *testing.T) {
	llmURL := os.Getenv("LLM_URL")
	if llmURL == "" {
		t.Skip("LLM_URL not set")
	}

	// Register
	res := query(t, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      "test_chat_llm",
			"type":      "llm-openai",
			"prefix":    "test_chat_llm",
			"as_module": false,
			"path":      llmURL,
		},
	})
	res.Close()
	res = query(t, `mutation { function { core { load_data_source(name: "test_chat_llm") { success } } } }`, nil)
	res.Close()

	res = query(t, `{ function { core { models { chat_completion(
		model: "test_chat_llm",
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
	assert.NotEmpty(t, result.Content)
	t.Logf("chat_completion: %q, finish=%s", result.Content, result.FinishReason)

	// Cleanup
	res2 := query(t, `mutation { function { core { unload_data_source(name: "test_chat_llm") { success } } } }`, nil)
	res2.Close()
	res3 := query(t, `mutation { core { delete_data_sources(filter: { name: { eq: "test_chat_llm" } }) { success } } }`, nil)
	res3.Close()
}

// --- US5: Discovery ---

func TestModels_Sources_WithRegistered(t *testing.T) {
	embedderURL := os.Getenv("EMBEDDER_URL")
	if embedderURL == "" {
		t.Skip("EMBEDDER_URL not set")
	}

	// Register
	res := query(t, `mutation($data: core_data_sources_mut_input_data!) {
		core { insert_data_sources(data: $data) { name } }
	}`, map[string]any{
		"data": map[string]any{
			"name":      "disc_embedder",
			"type":      "embedding",
			"prefix":    "disc_embedder",
			"as_module": false,
			"path":      embedderURL,
		},
	})
	res.Close()
	res = query(t, `mutation { function { core { load_data_source(name: "disc_embedder") { success } } } }`, nil)
	res.Close()

	// Discovery — table function
	res = query(t, `{ function { core { models { model_sources { name type provider model } } } } }`, nil)
	defer res.Close()

	var srcs []map[string]any
	scanErr := res.ScanData("function.core.models.model_sources", &srcs)
	require.NoError(t, scanErr)

	found := false
	for _, s := range srcs {
		if s["name"] == "disc_embedder" {
			found = true
			assert.Equal(t, "embedding", s["type"])
			assert.Equal(t, "openai", s["provider"])
			t.Logf("discovered: %+v", s)
		}
	}
	assert.True(t, found, "disc_embedder should appear in sources")

	// Cleanup
	res2 := query(t, `mutation { function { core { unload_data_source(name: "disc_embedder") { success } } } }`, nil)
	res2.Close()
	res3 := query(t, `mutation { core { delete_data_sources(filter: { name: { eq: "disc_embedder" } }) { success } } }`, nil)
	res3.Close()
}
