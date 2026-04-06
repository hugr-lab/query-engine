package sources

import (
	"context"
	"time"

	cs "github.com/hugr-lab/query-engine/pkg/catalog/sources"
	ctypes "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/types"
)

const (
	Postgres  types.DataSourceType = "postgres"
	DuckDB    types.DataSourceType = "duckdb"
	MySQL     types.DataSourceType = "mysql"
	MSSQL     types.DataSourceType = "mssql"
	Http      types.DataSourceType = "http"
	Runtime   types.DataSourceType = "runtime"
	Extension types.DataSourceType = "extension"
	Embedding types.DataSourceType = "embedding"

	Airport  types.DataSourceType = "airport"
	DuckLake types.DataSourceType = "ducklake"
	Iceberg  types.DataSourceType = "iceberg"
	HugrApp  types.DataSourceType = "hugr-app"

	LLMOpenAI    types.DataSourceType = "llm-openai"
	LLMAnthropic types.DataSourceType = "llm-anthropic"
	LLMGemini    types.DataSourceType = "llm-gemini"
)

type Source interface {
	Name() string
	Definition() types.DataSource
	Engine() engines.Engine
	IsAttached() bool
	ReadOnly() bool
	Attach(ctx context.Context, db *db.Pool) error
	Detach(ctx context.Context, db *db.Pool) error
}

// The data source is a catalog extension.
type ExtensionSource interface {
	IsExtension() bool
}

type SelfDescriber interface {
	CatalogSource(ctx context.Context, db *db.Pool) (cs.Catalog, error)
}

// Provisioner is implemented by sources that need to provision external
// resources (databases, schemas) after attachment. Called by the data source
// service after Attach() succeeds. Querier provides access to hugr's GraphQL
// API for registering/loading data sources and querying system configuration.
type Provisioner interface {
	Provision(ctx context.Context, querier types.Querier) error
}

// Querier provides GraphQL query access for provisioning operations.
type Querier = types.Querier

// Heartbeater is implemented by sources that need periodic health monitoring.
// The service calls StartHeartbeat after successful Attach and StopHeartbeat before Detach.
type Heartbeater interface {
	StartHeartbeat(
		config HeartbeatConfig,
		onSuspend func(ctx context.Context, name string) error,
		onRecover func(ctx context.Context, name string) error,
	)
	StopHeartbeat()
}

// HeartbeatConfig holds heartbeat monitoring settings.
type HeartbeatConfig struct {
	Interval   time.Duration
	Timeout    time.Duration
	MaxRetries int
}

// RuntimeSource is a data source that is attached on start and provides a catalog source.
type RuntimeSource interface {
	Name() string
	Engine() engines.Engine
	IsReadonly() bool
	AsModule() bool
	Attach(ctx context.Context, db *db.Pool) error
	Catalog(ctx context.Context) (cs.Catalog, error)
}

type RuntimeSourceQuerier interface {
	QueryEngineSetup(querier types.Querier)
}

type EmbeddingSource interface {
	ModelSource
	CreateEmbedding(ctx context.Context, input string) (*EmbeddingResult, error)
	CreateEmbeddings(ctx context.Context, input []string) (*EmbeddingsResult, error)
}

// ModelSource identifies a data source as an AI model.
type ModelSource interface {
	ModelInfo() ModelInfo
}

type ModelInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`     // "llm" or "embedding"
	Provider string `json:"provider"` // "openai", "anthropic", "gemini"
	Model    string `json:"model"`
}

// EmbeddingResult is an enriched embedding response with token count.
type EmbeddingResult struct {
	Vector     ctypes.Vector `json:"vector"`
	TokenCount int           `json:"token_count"`
}

// EmbeddingsResult is a batch embedding response with total token count.
type EmbeddingsResult struct {
	Vectors    []ctypes.Vector `json:"vectors"`
	TokenCount int             `json:"token_count"`
}

// LLMSource provides text generation capabilities.
type LLMSource interface {
	ModelSource
	CreateCompletion(ctx context.Context, prompt string, opts LLMOptions) (*LLMResult, error)
	CreateChatCompletion(ctx context.Context, messages []LLMMessage, opts LLMOptions) (*LLMResult, error)
}

type LLMMessage struct {
	Role       string        `json:"role"`
	Content    string        `json:"content"`
	ToolCalls  []LLMToolCall `json:"tool_calls,omitempty"`
	ToolCallID string        `json:"tool_call_id,omitempty"`
}

type LLMTool struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Parameters  any    `json:"parameters"`
}

type LLMToolCall struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Arguments any    `json:"arguments"`
}

type LLMOptions struct {
	MaxTokens   int       `json:"max_tokens,omitempty"`
	Temperature float64   `json:"temperature,omitempty"`
	TopP        float64   `json:"top_p,omitempty"`
	Stop        []string  `json:"stop,omitempty"`
	Tools       []LLMTool `json:"tools,omitempty"`
	ToolChoice  string    `json:"tool_choice,omitempty"`
}

type LLMResult struct {
	Content          string        `json:"content"`
	Model            string        `json:"model"`
	FinishReason     string        `json:"finish_reason"`
	PromptTokens     int           `json:"prompt_tokens"`
	CompletionTokens int           `json:"completion_tokens"`
	TotalTokens      int           `json:"total_tokens"`
	Provider         string        `json:"provider"`
	LatencyMs        int           `json:"latency_ms"`
	ToolCalls        []LLMToolCall `json:"tool_calls"`
}

// DataSourceResolver resolves registered data sources by name.
type DataSourceResolver interface {
	Resolve(name string) (Source, error)
	ResolveAll() []Source
}

// RuntimeSourceDataSourceUser is implemented by runtime sources that need
// access to registered data sources. Follows RuntimeSourceQuerier pattern.
type RuntimeSourceDataSourceUser interface {
	DataSourceServiceSetup(resolver DataSourceResolver)
}
