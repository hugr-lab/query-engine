package base

import "github.com/vektah/gqlparser/v2/ast"

// Phase represents a compilation phase.
type Phase int

const (
	PhaseValidate Phase = iota
	PhasePrepare
	PhaseGenerate
	PhaseAssemble
	PhaseFinalize
)

func (p Phase) String() string {
	switch p {
	case PhaseValidate:
		return "VALIDATE"
	case PhasePrepare:
		return "PREPARE"
	case PhaseGenerate:
		return "GENERATE"
	case PhaseAssemble:
		return "ASSEMBLE"
	case PhaseFinalize:
		return "FINALIZE"
	default:
		return "UNKNOWN"
	}
}

// Rule is the base interface for all compilation rules.
type Rule interface {
	Name() string
	Phase() Phase
}

// DefinitionRule processes individual source definitions matching a predicate.
type DefinitionRule interface {
	Rule
	Match(def *ast.Definition) bool
	Process(ctx CompilationContext, def *ast.Definition) error
}

// BatchRule performs cross-cutting work after all DefinitionRules in the same phase.
type BatchRule interface {
	Rule
	ProcessAll(ctx CompilationContext) error
}

// ObjectInfo caches shared metadata about a data object during compilation.
type ObjectInfo struct {
	Name         string
	OriginalName string
	TableName    string
	Module       string
	IsReplace    bool
	IsView       bool
	IsM2M        bool
	PrimaryKey   []string

	// Parameterized views (@args)
	InputArgsName string // input type name from @args(name: "...")
	RequiredArgs  bool   // true if any field in input type is NonNull or @args(required: true)
}
