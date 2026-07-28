package catalog

import (
	"context"
	"errors"
	"iter"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/vektah/gqlparser/v2/ast"
)

// ModuleInfo describes one node of the logical module tree.
type ModuleInfo struct {
	Name string // full dotted name; "" for the root module
	// Description is the CURATED text where a curation exists, the stored one
	// otherwise — the same overlay the entity views apply. LongDescription
	// exists only as curation and is empty without one.
	Description     string
	LongDescription string
	// RootTypes maps each PRESENT root kind to its type name
	// (sdl.ModuleTypeName). Key presence carries the information — a module
	// exists when at least one kind is present; the definition is resolved
	// LAZILY via Type(ctx, name) only when a consumer actually needs it (the
	// entity store synthesizes root types on demand — building five of them
	// per module lookup would defeat the on-the-fly model).
	RootTypes   map[sdl.ModuleObjectType]string
	DataSources []string // distinct data sources of direct members, sorted
}

// FunctionEntry describes one callable member of a module: a query function,
// a mutation function, or a subscription field.
type FunctionEntry struct {
	Field      *ast.FieldDefinition
	Kind       sdl.ModuleObjectType // ModuleFunction | ModuleMutationFunction | ModuleSubscription
	Module     string
	DataSource string
	IsTable    bool
	// LongDescription is the curated long form; empty without a curation
	// layer. The short description lives on Field, already overlaid.
	LongDescription string
}

// DataSourceInfo describes one data source as a LOGICAL entity: what it is
// and what it contributes, never how to connect to it (the registry's
// type/path are connection details — path is a DSN — and stay out of
// introspection).
//
// The flags were pointers while a second catalog storage existed that could
// not recover them from a compiled schema and had to report "not recorded".
// The storage records all three (data_source_meta, NOT NULL), so they are
// plain facts.
type DataSourceInfo struct {
	Name            string
	Engine          string // the source's engine type string (@catalog engine)
	Description     string
	LongDescription string
	ReadOnly        bool
	AsModule        bool
	IsExtension     bool
	Modules         []string // modules this source contributes members to, sorted; "" = root
}

type RelationDirection string

const (
	RelationForward RelationDirection = "FORWARD"
	RelationBack    RelationDirection = "BACK"
)

type RelationKind string

const (
	RelationFK   RelationKind = "FK"
	RelationM2M  RelationKind = "M2M"
	RelationJoin RelationKind = "JOIN"
)

// RelationInfo is one perspective on a logical edge between two data objects.
// FK and M2M edges are visible from both ends (FORWARD from the declaring
// side, BACK from the referenced side); JOIN edges are one-directional.
type RelationInfo struct {
	Name            string
	Direction       RelationDirection
	Kind            RelationKind
	FieldName       string // the field ON the viewed object materializing the edge
	Description     string // per-endpoint description (same perspective as FieldName)
	DataObject      string // far data object type name (M2M: the far leg, not the junction)
	Through         string // M2M junction type name; "" otherwise
	SourceKeys      []string
	DestinationKeys []string
	DataSource      string // declaring data source
}

// LogicalModel is the narrow logical-model surface consumed by the _catalog
// introspection resolvers. It returns the UNFILTERED model — permission
// filtering is applied by consumers from the request context. The catalog
// storage implements it natively over the catalog.* tables.
type LogicalModel interface {
	// Module resolves a module by full dotted name ("" = root); nil when absent.
	Module(ctx context.Context, name string) *ModuleInfo
	// Modules iterates the DIRECT child modules of parent.
	Modules(ctx context.Context, parent string) iter.Seq[*ModuleInfo]
	// DataObject resolves a data object by GraphQL type name; nil for
	// non-data-object types.
	DataObject(ctx context.Context, name string) *sdl.Object
	// DataObjects iterates the data objects that are members of a module.
	DataObjects(ctx context.Context, module string) iter.Seq[*sdl.Object]
	// Function resolves a callable member of a module by field name, probing
	// the function, mutation-function and subscription roots in that order.
	Function(ctx context.Context, module, name string) *FunctionEntry
	// Functions iterates all callable members of a module (all three kinds).
	Functions(ctx context.Context, module string) iter.Seq[*FunctionEntry]
	// Relations iterates the logical edges of a data object, both directions.
	Relations(ctx context.Context, object string) iter.Seq[*RelationInfo]
	// DataSource resolves an ACTIVE data source by name; nil when absent.
	DataSource(ctx context.Context, name string) *DataSourceInfo
	// DataSources iterates the active data sources, ordered by name.
	DataSources(ctx context.Context) iter.Seq[*DataSourceInfo]
	// Type resolves a GraphQL type definition by name; nil when absent.
	// (Entity storage: the ForName resolution chain.)
	Type(ctx context.Context, name string) *ast.Definition
	// SourceTypes iterates the residual base types DEFINED BY SOURCES —
	// structural objects, inputs (incl. @args), enums, interfaces, unions
	// that are not data objects, module roots, or compiler-generated
	// helpers (filters, mutation inputs, aggregations). This is exactly
	// the content of the future entity-storage types table.
	SourceTypes(ctx context.Context) iter.Seq2[string, *ast.Definition]
	// SystemTypes iterates engine-defined types: scalars, the introspection
	// and hugr system SDL, scalar filter/aggregation inputs — the future
	// binary-owned static prelude. Compiler-DERIVED types (filters,
	// aggregations, mutation inputs, module roots) belong to neither set.
	SystemTypes(ctx context.Context) iter.Seq2[string, *ast.Definition]
}

// ErrNoLogicalModel is returned when a Provider cannot answer logical-model
// queries. The catalog storage always can; a static schema (a test fixture, a
// hand-assembled prelude) has no logical model behind it at all.
var ErrNoLogicalModel = errors.New("catalog provider has no logical model")

// LogicalModelFromProvider returns the provider's logical model. The catalog
// storage implements LogicalModel natively over the catalog.* tables — this is
// the seam that lets a consumer take a Provider and ask logical questions of
// it without importing the storage.
func LogicalModelFromProvider(p Provider) (LogicalModel, error) {
	if lp, ok := p.(LogicalModel); ok {
		return lp, nil
	}
	return nil, ErrNoLogicalModel
}
